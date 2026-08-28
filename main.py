#!/usr/bin/env python3
import os,sys,json,time,signal as signal_module,logging,subprocess
from decimal import Decimal
from datetime import datetime,timedelta,timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait
from zoneinfo import ZoneInfo
from urllib.request import Request,urlopen
from urllib.parse import urlencode

def sdk():
    try:
        import polymarket
    except ImportError:
        subprocess.check_call([sys.executable,"-m","pip","install","--no-cache-dir","--root-user-action=ignore","polymarket-client==0.3.0b1"])
        os.execv(sys.executable,[sys.executable]+sys.argv)
sdk()
from polymarket import SecureClient

TZ=ZoneInfo("America/Sao_Paulo"); ET=ZoneInfo("America/New_York"); UTC=timezone.utc
GAMMA="https://gamma-api.polymarket.com"; BINANCE="https://api.binance.com"
PK=os.getenv("PRIVATE_KEY","").strip()
WALLET=os.getenv("POLYMARKET_DEPOSIT_WALLET","").strip()
LIVE=os.getenv("LIVE_TRADING","0").lower() in ("1","true","yes","on")
INITIAL=Decimal(os.getenv("INITIAL_BANKROLL","12.00"))
BASE=Decimal(os.getenv("BASE_ENTRY","5.00"))
EXTRA=Decimal(os.getenv("DIRECTIONAL_EXTRA","0.10"))
MAX=Decimal(os.getenv("MAX_ENTRY","1000"))
TARGET=Decimal(os.getenv("TARGET_BANKROLL","200000"))
ENTRY=int(os.getenv("ENTRY_SECONDS","15")); POLL=float(os.getenv("POLL_SECONDS","0.5"))
TFS={"5m":5,"15m":15,"1h":60}
DEFAULT_ROOT=Path("/data") if Path("/data").exists() else Path(__file__).resolve().parent
ROOT=Path(os.getenv("BOT_DIR",str(DEFAULT_ROOT))).resolve(); ROOT.mkdir(parents=True,exist_ok=True)
STATE=ROOT/"state.json"; TRADES=ROOT/"trades.jsonl"
logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s",handlers=[logging.StreamHandler(sys.stdout)])
log=logging.getLogger("bot"); STOP=False

def D(x): return Decimal(str(x))
def get(url,p=None):
    if p:url+="?"+urlencode(p)
    with urlopen(Request(url,headers={"User-Agent":"btc-bot/4"}),timeout=12) as r:return json.loads(r.read())
def js(x):
    if isinstance(x,list):return x
    if isinstance(x,str):
        try:return json.loads(x)
        except:return [x]
    return []
def fresh():
    s={"version":6,"strategies":{}}
    for tf in TFS:
        for ses in ("24h","day"):
            n=f"{tf}_{ses}"
            s["strategies"][n]={"name":n,"tf":tf,"session":ses,"bankroll":str(INITIAL),"loss_streak":0,"wins":0,"losses":0,"trades":0,"last_trigger":"","pending":None}
    return s
def load():
    if not STATE.exists():return fresh()
    try:
        old=json.loads(STATE.read_text()); new=fresh()
        for k,v in old.get("strategies",{}).items():
            if k in new["strategies"]:new["strategies"][k].update(v)
        return new
    except:return fresh()
def save(s):
    t=STATE.with_suffix(".tmp"); t.write_text(json.dumps(s,indent=2)); t.replace(STATE)
def audit(x):
    with TRADES.open("a") as f:f.write(json.dumps(x,default=str)+"\n")
def ema(v,n):
    k=2/(n+1); e=float(v[0]); out=[]
    for x in v:e=float(x)*k+e*(1-k);out.append(e)
    return out
def trading_signal(tf):
    rows=get(BINANCE+"/api/v3/klines",{"symbol":"BTCUSDT","interval":tf,"limit":120})
    now=int(time.time()*1000); c=[r for r in rows if int(r[6])<now]
    close=[float(r[4]) for r in c]; fast=ema(close,7); slow=ema(close,21)
    mac=[a-b for a,b in zip(fast,slow)]; sig=ema(mac,9); m,s=mac[-1],sig[-1]
    d="UP" if m>s and m>0 else "DOWN" if m<s and m<0 else None
    dirs=["UP" if float(r[4])>=float(r[1]) else "DOWN" for r in c[-2:]]
    return d,(d is not None and dirs==[d,d]),m,s
def bounds(now,mins):
    x=now.astimezone(TZ)
    if mins==60:a=x.replace(minute=0,second=0,microsecond=0);return a,a+timedelta(hours=1)
    a=x.replace(minute=(x.minute//mins)*mins,second=0,microsecond=0);return a,a+timedelta(minutes=mins)
def slug(tf,start):
    if tf!="1h":return f"btc-updown-{tf}-{int(start.astimezone(UTC).timestamp())}"
    e=start.astimezone(ET); return f"bitcoin-up-or-down-{e.strftime('%B').lower()}-{e.day}-{e.year}-{e.strftime('%I').lstrip('0')}{e.strftime('%p').lower()}-et"
def event(sl):
    try:return get(GAMMA+"/events/slug/"+sl)
    except:return None
def market(ev):
    if not ev:return None
    for m in ev.get("markets",[]) or [ev]:
        o=js(m.get("outcomes")); t=js(m.get("clobTokenIds"))
        if len(o)!=len(t):continue
        z={str(a).upper():str(b) for a,b in zip(o,t)}
        up=z.get("UP") or z.get("YES"); dn=z.get("DOWN") or z.get("NO")
        if up and dn:return {"up":up,"down":dn,"closed":bool(m.get("closed")),"outcomes":o,"prices":js(m.get("outcomePrices")),"min_order":D(m.get("orderMinSize") or 0)}
def winner(sl):
    m=market(event(sl))
    if not m or not m["closed"] or len(m["outcomes"])!=len(m["prices"]):return None
    z=[]
    for o,p in zip(m["outcomes"],m["prices"]):
        try:z.append((D(p),str(o).upper()))
        except:pass
    if not z:return None
    p,o=max(z)
    if p<D(".95"):return None
    return "UP" if o in ("UP","YES") else "DOWN" if o in ("DOWN","NO") else None
def allowed(st,now):
    return st["session"]=="24h" or 10<=now.astimezone(TZ).hour<16
def amount(st):return min(BASE*(D(2)**int(st["loss_streak"])),MAX)

class Bot:
    def __init__(self):
        self.s=load(); self.c=None
        if LIVE:
            if not PK or not WALLET:raise RuntimeError("Configure PRIVATE_KEY e POLYMARKET_DEPOSIT_WALLET")
            self.c=SecureClient.create(private_key=PK,wallet=WALLET)
            log.info("REAL | signer=%s wallet=%s type=%s",self.c.signer,self.c.wallet,self.c.wallet_type)
        else:log.info("SIMULACAO")
    def resolve(self,st):
        p=st.get("pending")
        if not p:return
        w=winner(p["slug"])
        if not w:return
        ok=w==p["direction"]; st["trades"]+=1
        if ok:st["wins"]+=1;st["loss_streak"]=0
        else:st["losses"]+=1;st["loss_streak"]+=1
        audit({"type":"resolution","strategy":st["name"],"slug":p["slug"],"winner":w,"signal":p["direction"],"win":ok,"ts":datetime.now(UTC).isoformat()})
        st["pending"]=None;save(self.s)
        log.info("%s | %s | %s",st["name"],w,"WIN" if ok else "LOSS")
    def buy(self,token,usd):
        if not LIVE:return {"simulation":True,"token":token,"amount":str(usd)}
        return self.c.place_market_order(token_id=token,side="BUY",amount=usd,order_type="FAK")
    def enter(self,st,start,d):
        sl=slug(st["tf"],start); m=market(event(sl))
        if not m:log.warning("%s | mercado nao encontrado %s",st["name"],sl);return
        b=amount(st); da=b+EXTRA; oa=b
        if da+oa>D(st["bankroll"]):log.warning("%s | entrada > bankroll logico",st["name"]);return
        if D(st["bankroll"])>=TARGET:return
        dt=m["up"] if d=="UP" else m["down"]; ot=m["down"] if d=="UP" else m["up"]
        min_order=D(m.get("min_order") or 0)
        if LIVE and min_order>0 and (da<min_order or oa<min_order):
            log.error("%s | ORDEM BLOQUEADA: stake DIR $%s / OPP $%s abaixo do minimo do mercado %s. Nenhum dinheiro enviado.",st["name"],da,oa,min_order)
            audit({"type":"blocked_min_order","strategy":st["name"],"slug":sl,"directional_amount":str(da),"opposite_amount":str(oa),"minimum_order_size":str(min_order),"ts":datetime.now(UTC).isoformat()})
            return
        log.info("%s | %s | PAR SIMULTANEO: DIR $%s + OPP $%s | %s",st["name"],d,da,oa,sl)
        if not LIVE:
            r1=self.buy(dt,da); r2=self.buy(ot,oa)
        else:
            # Dispara as duas BUYs de outcomes opostos em paralelo.
            # Na Polymarket, comprar UP e DOWN é a forma correta de obter exposição nos dois lados;
            # uma SELL exige possuir shares daquele outcome previamente.
            with ThreadPoolExecutor(max_workers=2,thread_name_prefix="pair") as ex:
                f_dir=ex.submit(self.buy,dt,da)
                f_opp=ex.submit(self.buy,ot,oa)
                wait([f_dir,f_opp])
                e1=f_dir.exception(); e2=f_opp.exception()
                r1=None if e1 else f_dir.result()
                r2=None if e2 else f_opp.result()
                if e1 or e2:
                    audit({"type":"PAIR_ERROR","strategy":st["name"],"slug":sl,
                           "directional_result":str(r1),"opposite_result":str(r2),
                           "directional_error":repr(e1) if e1 else None,
                           "opposite_error":repr(e2) if e2 else None,
                           "ts":datetime.now(UTC).isoformat()})
                    log.error("%s | PAR INCOMPLETO | DIR erro=%s | OPP erro=%s",st["name"],e1,e2)
                    # Não cria pending silenciosamente: registra a falha para intervenção/controle.
                    return
        st["pending"]={"slug":sl,"direction":d,"directional_amount":str(da),"opposite_amount":str(oa),"live":LIVE}
        audit({"type":"entry","strategy":st["name"],"slug":sl,"direction":d,"r1":str(r1),"r2":str(r2),"live":LIVE,"ts":datetime.now(UTC).isoformat()});save(self.s)
    def tick(self,st,now):
        self.resolve(st)
        if st.get("pending") or not allowed(st,now):return
        start,nxt=bounds(now,TFS[st["tf"]]); sec=(nxt-now.astimezone(TZ)).total_seconds()
        if not ENTRY-1.2<=sec<=ENTRY+.8:return
        key=nxt.astimezone(UTC).isoformat()
        if st["last_trigger"]==key:return
        st["last_trigger"]=key;save(self.s)
        d,two,m,s=trading_signal(st["tf"])
        if not d:log.info("%s | sem sinal MACD",st["name"]);return
        if st["loss_streak"] and not two:log.info("%s | aguardando 2 velas %s",st["name"],d);return
        self.enter(st,start,d)
    def run(self):
        log.info("POLYMARKET BTC V6 FINAL | LIVE=%s | 6 estrategias | MACD 7/21/9 | T-%ss | DATA=%s",LIVE,ENTRY,ROOT)
        hb=0
        while not STOP:
            now=datetime.now(UTC)
            for st in self.s["strategies"].values():
                try:self.tick(st,now)
                except Exception as e:log.exception("%s | %s",st["name"],e)
            if time.time()-hb>30:log.info("HEARTBEAT | LIVE=%s",LIVE);hb=time.time()
            time.sleep(POLL)
    def close(self):
        if self.c:self.c.close()

def stop(*_):
    global STOP;STOP=True
signal_module.signal(signal_module.SIGTERM,stop);signal_module.signal(signal_module.SIGINT,stop)
if __name__=="__main__":
    b=Bot()
    try:b.run()
    finally:b.close()
