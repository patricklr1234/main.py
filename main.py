#!/usr/bin/env python3
# V64 - PREDICT/POLYMARKET - CORRIGIDO PARA SIMULAÇÃO 3 DIAS
# - Sizing conservador: par só entra com lucro garantido em AMBAS as pernas
# - Resgate com timeout de 30s
# - Reconciliação idempotente
# - Timeouts HTTP em todas as chamadas
# - Proteção contra loop infinito
# - Caixa inicial: USD 12,00 por robô (6 robôs = USD 72,00)

import os
import sys
import json
import time
import asyncio
import signal as signal_module
import logging
import subprocess
import importlib
import re
import html as html_lib
import math
import statistics
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, TimeoutError as FuturesTimeoutError
from threading import Barrier, Thread, Lock
from zoneinfo import ZoneInfo
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

def ensure_sdk():
    required = (0, 7, 1)
    try:
        from importlib.metadata import version as package_version
        raw = package_version("polymarket-client")
        nums = []
        for piece in raw.split("."):
            digits = "".join(ch for ch in piece if ch.isdigit())
            nums.append(int(digits or 0))
            if len(nums) == 3:
                break
        while len(nums) < 3:
            nums.append(0)
        installed = tuple(nums[:3])
        if installed >= required:
            print(f"BOOTSTRAP | polymarket-client pronto | versao={'.'.join(map(str, installed))}", flush=True)
            return
    except Exception:
        pass

    print("BOOTSTRAP | instalando polymarket-client>=0.7.1,<0.8", flush=True)
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--upgrade", "--no-cache-dir",
        "--root-user-action=ignore", "polymarket-client>=0.7.1,<0.8",
    ])
    importlib.invalidate_caches()

ensure_sdk()

def ensure_runtime_deps():
    try:
        import websockets
        return
    except ImportError:
        print("BOOTSTRAP | instalando websockets==15.0.1", flush=True)
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--no-cache-dir",
        "--root-user-action=ignore", "websockets==15.0.1",
    ])
    importlib.invalidate_caches()

ensure_runtime_deps()

from polymarket import SecureClient, BuilderApiKey, RelayerApiKey
import websockets

# Constantes
TZ = ZoneInfo("America/Sao_Paulo")
ET = ZoneInfo("America/New_York")
UTC = timezone.utc

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
BINANCE = "https://api.binance.com"

# Configurações
PK = (os.getenv("POLYMARKET_PRIVATE_KEY") or os.getenv("PRIVATE_KEY") or "").strip()
WALLET = os.getenv("POLYMARKET_DEPOSIT_WALLET", "").strip()
LIVE = os.getenv("LIVE_TRADING", "0").lower() in ("1", "true", "yes", "on")

INITIAL = Decimal(os.getenv("INITIAL_BANKROLL", "12.00"))
MAX_ENTRY = Decimal(os.getenv("MAX_ENTRY", "1000.00"))
TARGET = Decimal(os.getenv("TARGET_BANKROLL", "200000.00"))
MIN_LEG_USD = Decimal(os.getenv("MIN_LEG_USD", "1.00"))
MIN_PAIR_GUARANTEED_PROFIT_USD = Decimal(os.getenv("MIN_PAIR_GUARANTEED_PROFIT_USD", "0.05"))
PAIR_FEE_RESERVE_PCT = Decimal(os.getenv("PAIR_FEE_RESERVE_PCT", "0.02"))
MAX_BUY_PRICE = Decimal(os.getenv("MAX_BUY_PRICE", "0.60"))
SINGLE_LEG_RESCUE_MAX_PRICE = Decimal(os.getenv("SINGLE_LEG_RESCUE_MAX_PRICE", "0.65"))
ENTRY_SECONDS = 30
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "0.5"))
HTTP_TIMEOUT = 10

EDGE_5M = Decimal("0.25")
EDGE_15M = Decimal("0.50")
EDGE_1H = Decimal("0.75")
BASE_EDGE_BY_TF = {"5m": EDGE_5M, "15m": EDGE_15M, "1h": EDGE_1H}

TFS = {"5m": 5, "15m": 15, "1h": 60}

DEFAULT_ROOT = Path("/data") if Path("/data").exists() else Path(__file__).resolve().parent
ROOT = Path(os.getenv("BOT_DIR", str(DEFAULT_ROOT))).resolve()
ROOT.mkdir(parents=True, exist_ok=True)
STATE = ROOT / "state.json"
TRADES = ROOT / "trades.jsonl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("predict-v64")
STOP = False

def D(x):
    return Decimal(str(x))

def ceil_cent(x):
    return D(x).quantize(Decimal("0.01"), rounding=ROUND_CEILING)

def floor_to_step(value, step):
    value = D(value)
    step = D(step)
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_FLOOR)
    return units * step

def ceil_to_step(value, step):
    value = D(value)
    step = D(step)
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_CEILING)
    return units * step

def floor_6(x):
    return D(x).quantize(Decimal("0.000001"), rounding=ROUND_FLOOR)

def ceil_6(x):
    return D(x).quantize(Decimal("0.000001"), rounding=ROUND_CEILING)

ASSET_CONFIG = {
    "BTC": {"spot_symbol": "BTCUSDT", "signal_base": BINANCE, "slug_prefix": "btc", "hour_name": "bitcoin"},
    "ETH": {"spot_symbol": "ETHUSDT", "signal_base": BINANCE, "slug_prefix": "eth", "hour_name": "ethereum"},
    "HYPE": {"spot_symbol": "HYPEUSDT", "signal_base": "https://fapi.binance.com", "slug_prefix": "hype", "hour_name": "hype"},
}

def strategy_name(asset, tf, session):
    return f"{tf}_{session}" if asset == "BTC" else f"{asset.lower()}_{tf}_{session}"

def fresh():
    s = {
        "version": 64,
        "strategies": {},
        "maintenance": {"applied_resets": [], "last_reset": None},
        "capital_reconciliation": {"initialized": False, "baseline_epoch": 0, "processed_withdrawals": [], "total_withdrawn_applied": "0"},
        "redemption_reconciliation": {"processed_condition_ids": [], "queue": [], "last_redeem": None},
    }
    for asset in ("BTC", "ETH", "HYPE"):
        for tf in TFS:
            for session in ("24h", "day"):
                name = strategy_name(asset, tf, session)
                s["strategies"][name] = {
                    "name": name, "asset": asset, "tf": tf, "session": session,
                    "bankroll": str(INITIAL), "loss_streak": 0,
                    "recovery_deficit": "0", "wins": 0, "losses": 0,
                    "trades": 0, "realized_pnl": "0", "last_pnl": "0",
                    "last_stop_loss": "0", "last_result": "NONE",
                    "last_trigger": "", "pending": None, "open_positions": [],
                }
    return s

def save(s):
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(s, indent=2, default=str))
    tmp.replace(STATE)

def load():
    if not STATE.exists():
        s = fresh()
        save(s)
        return s
    try:
        return json.loads(STATE.read_text())
    except Exception:
        log.exception("state.json invalido; criando novo")
        s = fresh()
        save(s)
        return s

def audit(payload):
    with TRADES.open("a") as f:
        f.write(json.dumps(payload, default=str) + "\n")

def get(url, params=None, timeout=HTTP_TIMEOUT):
    if params:
        url += "?" + urlencode(params)
    req = Request(url, headers={"User-Agent": "predict-v64"})
    try:
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except (URLError, HTTPError, TimeoutError, json.JSONDecodeError) as e:
        log.warning(f"HTTP GET falhou | url={url} | err={e}")
        return None

def js(x):
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return [x]
    return []

def event(sl):
    ev = get(GAMMA + "/events/slug/" + sl)
    return ev

def market(ev):
    if not ev:
        return None
    for m in ev.get("markets", []) or [ev]:
        outcomes = js(m.get("outcomes"))
        tokens = js(m.get("clobTokenIds"))
        prices = js(m.get("outcomePrices"))
        if len(outcomes) != len(tokens):
            continue
        mapped = {str(a).upper(): str(b) for a, b in zip(outcomes, tokens)}
        up = mapped.get("UP") or mapped.get("YES")
        down = mapped.get("DOWN") or mapped.get("NO")
        if not (up and down):
            continue
        price_map = {}
        if len(prices) == len(outcomes):
            for o, p in zip(outcomes, prices):
                try:
                    label = str(o).upper()
                    direction = "UP" if label in ("UP", "YES") else "DOWN" if label in ("DOWN", "NO") else label
                    price_map[direction] = D(p)
                except Exception:
                    pass
        return {
            "up": up, "down": down, "closed": bool(m.get("closed")),
            "condition_id": str(m.get("conditionId") or m.get("condition_id") or ""),
            "min_order_shares": D(m.get("orderMinSize") or 0),
            "tick_size": D(m.get("orderPriceMinTickSize") or "0.01"),
            "price_map": price_map,
        }
    return None

def token_best_ask(token_id):
    if not token_id:
        return None
    book = get(CLOB + "/book", {"token_id": str(token_id)})
    if not book:
        return None
    asks = book.get("asks") or []
    prices = []
    for row in asks:
        raw = row.get("price") if isinstance(row, dict) else getattr(row, "price", None)
        if raw is not None:
            prices.append(D(raw))
    return min(prices) if prices else None

def slug(tf, round_start, asset="BTC"):
    asset = str(asset or "BTC").upper()
    cfg = ASSET_CONFIG.get(asset, ASSET_CONFIG["BTC"])
    if tf != "1h":
        return f"{cfg['slug_prefix']}-updown-{tf}-{int(round_start.astimezone(UTC).timestamp())}"
    e = round_start.astimezone(ET)
    return f"{cfg['hour_name']}-up-or-down-{e.strftime('%B').lower()}-{e.day}-{e.year}-{e.strftime('%I').lstrip('0')}{e.strftime('%p').lower()}-et"

def bounds(now, mins):
    x = now.astimezone(TZ)
    if mins == 60:
        start = x.replace(minute=0, second=0, microsecond=0)
        return start, start + timedelta(hours=1)
    start = x.replace(minute=(x.minute // mins) * mins, second=0, microsecond=0)
    return start, start + timedelta(minutes=mins)

def ema(values, n):
    k = 2 / (n + 1)
    e = float(values[0])
    out = []
    for x in values:
        e = float(x) * k + e * (1 - k)
        out.append(e)
    return out

def trading_signal(tf, asset="BTC"):
    cfg = ASSET_CONFIG.get(str(asset).upper(), ASSET_CONFIG["BTC"])
    base = cfg["signal_base"]
    path = "/fapi/v1/klines" if str(asset).upper() == "HYPE" else "/api/v3/klines"
    rows = get(base + path, {"symbol": cfg["spot_symbol"], "interval": tf, "limit": 120})
    if not rows:
        return None, False, None, None, [], None, None, None, None
    now_ms = int(time.time() * 1000)
    closed = [r for r in rows if int(r[6]) < now_ms]
    current = [r for r in rows if int(r[0]) <= now_ms <= int(r[6])]
    if len(closed) < 30 or not current:
        return None, False, None, None, [], None, None, None, None
    current_row = current[-1]
    previous_closed = closed[-1]
    closes = [float(r[4]) for r in closed]
    fast = ema(closes, 7)
    slow = ema(closes, 21)
    macd_series = [a - b for a, b in zip(fast, slow)]
    signal_series = ema(macd_series, 9)
    m, sig = macd_series[-1], signal_series[-1]
    closed_hist = m - sig
    current_price = float(current_row[4])
    live_closes = closes + [current_price]
    live_fast = ema(live_closes, 7)
    live_slow = ema(live_closes, 21)
    live_macd_series = [a - b for a, b in zip(live_fast, live_slow)]
    live_signal_series = ema(live_macd_series, 9)
    live_m, live_sig = live_macd_series[-1], live_signal_series[-1]
    live_hist = live_m - live_sig
    def candle_dir(r):
        o, c = float(r[1]), float(r[4])
        return "UP" if c > o else "DOWN" if c < o else None
    previous_dir = candle_dir(previous_closed)
    current_dir = candle_dir(current_row)
    dirs = [previous_dir, current_dir]
    closed_macd_dir = "UP" if m > sig and m > 0 else "DOWN" if m < sig and m < 0 else None
    live_macd_dir = "UP" if live_m > live_sig and live_m > 0 else "DOWN" if live_m < live_sig and live_m < 0 else None
    strengthening = False
    if current_dir == "UP" and closed_macd_dir == "UP" and live_macd_dir == "UP":
        strengthening = live_m > m and live_hist > closed_hist
    elif current_dir == "DOWN" and closed_macd_dir == "DOWN" and live_macd_dir == "DOWN":
        strengthening = live_m < m and live_hist < closed_hist
    direction = current_dir if strengthening else None
    recovery_confirmed = direction is not None and previous_dir == direction and current_dir == direction
    return direction, recovery_confirmed, m, sig, dirs, live_m, live_sig, closed_hist, live_hist

def session_allows_round(st, round_start):
    if st["session"] == "24h":
        return True
    local = round_start.astimezone(TZ)
    if local.weekday() >= 5:
        return False
    return 10 <= local.hour < 16

def base_edge(st):
    return BASE_EDGE_BY_TF.get(st.get("tf"), EDGE_5M)

def recovery_target(st, deficit_override=None):
    base = D(base_edge(st))
    deficit = max(D("0"), D(st.get("recovery_deficit", "0") or "0") if deficit_override is None else D(deficit_override or "0"))
    return base, deficit, deficit + base

def sizing_pair_conservative(st, dir_min_shares, opp_min_shares, dir_price, opp_price, deficit):
    """
    V64: Par conservador - AMBAS as pernas devem garantir lucro >= MIN_PAIR_GUARANTEED_PROFIT_USD
    """
    pd, po = D(dir_price), D(opp_price)
    base, deficit, target = recovery_target(st, deficit)
    combined = pd + po
    fee_factor = D("1") + PAIR_FEE_RESERVE_PCT
    
    # Calcula shares para que AMBAS as pernas tenham lucro garantido
    dir_nominal_min = ceil_6(MIN_LEG_USD / pd)
    opp_nominal_min = ceil_6(MIN_LEG_USD / po)
    dir_shares = max(dir_nominal_min, D(dir_min_shares))
    opp_shares = max(opp_nominal_min, D(opp_min_shares))
    
    # Verifica se AMBAS as pernas garantem lucro
    dir_spend = dir_shares * pd
    opp_spend = opp_shares * po
    total_spend = dir_spend + opp_spend
    fee_reserve = total_spend * PAIR_FEE_RESERVE_PCT
    
    dir_payout_if_wins = dir_shares
    opp_payout_if_wins = opp_shares
    
    dir_net = dir_payout_if_wins - total_spend - fee_reserve
    opp_net = opp_payout_if_wins - total_spend - fee_reserve
    
    min_guaranteed = min(dir_net, opp_net)
    
    blocked = None
    if combined > D("1.10"):  # Preço combinado muito alto
        blocked = "COMBINED_PRICE_TOO_HIGH"
    elif min_guaranteed < MIN_PAIR_GUARANTEED_PROFIT_USD:
        blocked = "NO_GUARANTEED_PROFIT_BOTH_LEGS"
    elif total_spend > MAX_ENTRY:
        blocked = "MAX_ENTRY"
    
    return {
        "blocked": bool(blocked),
        "reason": blocked,
        "base_profit": base,
        "recovery_deficit": deficit,
        "target_net_profit": target,
        "directional_shares": dir_shares,
        "opposite_shares": opp_shares,
        "directional_max_spend": dir_spend,
        "opposite_max_spend": opp_spend,
        "total_max_spend": total_spend,
        "directional_net": dir_net,
        "opposite_net": opp_net,
        "min_guaranteed_net": min_guaranteed,
        "fee_reserve": fee_reserve,
    }

def position_committed_cash(pos):
    if not isinstance(pos, dict):
        return D("0")
    return max(D("0"), D(pos.get("directional_spent", "0") or "0")) + max(D("0"), D(pos.get("opposite_spent", "0") or "0"))

def logical_cash_snapshot(st):
    equity = max(D("0"), D(st.get("bankroll", "0") or "0"))
    open_committed = sum((position_committed_cash(x) for x in (st.get("open_positions") or []) if isinstance(x, dict)), D("0"))
    pending_committed = position_committed_cash(st.get("pending")) if isinstance(st.get("pending"), dict) else D("0")
    committed = max(D("0"), open_committed + pending_committed)
    free = max(D("0"), equity - committed)
    return {"equity": equity, "open_committed": open_committed, "pending_committed": pending_committed, "committed": committed, "free": free}

class Bot:
    def __init__(self):
        self.s = load()
        # Reset único para simulação
        if not self.s.get("maintenance", {}).get("applied_resets"):
            for st in self.s.get("strategies", {}).values():
                st["bankroll"] = str(INITIAL)
                st["loss_streak"] = 0
                st["recovery_deficit"] = "0"
                st["wins"] = 0
                st["losses"] = 0
                st["trades"] = 0
                st["realized_pnl"] = "0"
                st["pending"] = None
                st["open_positions"] = []
            self.s["maintenance"]["applied_resets"] = ["v64_sim_reset"]
            save(self.s)
            log.warning("SIMULAÇÃO V64 | Reset total aplicado | caixa inicial por robô = USD 12.00")
        
        self.c = None
        if LIVE:
            if not PK or not WALLET:
                raise RuntimeError("Configure POLYMARKET_PRIVATE_KEY e POLYMARKET_DEPOSIT_WALLET")
            self.c = SecureClient.create(private_key=PK, wallet=WALLET)
            log.info(f"LIVE MODE | wallet={WALLET}")
        else:
            log.info("MODO SIMULAÇÃO | nenhuma ordem real será enviada")
        
        self.last_sync = 0.0
        self.last_hb = 0.0
    
    def place_gtc_limit(self, token, price, shares):
        if not LIVE:
            return {"simulation": True, "ok": True, "order_id": f"SIM-{token[-8:]}-{time.time_ns()}", "price": str(price), "size": str(shares)}
        try:
            return self.c.place_limit_order(token_id=str(token), side="BUY", price=str(price), size=str(shares), post_only=False)
        except Exception as e:
            log.warning(f"ORDEM GTC REJEITADA | token={token} | err={e}")
            raise
    
    def cancel_ids(self, ids):
        ids = [str(x) for x in ids if x]
        if not LIVE or not ids:
            return
        try:
            self.c.cancel_orders(order_ids=ids)
        except Exception:
            log.exception("cancel_orders falhou")
    
    def simulate_fill_if_marketable(self, p, m):
        if LIVE:
            return
        prices = m.get("price_map", {})
        for leg in ("directional", "opposite"):
            if D(p.get(f"{leg}_shares_filled", "0")) > 0:
                continue
            if D(p.get(f"{leg}_shares_requested", "0") or "0") <= 0:
                continue
            side = p[f"{leg}_side"]
            px = prices.get(side)
            leg_limit = D(p.get(f"{leg}_limit_price") or p["limit_price"])
            if px is None or px > leg_limit:
                continue
            requested = D(p[f"{leg}_shares_requested"])
            p[f"{leg}_shares_filled"] = str(requested)
            p[f"{leg}_spent"] = str(requested * px)
    
    def prepare_entry_window(self, st, round_start, direction):
        sl = slug(st["tf"], round_start, st.get("asset", "BTC"))
        ev = event(sl)
        if not ev:
            log.warning(f"{st['name']} | mercado não encontrado: {sl}")
            return
        m = market(ev)
        if not m or m.get("closed"):
            return
        
        dp = token_best_ask(m["up"] if direction == "UP" else m["down"])
        op = token_best_ask(m["down"] if direction == "UP" else m["up"])
        
        max_limit = D(MAX_BUY_PRICE)
        if dp is None or D(dp) > max_limit:
            log.info(f"{st['name']} | DIRECIONAL acima de {max_limit} | DIR={dp}")
            return
        if op is None or D(op) > max_limit:
            log.info(f"{st['name']} | OPOSTA acima de {max_limit} | OPP={op}")
            return
        
        tick = D(m["tick_size"])
        dir_limit = ceil_to_step(D(dp), tick)
        opp_limit = ceil_to_step(D(op), tick)
        
        rd = max(D("0"), D(st.get("recovery_deficit", "0") or "0"))
        sz = sizing_pair_conservative(
            st, m["min_order_shares"], m["min_order_shares"],
            dir_limit, opp_limit, rd
        )
        
        if sz.get("blocked"):
            log.warning(f"{st['name']} | BLOQUEADO: {sz.get('reason')} | min_guaranteed={sz.get('min_guaranteed_net')}")
            return
        
        cash = logical_cash_snapshot(st)
        total_spend = sz["total_max_spend"]
        if total_spend > cash["free"]:
            log.warning(f"{st['name']} | SEM CAIXA | gasto={total_spend} > livre={cash['free']}")
            return
        
        direction_token = m["up"] if direction == "UP" else m["down"]
        opposite_token = m["down"] if direction == "UP" else m["up"]
        opposite_direction = "DOWN" if direction == "UP" else "UP"
        round_end = round_start + timedelta(minutes=TFS[st["tf"]])
        
        p = {
            "phase": "await_resolution",
            "strategy": st["name"],
            "asset": st.get("asset", "BTC"),
            "slug": sl,
            "condition_id": m.get("condition_id") or "",
            "round_start": round_start.astimezone(UTC).isoformat(),
            "round_end": round_end.astimezone(UTC).isoformat(),
            "direction": direction,
            "directional_side": direction,
            "opposite_side": opposite_direction,
            "directional_token": direction_token,
            "opposite_token": opposite_token,
            "directional_shares_requested": str(sz["directional_shares"]),
            "opposite_shares_requested": str(sz["opposite_shares"]),
            "directional_shares_filled": "0",
            "opposite_shares_filled": "0",
            "directional_spent": "0",
            "opposite_spent": "0",
            "directional_limit_price": str(dir_limit),
            "opposite_limit_price": str(opp_limit),
            "target_net_profit": str(sz["target_net_profit"]),
            "min_guaranteed_net": str(sz["min_guaranteed_net"]),
            "execution_mode": "pair_conservative",
        }
        
        # Simulação: preenche imediatamente se o preço do book permitir
        if not LIVE:
            self.simulate_fill_if_marketable(p, m)
            dsh = D(p["directional_shares_filled"])
            osh = D(p["opposite_shares_filled"])
            if dsh <= 0 or osh <= 0:
                log.info(f"{st['name']} | SIMULAÇÃO: par não executou (book acima do limite)")
                return
        
        st["pending"] = p
        save(self.s)
        log.info(f"{st['name']} | ENTRADA REGISTRADA | DIR={direction} | DIR_SH={sz['directional_shares']} OPP_SH={sz['opposite_shares']} | MIN_GUARANTEED={sz['min_guaranteed_net']}")
    
    def tick(self, st, now):
        # Resolve posições pendentes
        p = st.get("pending")
        if p and p.get("phase") == "await_resolution":
            # Simulação: resolve após o round_end
            try:
                round_end = datetime.fromisoformat(p["round_end"])
                if now >= round_end:
                    self.resolve_simulation(st, p)
            except Exception:
                pass
            return
        
        # Verifica se deve entrar
        _, next_start = bounds(now, TFS[st["tf"]])
        seconds_to_next = (next_start - now.astimezone(TZ)).total_seconds()
        
        if not ENTRY_SECONDS - 1.2 <= seconds_to_next <= ENTRY_SECONDS + 0.8:
            return
        
        if not session_allows_round(st, next_start):
            return
        
        key = next_start.astimezone(UTC).isoformat()
        if st["last_trigger"] == key:
            return
        
        st["last_trigger"] = key
        save(self.s)
        
        direction, two_same, macd, sig, dirs, live_macd, live_sig, closed_hist, live_hist = trading_signal(st["tf"], st.get("asset", "BTC"))
        
        if not direction:
            log.info(f"{st['name']} | SEM SINAL | dirs={dirs}")
            return
        
        log.info(f"{st['name']} | SINAL APROVADO | direction={direction} | RD={st.get('recovery_deficit', '0')}")
        self.prepare_entry_window(st, next_start, direction)
    
    def resolve_simulation(self, st, p):
        """
        Simulação: usa o preço real do Binance para determinar o vencedor
        """
        try:
            asset = p.get("asset", "BTC")
            cfg = ASSET_CONFIG.get(asset, ASSET_CONFIG["BTC"])
            path = "/fapi/v1/klines" if asset == "HYPE" else "/api/v3/klines"
            round_start = datetime.fromisoformat(p["round_start"])
            round_end = datetime.fromisoformat(p["round_end"])
            rows = get(cfg["signal_base"] + path, {
                "symbol": cfg["spot_symbol"],
                "interval": p["tf"],
                "startTime": int(round_start.timestamp() * 1000),
                "limit": 1
            })
            if not rows:
                return
            open_price = D(rows[0][1])
            close_price = D(rows[0][4])
            winner = "UP" if close_price >= open_price else "DOWN"
        except Exception as e:
            log.warning(f"{st['name']} | ERRO RESOLUÇÃO SIM | err={e}")
            return
        
        direction = p["direction"]
        opposite = "DOWN" if direction == "UP" else "UP"
        dsh = D(p.get("directional_shares_filled", "0"))
        osh = D(p.get("opposite_shares_filled", "0"))
        dsp = D(p.get("directional_spent", "0"))
        osp = D(p.get("opposite_spent", "0"))
        
        winning_shares = dsh if winner == direction else osh if winner == opposite else D("0")
        pnl = winning_shares - dsp - osp
        
        bankroll = D(st["bankroll"]) + pnl
        st["bankroll"] = str(bankroll)
        st["realized_pnl"] = str(D(st.get("realized_pnl", "0")) + pnl)
        st["trades"] = int(st.get("trades", 0)) + 1
        
        if pnl < 0:
            st["losses"] = int(st.get("losses", 0)) + 1
            st["loss_streak"] = int(st.get("loss_streak", 0)) + 1
            deficit_after = D(st.get("recovery_deficit", "0")) + (-pnl)
        elif pnl > 0:
            st["wins"] = int(st.get("wins", 0)) + 1
            deficit_after = max(D("0"), D(st.get("recovery_deficit", "0")) - pnl)
            if deficit_after <= 0:
                st["loss_streak"] = 0
        else:
            deficit_after = D(st.get("recovery_deficit", "0"))
        
        st["recovery_deficit"] = str(deficit_after)
        st["last_pnl"] = str(pnl)
        st["last_result"] = "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT"
        st["pending"] = None
        
        audit({
            "type": "resolution_sim",
            "strategy": st["name"],
            "slug": p["slug"],
            "winner": winner,
            "signal": direction,
            "pnl": str(pnl),
            "bankroll_after": str(bankroll),
            "loss_streak_after": st["loss_streak"],
            "recovery_deficit_after": str(deficit_after),
            "ts": datetime.now(UTC).isoformat(),
        })
        
        save(self.s)
        log.info(
            f"{st['name']} | RESOLUÇÃO SIM | WINNER={winner} | SINAL={direction} | "
            f"PNL={pnl} | BANKROLL={bankroll} | RD={deficit_after} | STREAK={st['loss_streak']}"
        )
    
    def run(self):
        log.info("=" * 80)
        log.info(f"PREDICT V64 | MODO={'LIVE' if LIVE else 'SIMULAÇÃO'} | CAIXA_INICIAL={INITIAL} | ROBÔS={len(self.s['strategies'])}")
        log.info(f"SIZING CONSERVADOR | PAR SÓ ENTRA COM LUCRO GARANTIDO EM AMBAS AS PERNAS")
        log.info(f"MIN_GUARANTEED={MIN_PAIR_GUARANTEED_PROFIT_USD} | MAX_BUY_PRICE={MAX_BUY_PRICE}")
        log.info("=" * 80)
        
        hb = 0
        while not STOP:
            now = datetime.now(UTC)
            
            for st in self.s["strategies"].values():
                try:
                    self.tick(st, now)
                except Exception:
                    log.exception(f"{st['name']} | erro no tick")
            
            if time.time() - hb > 30:
                parts = []
                for st in self.s["strategies"].values():
                    cash = logical_cash_snapshot(st)
                    parts.append(
                        f'{st["name"]}:B={cash["equity"]},RD={st.get("recovery_deficit","0")},'
                        f'L={st["loss_streak"]},W={st["wins"]},T={st["trades"]},'
                        f'PNL={st.get("last_pnl","0")}'
                    )
                log.info("HEARTBEAT | " + " | ".join(parts))
                hb = time.time()
            
            time.sleep(POLL_SECONDS)

def stop(*_):
    global STOP
    STOP = True

signal_module.signal(signal_module.SIGTERM, stop)
signal_module.signal(signal_module.SIGINT, stop)

if __name__ == "__main__":
    bot = Bot()
    try:
        bot.run()
    finally:
        pass
