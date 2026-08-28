#!/usr/bin/env python3
import os
import sys
import json
import time
import signal as signal_module
import logging
import subprocess
import importlib
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Barrier
from zoneinfo import ZoneInfo
from urllib.request import Request, urlopen
from urllib.parse import urlencode

# ============================================================
# POLYMARKET BTC V14 FINAL - T-30 + ENVIO SIMULTANEO + SAQUES AUTOMATICOS + PROPORTIONAL PARTIAL FILL
#
# 6 robos logicos independentes:
#   5m / 15m / 1h x 24h / 10:00-16:00 Brasilia
#
# SINAL
#   - entrada T-30s para a PROXIMA rodada
#   - ultimo candle FECHADO + MACD 7/21/9 na mesma direcao
#   - depois de LOSS: exige 2 candles FECHADOS consecutivos
#     na mesma direcao + MACD alinhado
#
# EXECUCAO
#   - duas BUY LIMIT GTC, uma em cada outcome
#   - preco limite <= 0.55 em ambos os lados
#   - as ordens ficam PENDENTES no livro ate o inicio da rodada
#   - se um lado executar antes, o outro continua pendente
#   - no inicio da rodada, todo saldo ainda aberto e cancelado
#   - se nenhum lado executou, a rodada e descartada
#   - se houve fill unilateral/parcial, ele NAO pode ser "cancelado";
#     a exposicao executada e registrada e acompanhada ate a resolucao
#
# SIZING
#   - caixa inicial independente: US$12
#   - lado oposto: minimum_order_size do mercado (EM SHARES)
#   - diferencial direcional inicial: US$0.10
#   - lucro acumulado > US$5:
#         diferencial = 1% do bankroll, arredondado para cima ao centavo
#   - bankroll > US$1000:
#         diferencial-base = US$10
#   - martingale dobra SOMENTE o diferencial
#   - maximo de US$1000 de gasto teorico por perna
#   - objetivo: US$200,000 por robo
#
# RECONCILIACAO DE SAQUES
#   - consulta a atividade WITHDRAWAL da propria carteira Polymarket
#   - aplica somente saques posteriores ao primeiro startup da V14
#   - cada saque reduz proporcionalmente os 6 bankrolls logicos
#   - trades, fills, ordens abertas, splits, merges e redeems NAO contam como saque
#   - eventos ja processados sao persistidos para nunca descontar duas vezes
# ============================================================


def ensure_sdk():
    try:
        import polymarket  # noqa: F401
        print("BOOTSTRAP | polymarket SDK ja disponivel", flush=True)
        return
    except ImportError:
        print("BOOTSTRAP | instalando polymarket-client==0.3.0b1", flush=True)

    subprocess.check_call([
        sys.executable,
        "-m",
        "pip",
        "install",
        "--no-cache-dir",
        "--root-user-action=ignore",
        "polymarket-client==0.3.0b1",
    ])
    importlib.invalidate_caches()

    try:
        import polymarket  # noqa: F401
        print("BOOTSTRAP | SDK instalado e importado com sucesso", flush=True)
    except ImportError as exc:
        raise RuntimeError(
            "polymarket-client foi instalado, mas o modulo polymarket nao pode ser importado"
        ) from exc


ensure_sdk()

from polymarket import SecureClient  # noqa: E402


TZ = ZoneInfo("America/Sao_Paulo")
ET = ZoneInfo("America/New_York")
UTC = timezone.utc

GAMMA = "https://gamma-api.polymarket.com"
BINANCE = "https://api.binance.com"

PK = (os.getenv("POLYMARKET_PRIVATE_KEY") or os.getenv("PRIVATE_KEY") or "").strip()
WALLET = os.getenv("POLYMARKET_DEPOSIT_WALLET", "").strip()
LIVE = os.getenv("LIVE_TRADING", "0").lower() in ("1", "true", "yes", "on")

INITIAL = Decimal(os.getenv("INITIAL_BANKROLL", "12.00"))
INITIAL_EDGE = Decimal(os.getenv("INITIAL_EDGE", "0.10"))
PROFIT_SWITCH = Decimal(os.getenv("PROFIT_SWITCH", "5.00"))
HIGH_BANKROLL_EDGE = Decimal(os.getenv("HIGH_BANKROLL_EDGE", "10.00"))
MAX_ENTRY = Decimal(os.getenv("MAX_ENTRY", "1000.00"))
TARGET = Decimal(os.getenv("TARGET_BANKROLL", "200000.00"))
WITHDRAWAL_SYNC_SECONDS = float(os.getenv("WITHDRAWAL_SYNC_SECONDS", "20"))

ENTRY_SECONDS = 30  # FIXO: trava o sinal 30s antes da proxima rodada
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "0.5"))
MAX_BUY_PRICE = Decimal(os.getenv("MAX_BUY_PRICE", "0.55"))

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
log = logging.getLogger("btc-polymarket-v14")
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


def floor_6(x):
    return D(x).quantize(Decimal("0.000001"), rounding=ROUND_FLOOR)


def get(url, params=None):
    if params:
        url += "?" + urlencode(params)
    with urlopen(
        Request(url, headers={"User-Agent": "btc-polymarket-v14"}),
        timeout=12,
    ) as r:
        return json.loads(r.read())


def js(x):
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return [x]
    return []


def fresh():
    s = {
        "version": 14,
        "strategies": {},
        "capital_reconciliation": {
            "initialized": False,
            "baseline_epoch": 0,
            "last_success_epoch": 0,
            "processed_withdrawals": [],
            "total_withdrawn_applied": "0",
            "last_withdrawal": None,
        },
    }
    for tf in TFS:
        for session in ("24h", "day"):
            name = f"{tf}_{session}"
            s["strategies"][name] = {
                "name": name,
                "tf": tf,
                "session": session,
                "bankroll": str(INITIAL),
                "loss_streak": 0,
                "wins": 0,
                "losses": 0,
                "trades": 0,
                "realized_pnl": "0",
                "last_trigger": "",
                "pending": None,
            }
    return s


def save(s):
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(s, indent=2))
    tmp.replace(STATE)


def load():
    if not STATE.exists():
        s = fresh()
        save(s)
        return s
    try:
        old = json.loads(STATE.read_text())
        new = fresh()
        for k, v in old.get("strategies", {}).items():
            if k not in new["strategies"]:
                continue
            dst = new["strategies"][k]
            for field in (
                "bankroll",
                "loss_streak",
                "wins",
                "losses",
                "trades",
                "realized_pnl",
                "last_trigger",
                "pending",
            ):
                if field in v:
                    dst[field] = v[field]

            # Compatibilidade com pending antigo (V9):
            p = dst.get("pending")
            if isinstance(p, dict) and "phase" not in p:
                p["phase"] = "await_resolution"
                p.setdefault("directional_spent", p.get("directional_amount", "0"))
                p.setdefault("opposite_spent", p.get("opposite_amount", "0"))
                p.setdefault("directional_shares", "0")
                p.setdefault("opposite_shares", "0")

        tracker = old.get("capital_reconciliation")
        if isinstance(tracker, dict):
            dst_tracker = new["capital_reconciliation"]
            for field in (
                "initialized",
                "baseline_epoch",
                "last_success_epoch",
                "processed_withdrawals",
                "total_withdrawn_applied",
                "last_withdrawal",
            ):
                if field in tracker:
                    dst_tracker[field] = tracker[field]

        new["version"] = 14
        save(new)
        return new
    except Exception:
        log.exception("state.json invalido; criando estado novo")
        s = fresh()
        save(s)
        return s


def audit(payload):
    with TRADES.open("a") as f:
        f.write(json.dumps(payload, default=str) + "\n")


def ema(values, n):
    k = 2 / (n + 1)
    e = float(values[0])
    out = []
    for x in values:
        e = float(x) * k + e * (1 - k)
        out.append(e)
    return out


def trading_signal(tf):
    """
    Usa apenas candles FECHADOS.

    Entrada normal:
      ultimo candle fechado == direcao MACD.

    Apos loss:
      os 2 ultimos candles fechados precisam estar na mesma direcao
      e essa direcao precisa estar alinhada ao MACD.
    """
    rows = get(
        BINANCE + "/api/v3/klines",
        {"symbol": "BTCUSDT", "interval": tf, "limit": 120},
    )
    now_ms = int(time.time() * 1000)
    closed = [r for r in rows if int(r[6]) < now_ms]
    if len(closed) < 30:
        return None, False, None, None, []

    closes = [float(r[4]) for r in closed]
    fast = ema(closes, 7)
    slow = ema(closes, 21)
    macd = [a - b for a, b in zip(fast, slow)]
    signal_line = ema(macd, 9)
    m, sig = macd[-1], signal_line[-1]

    def candle_dir(r):
        o = float(r[1])
        c = float(r[4])
        if c > o:
            return "UP"
        if c < o:
            return "DOWN"
        return None

    dirs = [candle_dir(r) for r in closed[-2:]]
    last = dirs[-1]

    macd_dir = (
        "UP"
        if m > sig and m > 0
        else "DOWN"
        if m < sig and m < 0
        else None
    )

    direction = last if last is not None and last == macd_dir else None
    two_same = (
        direction is not None
        and len(dirs) == 2
        and dirs[0] == direction
        and dirs[1] == direction
    )
    return direction, two_same, m, sig, dirs


def bounds(now, mins):
    x = now.astimezone(TZ)
    if mins == 60:
        start = x.replace(minute=0, second=0, microsecond=0)
        return start, start + timedelta(hours=1)

    start = x.replace(
        minute=(x.minute // mins) * mins,
        second=0,
        microsecond=0,
    )
    return start, start + timedelta(minutes=mins)


def slug(tf, round_start):
    if tf != "1h":
        return f"btc-updown-{tf}-{int(round_start.astimezone(UTC).timestamp())}"

    e = round_start.astimezone(ET)
    return (
        f"bitcoin-up-or-down-{e.strftime('%B').lower()}-"
        f"{e.day}-{e.year}-"
        f"{e.strftime('%I').lstrip('0')}{e.strftime('%p').lower()}-et"
    )


def event(sl):
    try:
        return get(GAMMA + "/events/slug/" + sl)
    except Exception:
        return None


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
                    direction = (
                        "UP"
                        if label in ("UP", "YES")
                        else "DOWN"
                        if label in ("DOWN", "NO")
                        else label
                    )
                    price_map[direction] = D(p)
                except Exception:
                    pass

        return {
            "up": up,
            "down": down,
            "closed": bool(m.get("closed")),
            "active": m.get("active"),
            "accepting_orders": m.get("acceptingOrders"),
            "enable_order_book": m.get("enableOrderBook"),
            "outcomes": outcomes,
            "prices": prices,
            "price_map": price_map,
            # IMPORTANTE: orderMinSize e tamanho minimo EM SHARES.
            "min_order_shares": D(m.get("orderMinSize") or 0),
            "tick_size": D(m.get("orderPriceMinTickSize") or "0.01"),
            "condition_id": str(m.get("conditionId") or m.get("condition_id") or ""),
        }

    return None


def winner(sl):
    m = market(event(sl))
    if not m or not m["closed"] or len(m["outcomes"]) != len(m["prices"]):
        return None

    ranked = []
    for o, p in zip(m["outcomes"], m["prices"]):
        try:
            ranked.append((D(p), str(o).upper()))
        except Exception:
            pass

    if not ranked:
        return None

    p, o = max(ranked)
    if p < D("0.95"):
        return None

    if o in ("UP", "YES"):
        return "UP"
    if o in ("DOWN", "NO"):
        return "DOWN"
    return None


def session_allows_round(st, round_start):
    if st["session"] == "24h":
        return True
    h = round_start.astimezone(TZ).hour
    return 10 <= h < 16


def base_edge(st):
    b = D(st["bankroll"])
    profit = b - INITIAL

    if b > D("1000"):
        return HIGH_BANKROLL_EDGE

    if profit > PROFIT_SWITCH:
        return ceil_cent(b * D("0.01"))

    return INITIAL_EDGE


def sizing(st, min_shares, limit_price):
    """
    min_shares: minimo do mercado em SHARES.

    O lado oposto usa exatamente o minimo de shares do mercado.
    O diferencial do lado direcional e definido em USDC.

    Como a ordem e LIMIT @ <=0.55, o gasto maximo teorico e:
        shares * limit_price
    """
    min_shares = D(min_shares)
    limit_price = D(limit_price)

    opposite_shares = min_shares
    opposite_max_spend = opposite_shares * limit_price

    edge = base_edge(st) * (D(2) ** int(st["loss_streak"]))
    edge = min(edge, MAX_ENTRY)

    directional_max_spend_target = min(
        opposite_max_spend + edge,
        MAX_ENTRY,
    )
    directional_shares = floor_6(directional_max_spend_target / limit_price)

    # Garante minimo e nao passa do teto em USDC.
    directional_shares = max(directional_shares, min_shares)
    if directional_shares * limit_price > MAX_ENTRY:
        directional_shares = floor_6(MAX_ENTRY / limit_price)

    directional_max_spend = directional_shares * limit_price
    actual_edge_at_limit = max(D("0"), directional_max_spend - opposite_max_spend)

    return {
        "opposite_shares": opposite_shares,
        "directional_shares": directional_shares,
        "opposite_max_spend": opposite_max_spend,
        "directional_max_spend": directional_max_spend,
        "edge": actual_edge_at_limit,
    }


def accepted(resp):
    return bool(resp is not None and getattr(resp, "ok", False))


def order_id_of(resp):
    if not accepted(resp):
        return None
    oid = getattr(resp, "order_id", None)
    return str(oid) if oid else None


def rejected_reason(resp):
    if resp is None:
        return "sem resposta"
    if accepted(resp):
        return None
    return f"{getattr(resp, 'code', 'unknown')}: {getattr(resp, 'message', str(resp))}"


class Bot:
    def __init__(self):
        self.s = load()
        self.c = None

        if LIVE:
            if not PK or not WALLET:
                raise RuntimeError(
                    "Configure POLYMARKET_PRIVATE_KEY/PRIVATE_KEY "
                    "e POLYMARKET_DEPOSIT_WALLET"
                )
            self.c = SecureClient.create(private_key=PK, wallet=WALLET)
            log.info(
                "REAL | signer=%s wallet=%s type=%s",
                self.c.signer,
                self.c.wallet,
                self.c.wallet_type,
            )
        else:
            log.info("SIMULACAO")

        self.last_withdrawal_sync = 0.0
        self.initialize_withdrawal_tracker()

    # -------------------- CAPITAL / WITHDRAWALS --------------------

    def initialize_withdrawal_tracker(self):
        tracker = self.s.setdefault("capital_reconciliation", {})
        tracker.setdefault("initialized", False)
        tracker.setdefault("baseline_epoch", 0)
        tracker.setdefault("last_success_epoch", 0)
        tracker.setdefault("processed_withdrawals", [])
        tracker.setdefault("total_withdrawn_applied", "0")
        tracker.setdefault("last_withdrawal", None)

        if not tracker.get("initialized"):
            now_epoch = int(time.time())
            tracker["initialized"] = True
            tracker["baseline_epoch"] = now_epoch
            tracker["last_success_epoch"] = now_epoch
            tracker["processed_withdrawals"] = []
            save(self.s)
            log.info(
                "SAQUES | baseline criado em %s | saques anteriores a este startup nao serao descontados",
                datetime.fromtimestamp(now_epoch, UTC).isoformat(),
            )
        else:
            log.info(
                "SAQUES | tracker restaurado | baseline=%s | total_descontado=%s",
                tracker.get("baseline_epoch"),
                tracker.get("total_withdrawn_applied", "0"),
            )

    @staticmethod
    def withdrawal_key(activity):
        tx = str(getattr(activity, "transaction_hash", "") or "")
        ts = getattr(activity, "timestamp", None)
        amount = D(getattr(activity, "amount", 0) or 0)
        ts_text = ts.isoformat() if hasattr(ts, "isoformat") else str(ts or "")
        return f"{tx}|{ts_text}|{amount}"

    def apply_withdrawal(self, amount, activity):
        amount = D(amount)
        if amount <= 0:
            return D("0")

        strategies = list(self.s["strategies"].values())
        bankrolls = [max(D("0"), D(st["bankroll"])) for st in strategies]
        total = sum(bankrolls, D("0"))

        if total <= 0:
            log.warning(
                "SAQUE DETECTADO | amount=%s | bankroll logico total ja esta zerado",
                amount,
            )
            return D("0")

        applied = min(amount, total)
        remaining = applied
        positive_indexes = [i for i, b in enumerate(bankrolls) if b > 0]

        adjustments = {}
        for pos, idx in enumerate(positive_indexes):
            st = strategies[idx]
            before = bankrolls[idx]

            if pos == len(positive_indexes) - 1:
                cut = min(before, remaining)
            else:
                cut = floor_6(applied * before / total)
                cut = min(before, cut, remaining)

            after = max(D("0"), before - cut)
            st["bankroll"] = str(after)
            remaining -= cut
            adjustments[st["name"]] = {
                "before": str(before),
                "deduction": str(cut),
                "after": str(after),
            }

        # Se arredondamentos de 6 casas deixarem residuo, tira do maior bankroll disponivel.
        if remaining > 0:
            for st in sorted(strategies, key=lambda x: D(x["bankroll"]), reverse=True):
                available = D(st["bankroll"])
                if available <= 0:
                    continue
                extra = min(available, remaining)
                before2 = available
                st["bankroll"] = str(before2 - extra)
                adj = adjustments.setdefault(st["name"], {
                    "before": str(before2),
                    "deduction": "0",
                    "after": str(before2),
                })
                adj["deduction"] = str(D(adj["deduction"]) + extra)
                adj["after"] = str(D(st["bankroll"]))
                remaining -= extra
                if remaining <= 0:
                    break

        effective_applied = applied - max(D("0"), remaining)
        tracker = self.s["capital_reconciliation"]
        tracker["total_withdrawn_applied"] = str(
            D(tracker.get("total_withdrawn_applied", "0")) + effective_applied
        )

        ts = getattr(activity, "timestamp", None)
        tx = str(getattr(activity, "transaction_hash", "") or "")
        tracker["last_withdrawal"] = {
            "amount": str(amount),
            "applied": str(effective_applied),
            "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts or ""),
            "transaction_hash": tx,
        }

        audit({
            "type": "withdrawal_reconciliation",
            "amount": str(amount),
            "applied": str(effective_applied),
            "logical_total_before": str(total),
            "logical_total_after": str(total - effective_applied),
            "transaction_hash": tx,
            "timestamp": tracker["last_withdrawal"]["timestamp"],
            "adjustments": adjustments,
        })

        log.warning(
            "SAQUE DETECTADO | amount=%s | aplicado=%s | bankroll_total %s -> %s | tx=%s",
            amount,
            effective_applied,
            total,
            total - effective_applied,
            tx or "-",
        )
        for name, adj in adjustments.items():
            log.warning(
                "SAQUE AJUSTE | %s | %s - %s = %s",
                name,
                adj["before"],
                adj["deduction"],
                adj["after"],
            )

        return effective_applied

    def sync_withdrawals(self, force=False):
        if not LIVE or not self.c:
            return

        now_monotonic = time.monotonic()
        if not force and now_monotonic - self.last_withdrawal_sync < WITHDRAWAL_SYNC_SECONDS:
            return
        self.last_withdrawal_sync = now_monotonic

        tracker = self.s["capital_reconciliation"]
        baseline = int(tracker.get("baseline_epoch", 0) or 0)
        last_success = int(tracker.get("last_success_epoch", baseline) or baseline)
        now_epoch = int(time.time())

        # Janela com overlap de 1h para absorver atraso eventual da Data API.
        # O conjunto processed_withdrawals impede qualquer desconto duplicado.
        start_epoch = max(baseline, last_success - 3600)
        processed = set(str(x) for x in tracker.get("processed_withdrawals", []))
        found = []

        try:
            paginator = self.c.list_activity(
                activity_types=("WITHDRAWAL",),
                start=start_epoch,
                end=now_epoch,
                page_size=100,
            )

            count = 0
            for page in paginator:
                for activity in page.items:
                    count += 1
                    if count > 1000:
                        raise RuntimeError("mais de 1000 saques na janela de reconciliacao")

                    if str(getattr(activity, "type", "")) != "WITHDRAWAL":
                        continue

                    ts = getattr(activity, "timestamp", None)
                    if ts is None:
                        continue
                    ts_epoch = int(ts.timestamp())
                    if ts_epoch <= baseline:
                        continue

                    amount = D(getattr(activity, "amount", 0) or 0)
                    if amount <= 0:
                        continue

                    key = self.withdrawal_key(activity)
                    if key in processed:
                        continue

                    found.append((ts_epoch, key, activity, amount))

            # Processa cronologicamente para o historico ficar deterministico.
            found.sort(key=lambda x: (x[0], x[1]))
            for _ts_epoch, key, activity, amount in found:
                self.apply_withdrawal(amount, activity)
                processed.add(key)

            # Retem chaves recentes; 2000 e muito acima do uso esperado e evita crescimento infinito.
            tracker["processed_withdrawals"] = list(processed)[-2000:]
            tracker["last_success_epoch"] = now_epoch
            save(self.s)

            if found:
                log.info("SAQUES | reconciliados=%s | scan_ate=%s", len(found), now_epoch)

        except Exception:
            # Nao altera last_success_epoch em caso de falha; assim a proxima tentativa cobre a mesma janela.
            log.exception("SAQUES | falha ao consultar/reconciliar withdrawals; sera tentado novamente")

    # ------------------------- ORDERS -------------------------

    def place_gtc_limit(self, token, price, shares):
        """
        BUY LIMIT GTC a no maximo 0.55.
        A ordem pode executar imediatamente ou permanecer no livro.
        """
        if not LIVE:
            return {
                "simulation": True,
                "ok": True,
                "order_id": f"SIM-{token[-8:]}-{time.time_ns()}",
                "price": str(price),
                "size": str(shares),
            }

        return self.c.place_limit_order(
            token_id=token,
            price=price,
            size=shares,
            side="BUY",
            post_only=False,
        )

    def cancel_ids(self, ids):
        ids = [str(x) for x in ids if x]
        if not LIVE or not ids:
            return

        try:
            self.c.cancel_orders(order_ids=ids)
            return
        except Exception:
            log.exception("cancel_orders em lote falhou; tentando individualmente")

        for oid in ids:
            try:
                self.c.cancel_order(order_id=oid)
            except Exception:
                log.exception("falha ao cancelar order_id=%s", oid)

    def get_order_snapshot(self, oid):
        if not LIVE or not oid:
            return None
        try:
            return self.c.get_order(order_id=str(oid))
        except Exception:
            return None

    def fills_for_order(self, oid, token, snapshot=None):
        """
        Apura shares e USDC gastos especificamente pelo order_id.
        """
        if not LIVE or not oid:
            return D("0"), D("0")

        oid = str(oid)
        trade_ids = []
        if snapshot is not None:
            trade_ids = [
                str(x)
                for x in getattr(snapshot, "associate_trades", ()) or ()
            ]

        shares = D("0")
        spent = D("0")
        seen = set()

        for trade_id in trade_ids:
            try:
                page = self.c.list_account_trades(id=trade_id).first_page()
                trades = page.items
            except Exception:
                continue

            for tr in trades:
                tid = str(getattr(tr, "id", ""))
                if tid and tid in seen:
                    continue

                matched = D("0")
                px = D("0")

                if str(getattr(tr, "taker_order_id", "")) == oid:
                    matched = D(getattr(tr, "size", 0) or 0)
                    px = D(getattr(tr, "price", 0) or 0)
                else:
                    for mo in getattr(tr, "maker_orders", ()) or ():
                        if str(getattr(mo, "order_id", "")) == oid:
                            matched = D(getattr(mo, "matched_amount", 0) or 0)
                            px = D(getattr(mo, "price", 0) or 0)
                            break

                if matched > 0 and px > 0:
                    shares += matched
                    spent += matched * px
                    if tid:
                        seen.add(tid)

        if shares > 0:
            return shares, spent

        # Fallback enquanto os trades ainda nao apareceram no endpoint.
        if snapshot is not None:
            matched = D(getattr(snapshot, "size_matched", 0) or 0)
            px = D(getattr(snapshot, "price", MAX_BUY_PRICE) or MAX_BUY_PRICE)
            if matched > 0:
                return matched, matched * px

        return D("0"), D("0")

    def refresh_fills(self, p):
        """
        Atualiza fills agregando TODO o historico de order_ids de cada perna.
        Isso e necessario porque um rebalanceamento parcial usa cancel+replace.
        """
        if not LIVE:
            return

        for leg in ("directional", "opposite"):
            token = p.get(f"{leg}_token")
            ids = p.get(f"{leg}_order_ids") or []
            if not ids and p.get(f"{leg}_order_id"):
                ids = [p.get(f"{leg}_order_id")]

            total_shares = D("0")
            total_spent = D("0")

            for oid in ids:
                if not oid:
                    continue
                snap = self.get_order_snapshot(oid)
                sh, spent = self.fills_for_order(oid, token, snap)
                total_shares += sh
                total_spent += spent

            p[f"{leg}_shares_filled"] = str(total_shares)
            p[f"{leg}_spent"] = str(total_spent)

    def cancel_leg(self, p, leg):
        oid = p.get(f"{leg}_order_id")
        if oid:
            self.cancel_ids([oid])
        p[f"{leg}_order_id"] = None

    def place_rebalanced_leg(self, p, leg, shares):
        """
        Cria uma nova GTC para a quantidade proporcional restante.
        Nao cria ordem abaixo do minimum_order_size.
        """
        shares = floor_6(D(shares))
        min_shares = D(p["minimum_order_shares"])

        if shares < min_shares:
            return False

        resp = self.place_gtc_limit(
            p[f"{leg}_token"],
            D(p["limit_price"]),
            shares,
        )

        oid = (
            resp.get("order_id")
            if isinstance(resp, dict)
            else order_id_of(resp)
        )
        if not oid:
            return False

        p[f"{leg}_order_id"] = str(oid)
        history = p.setdefault(f"{leg}_order_ids", [])
        history.append(str(oid))
        p[f"{leg}_rebalance_requested"] = str(shares)
        return True

    def rebalance_partial_pair(self, st, p):
        """
        Preserva matematicamente a mesma FRACAO de execucao dos tamanhos
        originalmente calculados.

        Exemplo:
          DIR original 20 shares, executou 8 = 40%
          OPP original 10 shares
          alvo OPP = 10 * 40% = 4 shares

        Se a perna de referencia executou 100%, a outra continua buscando 100%.

        Como a API nao possui alteracao in-place do tamanho, o ajuste e feito
        por cancelamento + nova limit GTC. Se a quantidade proporcional ficar
        abaixo do minimo do mercado, nao envia ordem invalida; nesse caso
        mantem a estrutura original buscando o preenchimento integral.
        """
        d_req = D(p["directional_shares_requested"])
        o_req = D(p["opposite_shares_requested"])
        d_fill = D(p.get("directional_shares_filled", "0"))
        o_fill = D(p.get("opposite_shares_filled", "0"))

        if d_req <= 0 or o_req <= 0:
            return False

        fd = min(D("1"), d_fill / d_req)
        fo = min(D("1"), o_fill / o_req)
        tol = D("0.000001")

        if abs(fd - fo) <= tol and fd > 0:
            self.cancel_leg(p, "directional")
            self.cancel_leg(p, "opposite")
            p["phase"] = "await_resolution"
            p["pair_complete"] = True
            p["balanced_fill_fraction"] = str(min(fd, fo))
            save(self.s)
            log.info(
                "%s | PAR PROPORCIONAL EQUILIBRADO | fracao=%s",
                st["name"],
                min(fd, fo),
            )
            return True

        if fd > fo:
            lead, lag = "directional", "opposite"
            lead_fraction = fd
            lag_req = o_req
            lag_fill = o_fill
        elif fo > fd:
            lead, lag = "opposite", "directional"
            lead_fraction = fo
            lag_req = d_req
            lag_fill = d_fill
        else:
            return False

        target_lag_total = floor_6(lag_req * lead_fraction)
        needed = floor_6(max(D("0"), target_lag_total - lag_fill))
        min_shares = D(p["minimum_order_shares"])

        # Se uma perna ja executou 100%, a outra continua buscando 100%.
        if lead_fraction >= D("0.999999"):
            return False

        # Se a proporcao exigir uma nova ordem menor que o minimo permitido,
        # nao cria ordem invalida. Mantem a estrutura original buscando 100%.
        if needed < min_shares:
            p["rebalance_blocked_by_minimum"] = {
                "lead": lead,
                "lag": lag,
                "lead_fraction": str(lead_fraction),
                "needed_shares": str(needed),
                "minimum_order_shares": str(min_shares),
            }
            save(self.s)
            return False

        # Congela a perna que esta na frente e recria a perna atrasada
        # somente com o necessario para atingir a mesma proporcao.
        self.cancel_leg(p, lead)
        self.cancel_leg(p, lag)

        ok = self.place_rebalanced_leg(p, lag, needed)
        if not ok:
            p["rebalance_error"] = {
                "lead": lead,
                "lag": lag,
                "needed_shares": str(needed),
            }
            save(self.s)
            log.error(
                "%s | FALHA REBALANCEAMENTO | %s precisava %s shares",
                st["name"],
                lag,
                needed,
            )
            return False

        p["phase"] = "proportional_rebalance"
        p["rebalance"] = {
            "lead": lead,
            "lag": lag,
            "lead_fraction": str(lead_fraction),
            "target_lag_total": str(target_lag_total),
            "needed_shares": str(needed),
        }
        save(self.s)

        log.warning(
            "%s | FILL PARCIAL REBALANCEADO | %s=%s%% | "
            "%s ajustado para total proporcional=%s shares",
            st["name"],
            lead,
            (lead_fraction * D("100")).quantize(D("0.01")),
            lag,
            target_lag_total,
        )
        return True

    def simulate_fill_if_marketable(self, p, m):
        """
        Simulacao: considera fill quando o snapshot daquele lado estiver <= limite.
        """
        if LIVE:
            return

        prices = m.get("price_map", {})
        limit_price = D(p["limit_price"])

        for leg in ("directional", "opposite"):
            if D(p.get(f"{leg}_shares_filled", "0")) > 0:
                continue

            side = p[f"{leg}_side"]
            px = prices.get(side)
            if px is None or px > limit_price:
                continue

            requested = D(p[f"{leg}_shares_requested"])
            p[f"{leg}_shares_filled"] = str(requested)
            p[f"{leg}_spent"] = str(requested * px)

    # ------------------------- PRE-START WINDOW -------------------------

    def prepare_entry_window(self, st, round_start, direction):
        """
        Em T-30 o sinal fica TRAVADO.
        Nenhuma ordem e enviada ainda.

        Do T-30 ate o inicio:
          - monitora os DOIS precos
          - somente quando AMBOS estiverem <= 0.55 simultaneamente,
            envia as duas BUY LIMIT GTC
          - se isso nunca ocorrer antes do inicio, descarta a rodada
        """
        sl = slug(st["tf"], round_start)
        m = market(event(sl))

        if not m:
            log.warning("%s | mercado futuro nao encontrado: %s", st["name"], sl)
            return

        if m["closed"]:
            return

        if m["accepting_orders"] is False or m["enable_order_book"] is False:
            log.warning(
                "%s | mercado futuro nao aceita ordens/orderbook: %s",
                st["name"],
                sl,
            )
            return

        min_shares = D(m["min_order_shares"])
        tick = D(m["tick_size"])
        if min_shares <= 0 or tick <= 0:
            log.warning(
                "%s | minimum_order_size/tick_size invalido | %s",
                st["name"],
                sl,
            )
            return

        limit_price = floor_to_step(MAX_BUY_PRICE, tick)
        if limit_price <= 0:
            return

        sz = sizing(st, min_shares, limit_price)
        worst_total = sz["directional_max_spend"] + sz["opposite_max_spend"]

        if worst_total > D(st["bankroll"]):
            log.warning(
                "%s | BLOQUEADO: pior gasto %s > bankroll %s",
                st["name"],
                worst_total,
                st["bankroll"],
            )
            return

        directional_token = m["up"] if direction == "UP" else m["down"]
        opposite_token = m["down"] if direction == "UP" else m["up"]
        opposite_direction = "DOWN" if direction == "UP" else "UP"

        round_end = round_start + timedelta(minutes=TFS[st["tf"]])

        st["pending"] = {
            "phase": "waiting_both_prices",
            "strategy": st["name"],
            "slug": sl,
            "round_start": round_start.astimezone(UTC).isoformat(),
            "round_end": round_end.astimezone(UTC).isoformat(),
            "direction": direction,
            "directional_side": direction,
            "opposite_side": opposite_direction,
            "directional_token": directional_token,
            "opposite_token": opposite_token,
            "directional_order_id": None,
            "opposite_order_id": None,
            "directional_shares_requested": str(sz["directional_shares"]),
            "opposite_shares_requested": str(sz["opposite_shares"]),
            "directional_shares_filled": "0",
            "opposite_shares_filled": "0",
            "directional_spent": "0",
            "opposite_spent": "0",
            "limit_price": str(limit_price),
            "minimum_order_shares": str(min_shares),
            "edge_at_limit": str(sz["edge"]),
            "signal_locked_at": datetime.now(UTC).isoformat(),
            "live": LIVE,
            "last_wait_log": 0,
        }
        save(self.s)

        log.info(
            "%s | SINAL TRAVADO %s | aguardando AMBOS <= %s ate %s | %s",
            st["name"],
            direction,
            limit_price,
            round_start.isoformat(),
            sl,
        )

        # Tenta imediatamente no proprio T-30.
        self.wait_for_both_prices(st, datetime.now(UTC))

    def wait_for_both_prices(self, st, now):
        p = st.get("pending")
        if not p or p.get("phase") != "waiting_both_prices":
            return

        round_start = datetime.fromisoformat(p["round_start"])
        if now >= round_start:
            audit({
                "type": "no_entry_price_condition",
                "strategy": st["name"],
                "slug": p["slug"],
                "reason": "os dois lados nao ficaram <= limite simultaneamente antes do inicio",
                "limit_price": p["limit_price"],
                "ts": now.isoformat(),
            })
            log.info(
                "%s | RODADA DESCARTADA | nunca houve AMBOS <= %s antes do inicio",
                st["name"],
                p["limit_price"],
            )
            st["pending"] = None
            save(self.s)
            return

        m = market(event(p["slug"]))
        if not m:
            return

        prices = m.get("price_map", {})
        dp = prices.get(p["directional_side"])
        op = prices.get(p["opposite_side"])
        limit_price = D(p["limit_price"])

        both_ok = (
            dp is not None
            and op is not None
            and D(dp) <= limit_price
            and D(op) <= limit_price
        )

        if not both_ok:
            # Evita inundar o log a cada 0.5 s.
            if time.time() - float(p.get("last_wait_log", 0)) >= 3:
                log.info(
                    "%s | AGUARDANDO PRECO | DIR=%s OPP=%s | precisa AMBOS <= %s",
                    st["name"],
                    dp,
                    op,
                    limit_price,
                )
                p["last_wait_log"] = time.time()
                save(self.s)
            return

        # Somente AQUI as duas ordens sao criadas.
        log.info(
            "%s | CONDICAO DE PRECO OK | DIR=%s OPP=%s <= %s | enviando par",
            st["name"],
            dp,
            op,
            limit_price,
        )

        r_dir = r_opp = None
        e_dir = e_opp = None

        # Disparo sincronizado do par. Cada perna usa uma requisicao separada,
        # portanto a exchange nao oferece atomicidade entre os dois outcomes;
        # a Barrier reduz ao minimo a diferenca local entre os dois envios.
        start_barrier = Barrier(3)

        def send_leg(token, shares):
            start_barrier.wait()
            return self.place_gtc_limit(token, limit_price, D(shares))

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="pair-v131") as ex:
            f_dir = ex.submit(
                send_leg,
                p["directional_token"],
                p["directional_shares_requested"],
            )
            f_opp = ex.submit(
                send_leg,
                p["opposite_token"],
                p["opposite_shares_requested"],
            )

            # Os dois workers ja estao posicionados na barreira; esta liberacao
            # faz as duas chamadas partirem juntas, tanto em LIVE quanto em SIM.
            start_barrier.wait()
            wait([f_dir, f_opp])

            e_dir = f_dir.exception()
            e_opp = f_opp.exception()

            if e_dir is None:
                r_dir = f_dir.result()
            if e_opp is None:
                r_opp = f_opp.result()

        dir_oid = (
            r_dir.get("order_id")
            if isinstance(r_dir, dict)
            else order_id_of(r_dir)
        )
        opp_oid = (
            r_opp.get("order_id")
            if isinstance(r_opp, dict)
            else order_id_of(r_opp)
        )

        p["directional_order_id"] = str(dir_oid) if dir_oid else None
        p["opposite_order_id"] = str(opp_oid) if opp_oid else None
        p["directional_order_ids"] = [str(dir_oid)] if dir_oid else []
        p["opposite_order_ids"] = [str(opp_oid)] if opp_oid else []
        p["orders_sent_at"] = now.isoformat()
        p["directional_error"] = repr(e_dir) if e_dir else rejected_reason(r_dir)
        p["opposite_error"] = repr(e_opp) if e_opp else rejected_reason(r_opp)

        log.info(
            "%s | RESULTADO ENVIO PAR | DIR order_id=%s | OPP order_id=%s | DIR_ERR=%s | OPP_ERR=%s",
            st["name"],
            p["directional_order_id"],
            p["opposite_order_id"],
            p["directional_error"],
            p["opposite_error"],
        )

        # Se as duas ordens foram aceitas, seguem GTC.
        # Se uma falhou na postagem, a outra NAO e cancelada:
        # o bot continua tentando criar a perna ausente ate o final da rodada.
        p["phase"] = "orders_active"
        save(self.s)

        if not LIVE:
            self.simulate_fill_if_marketable(p, m)
            save(self.s)

        audit({
            "type": "pair_orders_started",
            "strategy": st["name"],
            "slug": p["slug"],
            "directional_order_id": p["directional_order_id"],
            "opposite_order_id": p["opposite_order_id"],
            "directional_snapshot_price": str(dp),
            "opposite_snapshot_price": str(op),
            "limit_price": str(limit_price),
            "ts": now.isoformat(),
        })

    def retry_missing_order(self, p):
        """
        Se uma das duas ordens falhou ao ser criada, tenta manter a outra perna
        PENDENTE a 0.55 ate o final da rodada.
        """
        if not LIVE:
            return

        for leg in ("directional", "opposite"):
            if p.get(f"{leg}_order_id"):
                continue

            try:
                resp = self.place_gtc_limit(
                    p[f"{leg}_token"],
                    D(p["limit_price"]),
                    D(p[f"{leg}_shares_requested"]),
                )
                oid = order_id_of(resp)
                if oid:
                    p[f"{leg}_order_id"] = str(oid)
                    p.setdefault(f"{leg}_order_ids", []).append(str(oid))
                    p[f"{leg}_error"] = None
                    log.warning(
                        "%s | perna %s recriada e deixada GTC ate o final | order=%s",
                        p["strategy"],
                        leg,
                        oid,
                    )
            except Exception as e:
                p[f"{leg}_error"] = repr(e)

    def process_active_orders(self, st, now):
        """
        Regras definitivas depois que o par foi enviado:

        ANTES DO INICIO
          - deixa ambas GTC
          - se ambas executarem, cancela apenas eventuais restos e acompanha

        NO INICIO
          - se nenhum lado executou: cancela tudo e descarta a rodada
          - se os dois lados executaram: cancela restos e acompanha
          - se SOMENTE UM lado executou:
                cancela resto do lado que ja executou
                MANTEM/RECRIA o outro lado GTC @ <=0.55
                ate o FINAL da rodada

        DURANTE A RODADA
          - se a perna faltante executar, cancela eventual resto e acompanha
          - se nao executar, fica pendente ate o final

        NO FINAL
          - cancela qualquer saldo ainda aberto
          - contabiliza o que efetivamente executou
        """
        p = st.get("pending")
        if not p or p.get("phase") not in ("orders_active", "single_leg_recovery", "proportional_rebalance"):
            return

        round_start = datetime.fromisoformat(p["round_start"])
        round_end = datetime.fromisoformat(p["round_end"])

        if LIVE:
            self.refresh_fills(p)
        else:
            m = market(event(p["slug"]))
            if m:
                self.simulate_fill_if_marketable(p, m)

        dsh = D(p.get("directional_shares_filled", "0"))
        osh = D(p.get("opposite_shares_filled", "0"))

        # Ajuste matematico de fills parciais.
        # Ex.: 40% de uma perna => a outra passa a buscar 40% do tamanho original.
        # Se uma perna estiver 100%, a outra continua buscando 100%.
        if dsh > 0 or osh > 0:
            if self.rebalance_partial_pair(st, p):
                return

        # Se faltou criar uma ordem por erro, tenta novamente.
        if p.get("phase") == "orders_active" and (
            not p.get("directional_order_id") or not p.get("opposite_order_id")
        ):
            self.retry_missing_order(p)

        # Antes do inicio: se ambos ja tiveram fill, encerra a janela cedo.
        if now < round_start:
            if dsh > 0 and osh > 0:
                self.cancel_ids([
                    p.get("directional_order_id"),
                    p.get("opposite_order_id"),
                ])
                if LIVE:
                    self.refresh_fills(p)
                p["phase"] = "await_resolution"
                p["pair_complete"] = True
                save(self.s)
                log.info("%s | PAR EXECUTADO ANTES DO INICIO", st["name"])
            else:
                save(self.s)
            return

        # Do inicio ate o fim.
        if now < round_end:
            # Nenhum lado executou no momento em que a rodada iniciou:
            # nao queremos iniciar uma posicao depois do inicio.
            if p.get("phase") == "orders_active" and dsh <= 0 and osh <= 0:
                self.cancel_ids([
                    p.get("directional_order_id"),
                    p.get("opposite_order_id"),
                ])
                audit({
                    "type": "round_discarded_no_fill_at_start",
                    "strategy": st["name"],
                    "slug": p["slug"],
                    "ts": now.isoformat(),
                })
                log.info(
                    "%s | RODADA DESCARTADA | nenhuma perna executou ate o inicio",
                    st["name"],
                )
                st["pending"] = None
                save(self.s)
                return

            # Ambos ja executaram.
            if dsh > 0 and osh > 0:
                self.cancel_ids([
                    p.get("directional_order_id"),
                    p.get("opposite_order_id"),
                ])
                if LIVE:
                    self.refresh_fills(p)
                p["phase"] = "await_resolution"
                p["pair_complete"] = True
                save(self.s)
                log.info(
                    "%s | SEGUNDA PERNA EXECUTOU | par completo; restos cancelados",
                    st["name"],
                )
                return

            # Exatamente uma perna executou:
            # o outro lado deve continuar/restar pendente ate o FINAL.
            if (dsh > 0) ^ (osh > 0):
                filled_leg = "directional" if dsh > 0 else "opposite"
                missing_leg = "opposite" if dsh > 0 else "directional"

                # Cancela qualquer quantidade restante do lado que ja executou,
                # para nao aumentar a exposicao daquele lado.
                self.cancel_ids([p.get(f"{filled_leg}_order_id")])

                # Se a ordem do lado faltante nao existe por erro, recria.
                if not p.get(f"{missing_leg}_order_id"):
                    self.retry_missing_order(p)

                p["phase"] = "single_leg_recovery"
                p["recovery_leg"] = missing_leg
                save(self.s)

                log.warning(
                    "%s | APENAS UM LADO EXECUTOU | %s preenchido; "
                    "%s fica GTC @ %s ATE O FINAL DA RODADA",
                    st["name"],
                    filled_leg,
                    missing_leg,
                    p["limit_price"],
                )
                return

            save(self.s)
            return

        # FINAL DA RODADA: cancela tudo que ainda estiver resting.
        self.cancel_ids([
            p.get("directional_order_id"),
            p.get("opposite_order_id"),
        ])

        if LIVE:
            self.refresh_fills(p)
        else:
            m = market(event(p["slug"]))
            if m:
                self.simulate_fill_if_marketable(p, m)

        dsh = D(p.get("directional_shares_filled", "0"))
        osh = D(p.get("opposite_shares_filled", "0"))

        if dsh <= 0 and osh <= 0:
            st["pending"] = None
            save(self.s)
            return

        p["phase"] = "await_resolution"
        p["pair_complete"] = dsh > 0 and osh > 0
        p["orders_canceled_at_round_end"] = True
        save(self.s)

        audit({
            "type": "round_end_orders_closed",
            "strategy": st["name"],
            "slug": p["slug"],
            "directional_shares": str(dsh),
            "opposite_shares": str(osh),
            "pair_complete": p["pair_complete"],
            "ts": now.isoformat(),
        })

        if not p["pair_complete"]:
            log.warning(
                "%s | FINAL DA RODADA COM EXPOSICAO UNILATERAL | DIR=%s OPP=%s",
                st["name"],
                dsh,
                osh,
            )

    # ------------------------- RESOLUTION -------------------------

    def resolve(self, st):
        p = st.get("pending")
        if not p or p.get("phase") != "await_resolution":
            return

        w = winner(p["slug"])
        if not w:
            return

        direction = p["direction"]
        opposite_direction = "DOWN" if direction == "UP" else "UP"

        dir_shares = D(p.get("directional_shares_filled", "0"))
        opp_shares = D(p.get("opposite_shares_filled", "0"))
        dir_spent = D(p.get("directional_spent", "0"))
        opp_spent = D(p.get("opposite_spent", "0"))

        winning_shares = (
            dir_shares
            if w == direction
            else opp_shares
            if w == opposite_direction
            else D("0")
        )

        pnl = winning_shares - dir_spent - opp_spent
        bankroll = D(st["bankroll"]) + pnl

        st["bankroll"] = str(bankroll)
        st["realized_pnl"] = str(D(st.get("realized_pnl", "0")) + pnl)
        st["trades"] += 1

        directional_win = (w == direction)
        if directional_win:
            st["wins"] += 1
            st["loss_streak"] = 0
        else:
            st["losses"] += 1
            st["loss_streak"] += 1

        audit({
            "type": "resolution",
            "strategy": st["name"],
            "slug": p["slug"],
            "winner": w,
            "signal": direction,
            "directional_win": directional_win,
            "directional_shares": str(dir_shares),
            "opposite_shares": str(opp_shares),
            "directional_spent": str(dir_spent),
            "opposite_spent": str(opp_spent),
            "pnl": str(pnl),
            "bankroll_after": str(bankroll),
            "loss_streak_after": st["loss_streak"],
            "ts": datetime.now(UTC).isoformat(),
        })

        st["pending"] = None
        save(self.s)

        log.info(
            "%s | WINNER=%s | %s | PNL=%s | BANKROLL=%s | loss_streak=%s",
            st["name"],
            w,
            "WIN" if directional_win else "LOSS",
            pnl,
            bankroll,
            st["loss_streak"],
        )

    # ------------------------- LOOP -------------------------

    def tick(self, st, now):
        p = st.get("pending")

        if p:
            phase = p.get("phase")

            if phase == "waiting_both_prices":
                self.wait_for_both_prices(st, now)
            elif phase in ("orders_active", "single_leg_recovery", "proportional_rebalance"):
                self.process_active_orders(st, now)
            elif phase == "await_resolution":
                self.resolve(st)
            return

        if D(st["bankroll"]) >= TARGET:
            return

        _, next_start = bounds(now, TFS[st["tf"]])
        seconds_to_next = (next_start - now.astimezone(TZ)).total_seconds()

        # Captura o sinal apenas no T-30.
        if not ENTRY_SECONDS - 1.2 <= seconds_to_next <= ENTRY_SECONDS + 0.8:
            return

        if not session_allows_round(st, next_start):
            return

        key = next_start.astimezone(UTC).isoformat()
        if st["last_trigger"] == key:
            return

        st["last_trigger"] = key
        save(self.s)

        log.info(
            "%s | JANELA T-%ss ATINGIDA | proxima rodada=%s | avaliando candle+MACD",
            st["name"],
            ENTRY_SECONDS,
            next_start.isoformat(),
        )

        direction, two_same, macd, sig, dirs = trading_signal(st["tf"])

        if not direction:
            log.info(
                "%s | SEM ENTRADA: candle fechado e MACD nao alinhados | "
                "dirs=%s | macd=%s sig=%s",
                st["name"],
                dirs,
                macd,
                sig,
            )
            return

        if st["loss_streak"] > 0 and not two_same:
            log.info(
                "%s | MARTINGALE AGUARDA 2 CANDLES %s + MACD | dirs=%s",
                st["name"],
                direction,
                dirs,
            )
            return

        log.info(
            "%s | SINAL APROVADO | direction=%s | dirs=%s | macd=%s | signal=%s | loss_streak=%s",
            st["name"],
            direction,
            dirs,
            macd,
            sig,
            st["loss_streak"],
        )
        self.prepare_entry_window(st, next_start, direction)

    def run(self):
        log.info("STARTUP OK | codigo carregado | versao=14 | ENTRY_SECONDS=%s", ENTRY_SECONDS)
        log.info(
            "POLYMARKET BTC V14 FINAL | LIVE=%s | 6 ROBOS | "
            "BANKROLL_INICIAL=%s | MACD 7/21/9 | "
            "SINAL T-%ss | SO ENVIA SE AMBOS<=%s ANTES DO INICIO | "
            "PARTIAL=PROPORCIONAL | SE 1 FULL: OUTRO 100%% ATE FINAL | "
            "SAQUES=AUTO_PROPORCIONAL | TARGET=%s | DATA=%s",
            LIVE,
            INITIAL,
            ENTRY_SECONDS,
            MAX_BUY_PRICE,
            TARGET,
            ROOT,
        )

        hb = 0

        # Primeira consulta logo apos o startup; baseline impede desconto retroativo.
        self.sync_withdrawals(force=True)

        while not STOP:
            now = datetime.now(UTC)

            self.sync_withdrawals()

            for st in self.s["strategies"].values():
                try:
                    self.tick(st, now)
                except Exception:
                    log.exception("%s | erro no tick", st["name"])

            if time.time() - hb > 30:
                summary = " | ".join(
                    f'{st["name"]}:bank={st["bankroll"]},'
                    f'L={st["loss_streak"]},'
                    f'phase={(st.get("pending") or {}).get("phase","-")}'
                    for st in self.s["strategies"].values()
                )
                recon = self.s.get("capital_reconciliation", {})
                log.info(
                    "HEARTBEAT | LIVE=%s | withdrawn_applied=%s | %s",
                    LIVE,
                    recon.get("total_withdrawn_applied", "0"),
                    summary,
                )
                hb = time.time()

            time.sleep(POLL_SECONDS)

    def close(self):
        # Ao encerrar o processo, cancela ordens GTC conhecidas.
        if self.c:
            ids = []
            for st in self.s["strategies"].values():
                p = st.get("pending")
                if p and p.get("phase") in ("orders_active", "single_leg_recovery", "proportional_rebalance"):
                    ids.extend([
                        p.get("directional_order_id"),
                        p.get("opposite_order_id"),
                    ])
            try:
                self.cancel_ids(ids)
            finally:
                self.c.close()


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
        bot.close()
