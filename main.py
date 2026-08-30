#!/usr/bin/env python3
# V37 - MINIMO CLOB DINAMICO + RD/STOP LOSS EXPLICITO + PAR ATE DEFICIT >= USD1 + MACD LIVE
# - inicia os 6 robos em bankroll 12, loss_streak 0 e recovery_deficit 0
# - zera estatisticas logicas antigas (wins/losses/trades/realized_pnl/last_trigger)
# - reset e aplicado uma unica vez e somente sem pending ativo
# - mantem a confirmacao V33: MACD fechado + MACD ao vivo fortalecendo a direcao
# - V36 registra PNL/STOP_LOSS/RD explicitamente e usa PNL liquido para loss_streak/martingale
import os
import sys
import json
import time
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
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Barrier, Thread
from zoneinfo import ZoneInfo
from urllib.request import Request, urlopen
from urllib.parse import urlencode

# ============================================================
# POLYMARKET BTC V42 FINAL - ENTRADAS SOBREPOSTAS + MINIMO CLOB DINAMICO + RD/STOP LOSS + MACD AO VIVO + RESET V34 PRESERVADO
#
# 6 robos logicos independentes:
#   5m / 15m / 1h x 24h / 10:00-16:00 Brasilia
#
# SINAL
#   - entrada T-30s para a PROXIMA rodada
#   - candle ATUAL ainda ABERTO no T-30 + MACD 7/21/9 na mesma direcao
#   - depois de LOSS: exige candle ANTERIOR FECHADO + candle ATUAL ABERTO
#     na mesma direcao + MACD alinhado
#
# EXECUCAO
#   - modo PAR enquanto recovery_deficit < US$1,00
#   - modo DIRECIONAL-ONLY somente quando recovery_deficit >= US$1,00
#   - SEM fallback para DIRECIONAL-ONLY por falta de caixa: abaixo de US$1 de
#     deficit, se o PAR nao couber no bankroll, a rodada e bloqueada
#   - nenhuma ordem e enviada acima de 0.55
#   - no modo PAR, duas BUY LIMIT GTC, uma em cada outcome
#   - no modo DIRECIONAL-ONLY, envia somente a BUY da direcao do sinal
#   - no inicio da rodada, todo saldo ainda aberto e cancelado
#   - se nao houve fill ate o inicio, a rodada e descartada
#
# SIZING V29
#   - caixa inicial independente: US$12
#   - modo PAR: lado oposto = minimo nominal de US$1,00
#   - modo DIRECIONAL-ONLY: apenas a perna do sinal, minimo nominal de US$1,00
#   - lucro-base liquido: 5m=US$0.25 | 15m=US$0.50 | 1h=US$0.75
#   - recovery_deficit acumula todo PNL negativo ainda nao recuperado
#   - proxima meta liquida = recovery_deficit + lucro-base do timeframe
#   - PNL positivo abate o deficit; o deficit nunca fica negativo
#   - sizing usa US$1 na protecao e calcula a direcional para garantir a meta liquida no limite, assumindo fill integral
#   - maximo de US$1000 de gasto teorico por perna
#   - objetivo: US$200,000 por robo
#
#
# RESET UNICO V32
#   - no primeiro startup desta versao, redefine os 6 robos para US$12
#   - zera loss_streak e recovery_deficit de cada robo
#   - preserva historico, pending, reconciliacao de saques e resgates
#   - grava marcador persistente; reinicios/redeploys seguintes NAO resetam novamente
#
# FILTRO DE SESSAO / NOTICIAS V31
#   - robos *_day: somente segunda a sexta, 10:00 <= inicio da rodada < 16:00 Brasilia
#   - robos 24h continuam 7 dias por semana
#   - Trading Economics = fonte primaria quando TRADING_ECONOMICS_API_KEY estiver configurada
#   - Investing.com = fallback automatico
#   - ultimo calendario valido salvo persistentemente em /data/news_calendar_cache.json
#   - busca 7 dias adiante e atualiza em thread separada, nunca no gatilho T-30
#   - 5m/15m: bloqueio T-15min ate T+15min da noticia
#   - 1h: bloqueio T-60min ate T+60min da noticia
#   - falha temporaria das fontes NAO trava entrada: usa o cache valido
#   - cache muito antigo gera alerta; por padrao continua operando (fail-open)
# RECONCILIACAO DE SAQUES
#   - consulta a atividade WITHDRAWAL da propria carteira Polymarket
#   - aplica somente saques posteriores ao primeiro startup da V14
#   - cada saque reduz proporcionalmente os 6 bankrolls logicos
#   - trades, fills, ordens abertas, splits, merges e redeems NAO contam como saque
#   - eventos ja processados sao persistidos para nunca descontar duas vezes
#
# RESGATE AUTOMATICO V25
#   - quando a rodada resolve e ha shares vencedoras, enfileira condition_id
#   - setup_trading_approvals habilita o auto-redeem operator oficial
#   - nao envia redeem_positions()/POST /submit manualmente
#   - reconcilia ate a posicao desaparecer em 2 verificacoes
#   - evita resgate duplicado do mesmo condition_id
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

from polymarket import SecureClient, BuilderApiKey, RelayerApiKey  # noqa: E402


TZ = ZoneInfo("America/Sao_Paulo")
ET = ZoneInfo("America/New_York")
UTC = timezone.utc

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
BINANCE = "https://api.binance.com"

PK = (os.getenv("POLYMARKET_PRIVATE_KEY") or os.getenv("PRIVATE_KEY") or "").strip()
WALLET = os.getenv("POLYMARKET_DEPOSIT_WALLET", "").strip()
LIVE = os.getenv("LIVE_TRADING", "0").lower() in ("1", "true", "yes", "on")

# Credencial opcional/necessaria para operacoes gasless (Deposit Wallet).
# Prioridade: RELAYER, se os 2 campos existirem; caso contrario BUILDER,
# se os 3 campos existirem. Nenhum segredo e impresso nos logs.
RELAYER_API_KEY = os.getenv("POLYMARKET_RELAYER_API_KEY", "").strip()
RELAYER_API_KEY_ADDRESS = os.getenv("POLYMARKET_RELAYER_API_KEY_ADDRESS", "").strip()

BUILDER_API_KEY = os.getenv("POLYMARKET_BUILDER_API_KEY", "").strip()
BUILDER_SECRET = os.getenv("POLYMARKET_BUILDER_SECRET", "").strip()
BUILDER_PASSPHRASE = os.getenv("POLYMARKET_BUILDER_PASSPHRASE", "").strip()

INITIAL = Decimal(os.getenv("INITIAL_BANKROLL", "12.00"))
EDGE_5M = Decimal("0.25")
EDGE_15M = Decimal("0.50")
EDGE_1H = Decimal("0.75")
BASE_EDGE_BY_TF = {"5m": EDGE_5M, "15m": EDGE_15M, "1h": EDGE_1H}
# Compatibilidade nominal com estados/logs antigos; sizing V25 usa BASE_EDGE_BY_TF.
INITIAL_EDGE = EDGE_5M
PROFIT_SWITCH = Decimal(os.getenv("PROFIT_SWITCH", "5.00"))
HIGH_BANKROLL_EDGE = Decimal(os.getenv("HIGH_BANKROLL_EDGE", "10.00"))
MAX_ENTRY = Decimal(os.getenv("MAX_ENTRY", "1000.00"))
TARGET = Decimal(os.getenv("TARGET_BANKROLL", "200000.00"))
WITHDRAWAL_SYNC_SECONDS = float(os.getenv("WITHDRAWAL_SYNC_SECONDS", "20"))
BALANCE_SYNC_SECONDS = float(os.getenv("BALANCE_SYNC_SECONDS", "30"))

ENTRY_SECONDS = 30  # FIXO: trava o sinal 30s antes da proxima rodada
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "0.5"))
MAX_BUY_PRICE = Decimal(os.getenv("MAX_BUY_PRICE", "0.55"))
MIN_LEG_USD = Decimal(os.getenv("MIN_LEG_USD", "1.00"))  # minimo nominal por ponta

# V42 - previa probabilistica da rodada anterior no T-30 da proxima.
# O forecast NUNCA altera o RD contabil oficial; ele apenas cria um RD projetado
# para o sizing da nova entrada. A resolucao oficial continua sendo soberana.
PROB_PREVIEW_ENABLED = os.getenv("PROB_PREVIEW_ENABLED", "1").lower() in ("1", "true", "yes", "on")
PROB_PREVIEW_MIN_CONFIDENCE = float(os.getenv("PROB_PREVIEW_MIN_CONFIDENCE", "0.85"))
PROB_PREVIEW_MAX_SECONDS_TO_END = float(os.getenv("PROB_PREVIEW_MAX_SECONDS_TO_END", "45"))
PROB_PREVIEW_VOL_LOOKBACK_MIN = int(os.getenv("PROB_PREVIEW_VOL_LOOKBACK_MIN", "60"))
PROB_PREVIEW_MIN_SIGMA_1M = float(os.getenv("PROB_PREVIEW_MIN_SIGMA_1M", "0.00005"))

# V31 - calendario economico resiliente.
NEWS_FILTER_ENABLED = os.getenv("NEWS_FILTER_ENABLED", "1").lower() in ("1", "true", "yes", "on")
NEWS_REFRESH_SECONDS = float(os.getenv("NEWS_REFRESH_SECONDS", "10800"))  # 3 horas
NEWS_LOOKAHEAD_DAYS = int(os.getenv("NEWS_LOOKAHEAD_DAYS", "7"))
NEWS_MAX_STALE_SECONDS = float(os.getenv("NEWS_MAX_STALE_SECONDS", "172800"))  # 48 horas
NEWS_FAIL_CLOSED = os.getenv("NEWS_FAIL_CLOSED", "0").lower() in ("1", "true", "yes", "on")
NEWS_WINDOW_SHORT_MIN = int(os.getenv("NEWS_WINDOW_SHORT_MIN", "15"))
NEWS_WINDOW_1H_MIN = int(os.getenv("NEWS_WINDOW_1H_MIN", "60"))
TRADING_ECONOMICS_API_KEY = os.getenv("TRADING_ECONOMICS_API_KEY", "").strip()
TRADING_ECONOMICS_BASE = "https://api.tradingeconomics.com"
INVESTING_CALENDAR_URL = "https://www.investing.com/economic-calendar/Service/getCalendarFilteredData"

TFS = {"5m": 5, "15m": 15, "1h": 60}

DEFAULT_ROOT = Path("/data") if Path("/data").exists() else Path(__file__).resolve().parent
ROOT = Path(os.getenv("BOT_DIR", str(DEFAULT_ROOT))).resolve()
ROOT.mkdir(parents=True, exist_ok=True)
STATE = ROOT / "state.json"
TRADES = ROOT / "trades.jsonl"
NEWS_CACHE = ROOT / "news_calendar_cache.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("btc-polymarket-v38")
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


def get(url, params=None):
    if params:
        url += "?" + urlencode(params)
    with urlopen(
        Request(url, headers={"User-Agent": "btc-polymarket-v28"}),
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
        "version": 39,
        "strategies": {},
        "maintenance": {
            "applied_resets": [],
            "last_reset": None,
        },
        "capital_reconciliation": {
            "initialized": False,
            "baseline_epoch": 0,
            "last_success_epoch": 0,
            "processed_withdrawals": [],
            "total_withdrawn_applied": "0",
            "last_withdrawal": None,
        },
        "redemption_reconciliation": {
            "processed_condition_ids": [],
            "queue": [],
            "last_redeem": None,
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
                "martingale_base_edge": None,  # legado; V25 usa recovery_deficit
                "recovery_deficit": "0",
                "wins": 0,
                "losses": 0,
                "trades": 0,
                "realized_pnl": "0",
                "last_pnl": "0",
                "last_stop_loss": "0",
                "last_result": "NONE",
                "last_trigger": "",
                "pending": None,
                "open_positions": [],
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
                "martingale_base_edge",
                "recovery_deficit",
                "wins",
                "losses",
                "trades",
                "realized_pnl",
                "last_pnl",
                "last_stop_loss",
                "last_result",
                "last_trigger",
                "pending",
                "open_positions",
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

            # V38: posicoes ja iniciadas e aguardando resolucao ficam separadas
            # do unico slot operacional usado para preparar/enviar a proxima rodada.
            ops = dst.get("open_positions")
            if not isinstance(ops, list):
                dst["open_positions"] = []
            else:
                dst["open_positions"] = [x for x in ops if isinstance(x, dict)]

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

        redemption = old.get("redemption_reconciliation")
        if isinstance(redemption, dict):
            dst_redemption = new["redemption_reconciliation"]
            for field in ("processed_condition_ids", "queue", "last_redeem"):
                if field in redemption:
                    dst_redemption[field] = redemption[field]

        maintenance = old.get("maintenance")
        if isinstance(maintenance, dict):
            dst_maintenance = new["maintenance"]
            for field in ("applied_resets", "last_reset"):
                if field in maintenance:
                    dst_maintenance[field] = maintenance[field]

        # Migracao V25: cria um deficit financeiro persistente para cada robo.
        # Se o estado antigo ainda nao tinha esse campo, tenta reconstruir o
        # drawdown nao recuperado a partir das resolucoes auditadas em trades.jsonl.
        # Regra: LOSS adiciona abs(PNL); WIN financeiro abate o deficit; nunca < 0.
        old_strategies = old.get("strategies", {}) if isinstance(old, dict) else {}
        needs_rebuild = {
            name for name in new["strategies"]
            if "recovery_deficit" not in (old_strategies.get(name) or {})
        }
        rebuilt = {name: D("0") for name in needs_rebuild}
        if needs_rebuild and TRADES.exists():
            try:
                for line in TRADES.read_text().splitlines():
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    if rec.get("type") != "resolution":
                        continue
                    name = str(rec.get("strategy") or "")
                    if name not in rebuilt:
                        continue
                    pnl = D(rec.get("pnl") or "0")
                    if pnl < 0:
                        rebuilt[name] += -pnl
                    elif pnl > 0:
                        rebuilt[name] = max(D("0"), rebuilt[name] - pnl)
            except Exception:
                log.exception("MIGRACAO V30 | falha ao reconstruir recovery_deficit pelo audit")

        for name in needs_rebuild:
            st = new["strategies"][name]
            deficit = rebuilt.get(name, D("0"))
            # Fallback conservador quando nao existe audit persistente: se o PnL
            # logico acumulado do robo estiver negativo, pelo menos esse valor e
            # carregado como deficit.
            if deficit <= 0:
                try:
                    rpnl = D(st.get("realized_pnl", "0"))
                    if rpnl < 0:
                        deficit = -rpnl
                except Exception:
                    pass
            st["recovery_deficit"] = str(max(D("0"), deficit))
            st["martingale_base_edge"] = None

        new["version"] = 39
        save(new)
        return new
    except Exception:
        log.exception("state.json invalido; criando estado novo")
        s = fresh()
        save(s)
        return s


ONE_TIME_RESET_ID = "v34_total_fresh_start_live_macd"


def apply_one_time_all_robots_reset(s):
    """
    V34: reset TOTAL e unico dos seis robos para iniciar a nova metrica V33/V34
    sem carregar qualquer martingale/deficit/estatistica logica anterior.

    O reset so e aplicado quando NAO existe pending em nenhum robo. Isso evita
    apagar o acompanhamento de uma ordem/posicao real ainda aberta. Se houver
    pending, o reset fica adiado e sera tentado novamente no proximo ciclo/startup.

    Resetado para cada robo:
      - bankroll = INITIAL (12 USD)
      - loss_streak = 0
      - recovery_deficit = 0
      - martingale_base_edge = None
      - wins/losses/trades = 0
      - realized_pnl = 0
      - last_trigger = ""
      - pending = None (somente porque o reset exige nenhum pending ativo)

    Preservado: reconciliacao de saques, reconciliacao de resgates, cache de news
    e o arquivo de auditoria trades.jsonl. O historico fica apenas como auditoria;
    ele NAO alimenta o novo martingale porque recovery_deficit passa a existir = 0.
    """
    maintenance = s.setdefault("maintenance", {"applied_resets": [], "last_reset": None})
    applied = maintenance.setdefault("applied_resets", [])
    if ONE_TIME_RESET_ID in applied:
        return False

    active_pending = [
        name for name, st in s.get("strategies", {}).items()
        if isinstance(st.get("pending"), dict) or bool(st.get("open_positions"))
    ]
    if active_pending:
        log.warning(
            "RESET V34 TOTAL ADIADO | existem pending/posicoes abertas=%s | reset sera aplicado quando todos encerrarem",
            ",".join(sorted(active_pending)),
        )
        return False

    before = {}
    for name, st in s.get("strategies", {}).items():
        before[name] = {
            "bankroll": str(st.get("bankroll", "0")),
            "loss_streak": int(st.get("loss_streak", 0) or 0),
            "recovery_deficit": str(st.get("recovery_deficit", "0") or "0"),
            "wins": int(st.get("wins", 0) or 0),
            "losses": int(st.get("losses", 0) or 0),
            "trades": int(st.get("trades", 0) or 0),
            "realized_pnl": str(st.get("realized_pnl", "0") or "0"),
            "last_trigger": str(st.get("last_trigger", "") or ""),
        }
        st["bankroll"] = str(INITIAL)
        st["loss_streak"] = 0
        st["recovery_deficit"] = "0"
        st["martingale_base_edge"] = None
        st["wins"] = 0
        st["losses"] = 0
        st["trades"] = 0
        st["realized_pnl"] = "0"
        st["last_pnl"] = "0"
        st["last_stop_loss"] = "0"
        st["last_result"] = "NONE"
        st["last_trigger"] = ""
        st["pending"] = None
        st["open_positions"] = []

    ts = datetime.now(UTC).isoformat()
    applied.append(ONE_TIME_RESET_ID)
    maintenance["last_reset"] = {
        "id": ONE_TIME_RESET_ID,
        "timestamp": ts,
        "bankroll_each": str(INITIAL),
        "robots": sorted(s.get("strategies", {}).keys()),
        "reset_scope": "TOTAL_LOGICAL_STRATEGY_STATE",
        "before": before,
    }
    s["version"] = 39
    save(s)

    audit({
        "type": "maintenance_reset",
        "id": ONE_TIME_RESET_ID,
        "timestamp": ts,
        "bankroll_each": str(INITIAL),
        "robots": sorted(s.get("strategies", {}).keys()),
        "reset_scope": "TOTAL_LOGICAL_STRATEGY_STATE",
        "before": before,
    })
    log.warning(
        "RESET V34 TOTAL APLICADO UMA UNICA VEZ | robos=%s | bankroll_cada=%s | "
        "loss_streak=0 | recovery_deficit=0 | wins=0 | losses=0 | trades=0 | realized_pnl=0 | marcador=%s",
        len(s.get("strategies", {})), INITIAL, ONE_TIME_RESET_ID,
    )
    return True


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
    V33 - direcao pelo candle ATUAL aberto + confirmacao MACD ao vivo.

    O MACD nao e uma cotacao de BTC e, portanto, nao faz sentido comparar
    diretamente preco BTC (ex.: 60.000) com valor MACD (ex.: -25).

    Para impedir entrada com MACD "atrasado" durante uma reversao, V33 usa
    duas leituras:

      1) MACD FECHADO: 7/21/9 somente com candles integralmente fechados.
      2) MACD AO VIVO: o mesmo 7/21/9, acrescentando a cotacao atual do
         candle em formacao como ultimo ponto.

    Entrada UP somente quando:
      - candle atual esta UP;
      - MACD fechado esta UP (macd > signal e macd > 0);
      - MACD ao vivo continua UP;
      - a cotacao atual FORTALECE o MACD: live_macd > closed_macd e
        live_hist > closed_hist.

    Entrada DOWN somente quando:
      - candle atual esta DOWN;
      - MACD fechado esta DOWN (macd < signal e macd < 0);
      - MACD ao vivo continua DOWN;
      - a cotacao atual FORTALECE o MACD: live_macd < closed_macd e
        live_hist < closed_hist.

    Assim, se o MACD fechado ainda disser DOWN mas a cotacao estiver
    revertendo para cima e enfraquecendo o movimento baixista, NAO entra.
    O inverso vale para UP.

    Apos loss, permanece a confirmacao adicional: candle anterior fechado
    + candle atual aberto precisam estar na mesma direcao aprovada.
    """
    rows = get(
        BINANCE + "/api/v3/klines",
        {"symbol": "BTCUSDT", "interval": tf, "limit": 120},
    )
    now_ms = int(time.time() * 1000)
    closed = [r for r in rows if int(r[6]) < now_ms]
    current = [r for r in rows if int(r[0]) <= now_ms <= int(r[6])]
    if len(closed) < 30 or not current:
        return None, False, None, None, [], None, None, None, None

    current_row = current[-1]
    previous_closed = closed[-1]

    closes = [float(r[4]) for r in closed]

    # MACD fechado: referencia estrutural, sem contaminar com candle aberto.
    fast = ema(closes, 7)
    slow = ema(closes, 21)
    macd_series = [a - b for a, b in zip(fast, slow)]
    signal_series = ema(macd_series, 9)
    m, sig = macd_series[-1], signal_series[-1]
    closed_hist = m - sig

    # MACD ao vivo: incorpora a cotacao atual do candle em formacao.
    current_price = float(current_row[4])
    live_closes = closes + [current_price]
    live_fast = ema(live_closes, 7)
    live_slow = ema(live_closes, 21)
    live_macd_series = [a - b for a, b in zip(live_fast, live_slow)]
    live_signal_series = ema(live_macd_series, 9)
    live_m, live_sig = live_macd_series[-1], live_signal_series[-1]
    live_hist = live_m - live_sig

    def candle_dir(r):
        o = float(r[1])
        c = float(r[4])
        if c > o:
            return "UP"
        if c < o:
            return "DOWN"
        return None

    previous_dir = candle_dir(previous_closed)
    current_dir = candle_dir(current_row)
    dirs = [previous_dir, current_dir]

    closed_macd_dir = (
        "UP"
        if m > sig and m > 0
        else "DOWN"
        if m < sig and m < 0
        else None
    )
    live_macd_dir = (
        "UP"
        if live_m > live_sig and live_m > 0
        else "DOWN"
        if live_m < live_sig and live_m < 0
        else None
    )

    strengthening = False
    if current_dir == "UP" and closed_macd_dir == "UP" and live_macd_dir == "UP":
        strengthening = live_m > m and live_hist > closed_hist
    elif current_dir == "DOWN" and closed_macd_dir == "DOWN" and live_macd_dir == "DOWN":
        strengthening = live_m < m and live_hist < closed_hist

    direction = current_dir if strengthening else None

    recovery_confirmed = (
        direction is not None
        and previous_dir == direction
        and current_dir == direction
    )

    return (
        direction,
        recovery_confirmed,
        m,
        sig,
        dirs,
        live_m,
        live_sig,
        closed_hist,
        live_hist,
    )


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


def clob_constraints(condition_id, gamma_min_shares=Decimal("0"), gamma_tick=Decimal("0.01")):
    """
    V37: consulta o CLOB oficial imediatamente antes do sizing/envio.

    O endpoint /clob-markets/{condition_id} devolve:
      mos = minimum order size EM SHARES
      mts = minimum tick size

    Usa o maior minimo entre CLOB e Gamma. Se a consulta CLOB falhar,
    conserva o valor Gamma. Isso evita enviar uma ordem abaixo do minimo
    reportado pelo proprio CLOB, como `Size (1.96) lower than the minimum: 5`.
    """
    gamma_min_shares = max(D("0"), D(gamma_min_shares or 0))
    gamma_tick = D(gamma_tick or "0.01")
    if gamma_tick <= 0:
        gamma_tick = D("0.01")

    result = {
        "minimum_order_shares": gamma_min_shares,
        "tick_size": gamma_tick,
        "source": "GAMMA",
        "clob_min_shares": D("0"),
        "gamma_min_shares": gamma_min_shares,
    }

    if not condition_id:
        return result

    try:
        info = get(CLOB + "/clob-markets/" + str(condition_id))
        clob_min = D(info.get("mos") or 0)
        clob_tick = D(info.get("mts") or gamma_tick)
        if clob_tick <= 0:
            clob_tick = gamma_tick
        result["clob_min_shares"] = clob_min
        result["minimum_order_shares"] = max(gamma_min_shares, clob_min)
        result["tick_size"] = clob_tick
        result["source"] = "CLOB+GAMMA"
    except Exception as exc:
        log.warning(
            "CLOB CONSTRAINTS V37 | falha condition_id=%s | usando Gamma min_shares=%s tick=%s | err=%r",
            condition_id, gamma_min_shares, gamma_tick, exc,
        )

    return result


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


def winner_for_position(p):
    """
    V40: detecta o vencedor por múltiplas fontes e persiste o resultado na posição.
    A detecção pode ocorrer fora de ordem; a contabilização financeira continua FIFO.
    """
    cached = str((p or {}).get("resolved_winner") or "").upper()
    if cached in ("UP", "DOWN"):
        return cached

    slug = str((p or {}).get("slug") or "")
    if slug:
        try:
            w = winner(slug)
            if w in ("UP", "DOWN"):
                p["resolved_winner"] = w
                p["resolved_winner_source"] = "GAMMA"
                p["resolved_winner_at"] = datetime.now(UTC).isoformat()
                return w
        except Exception:
            pass

    condition_id = str((p or {}).get("condition_id") or "").strip()
    if not condition_id:
        return None

    # Parsing defensivo de respostas CLOB que exponham tokens/outcomes vencedores.
    for path in (f"/clob-markets/{condition_id}", f"/markets/{condition_id}"):
        try:
            info = get(CLOB + path)
        except Exception:
            continue

        candidates = []
        if isinstance(info, dict):
            for key in ("tokens", "outcomes"):
                val = info.get(key)
                if isinstance(val, list):
                    candidates.extend(val)

        for tok in candidates:
            if not isinstance(tok, dict):
                continue
            flag = tok.get("winner")
            if flag is not True and str(flag).lower() not in ("true", "1"):
                continue
            out = str(tok.get("outcome") or tok.get("name") or "").upper()
            if out in ("UP", "YES"):
                w = "UP"
            elif out in ("DOWN", "NO"):
                w = "DOWN"
            else:
                continue
            p["resolved_winner"] = w
            p["resolved_winner_source"] = "CLOB"
            p["resolved_winner_at"] = datetime.now(UTC).isoformat()
            return w

    return None


def session_allows_round(st, round_start):
    """V31: sessao day opera somente seg-sex, 10:00-16:00 Brasilia."""
    if st["session"] == "24h":
        return True
    local = round_start.astimezone(TZ)
    if local.weekday() >= 5:  # 5=sabado, 6=domingo
        return False
    return 10 <= local.hour < 16


def _strip_html(value):
    value = re.sub(r"<[^>]+>", " ", str(value or ""))
    value = html_lib.unescape(value)
    return " ".join(value.split())


def _parse_calendar_datetime(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        pass
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=UTC)
        except ValueError:
            pass
    return None


def _dedupe_news_events(events):
    unique = {}
    for ev in events:
        dt = ev.get("time_utc")
        if not isinstance(dt, datetime):
            continue
        dt = dt.astimezone(UTC).replace(microsecond=0)
        name = " ".join(str(ev.get("name") or "US high-impact event").split())
        unique[(dt.isoformat(), name.lower())] = {
            "time_utc": dt,
            "name": name,
            "source": str(ev.get("source") or "unknown"),
        }
    return sorted(unique.values(), key=lambda e: e["time_utc"])


def fetch_tradingeconomics_us_high_impact_calendar():
    """Fonte primaria: EUA + Importance=3."""
    if not TRADING_ECONOMICS_API_KEY:
        raise RuntimeError("TRADING_ECONOMICS_API_KEY nao configurada")
    now = datetime.now(UTC)
    date_from = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    date_to = (now + timedelta(days=max(1, NEWS_LOOKAHEAD_DAYS))).strftime("%Y-%m-%d")
    query = urlencode({"c": TRADING_ECONOMICS_API_KEY, "importance": "3", "f": "json"})
    url = f"{TRADING_ECONOMICS_BASE}/calendar/country/united%20states/{date_from}/{date_to}?{query}"
    req = Request(url, headers={"User-Agent": "btc-polymarket-v31/1.0", "Accept": "application/json"})
    with urlopen(req, timeout=15) as r:
        obj = json.loads(r.read().decode("utf-8", errors="replace"))
    if not isinstance(obj, list):
        raise RuntimeError("Trading Economics respondeu formato inesperado")
    events = []
    for item in obj:
        if not isinstance(item, dict):
            continue
        try:
            importance = int(item.get("Importance") or 0)
        except Exception:
            importance = 0
        country = str(item.get("Country") or item.get("OCountry") or "").lower().strip()
        if importance != 3 or country not in ("united states", "estados unidos"):
            continue
        dt = _parse_calendar_datetime(item.get("Date"))
        if dt is None:
            continue
        name = str(item.get("Event") or item.get("Category") or "US high-impact event").strip()
        events.append({"time_utc": dt, "name": name, "source": "TradingEconomics"})
    return _dedupe_news_events(events)


def fetch_investing_us_high_impact_calendar():
    """Fallback: Investing.com EUA + importancia 3."""
    now = datetime.now(UTC)
    date_from = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    date_to = (now + timedelta(days=max(1, NEWS_LOOKAHEAD_DAYS))).strftime("%Y-%m-%d")
    payload = urlencode([
        ("country[]", "5"), ("importance[]", "3"),
        ("dateFrom", date_from), ("dateTo", date_to),
        ("timeZone", "56"), ("timeFilter", "timeOnly"),
        ("currentTab", "custom"), ("submitFilters", "1"), ("limit_from", "0"),
    ]).encode("utf-8")
    req = Request(
        INVESTING_CALENDAR_URL, data=payload,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json,text/plain,*/*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": "https://www.investing.com/economic-calendar/",
        }, method="POST",
    )
    with urlopen(req, timeout=15) as r:
        obj = json.loads(r.read().decode("utf-8", errors="replace"))
    calendar_html = str(obj.get("data") or "") if isinstance(obj, dict) else ""
    if not calendar_html:
        raise RuntimeError("Investing calendar respondeu sem campo data")
    rows = re.findall(r"<tr\b(?=[^>]*\bjs-event-item\b)[^>]*>.*?</tr>", calendar_html, flags=re.I | re.S)
    events = []
    for row in rows:
        mdt = re.search(r'data-event-datetime=["\']([^"\']+)["\']', row, flags=re.I)
        if not mdt:
            continue
        dt = _parse_calendar_datetime(mdt.group(1))
        if dt is None:
            continue
        mev = re.search(r'<td[^>]*class=["\'][^"\']*\bevent\b[^"\']*["\'][^>]*>(.*?)</td>',
                        row, flags=re.I | re.S)
        name = _strip_html(mev.group(1) if mev else "US high-impact event") or "US high-impact event"
        events.append({"time_utc": dt, "name": name, "source": "Investing.com"})
    if rows and not events:
        raise RuntimeError("Investing calendar mudou o formato")
    return _dedupe_news_events(events)


def save_news_cache(events, source):
    payload = {
        "version": 31,
        "saved_at_utc": datetime.now(UTC).isoformat(),
        "source": source,
        "events": [{
            "time_utc": e["time_utc"].astimezone(UTC).isoformat(),
            "name": e.get("name", "US high-impact event"),
            "source": e.get("source", source),
        } for e in events if isinstance(e.get("time_utc"), datetime)],
    }
    tmp = NEWS_CACHE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(NEWS_CACHE)


def load_news_cache():
    if not NEWS_CACHE.exists():
        return [], 0.0, "", "cache inexistente"
    try:
        obj = json.loads(NEWS_CACHE.read_text(encoding="utf-8"))
        saved = datetime.fromisoformat(str(obj.get("saved_at_utc", "")).replace("Z", "+00:00"))
        if saved.tzinfo is None:
            saved = saved.replace(tzinfo=UTC)
        events = []
        for item in obj.get("events", []):
            if not isinstance(item, dict):
                continue
            dt = _parse_calendar_datetime(item.get("time_utc"))
            if dt:
                events.append({
                    "time_utc": dt,
                    "name": str(item.get("name") or "US high-impact event"),
                    "source": str(item.get("source") or obj.get("source") or "cache"),
                })
        return _dedupe_news_events(events), saved.timestamp(), str(obj.get("source") or "cache"), ""
    except Exception as exc:
        return [], 0.0, "", repr(exc)


def fetch_us_high_impact_calendar_resilient():
    errors = []
    if TRADING_ECONOMICS_API_KEY:
        try:
            return fetch_tradingeconomics_us_high_impact_calendar(), "TradingEconomics", errors
        except Exception as exc:
            errors.append(f"TradingEconomics={exc!r}")
    else:
        errors.append("TradingEconomics=SEM_API_KEY")
    try:
        return fetch_investing_us_high_impact_calendar(), "Investing.com", errors
    except Exception as exc:
        errors.append(f"Investing.com={exc!r}")
    raise RuntimeError(" | ".join(errors))


def base_edge(st):
    """V29: diferencial/lucro-base fixo por timeframe."""
    return BASE_EDGE_BY_TF.get(st.get("tf"), EDGE_5M)


def recovery_target(st, deficit_override=None):
    """
    Meta liquida V42.

    Sem override, usa o recovery_deficit OFICIAL persistido.
    Com deficit_override, usa apenas para o sizing provisório da entrada atual;
    isso NAO modifica o RD real e NAO antecipa contabilizacao de resultado.
    """
    base = D(base_edge(st))
    if deficit_override is None:
        deficit = max(D("0"), D(st.get("recovery_deficit", "0") or "0"))
    else:
        deficit = max(D("0"), D(deficit_override or "0"))
    return base, deficit, deficit + base


def projected_position_pnl(pos, predicted_winner):
    """PNL que a posicao teria se o winner previsto fosse o winner oficial."""
    direction = str(pos.get("direction") or "").upper()
    opposite = "DOWN" if direction == "UP" else "UP"
    w = str(predicted_winner or "").upper()
    dsh = D(pos.get("directional_shares_filled", "0") or "0")
    osh = D(pos.get("opposite_shares_filled", "0") or "0")
    dsp = D(pos.get("directional_spent", "0") or "0")
    osp = D(pos.get("opposite_spent", "0") or "0")
    winning = dsh if w == direction else osh if w == opposite else D("0")
    return winning - dsp - osp


def binance_round_resolution_probability(tf, round_start, round_end):
    """
    V42: estima P(UP) da rodada usando distancia do BTC ao preco de abertura
    e volatilidade realizada de retornos de 1 minuto. Modelo sem drift:
      log(P_final/P_atual) ~ N(0, sigma_1m^2 * segundos_restantes/60).

    E uma estimativa probabilistica, nao uma resolucao oficial.
    """
    now = datetime.now(UTC)
    seconds_left = max(0.0, (round_end - now).total_seconds())
    if seconds_left <= 0 or seconds_left > PROB_PREVIEW_MAX_SECONDS_TO_END:
        return None

    start_ms = int(round_start.timestamp() * 1000)
    row = get(BINANCE + "/api/v3/klines", {
        "symbol": "BTCUSDT", "interval": tf, "startTime": start_ms, "limit": 1
    })
    if not row:
        return None
    open_price = float(row[0][1])

    ticker = get(BINANCE + "/api/v3/ticker/price", {"symbol": "BTCUSDT"})
    current_price = float(ticker["price"])
    if open_price <= 0 or current_price <= 0:
        return None

    rows = get(BINANCE + "/api/v3/klines", {
        "symbol": "BTCUSDT", "interval": "1m",
        "limit": max(20, min(1000, PROB_PREVIEW_VOL_LOOKBACK_MIN + 2)),
    })
    now_ms = int(time.time() * 1000)
    closed = [r for r in rows if int(r[6]) < now_ms]
    closes = [float(r[4]) for r in closed[-(PROB_PREVIEW_VOL_LOOKBACK_MIN + 1):]]
    if len(closes) < 15:
        return None
    rets = [math.log(b / a) for a, b in zip(closes[:-1], closes[1:]) if a > 0 and b > 0]
    if len(rets) < 10:
        return None
    sigma_1m = max(float(statistics.pstdev(rets)), PROB_PREVIEW_MIN_SIGMA_1M)
    sigma_h = sigma_1m * math.sqrt(max(seconds_left, 0.001) / 60.0)
    z = math.log(current_price / open_price) / sigma_h
    p_up = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
    p_up = min(0.9999, max(0.0001, p_up))
    return {
        "p_up": p_up,
        "p_down": 1.0 - p_up,
        "predicted_winner": "UP" if p_up >= 0.5 else "DOWN",
        "confidence": max(p_up, 1.0 - p_up),
        "open_price": open_price,
        "current_price": current_price,
        "seconds_left": seconds_left,
        "sigma_1m": sigma_1m,
        "model": "BINANCE_DISTANCE+REALIZED_VOL_NORMAL",
    }


def sizing(st, min_shares, directional_limit_price, opposite_limit_price, recovery_deficit_override=None):
    """
    V37: PAR respeitando DOIS pisos em cada ponta:
      1) valor nominal minimo configurado (MIN_LEG_USD, normalmente US$1);
      2) minimum order size do mercado/CLOB EM SHARES.

    A perna oposta usa:
      qo = max(ceil(MIN_LEG_USD / po), minimum_order_shares)

    A perna direcional usa o maior entre:
      - shares necessarias para garantir target liquido;
      - shares para o minimo nominal de US$1;
      - minimum_order_shares do CLOB.

    Assim nenhuma das duas pernas e enviada abaixo do `mos` oficial.
    """
    pd = D(directional_limit_price)
    po = D(opposite_limit_price)
    market_min = max(D("0"), D(min_shares or 0))
    if pd <= 0 or pd >= 1 or po <= 0 or po >= 1:
        raise ValueError("precos invalidos para sizing V37")

    base, deficit, target = recovery_target(st, recovery_deficit_override)

    opposite_nominal_min = ceil_6(MIN_LEG_USD / po)
    opposite_shares = max(opposite_nominal_min, market_min)
    opposite_max_spend = opposite_shares * po

    shares_for_target = ceil_6((target + opposite_max_spend) / (D("1") - pd))
    directional_nominal_min = ceil_6(MIN_LEG_USD / pd)
    directional_shares = max(shares_for_target, directional_nominal_min, market_min)

    directional_max_spend = directional_shares * pd
    guaranteed_net_at_limit = directional_shares - directional_max_spend - opposite_max_spend

    blocked = None
    if market_min <= 0:
        blocked = "MIN_ORDER_SHARES_UNAVAILABLE"
    elif opposite_max_spend > MAX_ENTRY or directional_max_spend > MAX_ENTRY:
        blocked = "MAX_ENTRY"
    elif guaranteed_net_at_limit < target:
        blocked = "TARGET_NOT_GUARANTEED"

    return {
        "blocked": bool(blocked),
        "reason": blocked,
        "base_profit": base,
        "recovery_deficit": deficit,
        "target_net_profit": target,
        "opposite_shares": opposite_shares,
        "directional_shares": directional_shares,
        "opposite_max_spend": opposite_max_spend,
        "directional_max_spend": directional_max_spend,
        "guaranteed_net_at_limit": guaranteed_net_at_limit,
        "edge": target,
        "martingale_base_edge": base,
        "directional_limit_price": pd,
        "opposite_limit_price": po,
        "minimum_leg_usd": MIN_LEG_USD,
        "reported_market_min_shares": market_min,
        "opposite_nominal_min_shares": opposite_nominal_min,
        "directional_nominal_min_shares": directional_nominal_min,
    }


def sizing_directional_only(st, min_shares, directional_limit_price, recovery_deficit_override=None):
    """
    V37: DIRECIONAL-ONLY respeitando simultaneamente:
      - target financeiro;
      - minimo nominal de US$1;
      - minimum order size oficial do mercado/CLOB em shares.
    """
    pd = D(directional_limit_price)
    market_min = max(D("0"), D(min_shares or 0))
    if pd <= 0 or pd >= 1:
        raise ValueError("preco invalido para sizing directional-only V37")

    base, deficit, target = recovery_target(st, recovery_deficit_override)
    shares_for_profit = ceil_6(target / (D("1") - pd))
    shares_for_min_usd = ceil_6(MIN_LEG_USD / pd)
    directional_shares = max(shares_for_profit, shares_for_min_usd, market_min)
    directional_max_spend = directional_shares * pd
    guaranteed_net_at_limit = directional_shares - directional_max_spend

    blocked = None
    if market_min <= 0:
        blocked = "MIN_ORDER_SHARES_UNAVAILABLE"
    elif directional_max_spend > MAX_ENTRY:
        blocked = "MAX_ENTRY"
    elif guaranteed_net_at_limit < target:
        blocked = "TARGET_NOT_GUARANTEED"

    return {
        "blocked": bool(blocked),
        "reason": blocked,
        "base_profit": base,
        "recovery_deficit": deficit,
        "target_net_profit": target,
        "opposite_shares": D("0"),
        "directional_shares": directional_shares,
        "opposite_max_spend": D("0"),
        "directional_max_spend": directional_max_spend,
        "guaranteed_net_at_limit": guaranteed_net_at_limit,
        "edge": target,
        "martingale_base_edge": base,
        "directional_limit_price": pd,
        "opposite_limit_price": None,
        "minimum_leg_usd": MIN_LEG_USD,
        "reported_market_min_shares": market_min,
        "shares_for_min_usd": shares_for_min_usd,
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



def build_gasless_api_key():
    """
    Monta a credencial que o SDK oficial usa para transacoes gasless/relayer.

    Aceita UM dos dois formatos:
      RELAYER:
        POLYMARKET_RELAYER_API_KEY
        POLYMARKET_RELAYER_API_KEY_ADDRESS

      BUILDER:
        POLYMARKET_BUILDER_API_KEY
        POLYMARKET_BUILDER_SECRET
        POLYMARKET_BUILDER_PASSPHRASE

    RELAYER tem prioridade quando ambos os conjuntos estiverem configurados.
    """
    relayer_any = bool(RELAYER_API_KEY or RELAYER_API_KEY_ADDRESS)
    relayer_all = bool(RELAYER_API_KEY and RELAYER_API_KEY_ADDRESS)

    builder_any = bool(BUILDER_API_KEY or BUILDER_SECRET or BUILDER_PASSPHRASE)
    builder_all = bool(BUILDER_API_KEY and BUILDER_SECRET and BUILDER_PASSPHRASE)

    if relayer_any and not relayer_all:
        raise RuntimeError(
            "Credencial RELAYER incompleta. Configure as duas variaveis: "
            "POLYMARKET_RELAYER_API_KEY e POLYMARKET_RELAYER_API_KEY_ADDRESS."
        )

    if builder_any and not builder_all:
        raise RuntimeError(
            "Credencial BUILDER incompleta. Configure as tres variaveis: "
            "POLYMARKET_BUILDER_API_KEY, POLYMARKET_BUILDER_SECRET e "
            "POLYMARKET_BUILDER_PASSPHRASE."
        )

    if relayer_all:
        return (
            RelayerApiKey(
                key=RELAYER_API_KEY,
                address=RELAYER_API_KEY_ADDRESS,
            ),
            "RELAYER",
        )

    if builder_all:
        return (
            BuilderApiKey(
                key=BUILDER_API_KEY,
                secret=BUILDER_SECRET,
                passphrase=BUILDER_PASSPHRASE,
            ),
            "BUILDER",
        )

    return None, "NONE"


class Bot:
    def __init__(self):
        self.s = load()
        apply_one_time_all_robots_reset(self.s)
        self.c = None

        if LIVE:
            if not PK or not WALLET:
                raise RuntimeError(
                    "Configure POLYMARKET_PRIVATE_KEY/PRIVATE_KEY "
                    "e POLYMARKET_DEPOSIT_WALLET"
                )
            gasless_api_key, gasless_mode = build_gasless_api_key()

            self.c = SecureClient.create(
                private_key=PK,
                wallet=WALLET,
                api_key=gasless_api_key,
            )
            log.info(
                "REAL | signer=%s wallet=%s type=%s | gasless_auth=%s",
                self.c.signer,
                self.c.wallet,
                self.c.wallet_type,
                gasless_mode,
            )
        else:
            log.info("SIMULACAO")

        self.last_withdrawal_sync = 0.0
        self.last_redemption_sync = 0.0
        self.last_balance_sync = 0.0
        self.last_balance_snapshot = None
        self.initialize_withdrawal_tracker()

        # V31: cache persistente carregado antes da thread; rede nunca e consultada no T-30.
        cached_events, cached_ts, cached_source, cached_error = load_news_cache()
        self.news_events = cached_events
        self.news_last_success = cached_ts
        self.news_last_attempt = 0.0
        self.news_source = cached_source or "nenhuma"
        self.news_last_error = cached_error or ""
        self.news_thread = None
        if cached_events:
            age = max(0.0, time.time() - cached_ts)
            log.info("NEWS V31 | CACHE CARREGADO | fonte=%s | eventos=%s | idade=%.1fh | arquivo=%s",
                     self.news_source, len(cached_events), age / 3600.0, NEWS_CACHE)
        elif cached_error:
            log.warning("NEWS V31 | cache nao disponivel | detalhe=%s", cached_error)
        if NEWS_FILTER_ENABLED:
            self.news_thread = Thread(target=self._news_calendar_loop, name="news-calendar-v31", daemon=True)
            self.news_thread.start()

        if LIVE:
            self.sync_balance(force=True)

    # -------------------- V31 ECONOMIC NEWS FILTER --------------------

    def _news_calendar_loop(self):
        while not STOP:
            self.news_last_attempt = time.time()
            try:
                events, source, prior_errors = fetch_us_high_impact_calendar_resilient()
                self.news_events = events
                self.news_last_success = time.time()
                self.news_source = source
                self.news_last_error = " | ".join(prior_errors)
                save_news_cache(events, source)
                now_utc = datetime.now(UTC)
                upcoming = [e for e in events if e["time_utc"] >= now_utc - timedelta(hours=2)]
                preview = "; ".join(
                    f'{e["time_utc"].astimezone(TZ).strftime("%d/%m %H:%M BRT")} {e["name"]}'
                    for e in upcoming[:8]
                ) or "nenhum evento alto impacto no horizonte"
                log.info("NEWS V31 | ATUALIZADO | fonte=%s | eventos=%s | horizonte=%sd | proximos=%s%s",
                         source, len(events), NEWS_LOOKAHEAD_DAYS, preview,
                         (" | fallback_info=" + self.news_last_error) if self.news_last_error else "")
            except Exception as exc:
                self.news_last_error = repr(exc)
                age = time.time() - self.news_last_success if self.news_last_success else float("inf")
                log.warning("NEWS V31 | FONTES INDISPONIVEIS | mantendo cache | fonte_cache=%s | "
                            "eventos_cache=%s | idade_cache=%s | erro=%r",
                            self.news_source, len(self.news_events),
                            (f"{age/3600.0:.1f}h" if age != float("inf") else "SEM_CACHE"), exc)
            deadline = time.time() + max(60.0, NEWS_REFRESH_SECONDS)
            while not STOP and time.time() < deadline:
                time.sleep(1.0)

    def news_allows_round(self, st, round_start):
        """Somente consulta memoria/cache; nunca acessa internet no T-30."""
        if not NEWS_FILTER_ENABLED:
            return True, None, "NEWS_FILTER_DISABLED"
        age = time.time() - self.news_last_success if self.news_last_success else float("inf")
        if not self.news_last_success:
            if NEWS_FAIL_CLOSED:
                return False, None, f"SEM_CALENDARIO erro={self.news_last_error}"
            return True, None, f"SEM_CALENDARIO_FAIL_OPEN erro={self.news_last_error}"
        if age > NEWS_MAX_STALE_SECONDS and NEWS_FAIL_CLOSED:
            return False, None, f"CALENDARIO_MUITO_ANTIGO age={age:.0f}s fonte={self.news_source}"

        minutes = NEWS_WINDOW_1H_MIN if st.get("tf") == "1h" else NEWS_WINDOW_SHORT_MIN
        before = timedelta(minutes=minutes)
        after = timedelta(minutes=minutes)
        rs = round_start.astimezone(UTC)
        for ev in self.news_events:
            et = ev.get("time_utc")
            if isinstance(et, datetime) and et - before <= rs < et + after:
                return False, ev, f"NEWS_BLACKOUT_{minutes}MIN"
        if age > NEWS_MAX_STALE_SECONDS:
            return True, None, f"OK_CACHE_STALE_FAIL_OPEN age={age:.0f}s fonte={self.news_source}"
        return True, None, f"OK fonte={self.news_source}"

    # -------------------- AUTOMATIC POSITION REDEMPTION --------------------

    def ensure_auto_redeem_operator(self):
        """Best-effort setup of official trading/auto-redeem approvals."""
        if not LIVE or not self.c:
            return
        fn = getattr(self.c, "setup_trading_approvals", None)
        if not callable(fn):
            log.warning("AUTO-REDEEM OPERATOR | SDK sem setup_trading_approvals; seguindo sem bloquear trading")
            return
        try:
            handle = fn()
            outcome = handle.wait() if hasattr(handle, "wait") else handle
            log.info("AUTO-REDEEM OPERATOR | APROVACOES OK | outcome=%s", outcome)
        except Exception as exc:
            log.warning("AUTO-REDEEM OPERATOR | falha ao confirmar aprovacoes; trading continua | erro=%r", exc)

    def enqueue_redemption(self, pending, winning_shares):
        """Persist a resolved winning condition for gasless redemption.

        Logical PnL accounting is independent from the on-chain redemption.  The
        queue prevents a temporary relayer/market-finalization error from losing
        the redemption request when the strategy moves on to its next round.
        """
        if not LIVE or D(winning_shares) <= 0:
            return

        rec = self.s.setdefault("redemption_reconciliation", {
            "processed_condition_ids": [], "queue": [], "last_redeem": None
        })
        processed = {str(x) for x in rec.get("processed_condition_ids", [])}
        queue = rec.setdefault("queue", [])

        condition_id = str(pending.get("condition_id") or "").strip()
        slug = str(pending.get("slug") or "").strip()

        if not condition_id and slug:
            try:
                mm = market(event(slug))
                if mm:
                    condition_id = str(mm.get("condition_id") or "").strip()
            except Exception:
                condition_id = ""

        if condition_id and condition_id in processed:
            return
        if any(
            (condition_id and str(item.get("condition_id") or "") == condition_id)
            or (not condition_id and slug and str(item.get("slug") or "") == slug)
            for item in queue if isinstance(item, dict)
        ):
            return

        queue.append({
            "condition_id": condition_id,
            "slug": slug,
            "winning_shares": str(winning_shares),
            "attempts": 0,
            "next_try_epoch": 0,
            "queued_at": datetime.now(UTC).isoformat(),
        })
        save(self.s)
        log.info(
            "RESGATE | ENFILEIRADO | condition_id=%s | slug=%s | winning_shares=%s",
            condition_id or "PENDENTE",
            slug,
            winning_shares,
        )

    def _inspect_redeemable_position(self, condition_id):
        """Return the wallet position state for one condition.

        The official Positions API is consulted BEFORE sending a relayer redeem.
        Status values:
          REDEEMABLE   -> at least one live position for the condition is redeemable
          NOT_READY    -> position exists with size > 0, but is not redeemable yet
          ABSENT       -> no positive-size open position is returned for the condition
          ERROR        -> inspection failed; never assume redemption is complete

        ABSENT is intentionally not treated as final on the first observation.
        Data APIs can lag, so process_redemptions requires two consecutive ABSENT
        confirmations before reconciling the queue as already settled/redeemed.
        """
        try:
            positions = list(
                self.c.list_positions(
                    user=WALLET,
                    page_size=100,
                ).iter_items()
            )
        except Exception as exc:
            return {
                "status": "ERROR",
                "error": repr(exc),
                "positions": [],
                "total_size": D("0"),
                "redeemable_size": D("0"),
            }

        matches = []
        total_size = D("0")
        redeemable_size = D("0")

        for pos in positions:
            cid = str(self._obj_field(pos, "condition_id", "conditionId", default="") or "").strip()
            if cid.lower() != str(condition_id).lower():
                continue

            try:
                size = D(self._obj_field(pos, "size", default="0") or "0")
            except Exception:
                size = D("0")
            if size <= 0:
                continue

            redeemable = bool(self._obj_field(pos, "redeemable", default=False))
            total_size += size
            if redeemable:
                redeemable_size += size

            matches.append({
                "size": str(size),
                "redeemable": redeemable,
                "outcome": str(self._obj_field(pos, "outcome", default="") or ""),
                "token_id": str(self._obj_field(pos, "token_id", "tokenId", default="") or ""),
            })

        if redeemable_size > 0:
            status = "REDEEMABLE"
        elif total_size > 0:
            status = "NOT_READY"
        else:
            status = "ABSENT"

        return {
            "status": status,
            "positions": matches,
            "total_size": total_size,
            "redeemable_size": redeemable_size,
            "error": None,
        }

    def _finish_redemption_reconciliation(self, rec, processed, item, reason, inspection=None, outcome=None):
        """Mark one condition reconciled and write an auditable terminal record."""
        condition_id = str(item.get("condition_id") or "").strip()
        slug = str(item.get("slug") or "").strip()
        processed.add(condition_id)

        rec["last_redeem"] = {
            "condition_id": condition_id,
            "slug": slug,
            "timestamp": datetime.now(UTC).isoformat(),
            "reason": reason,
            "outcome": str(outcome) if outcome is not None else None,
            "inspection": {
                "status": (inspection or {}).get("status"),
                "total_size": str((inspection or {}).get("total_size", "0")),
                "redeemable_size": str((inspection or {}).get("redeemable_size", "0")),
            },
        }

        audit({
            "type": "automatic_redeem_reconciliation",
            "condition_id": condition_id,
            "slug": slug,
            "winning_shares": str(item.get("winning_shares") or "0"),
            "reason": reason,
            "outcome": str(outcome) if outcome is not None else None,
            "ts": datetime.now(UTC).isoformat(),
        })

    def process_redemptions(self, force=False):
        """
        V29/V22: nao envia redeem_positions() manualmente para Deposit Wallet.
        O operator oficial faz o auto-redeem; esta fila apenas reconcilia a
        posicao ate ela desaparecer. Isso evita o POST /submit que ja se mostrou
        sujeito a revert no relayer.
        """
        if not LIVE or not self.c:
            return

        now_mono = time.monotonic()
        if not force and now_mono - self.last_redemption_sync < 5.0:
            return
        self.last_redemption_sync = now_mono

        rec = self.s.setdefault("redemption_reconciliation", {
            "processed_condition_ids": [], "queue": [], "last_redeem": None
        })
        processed = {str(x) for x in rec.get("processed_condition_ids", [])}
        queue = rec.setdefault("queue", [])
        if not queue:
            return

        changed = False
        now_epoch = time.time()
        kept = []
        processed_now = 0

        for item in list(queue):
            if not isinstance(item, dict):
                changed = True
                continue
            if processed_now >= 3:
                kept.append(item)
                continue
            if float(item.get("next_try_epoch") or 0) > now_epoch:
                kept.append(item)
                continue

            condition_id = str(item.get("condition_id") or "").strip()
            slug = str(item.get("slug") or "").strip()
            if not condition_id and slug:
                try:
                    mm = market(event(slug))
                    if mm:
                        condition_id = str(mm.get("condition_id") or "").strip()
                        item["condition_id"] = condition_id
                        changed = True
                except Exception:
                    pass

            if not condition_id:
                item["next_try_epoch"] = now_epoch + 30
                kept.append(item)
                changed = True
                continue
            if condition_id in processed:
                changed = True
                continue

            processed_now += 1
            inspection = self._inspect_redeemable_position(condition_id)
            status = inspection.get("status")

            if status == "ERROR":
                item["next_try_epoch"] = now_epoch + 120
                item["last_error"] = inspection.get("error")
                kept.append(item)
                changed = True
                log.warning("RESGATE | PRE-CHECK FALHOU | condition_id=%s | retry=120s | erro=%s", condition_id, inspection.get("error"))
                continue

            if status == "ABSENT":
                n = int(item.get("absent_confirmations") or 0) + 1
                item["absent_confirmations"] = n
                changed = True
                if n >= 2:
                    self._finish_redemption_reconciliation(
                        rec, processed, item,
                        reason="auto_operator_settled_or_no_open_position",
                        inspection=inspection,
                    )
                    log.info("RESGATE RECONCILIADO | condition_id=%s | sem posicao aberta em 2 verificacoes; considerado ja liquidado/resgatado", condition_id)
                    continue
                item["next_try_epoch"] = now_epoch + 30
                kept.append(item)
                log.info("RESGATE | SEM POSICAO ABERTA | condition_id=%s | confirmacao=1/2 | retry=30s", condition_id)
                continue

            if item.get("absent_confirmations"):
                item["absent_confirmations"] = 0
                changed = True

            if status == "NOT_READY":
                item["next_try_epoch"] = now_epoch + 60
                kept.append(item)
                changed = True
                log.info("RESGATE | AINDA NAO RESGATAVEL | condition_id=%s | size=%s | retry=60s", condition_id, inspection.get("total_size"))
                continue

            # REDEEMABLE: deixa o operator oficial liquidar; zero POST /submit.
            queued_at = item.get("queued_at")
            age = 0
            try:
                age = max(0, now_epoch - datetime.fromisoformat(queued_at).timestamp()) if queued_at else 0
            except Exception:
                age = 0
            delay = 120 if age >= 600 else 60
            item["next_try_epoch"] = now_epoch + delay
            kept.append(item)
            changed = True
            log.info(
                "RESGATE | REDEEMABLE CONFIRMADO | condition_id=%s | redeemable_size=%s | AUTO-OPERATOR aguardando | sem POST /submit | retry=%ss",
                condition_id, inspection.get("redeemable_size"), delay,
            )
            if age >= 1800 and not item.get("warned_30m"):
                item["warned_30m"] = True
                log.warning("AUTO-OPERATOR AINDA PENDENTE HA >30min | condition_id=%s | posicao continua redeemable", condition_id)

        if changed:
            rec["processed_condition_ids"] = list(processed)[-5000:]
            rec["queue"] = kept
            save(self.s)

    # -------------------- REAL WALLET BALANCE --------------------

    @staticmethod
    def _obj_field(obj, *names, default=None):
        for name in names:
            if isinstance(obj, dict) and name in obj:
                return obj.get(name)
            if hasattr(obj, name):
                return getattr(obj, name)
        return default

    def sync_balance(self, force=False):
        """
        Consulta o saldo/allowance de COLLATERAL da carteira autenticada.

        Isto NAO altera os bankrolls logicos; serve para confirmar que a carteira
        configurada no Railway e a carteira que possui collateral disponivel para
        negociacao no CLOB da Polymarket.
        """
        if not LIVE or not self.c:
            return None

        now_monotonic = time.monotonic()
        if not force and now_monotonic - self.last_balance_sync < BALANCE_SYNC_SECONDS:
            return self.last_balance_snapshot
        self.last_balance_sync = now_monotonic

        try:
            info = self.c.get_balance_allowance(asset_type="COLLATERAL")

            balance = self._obj_field(info, "balance", default=None)
            allowances = self._obj_field(info, "allowances", "allowance", default=None)

            self.last_balance_snapshot = {
                "balance": str(balance) if balance is not None else None,
                "allowances": allowances,
                "wallet": str(self.c.wallet),
                "signer": str(self.c.signer),
                "raw_type": type(info).__name__,
            }

            log.info(
                "CARTEIRA OK | wallet=%s | signer=%s | collateral_balance=%s | allowance=%s",
                self.c.wallet,
                self.c.signer,
                balance,
                allowances,
            )

            # Diagnostico objetivo para Deposit Wallet: se todos os allowances
            # conhecidos estiverem zerados e nenhuma credencial gasless foi
            # configurada, a primeira ordem seria inevitavelmente rejeitada.
            try:
                allowance_values = []
                if isinstance(allowances, dict):
                    for value in allowances.values():
                        try:
                            allowance_values.append(D(value))
                        except Exception:
                            pass

                all_zero = bool(allowance_values) and all(v <= 0 for v in allowance_values)
                gasless_api_key, gasless_mode = build_gasless_api_key()

                if all_zero and gasless_api_key is None:
                    log.error(
                        "ALLOWANCE BLOQUEADO | todos os allowances de COLLATERAL estao em 0 "
                        "| configure RELAYER (2 vars) ou BUILDER (3 vars) no Railway "
                        "antes de permitir ordens LIVE"
                    )
                elif all_zero and gasless_api_key is not None:
                    log.warning(
                        "ALLOWANCE=0 | gasless_auth=%s configurada; o SDK tentara "
                        "aprovar automaticamente no primeiro envio de ordem",
                        gasless_mode,
                    )
                elif allowance_values:
                    log.info(
                        "ALLOWANCE PRONTO | ao menos um spender possui allowance > 0"
                    )
            except Exception:
                log.exception("ALLOWANCE DIAGNOSTICO ERRO")

            return self.last_balance_snapshot

        except Exception:
            log.exception(
                "CARTEIRA ERRO | nao foi possivel consultar balance/allowance de COLLATERAL"
            )
            return None

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
        """
        Reconcilia saques pela Data API em JSON bruto.

        Motivo: polymarket-client 0.3.0b1 pode falhar ao desserializar a lista
        de activity quando um item TRADE vem sem outcomeIndex. Um item invalido
        derruba a pagina inteira antes de chegarmos aos WITHDRAWAL. Aqui usamos
        o mesmo endpoint oficial, mas filtramos type=WITHDRAWAL antes de qualquer
        validacao Pydantic do SDK.
        """
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
        start_epoch = max(baseline, last_success - 3600)
        processed = set(str(x) for x in tracker.get("processed_withdrawals", []))
        found = []

        try:
            offset = 0
            limit = 100
            scanned = 0

            while True:
                params = urlencode({
                    "user": WALLET,
                    "start": start_epoch,
                    "end": now_epoch,
                    "limit": limit,
                    "offset": offset,
                })
                req = Request(
                    f"https://data-api.polymarket.com/activity?{params}",
                    headers={"User-Agent": "btc-polymarket-v28"},
                )
                with urlopen(req, timeout=15) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))

                if not isinstance(payload, list):
                    raise RuntimeError(f"Data API activity retornou {type(payload).__name__}, esperado list")

                for item in payload:
                    scanned += 1
                    if scanned > 5000:
                        raise RuntimeError("mais de 5000 atividades na janela de reconciliacao")
                    if not isinstance(item, dict):
                        continue
                    if str(item.get("type", "")).upper() != "WITHDRAWAL":
                        continue

                    raw_ts = item.get("timestamp")
                    if raw_ts is None:
                        continue
                    try:
                        if isinstance(raw_ts, (int, float)) or str(raw_ts).isdigit():
                            ts_epoch = int(raw_ts)
                            ts = datetime.fromtimestamp(ts_epoch, UTC)
                        else:
                            ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
                            ts_epoch = int(ts.timestamp())
                    except Exception:
                        log.warning("SAQUES | WITHDRAWAL ignorado por timestamp invalido: %r", raw_ts)
                        continue

                    if ts_epoch <= baseline:
                        continue

                    amount = D(item.get("amount", 0) or 0)
                    if amount <= 0:
                        continue

                    tx = str(
                        item.get("transactionHash")
                        or item.get("transaction_hash")
                        or item.get("txHash")
                        or ""
                    )
                    key = f"{tx}|{ts.isoformat()}|{amount}"
                    if key in processed:
                        continue

                    # Objeto minimo compativel com apply_withdrawal(), sem depender
                    # dos modelos quebrados de activity do SDK beta.
                    from types import SimpleNamespace
                    activity = SimpleNamespace(
                        timestamp=ts,
                        transaction_hash=tx,
                        amount=amount,
                        type="WITHDRAWAL",
                    )
                    found.append((ts_epoch, key, activity, amount))

                if len(payload) < limit:
                    break
                offset += limit
                if offset >= 5000:
                    raise RuntimeError("paginacao de activity excedeu 5000 registros")

            found.sort(key=lambda x: (x[0], x[1]))
            for _ts_epoch, key, activity, amount in found:
                self.apply_withdrawal(amount, activity)
                processed.add(key)

            tracker["processed_withdrawals"] = list(processed)[-2000:]
            tracker["last_success_epoch"] = now_epoch
            save(self.s)

            if found:
                log.info("SAQUES | reconciliados=%s | scan_ate=%s", len(found), now_epoch)

        except Exception:
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

        if LIVE:
            snap = self.last_balance_snapshot or self.sync_balance(force=True)
            if snap:
                allowances = snap.get("allowances")
                vals = []
                if isinstance(allowances, dict):
                    for value in allowances.values():
                        try:
                            vals.append(D(value))
                        except Exception:
                            pass
                all_zero = bool(vals) and all(v <= 0 for v in vals)
                gasless_api_key, gasless_mode = build_gasless_api_key()
                if all_zero and gasless_api_key is None:
                    raise RuntimeError(
                        "ALLOWANCE=0 e nenhuma credencial gasless configurada. "
                        "Configure RELAYER ou BUILDER no Railway."
                    )

        try:
            return self.c.place_limit_order(
                token_id=token,
                price=price,
                size=shares,
                side="BUY",
                post_only=False,
            )
        except Exception as exc:
            log.exception(
                "ORDEM REJEITADA | token=%s | price=%s | shares=%s | erro=%s | tipo=%s",
                token,
                price,
                shares,
                exc,
                type(exc).__name__,
            )
            # Forca uma leitura de saldo/allowance logo apos a rejeicao.
            # Se a leitura tambem falhar, sync_balance registra o traceback
            # separadamente sem esconder o motivo original da ordem.
            try:
                self.sync_balance(force=True)
            except Exception:
                pass
            raise

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
        V29 usa piso nominal local de US$1,00 para ordens de reposicao.
        """
        shares = floor_6(D(shares))
        price = D(p.get(f"{leg}_limit_price") or p["limit_price"])
        min_shares_usd = ceil_6(MIN_LEG_USD / price)

        if shares < min_shares_usd:
            return False

        resp = self.place_gtc_limit(
            p[f"{leg}_token"],
            price,
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
        lag_price = D(p.get(f"{lag}_limit_price") or p["limit_price"])
        min_shares = ceil_6(MIN_LEG_USD / lag_price)

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

    # ------------------------- PRE-START WINDOW -------------------------

    def prepare_entry_window(self, st, round_start, direction, recovery_preview=None):
        """
        Em T-30 o sinal fica TRAVADO.
        Nenhuma ordem e enviada ainda.

        Do T-30 ate o inicio:
          - monitora o preco direcional e, quando necessario, o oposto
          - se target >= valor minimo da perna direcional, usa UMA ponta
          - caso contrario, usa o par quando AMBOS estiverem <= 0.55
          - se o par exceder o bankroll, tenta fallback direcional-only
          - se nenhuma estrutura valida ocorrer antes do inicio, descarta
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
        if tick <= 0:
            log.warning(
                "%s | tick_size invalido | %s",
                st["name"],
                sl,
            )
            return

        max_limit_price = floor_to_step(MAX_BUY_PRICE, tick)
        if max_limit_price <= 0:
            return

        # V29: o modo de execucao e decidido com o preco real da perna direcional.
        # Se a meta atingir o valor minimo negociavel dessa perna, passa a uma
        # unica ponta direcional. Abaixo disso, conserva o par minimo.
        recovery_preview = recovery_preview or {}
        rd_official = max(D("0"), D(st.get("recovery_deficit", "0") or "0"))
        rd_for_entry = D(recovery_preview.get("projected_recovery_deficit", rd_official))
        base_profit, recovery_deficit, target_net_profit = recovery_target(st, rd_for_entry)
        st["martingale_base_edge"] = None

        directional_token = m["up"] if direction == "UP" else m["down"]
        opposite_token = m["down"] if direction == "UP" else m["up"]
        opposite_direction = "DOWN" if direction == "UP" else "UP"

        round_end = round_start + timedelta(minutes=TFS[st["tf"]])

        st["pending"] = {
            "phase": "waiting_both_prices",
            "execution_mode": "undecided",
            "strategy": st["name"],
            "slug": sl,
            "condition_id": m.get("condition_id") or "",
            "round_start": round_start.astimezone(UTC).isoformat(),
            "round_end": round_end.astimezone(UTC).isoformat(),
            "direction": direction,
            "directional_side": direction,
            "opposite_side": opposite_direction,
            "directional_token": directional_token,
            "opposite_token": opposite_token,
            "directional_order_id": None,
            "opposite_order_id": None,
            "directional_shares_requested": "0",
            "opposite_shares_requested": "0",
            "directional_shares_filled": "0",
            "opposite_shares_filled": "0",
            "directional_spent": "0",
            "opposite_spent": "0",
            "limit_price": str(max_limit_price),  # teto global de entrada
            "directional_limit_price": None,
            "opposite_limit_price": None,
            "minimum_order_shares": str(min_shares),
            "tick_size": str(tick),
            "edge_at_limit": str(target_net_profit),
            "base_profit_target": str(base_profit),
            "recovery_deficit_before": str(recovery_deficit),
            "recovery_deficit_official_at_signal": str(rd_official),
            "recovery_deficit_for_entry": str(recovery_deficit),
            "probability_preview_applied": bool(recovery_preview.get("applied")),
            "probability_preview": recovery_preview,
            "recovery_active_at_entry": bool(recovery_deficit > 0),
            "open_positions_at_entry": len(st.get("open_positions") or []),
            "target_net_profit": str(target_net_profit),
            "guaranteed_net_at_limit": None,
            "directional_min_notional": None,
            "single_leg_reason": None,
            "martingale_base_edge": None,
            "signal_locked_at": datetime.now(UTC).isoformat(),
            "live": LIVE,
            "last_wait_log": 0,
        }
        save(self.s)

        log.info(
            "%s | SINAL TRAVADO %s | V30 aguardando estrutura valida <= %s ate %s | %s",
            st["name"],
            direction,
            max_limit_price,
            round_start.isoformat(),
            sl,
        )

        # Tenta imediatamente no proprio T-30.
        self.wait_for_both_prices(st, datetime.now(UTC))

    def wait_for_both_prices(self, st, now):
        """
        V35 - seleciona entre PAR e DIRECIONAL-ONLY pelo DEFICIT acumulado.

        Regra:
          recovery_deficit < US$1,00:
              usa obrigatoriamente o PAR (direcional + oposta).

          recovery_deficit >= US$1,00:
              passa para DIRECIONAL-ONLY.

        A meta financeira continua sendo:
          target = recovery_deficit + lucro-base.

        Importante: nao existe mais fallback para DIRECIONAL-ONLY quando o PAR
        excede o bankroll. Abaixo do limiar de US$1 de deficit, ou entra em PAR
        ou nao entra.
        """
        p = st.get("pending")
        if not p or p.get("phase") != "waiting_both_prices":
            return

        round_start = datetime.fromisoformat(p["round_start"])
        if now >= round_start:
            audit({
                "type": "no_entry_price_condition",
                "strategy": st["name"],
                "slug": p["slug"],
                "reason": "nenhuma estrutura V30 valida antes do inicio",
                "limit_price": p["limit_price"],
                "ts": now.isoformat(),
            })
            log.info(
                "%s | RODADA DESCARTADA | nenhuma estrutura V30 valida antes do inicio",
                st["name"],
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
        max_limit_price = D(p["limit_price"])

        # Sem preco direcional <= teto nao existe entrada em nenhum dos modos.
        if dp is None or D(dp) > max_limit_price:
            if time.time() - float(p.get("last_wait_log", 0)) >= 3:
                log.info(
                    "%s | AGUARDANDO PRECO V30 | DIR=%s OPP=%s | DIR precisa <= %s",
                    st["name"], dp, op, max_limit_price,
                )
                p["last_wait_log"] = time.time()
                save(self.s)
            return

        # V37: revalida os limites do mercado no CLOB oficial imediatamente
        # antes do sizing/envio. Nao confia apenas no valor salvo em T-30.
        constraints = clob_constraints(
            m.get("condition_id") or p.get("condition_id"),
            m.get("min_order_shares", p.get("minimum_order_shares", "0")),
            m.get("tick_size", p.get("tick_size", "0.01")),
        )
        tick = D(constraints["tick_size"])
        min_shares = D(constraints["minimum_order_shares"])
        p["minimum_order_shares"] = str(min_shares)
        p["minimum_order_shares_gamma"] = str(constraints["gamma_min_shares"])
        p["minimum_order_shares_clob"] = str(constraints["clob_min_shares"])
        p["minimum_order_source"] = constraints["source"]
        p["tick_size"] = str(tick)
        save(self.s)

        if min_shares <= 0:
            log.warning(
                "%s | BLOQUEADO V37 | minimum_order_shares indisponivel | condition_id=%s | fonte=%s",
                st["name"], m.get("condition_id") or p.get("condition_id"), constraints["source"],
            )
            st["pending"] = None
            save(self.s)
            return

        dir_limit = ceil_to_step(D(dp), tick)
        if dir_limit > max_limit_price:
            return

        rd_for_entry = D(p.get("recovery_deficit_for_entry", st.get("recovery_deficit", "0")) or "0")
        base_profit, recovery_deficit, target_net_profit = recovery_target(st, rd_for_entry)
        dir_min_notional = MIN_LEG_USD
        p["directional_min_notional"] = str(dir_min_notional)

        # V35: o switch depende SOMENTE do deficit financeiro acumulado.
        # O lucro-base do timeframe nao antecipa mais a troca para uma ponta.
        # Assim, todo ciclo novo (deficit=0) obrigatoriamente comeca em PAR.
        use_single = recovery_deficit >= dir_min_notional
        single_reason = "RECOVERY_DEFICIT_ATINGIU_USD1" if use_single else None

        pair_sz = None
        pair_total = None
        opp_limit = None

        if not use_single:
            both_ok = op is not None and D(op) <= max_limit_price
            if not both_ok:
                if time.time() - float(p.get("last_wait_log", 0)) >= 3:
                    log.info(
                        "%s | AGUARDANDO PRECO V37 | modo=PAR | DIR=%s OPP=%s | precisa OPP<=%s | TARGET=%s | DEFICIT=%s < SWITCH_USD=%s",
                        st["name"], dp, op, max_limit_price, target_net_profit, recovery_deficit, dir_min_notional,
                    )
                    p["last_wait_log"] = time.time()
                    save(self.s)
                return

            opp_limit = ceil_to_step(D(op), tick)
            if opp_limit > max_limit_price:
                return

            pair_sz = sizing(st, min_shares, dir_limit, opp_limit, recovery_deficit)
            pair_total = pair_sz["directional_max_spend"] + pair_sz["opposite_max_spend"]

            # V35: SEM fallback para uma ponta por falta de caixa.
            # Enquanto recovery_deficit < US$1, a estrategia permanece PAR.
            # Se o PAR nao couber no bankroll individual, a rodada sera bloqueada
            # mais abaixo em vez de transformar a entrada em direcional-only.

        if use_single:
            sz = sizing_directional_only(st, min_shares, dir_limit, recovery_deficit)
            total_spend = sz["directional_max_spend"]
            if sz.get("blocked"):
                log.warning(
                    "%s | BLOQUEADO V37 DIRECIONAL-ONLY | motivo=%s | DIR_LIMIT=%s | base=%s | deficit=%s | target=%s",
                    st["name"], sz.get("reason"), dir_limit, sz["base_profit"],
                    sz["recovery_deficit"], sz["target_net_profit"],
                )
                st["pending"] = None
                save(self.s)
                return
            if total_spend > D(st["bankroll"]):
                log.warning(
                    "%s | BLOQUEADO V37 DIRECIONAL-ONLY | gasto=%s > bankroll=%s | target=%s",
                    st["name"], total_spend, st["bankroll"], sz["target_net_profit"],
                )
                st["pending"] = None
                save(self.s)
                return

            p["execution_mode"] = "directional_only"
            p["single_leg_reason"] = single_reason
            p["directional_limit_price"] = str(dir_limit)
            p["opposite_limit_price"] = None
            p["directional_shares_requested"] = str(sz["directional_shares"])
            p["opposite_shares_requested"] = "0"
            p["guaranteed_net_at_limit"] = str(sz["guaranteed_net_at_limit"])
            p["edge_at_limit"] = str(sz["edge"])
            save(self.s)

            log.info(
                "%s | SIZING V37 DIRECIONAL-ONLY | MOTIVO=%s | MIN_SHARES_EFETIVO=%s | MIN_GAMMA=%s | MIN_CLOB=%s | FONTE_MIN=%s | SWITCH_DEFICIT_USD=%s | DIR_PX=%s | BASE=%s | DEFICIT=%s | TARGET_LIQUIDO=%s | DIR_SHARES=%s | OPP_SHARES=0 | GASTO_MAX=%s | LUCRO_MIN=%s",
                st["name"], single_reason, p["minimum_order_shares"], p.get("minimum_order_shares_gamma"), p.get("minimum_order_shares_clob"), p.get("minimum_order_source"), dir_min_notional,
                dir_limit, sz["base_profit"], sz["recovery_deficit"], sz["target_net_profit"],
                sz["directional_shares"], total_spend, sz["guaranteed_net_at_limit"],
            )

            r_dir = None
            e_dir = None
            try:
                r_dir = self.place_gtc_limit(
                    p["directional_token"],
                    D(p["directional_limit_price"]),
                    D(p["directional_shares_requested"]),
                )
            except Exception as exc:
                e_dir = exc

            dir_oid = (
                r_dir.get("order_id")
                if isinstance(r_dir, dict)
                else order_id_of(r_dir)
            )
            p["directional_order_id"] = str(dir_oid) if dir_oid else None
            p["opposite_order_id"] = None
            p["directional_order_ids"] = [str(dir_oid)] if dir_oid else []
            p["opposite_order_ids"] = []
            p["orders_sent_at"] = now.isoformat()
            p["directional_error"] = repr(e_dir) if e_dir else rejected_reason(r_dir)
            p["opposite_error"] = None
            p["phase"] = "orders_active"
            save(self.s)

            log.info(
                "%s | RESULTADO ENVIO DIRECIONAL-ONLY | DIR order_id=%s | DIR_ERR=%s | sem ordem oposta",
                st["name"], p["directional_order_id"], p["directional_error"],
            )

            if not LIVE:
                self.simulate_fill_if_marketable(p, m)
                save(self.s)

            audit({
                "type": "directional_only_order_started",
                "strategy": st["name"],
                "slug": p["slug"],
                "reason": single_reason,
                "directional_order_id": p["directional_order_id"],
                "directional_snapshot_price": str(dp),
                "directional_limit_price": str(dir_limit),
                "directional_min_notional": str(dir_min_notional),
                "target_net_profit": str(sz["target_net_profit"]),
                "directional_shares": str(sz["directional_shares"]),
                "max_spend": str(total_spend),
                "ts": now.isoformat(),
            })
            return

        # MODE 2: PAR minimo tradicional.
        sz = pair_sz
        if sz is None or opp_limit is None:
            return
        total_spend = pair_total
        if sz.get("blocked"):
            log.warning(
                "%s | BLOQUEADO V37 PAR | motivo=%s | DIR_LIMIT=%s | OPP_LIMIT=%s | base=%s | deficit=%s | target=%s",
                st["name"], sz.get("reason"), dir_limit, opp_limit, sz["base_profit"],
                sz["recovery_deficit"], sz["target_net_profit"],
            )
            st["pending"] = None
            save(self.s)
            return
        if total_spend > D(st["bankroll"]):
            log.warning(
                "%s | BLOQUEADO V37 PAR | gasto=%s > bankroll=%s | SEM_FALLBACK_DIRECIONAL | deficit=%s | target=%s",
                st["name"], total_spend, st["bankroll"], sz["recovery_deficit"], sz["target_net_profit"],
            )
            st["pending"] = None
            save(self.s)
            return

        p["execution_mode"] = "pair"
        p["single_leg_reason"] = None
        p["directional_limit_price"] = str(dir_limit)
        p["opposite_limit_price"] = str(opp_limit)
        p["directional_shares_requested"] = str(sz["directional_shares"])
        p["opposite_shares_requested"] = str(sz["opposite_shares"])
        p["guaranteed_net_at_limit"] = str(sz["guaranteed_net_at_limit"])
        p["edge_at_limit"] = str(sz["edge"])
        save(self.s)

        log.info(
            "%s | SIZING V37 PAR-MIN-CLOB | MIN_SHARES_EFETIVO=%s | MIN_GAMMA=%s | MIN_CLOB=%s | FONTE_MIN=%s | MIN_USD_PONTA=%s | DIR_PX=%s | OPP_PX=%s | BASE=%s | DEFICIT=%s | TARGET_LIQUIDO=%s | DIR_SHARES=%s | OPP_SHARES=%s | GASTO_MAX=%s | LUCRO_MIN=%s",
            st["name"], p["minimum_order_shares"], p.get("minimum_order_shares_gamma"), p.get("minimum_order_shares_clob"), p.get("minimum_order_source"), dir_min_notional, dir_limit, opp_limit,
            sz["base_profit"], sz["recovery_deficit"], sz["target_net_profit"],
            sz["directional_shares"], sz["opposite_shares"], total_spend,
            sz["guaranteed_net_at_limit"],
        )
        log.info(
            "%s | CONDICAO DE PRECO OK | modo=PAR | DIR=%s OPP=%s | enviando par",
            st["name"], dp, op,
        )

        r_dir = r_opp = None
        e_dir = e_opp = None
        start_barrier = Barrier(3)

        def send_leg(token, shares, price):
            start_barrier.wait()
            return self.place_gtc_limit(token, D(price), D(shares))

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="pair-v37") as ex:
            f_dir = ex.submit(
                send_leg, p["directional_token"],
                p["directional_shares_requested"], p["directional_limit_price"],
            )
            f_opp = ex.submit(
                send_leg, p["opposite_token"],
                p["opposite_shares_requested"], p["opposite_limit_price"],
            )
            start_barrier.wait()
            wait([f_dir, f_opp])
            e_dir = f_dir.exception()
            e_opp = f_opp.exception()
            if e_dir is None:
                r_dir = f_dir.result()
            if e_opp is None:
                r_opp = f_opp.result()

        dir_oid = r_dir.get("order_id") if isinstance(r_dir, dict) else order_id_of(r_dir)
        opp_oid = r_opp.get("order_id") if isinstance(r_opp, dict) else order_id_of(r_opp)
        p["directional_order_id"] = str(dir_oid) if dir_oid else None
        p["opposite_order_id"] = str(opp_oid) if opp_oid else None
        p["directional_order_ids"] = [str(dir_oid)] if dir_oid else []
        p["opposite_order_ids"] = [str(opp_oid)] if opp_oid else []
        p["orders_sent_at"] = now.isoformat()
        p["directional_error"] = repr(e_dir) if e_dir else rejected_reason(r_dir)
        p["opposite_error"] = repr(e_opp) if e_opp else rejected_reason(r_opp)
        p["phase"] = "orders_active"
        save(self.s)

        log.info(
            "%s | RESULTADO ENVIO PAR V30 | DIR order_id=%s | OPP order_id=%s | DIR_ERR=%s | OPP_ERR=%s",
            st["name"], p["directional_order_id"], p["opposite_order_id"],
            p["directional_error"], p["opposite_error"],
        )

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
            "max_limit_price": str(max_limit_price),
            "directional_limit_price": str(dir_limit),
            "opposite_limit_price": str(opp_limit),
            "directional_min_notional": str(dir_min_notional),
            "target_net_profit": str(sz["target_net_profit"]),
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
            # V29 directional-only nunca deve recriar uma perna oposta inexistente.
            if D(p.get(f"{leg}_shares_requested", "0") or "0") <= 0:
                continue

            try:
                resp = self.place_gtc_limit(
                    p[f"{leg}_token"],
                    D(p.get(f"{leg}_limit_price") or p["limit_price"]),
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

        # V29: modo de UMA ponta. Nao existe rebalanceamento nem recuperacao
        # da perna oposta. A ordem direcional deve executar antes do inicio;
        # se executar, acompanha a posicao ate a resolucao.
        if p.get("execution_mode") == "directional_only":
            if now < round_start:
                if dsh > 0:
                    self.cancel_ids([p.get("directional_order_id")])
                    if LIVE:
                        self.refresh_fills(p)
                    p["phase"] = "await_resolution"
                    p["pair_complete"] = False
                    save(self.s)
                    log.info(
                        "%s | DIRECIONAL-ONLY EXECUTADO ANTES DO INICIO | DIR=%s shares | acompanhando resolucao",
                        st["name"], p.get("directional_shares_filled", "0"),
                    )
                elif not p.get("directional_order_id"):
                    self.retry_missing_order(p)
                    save(self.s)
                else:
                    save(self.s)
                return

            # Chegou ao inicio sem fill: cancela e descarta.
            if now < round_end:
                if dsh <= 0:
                    self.cancel_ids([p.get("directional_order_id")])
                    if LIVE:
                        self.refresh_fills(p)
                    dsh = D(p.get("directional_shares_filled", "0"))
                    if dsh <= 0:
                        audit({
                            "type": "directional_only_discarded_no_fill_at_start",
                            "strategy": st["name"],
                            "slug": p["slug"],
                            "ts": now.isoformat(),
                        })
                        log.info(
                            "%s | DIRECIONAL-ONLY DESCARTADO | sem fill ate o inicio",
                            st["name"],
                        )
                        st["pending"] = None
                        save(self.s)
                        return

                self.cancel_ids([p.get("directional_order_id")])
                p["phase"] = "await_resolution"
                p["pair_complete"] = False
                save(self.s)
                return


            # Fallback defensivo se o loop so voltar depois do fim.
            self.cancel_ids([p.get("directional_order_id")])
            if LIVE:
                self.refresh_fills(p)
            dsh = D(p.get("directional_shares_filled", "0"))
            if dsh <= 0:
                st["pending"] = None
            else:
                p["phase"] = "await_resolution"
                p["pair_complete"] = False
            save(self.s)
            return

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
                    p.get(f"{missing_leg}_limit_price") or p["limit_price"],
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

    def resolve(self, st, p=None):
        # V40: aceita tanto o pending legado quanto uma posicao da fila
        # open_positions. Retorna True somente quando a resolucao foi concluida.
        from_open_positions = p is not None
        if p is None:
            p = st.get("pending")
        if not p or p.get("phase") != "await_resolution":
            return False

        w = winner_for_position(p)
        if not w:
            return False

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

        # O resultado logico e contabilizado imediatamente; se houver shares
        # vencedoras, o resgate real fica persistido numa fila com retry automatico.
        self.enqueue_redemption(p, winning_shares)

        st["bankroll"] = str(bankroll)
        st["realized_pnl"] = str(D(st.get("realized_pnl", "0")) + pnl)
        st["trades"] += 1

        directional_win = (w == direction)

        # V36: martingale/stop-loss passam a seguir EXCLUSIVAMENTE o resultado
        # financeiro liquido da operacao, e nao apenas se a direcao do sinal venceu.
        # Isso evita inconsistencias em PAR/parcial: uma direcao pode perder e o
        # hedge ainda gerar PNL positivo, ou a direcao pode vencer com PNL liquido
        # negativo por fills assimetricos.
        deficit_before = max(D("0"), D(st.get("recovery_deficit", "0") or "0"))
        stop_loss = max(D("0"), -pnl)

        if pnl < 0:
            financial_result = "LOSS"
            deficit_after = deficit_before + stop_loss
            st["losses"] += 1
            st["loss_streak"] += 1
        elif pnl > 0:
            financial_result = "WIN"
            deficit_after = max(D("0"), deficit_before - pnl)
            st["wins"] += 1
            # V40: enquanto ainda existir RD, o ciclo de recuperacao continua ativo.
            # Uma vitoria parcial reduz o deficit, mas NAO encerra o martingale.
            # O ciclo so volta ao zero quando o RD financeiro chega exatamente a 0.
            if deficit_after <= 0:
                deficit_after = D("0")
                st["loss_streak"] = 0
        else:
            financial_result = "FLAT"
            deficit_after = deficit_before

        st["recovery_deficit"] = str(deficit_after)
        st["martingale_base_edge"] = None
        st["last_pnl"] = str(pnl)
        st["last_stop_loss"] = str(stop_loss)
        st["last_result"] = financial_result

        # V42: se B/C usou uma previa probabilistica desta posicao e o winner
        # oficial contradisse a previsao, congela novas entradas ate a cadeia
        # aberta terminar. O RD acima ja foi atualizado com o resultado oficial.
        self.detect_preview_mismatch_and_freeze(st, p, w)

        audit({
            "type": "resolution",
            "strategy": st["name"],
            "slug": p["slug"],
            "winner": w,
            "signal": direction,
            "directional_win": directional_win,
            "financial_result": financial_result,
            "stop_loss": str(stop_loss),
            "directional_shares": str(dir_shares),
            "opposite_shares": str(opp_shares),
            "directional_spent": str(dir_spent),
            "opposite_spent": str(opp_spent),
            "pnl": str(pnl),
            "bankroll_after": str(bankroll),
            "loss_streak_after": st["loss_streak"],
            "martingale_base_edge_after": st.get("martingale_base_edge"),
            "recovery_deficit_before": str(deficit_before),
            "recovery_deficit_after": str(deficit_after),
            "next_base_profit": str(base_edge(st)),
            "next_target_if_signal": str(deficit_after + D(base_edge(st))),
            "ts": datetime.now(UTC).isoformat(),
        })

        if from_open_positions:
            ops = st.setdefault("open_positions", [])
            try:
                ops.remove(p)
            except ValueError:
                # Fallback por identidade logica para estados reserializados.
                slug = str(p.get("slug") or "")
                rs = str(p.get("round_start") or "")
                st["open_positions"] = [
                    x for x in ops
                    if not (str(x.get("slug") or "") == slug and str(x.get("round_start") or "") == rs)
                ]
        else:
            st["pending"] = None
        save(self.s)

        log.info(
            "%s | RESOLUCAO V42 CASCATA | WINNER=%s | SINAL=%s | RESULTADO_FINANCEIRO=%s | PNL=%s | STOP_LOSS=%s | BANKROLL=%s | loss_streak=%s | RD_ANTES=%s | RD_DEPOIS=%s | PROX_TARGET=%s | RECOVERY_ACTIVE=%s",
            st["name"],
            w,
            direction,
            financial_result,
            pnl,
            stop_loss,
            bankroll,
            st["loss_streak"],
            deficit_before,
            deficit_after,
            deficit_after + D(base_edge(st)),
            deficit_after > 0,
        )
        return True

    def archive_awaiting_resolution(self, st):
        """V40: libera o slot pending assim que a ordem passa a aguardar resultado."""
        p = st.get("pending")
        if not isinstance(p, dict) or p.get("phase") != "await_resolution":
            return False

        ops = st.setdefault("open_positions", [])
        slug = str(p.get("slug") or "")
        rs = str(p.get("round_start") or "")
        duplicate = any(
            str(x.get("slug") or "") == slug and str(x.get("round_start") or "") == rs
            for x in ops if isinstance(x, dict)
        )
        if not duplicate:
            ops.append(p)
            log.info(
                "%s | V42 POSICAO EM ANDAMENTO DESACOPLADA | slug=%s | open_positions=%s | proxima rodada pode ser avaliada sem esperar resolucao",
                st["name"], slug, len(ops),
            )
        st["pending"] = None
        save(self.s)
        return True

    def detect_preview_mismatch_and_freeze(self, st, resolved_pos, official_winner):
        """
        V42: se uma entrada posterior B usou uma previa probabilistica de A e
        o resultado oficial de A contradiz a previsao, congela NOVAS entradas
        desse robo ate todas as posicoes/pending atuais terminarem e o RD ficar
        totalmente consolidado.

        A resolucao oficial continua soberana. O congelamento nao cancela nem
        altera B ja executada; apenas impede que C/D... nascam sobre um estado
        financeiro ainda provisoriamente influenciado pela previsao errada.
        """
        slug_a = str(resolved_pos.get("slug") or "")
        if not slug_a:
            return False

        related = []
        candidates = []
        pending = st.get("pending")
        if isinstance(pending, dict):
            candidates.append(pending)
        candidates.extend(x for x in (st.get("open_positions") or []) if isinstance(x, dict))

        for pos in candidates:
            if pos is resolved_pos:
                continue
            preview = pos.get("probability_preview") or {}
            if not isinstance(preview, dict) or not pos.get("probability_preview_applied"):
                continue
            if str(preview.get("slug_a") or "") != slug_a:
                continue
            predicted = str(preview.get("predicted_winner") or "").upper()
            if predicted not in ("UP", "DOWN"):
                continue
            if predicted == str(official_winner or "").upper():
                continue
            related.append({
                "slug": str(pos.get("slug") or ""),
                "round_start": str(pos.get("round_start") or ""),
                "predicted_winner_a": predicted,
            })

        if not related:
            return False

        freeze = st.get("probability_preview_mismatch_freeze")
        if not isinstance(freeze, dict):
            freeze = {}
        freeze.update({
            "active": True,
            "trigger_slug_a": slug_a,
            "predicted_winner_a": related[0]["predicted_winner_a"],
            "official_winner_a": str(official_winner or "").upper(),
            "dependent_positions": related,
            "activated_at": datetime.now(UTC).isoformat(),
            "reason": "PREVISAO_A_DIVERGIU_DO_RESULTADO_OFICIAL",
        })
        st["probability_preview_mismatch_freeze"] = freeze
        save(self.s)
        log.warning(
            "%s | CONGELAMENTO V42 ATIVADO | previa de A errou | A=%s | previsto=%s | oficial=%s | dependentes=%s | NOVAS_ENTRADAS=BLOQUEADAS ate pending/open_positions zerarem e RD consolidar",
            st["name"], slug_a, freeze["predicted_winner_a"], freeze["official_winner_a"],
            [x.get("slug") for x in related],
        )
        audit({
            "type": "probability_preview_mismatch_freeze_v42",
            "strategy": st["name"],
            "slug_a": slug_a,
            "predicted_winner_a": freeze["predicted_winner_a"],
            "official_winner_a": freeze["official_winner_a"],
            "dependent_positions": related,
            "recovery_deficit_at_freeze": str(st.get("recovery_deficit", "0")),
            "ts": datetime.now(UTC).isoformat(),
        })
        return True

    def preview_mismatch_freeze_blocks_new_entry(self, st):
        """
        V42: enquanto um erro de previa estiver congelando a cadeia, continua
        processando/resolvendo as posicoes existentes, mas nao permite nova
        entrada. Libera automaticamente somente quando pending e open_positions
        estiverem vazios; nesse ponto o RD oficial ja incorporou toda a cadeia.
        """
        freeze = st.get("probability_preview_mismatch_freeze")
        if not isinstance(freeze, dict) or not freeze.get("active"):
            return False

        pending = st.get("pending")
        ops = [x for x in (st.get("open_positions") or []) if isinstance(x, dict)]
        if isinstance(pending, dict) or ops:
            return True

        freeze["active"] = False
        freeze["released_at"] = datetime.now(UTC).isoformat()
        freeze["recovery_deficit_consolidated"] = str(st.get("recovery_deficit", "0"))
        st["probability_preview_mismatch_freeze"] = freeze
        save(self.s)
        log.warning(
            "%s | CONGELAMENTO V42 LIBERADO | todas as posicoes da cadeia encerradas | RD_CONSOLIDADO=%s | proxima entrada volta a usar somente estado oficial",
            st["name"], st.get("recovery_deficit", "0"),
        )
        audit({
            "type": "probability_preview_mismatch_freeze_released_v42",
            "strategy": st["name"],
            "recovery_deficit_consolidated": str(st.get("recovery_deficit", "0")),
            "ts": datetime.now(UTC).isoformat(),
        })
        return False

    def resolve_open_positions(self, st):
        """
        V40: detecção fora de ordem + aplicação financeira FIFO.

        Varre TODAS as posições abertas. B/C podem ter o winner detectado e salvo
        mesmo enquanto A ainda está atrasada. Depois, bankroll/PNL/RD são aplicados
        somente na ordem cronológica A -> B -> C.
        """
        ops = st.setdefault("open_positions", [])
        if not ops:
            return

        def pos_key(x):
            return (
                str(x.get("round_end") or ""),
                str(x.get("round_start") or ""),
                str(x.get("slug") or ""),
            )

        ops.sort(key=pos_key)

        # Detecta e cacheia resultados de TODAS as posições, sem bloquear em A.
        cache_changed = False
        for idx, pos in enumerate(list(ops)):
            if not isinstance(pos, dict):
                continue
            before = str(pos.get("resolved_winner") or "")
            try:
                w = winner_for_position(pos)
            except Exception:
                log.exception(
                    "%s | erro detectando winner V42 | slug=%s",
                    st["name"], pos.get("slug"),
                )
                continue

            after = str(pos.get("resolved_winner") or "")
            if after and after != before:
                cache_changed = True
                log.info(
                    "%s | V42 RESULTADO CACHEADO | slug=%s | winner=%s | fonte=%s | posicao_fifo=%s",
                    st["name"],
                    pos.get("slug"),
                    w,
                    pos.get("resolved_winner_source"),
                    idx + 1,
                )

        if cache_changed:
            save(self.s)

        # Aplica resultados somente em FIFO, consumindo toda sequência já resolvida.
        while True:
            ops = st.setdefault("open_positions", [])
            if not ops:
                return
            ops.sort(key=pos_key)
            oldest = ops[0]

            if str(oldest.get("resolved_winner") or "").upper() not in ("UP", "DOWN"):
                try:
                    if not winner_for_position(oldest):
                        return
                    save(self.s)
                except Exception:
                    log.exception(
                        "%s | erro resolvendo posicao V42 FIFO | slug=%s",
                        st["name"], oldest.get("slug"),
                    )
                    return

            try:
                if not self.resolve(st, oldest):
                    return
            except Exception:
                log.exception(
                    "%s | erro aplicando resolucao V42 FIFO | slug=%s",
                    st["name"], oldest.get("slug"),
                )
                return

    # ------------------------- LOOP -------------------------

    def probability_preview_for_next_entry(self, st, next_start):
        """
        V42: antes de dimensionar B, estima o desfecho de A se A termina no
        instante em que B comeca. O RD oficial permanece intocado.

        Se a confianca ficar abaixo do limiar, usa o RD oficial (conservador).
        Se houver uma posicao FIFO mais antiga ainda sem resultado, tambem nao
        antecipa A, pois isso quebraria a ordem cronologica do martingale.
        """
        rd_official = max(D("0"), D(st.get("recovery_deficit", "0") or "0"))
        result = {
            "applied": False,
            "official_recovery_deficit": str(rd_official),
            "projected_recovery_deficit": str(rd_official),
            "reason": "NAO_APLICADO",
        }
        if not PROB_PREVIEW_ENABLED or rd_official <= 0:
            result["reason"] = "DESABILITADO_OU_SEM_RD"
            return result

        ops = [x for x in (st.get("open_positions") or []) if isinstance(x, dict)]
        if not ops:
            result["reason"] = "SEM_POSICAO_ANTERIOR_ABERTA"
            return result

        def dtv(v):
            try:
                d = datetime.fromisoformat(str(v))
                return d if d.tzinfo else d.replace(tzinfo=UTC)
            except Exception:
                return None

        target_end = next_start.astimezone(UTC)
        candidates = []
        for pos in ops:
            if str(pos.get("resolved_winner") or "").upper() in ("UP", "DOWN"):
                continue
            rend = dtv(pos.get("round_end"))
            rstart = dtv(pos.get("round_start"))
            if not rend or not rstart:
                continue
            if abs((rend - target_end).total_seconds()) <= 2.0:
                candidates.append((rend, rstart, pos))
        if not candidates:
            result["reason"] = "SEM_RODADA_A_TERMINANDO_COM_B"
            return result
        candidates.sort(key=lambda x: (x[0], x[1], str(x[2].get("slug") or "")))
        rend, rstart, pos = candidates[0]

        # Se existe algo cronologicamente anterior ainda sem winner, nao adivinha
        # uma etapa posterior do FIFO.
        for other in ops:
            if other is pos or str(other.get("resolved_winner") or "").upper() in ("UP", "DOWN"):
                continue
            oend = dtv(other.get("round_end"))
            if oend and oend < rend - timedelta(seconds=2):
                result["reason"] = "FIFO_ANTERIOR_AINDA_PENDENTE"
                return result

        try:
            forecast = binance_round_resolution_probability(st["tf"], rstart, rend)
        except Exception as exc:
            log.warning("%s | PREVIA V42 indisponivel | erro=%r", st["name"], exc)
            result["reason"] = "ERRO_FORECAST"
            return result
        if not forecast:
            result["reason"] = "FORECAST_INDISPONIVEL"
            return result

        result.update({
            "slug_a": str(pos.get("slug") or ""),
            "p_up": round(float(forecast["p_up"]), 6),
            "p_down": round(float(forecast["p_down"]), 6),
            "confidence": round(float(forecast["confidence"]), 6),
            "predicted_winner": forecast["predicted_winner"],
            "open_price": forecast["open_price"],
            "current_price": forecast["current_price"],
            "seconds_left": round(float(forecast["seconds_left"]), 3),
            "sigma_1m": forecast["sigma_1m"],
            "model": forecast["model"],
        })
        if float(forecast["confidence"]) < PROB_PREVIEW_MIN_CONFIDENCE:
            result["reason"] = "CONFIANCA_ABAIXO_LIMIAR"
            log.info(
                "%s | PREVIA V42 NAO APLICADA | A=%s | P_UP=%.2f%% | P_DOWN=%.2f%% | confianca=%.2f%% < limiar=%.2f%% | RD_OFICIAL=%s",
                st["name"], pos.get("slug"), forecast["p_up"]*100, forecast["p_down"]*100,
                forecast["confidence"]*100, PROB_PREVIEW_MIN_CONFIDENCE*100, rd_official,
            )
            return result

        predicted = forecast["predicted_winner"]
        projected_pnl = projected_position_pnl(pos, predicted)
        # V42 e assimetrico de proposito:
        # - se A provavelmente RECUPERA dinheiro, B pode reduzir/zerar o RD provisoriamente;
        # - se A provavelmente PERDE, B apenas continua com o RD oficial ja conhecido.
        #   Nao soma uma perda ainda nao oficial. Se a previsao de A estiver errada,
        #   a resolucao oficial ativa o CONGELAMENTO V42: nenhuma C e aberta enquanto
        #   B (e qualquer outra posicao ja existente da cadeia) nao terminar. Depois,
        #   a proxima entrada usa o RD oficial completamente consolidado.
        if projected_pnl > 0:
            projected_rd = max(D("0"), rd_official - projected_pnl)
        else:
            projected_rd = rd_official

        result.update({
            "applied": True,
            "reason": "CONFIANCA_APROVADA",
            "projected_pnl_a": str(projected_pnl),
            "projected_recovery_deficit": str(projected_rd),
        })
        log.info(
            "%s | PREVIA V42 A->B APLICADA | A=%s | winner_previsto=%s | P_UP=%.2f%% | P_DOWN=%.2f%% | confianca=%.2f%% | preco_abertura=%.2f | preco_atual=%.2f | faltam=%.1fs | PNL_A_PROJETADO=%s | RD_OFICIAL=%s | RD_PROJETADO_B=%s | TARGET_B=%s",
            st["name"], pos.get("slug"), predicted, forecast["p_up"]*100, forecast["p_down"]*100,
            forecast["confidence"]*100, forecast["open_price"], forecast["current_price"], forecast["seconds_left"],
            projected_pnl, rd_official, projected_rd, projected_rd + D(base_edge(st)),
        )
        audit({
            "type": "probability_preview_v41", "strategy": st["name"], "slug_a": pos.get("slug"),
            "next_round": next_start.astimezone(UTC).isoformat(), **result, "ts": datetime.now(UTC).isoformat(),
        })
        return result

    def tick(self, st, now):
        # V40: uma rodada ja iniciada nao bloqueia a captura T-30 da rodada seguinte.
        # Assim que uma operacao entra em await_resolution ela e movida para
        # open_positions; o slot pending fica livre para preparar a proxima rodada.
        if isinstance(st.get("pending"), dict) and st["pending"].get("phase") == "await_resolution":
            self.archive_awaiting_resolution(st)

        # Resolucao das rodadas anteriores ocorre em paralelo logico com a busca
        # por uma nova entrada. Se ainda nao houver winner, a posicao permanece na fila.
        self.resolve_open_positions(st)

        p = st.get("pending")
        if p:
            phase = p.get("phase")

            if phase == "waiting_both_prices":
                self.wait_for_both_prices(st, now)
            elif phase in ("orders_active", "single_leg_recovery", "proportional_rebalance"):
                self.process_active_orders(st, now)

            # A rotina acima pode ter acabado de transformar o pending em
            # await_resolution; desacopla imediatamente para nao bloquear a proxima.
            if isinstance(st.get("pending"), dict) and st["pending"].get("phase") == "await_resolution":
                self.archive_awaiting_resolution(st)
            return

        # V42: se uma previa usada numa entrada anterior foi desmentida pelo
        # resultado oficial, nao abre C/D... enquanto ainda houver qualquer
        # posicao da cadeia em aberto. A resolucao acima continua rodando normalmente.
        if self.preview_mismatch_freeze_blocks_new_entry(st):
            return

        if D(st["bankroll"]) >= TARGET:
            return

        _, next_start = bounds(now, TFS[st["tf"]])
        seconds_to_next = (next_start - now.astimezone(TZ)).total_seconds()

        # Captura o sinal apenas no T-30.
        if not ENTRY_SECONDS - 1.2 <= seconds_to_next <= ENTRY_SECONDS + 0.8:
            return

        if not session_allows_round(st, next_start):
            # *_day: V31 bloqueia fim de semana e fora de 10:00-16:00 BRT.
            return

        news_ok, news_event, news_reason = self.news_allows_round(st, next_start)
        if not news_ok:
            key = next_start.astimezone(UTC).isoformat()
            if st.get("last_news_block_log") != key:
                st["last_news_block_log"] = key
                save(self.s)
                if news_event:
                    log.warning(
                        "%s | ENTRADA BLOQUEADA NEWS V31 | rodada=%s | motivo=%s | evento=%s | evento_BRT=%s",
                        st["name"], next_start.isoformat(), news_reason,
                        news_event.get("name"),
                        news_event["time_utc"].astimezone(TZ).isoformat(),
                    )
                else:
                    log.warning(
                        "%s | ENTRADA BLOQUEADA NEWS V31 | rodada=%s | motivo=%s",
                        st["name"], next_start.isoformat(), news_reason,
                    )
            return

        key = next_start.astimezone(UTC).isoformat()
        if st["last_trigger"] == key:
            return

        st["last_trigger"] = key
        save(self.s)

        log.info(
            "%s | JANELA T-%ss ATINGIDA | proxima rodada=%s | avaliando candle ATUAL aberto + MACD FECHADO + MACD AO VIVO",
            st["name"],
            ENTRY_SECONDS,
            next_start.isoformat(),
        )

        direction, two_same, macd, sig, dirs, live_macd, live_sig, closed_hist, live_hist = trading_signal(st["tf"])

        if not direction:
            log.info(
                "%s | SEM ENTRADA V34: candle/MACD fechado/MACD ao vivo sem fortalecimento na mesma direcao | "
                "dirs=%s | macd_fechado=%s sig_fechado=%s hist_fechado=%s | "
                "macd_live=%s sig_live=%s hist_live=%s",
                st["name"],
                dirs,
                macd,
                sig,
                closed_hist,
                live_macd,
                live_sig,
                live_hist,
            )
            return

        recovery_active = max(D("0"), D(st.get("recovery_deficit", "0") or "0")) > 0
        if recovery_active and not two_same:
            log.info(
                "%s | MARTINGALE V40 EM CASCATA AGUARDA candle ANTERIOR fechado + ATUAL aberto %s + MACD | RD=%s | OPEN=%s | dirs=[anterior_fechado, atual_aberto]=%s",
                st["name"],
                direction,
                st.get("recovery_deficit", "0"),
                len(st.get("open_positions") or []),
                dirs,
            )
            return

        log.info(
            "%s | SINAL APROVADO V34 | direction=%s | dirs=%s | "
            "macd_fechado=%s signal_fechado=%s hist_fechado=%s | "
            "macd_live=%s signal_live=%s hist_live=%s | loss_streak=%s | recovery_deficit=%s | recovery_active=%s | open_positions=%s",
            st["name"],
            direction,
            dirs,
            macd,
            sig,
            closed_hist,
            live_macd,
            live_sig,
            live_hist,
            st["loss_streak"],
            st.get("recovery_deficit", "0"),
            recovery_active,
            len(st.get("open_positions") or []),
        )
        recovery_preview = self.probability_preview_for_next_entry(st, next_start)
        self.prepare_entry_window(st, next_start, direction, recovery_preview)

    def run(self):
        log.info("STARTUP OK | codigo carregado | versao=42 | OVERLAP_ROUNDS=ON | MARTINGALE_CASCATA=FIFO_CACHE+PREVIA_PROBABILISTICA+FREEZE_SE_ERRO | RESOLUCAO=MULTIFONTE_GAMMA+CLOB | PREVIA_ERRO=AGUARDA_TODAS_POSICOES_RD_CONSOLIDADO | MIN_ORDER=CLOB_DINAMICO+USD1 | RESET_TOTAL_UNICO=V34_PRESERVADO | SINAL=MACD_FECHADO+MACD_LIVE_FORTALECENDO | SWITCH_1PONTA=RECOVERY_DEFICIT>=USD1 | SEM_FALLBACK_DIRECIONAL | ENTRY_SECONDS=%s | MIN_USD_PONTA=1.00 | EDGE_5M=%s | EDGE_15M=%s | EDGE_1H=%s | MARTINGALE=PNL_LIQUIDO_NEGATIVO->RD+BASE | STOP_LOSS=EXPLICITO | DAY=SEG-SEX_10-16_BRT | NEWS_CACHE=PERSISTENTE | NEWS_FAIL_OPEN=%s | NEWS_5M15M=+-15M | NEWS_1H=+-60M", ENTRY_SECONDS, EDGE_5M, EDGE_15M, EDGE_1H, not NEWS_FAIL_CLOSED)
        _, gasless_mode = build_gasless_api_key()
        log.info(
            "POLYMARKET BTC V42 FINAL | OVERLAP_ROUNDS=ON | MARTINGALE_CASCATA=FIFO_CACHE+PREVIA_PROBABILISTICA+FREEZE_SE_ERRO | RESOLUCAO=MULTIFONTE_GAMMA+CLOB | PREVIA_ERRO=AGUARDA_TODAS_POSICOES_RD_CONSOLIDADO | MIN_ORDER=CLOB_DINAMICO+USD1 | LIVE=%s | GASLESS_AUTH=%s | 6 ROBOS | "
            "BANKROLL_INICIAL=%s | MACD 7/21/9 | "
            "SINAL T-%ss | PRECO<=%s | PAR OU DIRECIONAL-ONLY ANTES DO INICIO | "
            "SWITCH_1PONTA=RECOVERY_DEFICIT>=USD1 | SEM_FALLBACK_1PONTA | PARTIAL_PAR=PROPORCIONAL | "
            "DAY=SEG-SEX_10-16_BRT | NEWS=US_HIGH_3ESTRELAS_RESILIENTE | NEWS_5M15M=+-15M | NEWS_1H=+-60M | "
            "SAQUES=AUTO_PROPORCIONAL | RESGATE=AUTO_OPERATOR | BALANCE=MONITORADO | MARTINGALE=DEFICIT_ACUMULADO+BASE | SIZING=USD1_POR_PONTA+AUTO_DIRECIONAL_ONLY | EDGE_5M=%s | EDGE_15M=%s | EDGE_1H=%s | TARGET=%s | DATA=%s",
            LIVE,
            gasless_mode,
            INITIAL,
            ENTRY_SECONDS,
            MAX_BUY_PRICE,
            EDGE_5M,
            EDGE_15M,
            EDGE_1H,
            TARGET,
            ROOT,
        )

        hb = 0

        # Confirma approvals do operator oficial antes das reconciliacoes.
        self.ensure_auto_redeem_operator()

        # Primeira consulta logo apos o startup; baseline impede desconto retroativo.
        self.sync_withdrawals(force=True)
        self.sync_balance(force=True)
        self.process_redemptions(force=True)

        while not STOP:
            now = datetime.now(UTC)

            self.sync_withdrawals()
            self.sync_balance()
            self.process_redemptions()

            for st in self.s["strategies"].values():
                try:
                    self.tick(st, now)
                except Exception:
                    log.exception("%s | erro no tick", st["name"])

            if time.time() - hb > 30:
                summary = " | ".join(
                    f'{st["name"]}:bank={st["bankroll"]},'
                    f'L={st["loss_streak"]},'
                    f'RD={st.get("recovery_deficit", "0")},'
                    f'PNL={st.get("last_pnl", "0")},'
                    f'SL={st.get("last_stop_loss", "0")},'
                    f'R={st.get("last_result", "NONE")},'
                    f'OPEN={len(st.get("open_positions") or [])},'
                    f'phase={(st.get("pending") or {}).get("phase","-")}'
                    for st in self.s["strategies"].values()
                )
                recon = self.s.get("capital_reconciliation", {})
                bal = self.last_balance_snapshot or {}
                log.info(
                    "HEARTBEAT | LIVE=%s | wallet_balance=%s | withdrawn_applied=%s | %s",
                    LIVE,
                    bal.get("balance", "?"),
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
