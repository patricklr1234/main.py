#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
POLYMARKET BTC BOT - SINGLE FILE
Compatível com Railway.

IMPORTANTE:
- Este arquivo NÃO coloca PRIVATE_KEY no código.
- LIVE_TRADING=0 por padrão.
- O bot monitora o CLOB/Gamma e registra oportunidades.
- Para habilitar execução real, LIVE_TRADING=1.
"""

import os
import sys
import json
import time
import signal
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timezone

# ============================================================
# 1. DIRETÓRIO FIXO DO PROJETO
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

STATE_FILE = BASE_DIR / "state.json"
TRADES_FILE = BASE_DIR / "trades.jsonl"
LOG_FILE = BASE_DIR / "bot.log"

# ============================================================
# 2. CONFIGURAÇÕES
# ============================================================

HOST = os.getenv(
    "CLOB_HOST",
    "https://clob.polymarket.com"
)

GAMMA_HOST = os.getenv(
    "GAMMA_HOST",
    "https://gamma-api.polymarket.com"
)

CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))

PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()

LIVE_TRADING = (
    os.getenv("LIVE_TRADING", "0").strip().lower()
    in ("1", "true", "yes", "on")
)

ACTIVE_ASSET = os.getenv(
    "ACTIVE_ASSET",
    "BTC"
)

POLL_SECONDS = int(
    os.getenv("POLL_SECONDS", "15")
)

# ============================================================
# 3. DEPENDÊNCIAS
# ============================================================

def import_dependencies():

    try:
        import requests
        from eth_account import Account
    except ImportError:

        print(
            "Dependências ausentes. "
            "Instalando automaticamente..."
        )

        packages = [
            "requests",
            "eth-account",
            "py-clob-client-v2",
        ]

        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-cache-dir",
                *packages,
            ]
        )

        import requests
        from eth_account import Account

    try:
        from py_clob_client_v2 import ClobClient
    except ImportError as e:
        raise RuntimeError(
            "Não foi possível importar py_clob_client_v2. "
            f"Erro: {e}"
        )

    return requests, Account, ClobClient


requests, Account, ClobClient = import_dependencies()

# ============================================================
# 4. LOG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            LOG_FILE,
            encoding="utf-8"
        ),
    ],
)

log = logging.getLogger("POLYMARKET-BOT")

# ============================================================
# 5. ESTADO
# ============================================================

def load_state():

    if not STATE_FILE.exists():

        return {
            "started_at": None,
            "last_scan": None,
            "markets_found": 0,
            "last_market": None,
            "last_error": None,
        }

    try:

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            return json.load(f)

    except Exception as e:

        log.warning(
            "Não foi possível carregar state.json: %s",
            e,
        )

        return {
            "started_at": None,
            "last_scan": None,
            "markets_found": 0,
            "last_market": None,
            "last_error": str(e),
        }


def save_state(state):

    temp = STATE_FILE.with_suffix(".tmp")

    try:

        with open(
            temp,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                state,
                f,
                indent=2,
                ensure_ascii=False,
            )

        temp.replace(STATE_FILE)

    except Exception as e:

        log.error(
            "Erro salvando state.json: %s",
            e,
        )


def record_trade(data):

    try:

        with open(
            TRADES_FILE,
            "a",
            encoding="utf-8"
        ) as f:

            f.write(
                json.dumps(
                    data,
                    ensure_ascii=False,
                )
                + "\n"
            )

    except Exception as e:

        log.error(
            "Erro salvando trades.jsonl: %s",
            e,
        )

# ============================================================
# 6. CLOB
# ============================================================

class PolymarketBot:

    def __init__(self):

        self.state = load_state()

        self.state["started_at"] = (
            datetime.now(timezone.utc)
            .isoformat()
        )

        if not PRIVATE_KEY:

            raise RuntimeError(
                "PRIVATE_KEY não está configurada "
                "nas Variables do Railway."
            )

        self.signer = Account.from_key(
            PRIVATE_KEY
        )

        self.signer_address = (
            self.signer.address
        )

        log.info(
            "Signer: %s",
            self.signer_address,
        )

        log.info(
            "Inicializando CLOB..."
        )

        try:

            self.client = ClobClient(
                host=HOST,
                chain_id=CHAIN_ID,
                key=PRIVATE_KEY,
            )

        except TypeError:

            self.client = ClobClient(
                host=HOST,
                chain_id=CHAIN_ID,
                key=PRIVATE_KEY,
            )

        log.info(
            "Obtendo credenciais CLOB..."
        )

        try:

            creds = (
                self.client
                .create_or_derive_api_key()
            )

            self.client = ClobClient(
                host=HOST,
                chain_id=CHAIN_ID,
                key=PRIVATE_KEY,
                creds=creds,
            )

            log.info(
                "CLOB autenticado."
            )

        except Exception as e:

            log.warning(
                "Falha na criação/derivação "
                "da API key: %s",
                e,
            )

            log.info(
                "Tentando continuar com "
                "cliente CLOB inicial."
            )

    # ========================================================
    # CONEXÃO
    # ========================================================

    def test_connection(self):

        try:

            response = requests.get(
                HOST + "/time",
                timeout=15,
            )

            response.raise_for_status()

            log.info(
                "CLOB online. Server time: %s",
                response.text,
            )

            return True

        except Exception as e:

            log.error(
                "Falha CLOB: %s",
                e,
            )

            return False

    # ========================================================
    # GAMMA
    # ========================================================

    def get_markets_page(
        self,
        offset=0,
        limit=100,
    ):

        params = {
            "active": "true",
            "closed": "false",
            "limit": limit,
            "offset": offset,
        }

        response = requests.get(
            GAMMA_HOST + "/markets",
            params=params,
            timeout=20,
        )

        response.raise_for_status()

        data = response.json()

        if isinstance(data, dict):

            markets = (
                data.get("data")
                or data.get("markets")
                or []
            )

        elif isinstance(data, list):

            markets = data

        else:

            markets = []

        return markets

    # ========================================================
    # BUSCA BTC
    # ========================================================

    def find_btc_markets(self):

        found = []

        # Gamma rejeitou offset 2100 anteriormente.
        # Portanto trabalhamos em páginas menores.

        offsets = range(0, 2100, 100)

        for offset in offsets:

            try:

                markets = self.get_markets_page(
                    offset=offset,
                    limit=100,
                )

            except requests.HTTPError as e:

                log.warning(
                    "Gamma rejeitou offset=%s: %s",
                    offset,
                    e,
                )

                break

            except Exception as e:

                log.warning(
                    "Erro Gamma offset=%s: %s",
                    offset,
                    e,
                )

                break

            if not markets:

                break

            for market in markets:

                text = " ".join(
                    str(
                        market.get(field, "")
                    )
                    for field in (
                        "question",
                        "title",
                        "slug",
                        "description",
                    )
                ).lower()

                if (
                    "bitcoin" in text
                    or "btc" in text
                ):

                    found.append(
                        market
                    )

        return found

    # ========================================================
    # FILTRO BTC CURTO
    # ========================================================

    @staticmethod
    def is_short_btc_market(market):

        text = " ".join(
            str(
                market.get(field, "")
            )
            for field in (
                "question",
                "title",
                "slug",
                "description",
            )
        ).lower()

        short_terms = [
            "5m",
            "5-min",
            "5 min",
            "5 minute",
            "5 minutes",
            "15m",
            "15-min",
            "15 min",
            "15 minute",
            "15 minutes",
            "1h",
            "1-hour",
            "1 hour",
            "hourly",
        ]

        return any(
            term in text
            for term in short_terms
        )

    # ========================================================
    # INSPEÇÃO DOS MERCADOS
    # ========================================================

    def scan(self):

        log.info(
            "Buscando mercados BTC..."
        )

        markets = (
            self.find_btc_markets()
        )

        self.state[
            "markets_found"
        ] = len(markets)

        self.state[
            "last_scan"
        ] = datetime.now(
            timezone.utc
        ).isoformat()

        if not markets:

            log.warning(
                "Nenhum mercado BTC encontrado "
                "na busca atual."
            )

            save_state(self.state)

            return []

        log.info(
            "Mercados BTC encontrados: %s",
            len(markets),
        )

        short_markets = [
            m
            for m in markets
            if self.is_short_btc_market(m)
        ]

        if short_markets:

            log.info(
                "Mercados BTC de curto prazo: %s",
                len(short_markets),
            )

        for market in (
            short_markets[:10]
            if short_markets
            else markets[:10]
        ):

            question = (
                market.get("question")
                or market.get("title")
                or "Sem título"
            )

            slug = market.get(
                "slug",
                "",
            )

            end = (
                market.get("endDate")
                or market.get("endDateIso")
            )

            tokens = (
                market.get("clobTokenIds")
                or market.get("clob_token_ids")
            )

            log.info(
                "BTC | %s",
                question,
            )

            log.info(
                "SLUG | %s",
                slug,
            )

            log.info(
                "END | %s",
                end,
            )

            log.info(
                "TOKENS | %s",
                tokens,
            )

            self.state[
                "last_market"
            ] = {
                "question": question,
                "slug": slug,
                "end": end,
                "tokens": tokens,
            }

        save_state(self.state)

        return short_markets or markets

    # ========================================================
    # LOOP
    # ========================================================

    def run(self):

        log.info(
            "=========================================="
        )

        log.info(
            "POLYMARKET BTC BOT"
        )

        log.info(
            "LIVE_TRADING: %s",
            LIVE_TRADING,
        )

        log.info(
            "Signer: %s",
            self.signer_address,
        )

        log.info(
            "=========================================="
        )

        if LIVE_TRADING:

            log.warning(
                "!!! LIVE TRADING ATIVADO !!!"
            )

        else:

            log.info(
                "MODO MONITORAMENTO: "
                "nenhuma ordem será enviada."
            )

        if not self.test_connection():

            raise RuntimeError(
                "CLOB indisponível."
            )

        log.info(
            "Loop iniciado."
        )

        while True:

            try:

                now = datetime.now(
                    timezone.utc
                )

                log.info(
                    "Bot ativo | UTC %s",
                    now.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                )

                markets = self.scan()

                log.info(
                    "Scan concluído | BTC=%s",
                    len(markets),
                )

                time.sleep(
                    POLL_SECONDS
                )

            except KeyboardInterrupt:

                log.info(
                    "Interrompido pelo usuário."
                )

                break

            except Exception as e:

                log.exception(
                    "Erro no ciclo: %s",
                    e,
                )

                self.state[
                    "last_error"
                ] = str(e)

                save_state(
                    self.state
                )

                time.sleep(
                    POLL_SECONDS
                )

# ============================================================
# 7. SHUTDOWN
# ============================================================

BOT = None


def shutdown(
    signum,
    frame,
):

    log.info(
        "Sinal recebido. Encerrando..."
    )

    if BOT is not None:

        try:
            save_state(
                BOT.state
            )
        except Exception:
            pass

    raise SystemExit(0)


signal.signal(
    signal.SIGTERM,
    shutdown,
)

signal.signal(
    signal.SIGINT,
    shutdown,
)

# ============================================================
# 8. MAIN
# ============================================================

if __name__ == "__main__":

    try:

        BOT = PolymarketBot()

        BOT.run()

    except Exception as e:

        log.exception(
            "BOT NÃO INICIADO: %s",
            e,
        )

        sys.exit(1)
