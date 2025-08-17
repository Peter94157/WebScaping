#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto OHLCV Collector – Bybit (Python)

- Backfill histórico e atualização incremental.
- Normalização para schema: timestamp | open | high | low | close | volume
- Deduplicação e (opcional) preenchimento de gaps alinhados ao timeframe.
- Exporta CSV e XLSX por símbolo/timeframe.

Autor: você 😎
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import ccxt
import pandas as pd
from dateutil import tz

# ------------------------------ Config padrão ------------------------------ #
SUPPORTED_TIMEFRAMES = [
    "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"
]

TIMEFRAME_MS = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
    "1w": 7 * 24 * 60 * 60_000,  # aproximação compatível com exchanges que usam 7 dias
}

DATA_DIR = os.environ.get("DATA_DIR", "data")
DEFAULT_SYMBOLS = ["BTC/USDT", "ETH/USDT", "HYPER/USDT"]  # ajuste se necessário

# Bybit limita tamanho por requisição; ccxt tipicamente 1000 velas por call
OHLCV_LIMIT = 1000
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 1.8

# --------------------------- Utilidades de arquivos ------------------------- #

def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def path_csv(symbol: str, timeframe: str) -> str:
    sym = symbol.replace("/", "-")
    d = os.path.join(DATA_DIR, sym)
    ensure_dir(d)
    return os.path.join(d, f"{timeframe}.csv")


def path_xlsx(symbol: str, timeframe: str) -> str:
    sym = symbol.replace("/", "-")
    d = os.path.join(DATA_DIR, sym)
    ensure_dir(d)
    return os.path.join(d, f"{timeframe}.xlsx")

# ------------------------------ Exchange client --------------------------- #

def make_exchange() -> ccxt.Exchange:
    exchange = ccxt.bybit({
        "enableRateLimit": True,
        # "options": {"defaultType": "spot"},  # descomente se quiser forçar spot
    })
    return exchange


# --------------------------- Coleta via CCXT (OHLCV) ----------------------- #

def fetch_ohlcv_chunk(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since_ms: Optional[int] = None,
    limit: int = OHLCV_LIMIT,
) -> List[List[float]]:
    """Puxa um chunk de OHLCV (lista de candles) a partir de 'since_ms'."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=limit)
        except ccxt.NetworkError as e:
            wait = RETRY_BACKOFF_BASE ** attempt
            print(f"[warn] NetworkError (tentativa {attempt}) para {symbol} {timeframe}: {e}. Aguardando {wait:.1f}s...", file=sys.stderr)
            time.sleep(wait)
        except ccxt.ExchangeError as e:
            # erros permanentes como símbolo inexistente/timeframe não suportado
            print(f"[error] ExchangeError para {symbol} {timeframe}: {e}", file=sys.stderr)
            return []
    return []


def fetch_ohlcv_range(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: Optional[int] = None,
) -> List[List[float]]:
    """Backfill do intervalo [start_ms, end_ms]. Se end_ms=None, usa agora."""
    assert timeframe in SUPPORTED_TIMEFRAMES, f"Timeframe não suportado: {timeframe}"
    tf_ms = TIMEFRAME_MS[timeframe]
    if end_ms is None:
        end_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    out: List[List[float]] = []
    cursor = start_ms

    while True:
        chunk = fetch_ohlcv_chunk(exchange, symbol, timeframe, since_ms=cursor)
        if not chunk:
            break

        out.extend(chunk)

        last_ts = chunk[-1][0]
        next_cursor = last_ts + tf_ms

        if next_cursor >= end_ms:
            break

        # segurança para não travar
        if next_cursor <= cursor:
            next_cursor = cursor + tf_ms
        cursor = next_cursor

    return out

# --------------------------- Normalização & limpeza ------------------------ #
def to_dataframe(rows: List[List[float]]) -> pd.DataFrame:
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    df = pd.DataFrame(rows, columns=cols)
    # timestamp em ms UTC → converte para HH:MM dd/mm/aaaa
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["timestamp"] = df["timestamp"].dt.strftime("%H:%M %d/%m/%Y")
    return df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Garante colunas no padrão e ordena por timestamp."""
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    df = df[cols].copy()
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    return df.reset_index(drop=True)


def align_and_fill_gaps(df: pd.DataFrame, timeframe: str, method: str = "ffill_close") -> pd.DataFrame:
    """
    Reindexa a série para timestamps perfeitos alinhados ao timeframe e
    opcionalmente preenche gaps.

    method:
      - "none": não preenche, deixa NaN para OHLCV ausentes.
      - "ffill_close": para candles ausentes, usa close anterior para O=H=L=C e volume=0.
    """
    tf_ms = TIMEFRAME_MS[timeframe]
    if df.empty:
        return df

    start = df["timestamp"].iloc[0]
    end = df["timestamp"].iloc[-1]

    # Garante alinhamento ao múltiplo do timeframe
    start = start - (start % tf_ms)
    end = end - (end % tf_ms)

    full_index = pd.RangeIndex(start=start, stop=end + tf_ms, step=tf_ms, name="timestamp")

    df2 = df.set_index("timestamp").reindex(full_index)

    if method == "ffill_close":
        # Preenche usando o close anterior
        df2["close"] = df2["close"].ffill()
        # Para O/H/L: igual ao close prévio quando faltar
        for col in ("open", "high", "low"):
            df2[col] = df2[col].fillna(df2["close"])  # após ffill de close
        # Volume vira 0 para gaps
        df2["volume"] = df2["volume"].fillna(0)
    elif method == "none":
        pass
    else:
        raise ValueError("method inválido; use 'none' ou 'ffill_close'")

    return df2.reset_index()


def merge_existing(new_df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """Une com CSV existente (se houver) e deduplica por timestamp."""
    if os.path.exists(csv_path):
        old = pd.read_csv(csv_path)
        merged = pd.concat([old, new_df], axis=0, ignore_index=True)
        merged = merged.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
        return merged.reset_index(drop=True)
    return new_df

# ------------------------------- Exportação -------------------------------- #

def export_csv_xlsx(df: pd.DataFrame, symbol: str, timeframe: str, to_csv: bool = True, to_xlsx: bool = False) -> Tuple[Optional[str], Optional[str]]:
    csv_p = xlsx_p = None
    df_export = df.copy()
    # Formata timestamp como string apenas para exportação
    df_export["timestamp"] = df_export["timestamp"].dt.strftime("%H:%M %d/%m/%Y")
    
    if to_csv:
        csv_p = path_csv(symbol, timeframe)
        df_export.to_csv(csv_p, index=False)
    if to_xlsx:
        xlsx_p = path_xlsx(symbol, timeframe)
        with pd.ExcelWriter(xlsx_p, engine="xlsxwriter") as writer:
            df_export.to_excel(writer, index=False, sheet_name="OHLCV")
    return csv_p, xlsx_p


# ------------------------------- Pipelines --------------------------------- #

def parse_dt_to_ms(dt_str: str) -> int:
    """Aceita ISO-8601 (ex: '2020-01-01T00:00:00Z') ou 'YYYY-MM-DD'. Assume UTC."""
    if "T" in dt_str or "Z" in dt_str:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    else:
        dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def backfill(
    symbols: Iterable[str],
    timeframes: Iterable[str],
    start: str,
    end: Optional[str] = None,
    fill_method: str = "ffill_close",
    export_csv: bool = True,
    export_xlsx: bool = False,
    out_dir_base: str = DATA_DIR,
) -> None:
    ex = make_exchange()
    markets = ex.load_markets()

    start_ms = parse_dt_to_ms(start)
    end_ms = parse_dt_to_ms(end) if end else None

    for symbol in symbols:
        if symbol not in markets:
            print(f"[warn] Símbolo não encontrado em Bybit: {symbol}. Pulando.", file=sys.stderr)
            continue

        for tf in timeframes:
            if tf not in SUPPORTED_TIMEFRAMES or tf not in ex.timeframes:
                print(f"[warn] Timeframe não suportado pela exchange: {tf}. Pulando {symbol}.", file=sys.stderr)
                continue

            print(f"[info] Backfill {symbol} {tf}...")
            rows = fetch_ohlcv_range(ex, symbol, tf, start_ms=start_ms, end_ms=end_ms)
            if not rows:
                print(f"[warn] Sem dados retornados para {symbol} {tf}")
                continue

            df = normalize(to_dataframe(rows))
            df = align_and_fill_gaps(df, tf, method=fill_method)

            # Define diretório de saída usando Path
            out_dir = Path(out_dir_base) / symbol.replace("/", "-")
            out_dir.mkdir(parents=True, exist_ok=True)

            # Exporta CSV/XLSX usando a função existente
            csv_path = out_dir / f"{tf}.csv"
            export_csv_xlsx(df, symbol, tf, to_csv=export_csv, to_xlsx=export_xlsx)

            print(f"[ok] {symbol} {tf} ⇒ linhas: {len(df)}")



def incremental_update(
    symbols: Iterable[str],
    timeframes: Iterable[str],
    fill_method: str = "ffill_close",
    export_csv: bool = True,
    export_xlsx: bool = False,
    out_dir_base: str = DATA_DIR,
) -> None:
    ex = make_exchange()
    markets = ex.load_markets()

    for symbol in symbols:
        if symbol not in markets:
            print(f"[warn] Símbolo não encontrado em Bybit: {symbol}. Pulando.", file=sys.stderr)
            continue
        for tf in timeframes:
            if tf not in SUPPORTED_TIMEFRAMES or tf not in ex.timeframes:
                print(f"[warn] Timeframe não suportado pela  exchange: {tf}. Pulando.", file=sys.stderr)
            continue
        print(f"[info] Coletando {symbol} - {tf}...")
        df = fetch_ohlcv_chunk(ex, symbol, tf, since=None)

        if df is None or df.empty:
            print(f"[warn] Nenhum dado retornado para {symbol}-{tf}", file=sys.stderr)
            continue

        df = normalize(df, tf)

        out_dir = Path(args.out) / symbol.replace("/", "")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{tf}.csv"

        export_csv_xlsx(df, out_file)
        export_csv_xlsx(df, symbol, tf, to_csv=export_csv, to_xlsx=export_xlsx)

if __name__ == "__main__":
    import ccxt
    from datetime import datetime, timezone

    # conecta na exchange (Bybit é gratuita para OHLCV público)
    exchange = ccxt.bybit()

    symbol = "BTC/USDT"
    timeframe = "1h"

    # define o intervalo de datas corretamente
    start_str = "2024-01-01T00:00:00Z"
    start = exchange.parse8601(start_str)
    end = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    # busca os dados
    candles = fetch_ohlcv_range(exchange, symbol, timeframe, start, end)

    # converte para dataframe e normaliza
    import pandas as pd
    df = normalize(to_dataframe(candles))

    # exporta CSV/XLSX com timestamp formatado
    export_csv_xlsx(df, symbol, timeframe, to_csv=True, to_xlsx=False)

    print(f"Baixados {len(candles)} candles para {symbol} ({timeframe})")
    print("Primeiros 2 candles:")
    print(df.head(2))

