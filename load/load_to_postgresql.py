"""
load_to_postgresql.py

------------------------------------------

Loads raw extracted data from JSON files intos PostgreSQL.

Design principles applied here:
- Raw tables mirror the API response exactly(no transformations)
- Idempotent loads: Running twice won't duplicate data.
- Schema-first: Tables are created before data is inserted.
- Every table tracks when the record was extracted (extracted_at) for lineage and debugging purposes.
"""

import json
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime

# -------------------------------------------------------------------------
# Load environment variables from .env
# In production: AWS Secrets Manager, HashiCorp Vault, or K8s secrets
# The pattern is the same - never hardcode credentials in code


load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":       os.getenv("DB_NAME", "binance_db"),
    "user":     os.getenv("DB_USER", "binance_pipeline"),
    "password":     os.getenv("DB_PASSWORD")
}

# -------------------------------------------------------------------------
# Database Connection
# Using a context manager ensures the connection always closes cleanly
# even if an error occurs - no connection leaks in production
# -------------------------------------------------------------------------

def get_connection():
    """Returns a live PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)


# -------------------------------------------------------------------------
# Schema creation
# We create tables with IF NOT EXISTS - safe to run repeatedly.
# Notice we keep all columns as TEXT or NUMERIC for the raw layer.
# Type casting happens in dbt staging models, not here.
# --------------------------------------------------------------------------

CREATE_KLINES_TABLE = """
CREATE TABLE IF NOT EXISTS raw_binance_klines (
    id      SERIAL PRIMARY KEY,
    symbol      TEXT NOT NULL,
    open_time       BIGINT,
    open_price      NUMERIC,
    high_price      NUMERIC,
    low_price       NUMERIC,
    close_price     NUMERIC,
    volume      NUMERIC,
    close_time      BIGINT,
    quote_asset_volume      NUMERIC,
    number_of_trades        INTEGER,
    taker_buy_base_volume       NUMERIC,
    taker_buy_quote_volume      NUMERIC,
    extracted_at        TIMESTAMP,
    UNIQUE (symbol, open_time)

);
"""

CREATE_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS raw_binance_trades (
    id      SERIAL PRIMARY KEY,
    symbol      TEXT NOT NULL,
    trade_id        BIGINT,
    price       NUMERIC,
    quantity        NUMERIC,
    quote_qty       NUMERIC,
    trade_time      BIGINT,
    is_buyer_maker      BOOLEAN,
    extracted_at        TIMESTAMP,
    UNIQUE (symbol, trade_id)
)
"""


CREATE_TICKERS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_binance_tickers (
    id      SERIAL PRIMARY KEY,
    symbol      TEXT NOT NULL,
    price_change        NUMERIC,
    price_change_pct        NUMERIC,
    weighted_avg_price      NUMERIC,
    prev_close_price        NUMERIC,
    last_price      NUMERIC,
    open_price      NUMERIC,
    high_price      NUMERIC,
    low_price       NUMERIC,
    volume      NUMERIC,
    quote_volume        NUMERIC,
    open_time       BIGINT,
    close_time      BIGINT,
    count       INTEGER,
    extracted_at        TIMESTAMP,
    UNIQUE (symbol, open_time)
);
"""

def create_tables(conn):
    """
    Creates all raw tables if they don't exist.
    Safe to run on every pipeline execution.
    """

    with conn.cursor() as cur:
        print(" Creating tables if not exist...")
        cur.execute(CREATE_KLINES_TABLE)
        cur.execute(CREATE_TRADES_TABLE)
        cur.execute(CREATE_TICKERS_TABLE)
        conn.commit()
        print(" [OK] Tables ready")



# -------------------------------------------------------------------------
# Loaders
# execute_values() is far more efficient than row-by-row inserts.
# In production with millions of rows, this difference is massive.
# ON CONFLICT DO NOTHING = idempotent - safe to re-run anytime.
# -------------------------------------------------------------------------


def load_klines(conn, records: list):
    """Loads kline records into raw_binance_klines."""
    if not records:
        print(" [SKIP] No kline records to load")
        return
    
    rows = [(
        r["symbol"],
        r["open_time"],
        r["open_price"],
        r["high_price"],
        r["low_price"],
        r["close_price"],
        r["volume"],
        r["close_time"],
        r["quote_asset_volume"],
        r["number_of_trades"],
        r["taker_buy_base_volume"],
        r["taker_buy_quote_volume"],
        r["extracted_at"]
    ) for r in records]
    
    sql = """
        INSERT INTO raw_binance_klines (
            symbol, open_time, open_price, high_price, low_price,
            close_price, volume, close_time, quote_asset_volume,
            number_of_trades, taker_buy_base_volume,
            taker_buy_quote_volume, extracted_at
        ) VALUES %s
        ON CONFLICT (symbol, open_time) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        conn.commit()
        print(f" [OK] Loaded {len(rows)} kline records")



def load_trades(conn, records: list):
    """Loads trade records into raw_binance_trades."""
    if not records:
        print(" [SKIP] No trade records to load")
        return
    
    rows = [(
        r["symbol"],
        r["trade_id"],
        r["price"],
        r["quantity"],
        r["quote_qty"],
        r["trade_time"],
        r["is_buyer_maker"],
        r["extracted_at"]
    ) for r in records]

    sql = """
        INSERT INTO raw_binance_trades (
            symbol, trade_id, price, quantity, quote_qty,
            trade_time, is_buyer_maker, extracted_at
        ) VALUES %s
        ON CONFLICT (symbol, trade_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        conn.commit()
        print(f" [OK] Loaded {len(rows)} trade records")


def load_tickers(conn, records: list):
    """Loads ticker records into raw_binance_tickers."""
    if not records:
        print(" [SKIP] No ticker records to load")
        return
    

    rows = [(
        r["symbol"],
        r["price_change"],
        r["price_change_pct"],
        r["weighted_avg_price"],
        r["prev_close_price"],
        r["last_price"],
        r["open_price"],
        r["high_price"],
        r["low_price"],
        r["volume"],
        r["quote_volume"],
        r["open_time"],
        r["close_time"],
        r["count"],
        r["extracted_at"]
    ) for r in records]


    sql = """
        INSERT INTO raw_binance_tickers (
            symbol, price_change, price_change_pct, weighted_avg_price,
            prev_close_price, last_price, open_price, high_price, low_price,
            volume, quote_volume, open_time,
            close_time, count, extracted_at
            ) VALUES %s
            ON CONFLICT (symbol, open_time) DO NOTHING;
    """


    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        conn.commit()
        print(f" [OK] loaded {len(rows)} ticker records")


# -------------------------------------------------------------------------
# Main orchestrator
# -------------------------------------------------------------------------

def run_load():
    print("=" * 55)
    print(" POSTGRESQL LOAD - starting")
    print(f" Time : {datetime.utcnow().isoformat()}")
    print("=" * 55)

    # Read JSON files produced by extracting step

    with open("extract/raw_output/klines.json") as f: klines = json.load(f)
    with open("extract/raw_output/trades.json") as f: trades = json.load(f)
    with open("extract/raw_output/tickers.json") as f: tickers = json.load(f)

    print(f"\n Records read from disk:")
    print(f" Klines : {len(klines)}")
    print(f" Trades: {len(trades)}")
    print(f" Tickers : {len(tickers)}")

    print(f"\n Connecting to PostgreSQL...")
    conn = get_connection()
    print(f" [OK] Connected to {DB_CONFIG['dbname']}")

    print("\n Setting up schema...")
    create_tables(conn)

    print("\n Loading data ...")
    load_klines(conn, klines)
    load_trades(conn, trades)
    load_tickers(conn, tickers)

    conn.close()

    print("\n" + "=" * 55)
    print(" LOAD COMPLETE")
    print(" Raw tables populated in PostgreSQL")
    print(" Next step: dbt staging models")
    print("=" * 55)


if __name__ == "__main__":
    run_load()