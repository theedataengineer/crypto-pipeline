"""
binance_extract.py

- Extracts cryptocurrency market data from the Binance public REST API.

Endpoints used:
    - /api/v3/klines        -> OHLCV candlestick data
    - /api/v3/trades        -> Recent trade transactions
    - /api/v3/ticker/24hr   -> 24-hour market statistics

ELT principle: raw data is stored exactly as received from the API
No transformations happen here. That is dbt's job.
"""

import requests
import json
import os
from datetime import datetime

# Configuration

BASE_URL = "https://api.binance.com"

TRADING_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

# Kline interval - "1h" means one candlestick per hour

KLINE_INTERVAL = "1h"

KLINE_LIMIT = 100

TRADES_LIMIT = 100


# Helper: make a safe API request with error handling

def fetch(endpoint: str, params: dict) -> list | dict | None:
    """
    Makes a GET request to the Binance API.
    Returns parsed JSON on success, None on failure.
    Always logs what it's doing. Observability matters in production guysss
    """

    url = f"{BASE_URL}{endpoint}"
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # raises exception for 4xx/5xx erros
        print(f" [OK] GET {endpoint} | params: {params} | status: {response.status_code}")
        return response.json()
    except requests.exceptions.Timeout:
        print(f" [ERROR] Timeout hitting {endpoint}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f" [ERROR] HTTP error {e} on {endpoint}")
        return None
    except requests.exceptions.ConnectionError:
        print(f" [ERROR] Connection failed. Check your internet.")
        return None
    

# Extractor 1: Klines (candlestick / OHLCV data)

# This is the most important dataset - price history over time.
# Used for: trend analysis, moving averages, volatility calculations.


def extract_klines(symbol: str) -> list:
    """
    Fetches OHLCV candlestick data for a trading pair.
    Each records = one time interval (e.g. 1 hour) of price activity.
    """

    print(f"\n Extracting klines for {symbol}...")
    data = fetch("/api/v3/klines", {
        "symbol": symbol,
        "interval": KLINE_INTERVAL,
        "limit": KLINE_LIMIT
    })

    if not data:
        return []
    

    # Binance returns klines as a list of lists. We add context
    # So when this lands in the database, we know which pair it belongs to

    records = []
    for candle in data:
        records.append({
            "symbol":                           symbol,
            "open_time":                        candle[0], # unix timestamp ms
            "open_price":                       candle[1],
            "high_price":                       candle[2],
            "low_price":                        candle[3],
            "close_price":                      candle[4],
            "volume":                           candle[5],
            "close_time":                       candle[6],
            "quote_asset_volume":               candle[7],
            "number_of_trades":                 candle[8],
            "taker_buy_base_volume":           candle[9],
            "taker_buy_quote_volume":           candle[10],
            "extracted_at":                     datetime.utcnow().isoformat()
        })

    print(f" Extracted {len(records)} klines for {symbol}")
    return records
    

# Extractor 2: Recent trades
# Individual buy/sell transactions.
# Used for: trade volume analysis, buyer/seller ratio, liquidity.

def extract_trades(symbol: str) -> list:
    """
    Fetches the most recent trades for a trading pair.
    Each record = one individual trade transaction.
    """

    print(f"\n Extracting trades for {symbol}...")
    data = fetch("/api/v3/trades", {
        "symbol": symbol,
        "limit": TRADES_LIMIT
    })

    if not data:
        return []
    
    records = []
    for trade in data:
        records.append({
            "symbol":           symbol,
            "trade_id":         trade["id"],
            "price":            trade["price"],
            "quantity":         trade["qty"],
            "quote_qty":        trade["quoteQty"],  # price x quantity
            "trade_time":       trade["time"],  # Unix timestamp ms
            "is_buyer_maker":   trade["isBuyerMaker"],
            "extracted_at":     datetime.utcnow().isoformat()
        })

    print(f" Extracted {len(records)} trades for {symbol}")
    return records



# Extractor 3: 24hr ticker statistics

# High-level market summary for each trading pair.
# Used for: daily summaries, price change %, volume rankings.


def extract_tickers() -> list:
    """
    Fetches 24-hour rolling market statistics for all our trading pairs.
    One record per symbol - a snapshot of market health.
    """

    print(f"\n Extracting 24hr tickers...")
    records = []

    for symbol in TRADING_PAIRS:
        data = fetch("/api/v3/ticker/24hr", {"symbol": symbol})
        if not data:
            continue

        records.append({
            "symbol":                   data["symbol"],
            "price_change":             data["priceChange"],
            "price_change_pct":         data["priceChangePercent"],
            "weighted_avg_price":       data["weightedAvgPrice"],
            "prev_close_price":         data["prevClosePrice"],
            "last_price":               data["lastPrice"],
            "open_price":               data["openPrice"],
            "high_price":               data["highPrice"],
            "low_price":                data["lowPrice"],
            "volume":                   data["volume"],
            "quote_volume":             data["quoteVolume"],
            "open_time":                data["openTime"],
            "close_time":               data["closeTime"],
            "count":                    data["count"],   # NUmber of trades in 24hr
            "extracted_at":             datetime.utcnow().isoformat()
         })
        

    print(f" Extracted {len(records)} ticker records")
    return records
    




# Main Orchestrator
# In production this would be triggered by Airflow, cron, or an
# event. Here we run it directly. Same logic, different trigger.



def run_extraction():
    """
    Runs the full extraction for all symbols and all endpoints.
    Saves results to JSON files in the extract/ folder.
    JSON is our staging area before loading into PostgreSQL.
    """

    print("=" * 55)
    print(" BINANCE API EXTRACTION - starting")
    print(f" Pairs : {TRADING_PAIRS}")
    print(f" Time :  {datetime.utcnow().isoformat()}")
    print("=" * 55)


    all_klines   = []
    all_trades   = []
    all_tickers  = []


    for symbol in TRADING_PAIRS:
        all_klines += extract_klines(symbol)
        all_trades += extract_trades(symbol)


    all_tickers = extract_tickers()


    # Save to JSON - this is our raw data landing zone
    # In a cloud pipeline this would go to S3, GCS, or Azure Blob
    
    os.makedirs("extract/raw_output", exist_ok=True)


    with open("extract/raw_output/klines.json", "w") as f:
        json.dump(all_klines, f, indent=2)

    with open("extract/raw_output/trades.json", "w") as f:
        json.dump(all_trades, f, indent=2)

    with open("extract/raw_output/tickers.json", "w") as f:
        json.dump(all_tickers, f, indent=2)



    print("\n" + "=" * 55)
    print(" EXTRACTION COMPLETE")
    print(f" Klines : {len(all_klines)} records")
    print(f" Trades : {len(all_trades)} records")
    print(f" Tickers : {len(all_tickers)} records")
    print(" Output : extract/raw_output/")
    print("=" * 55)


if __name__ == "__main__":
    run_extraction()