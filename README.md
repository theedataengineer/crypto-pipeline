# Crypto Market Analytics Pipeline

A production-style ELT data pipeline that extracts live cryptocurrency
market data from the Binance API, loads it into PostgreSQL, transforms
it using dbt, and visualizes insights in a Grafana dashboard.

---

## Architecture
```
Binance Public API
       ↓
Python Extraction (requests)
       ↓
Raw PostgreSQL Tables
       ↓
dbt Staging Models   → clean, cast, deduplicate
       ↓
dbt Intermediate     → moving averages, volatility, volume metrics
       ↓
dbt Mart Tables      → facts, dimensions, daily summaries
       ↓
Grafana Dashboard    → live crypto market intelligence
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3 | API extraction and data loading |
| PostgreSQL | Data warehouse |
| dbt Core | Data transformation and modeling |
| Grafana | Dashboard and visualization |
| Git & GitHub | Version control |

---

## Data Sources

All data comes from the **Binance Public REST API** — no API key required.

| Endpoint | Data |
|---|---|
| `/api/v3/klines` | OHLCV candlestick data (1hr intervals) |
| `/api/v3/trades` | Individual trade transactions |
| `/api/v3/ticker/24hr` | 24-hour market statistics |

**Trading pairs tracked:** BTCUSDT · ETHUSDT · BNBUSDT

---

## Pipeline Layers

### Raw Layer (PostgreSQL)
Stores data exactly as received from the API. No transformations.

| Table | Description |
|---|---|
| `raw_binance_klines` | OHLCV candlestick records |
| `raw_binance_trades` | Individual trade transactions |
| `raw_binance_tickers` | 24hr market statistics |

### Staging Layer (dbt views)
Cleans and standardizes raw data — renames columns, converts
timestamps, casts types, removes duplicates.

| Model | Description |
|---|---|
| `stg_binance_klines` | Clean candlestick data with candle direction |
| `stg_binance_trades` | Clean trades with buy/sell direction |
| `stg_binance_tickers` | Clean 24hr market stats |

### Intermediate Layer (dbt views)
Enriches data with business metrics — moving averages,
volatility, volume analysis, buy/sell pressure.

| Model | Description |
|---|---|
| `int_price_metrics` | MA 7/14/30, volatility, returns, volume ratio |
| `int_symbol_volume` | Volume aggregation, dominance %, rankings |

### Mart Layer (dbt tables)
Analytics-ready tables used directly by Grafana.

| Model | Description |
|---|---|
| `fct_crypto_candles` | Core price fact table with all signals |
| `fct_crypto_trades` | Trade facts with whale classification |
| `dim_symbols` | Trading pair dimension with market sentiment |
| `mart_daily_market_summary` | Pre-aggregated daily market view |

---

## Key Analytics

This pipeline answers the following business questions:

- What is the daily average price of each cryptocurrency?
- Which cryptocurrency has the highest trading volume?
- What is the hourly price volatility for each coin?
- Which trading pair has the largest price movement?
- What is the 7-day rolling average price trend?
- Are there unusual volume spikes signaling market activity?
- What is the current buy vs sell pressure per coin?

---

## Project Structure
```
crypto-pipeline/
├── extract/
│   └── binance_extract.py      # Binance API extraction script
├── load/
│   └── load_to_postgres.py     # PostgreSQL loading script
├── dbt_project/
│   ├── models/
│   │   ├── staging/            # Cleaning and standardization
│   │   ├── intermediate/       # Enrichment and aggregation
│   │   └── marts/              # Analytics-ready tables
│   └── dbt_project.yml
├── .env.example                # Environment variable template
├── .gitignore
└── README.md
```

---

## Setup & Installation

### Prerequisites
- Ubuntu Linux
- Python 3.8+
- PostgreSQL 14+
- dbt Core (`pip install dbt-postgres`)
- Grafana (local install)

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/crypto-pipeline.git
cd crypto-pipeline
```

### 2. Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 4. Set up PostgreSQL
```bash
sudo -u postgres psql
```
```sql
CREATE USER binance_pipeline WITH PASSWORD 'your_password';
CREATE DATABASE binance_db OWNER binance_pipeline;
GRANT ALL PRIVILEGES ON DATABASE binance_db TO binance_pipeline;
```

### 5. Run the pipeline
```bash
# Extract from Binance API
python extract/binance_extract.py

# Load into PostgreSQL
python load/load_to_postgres.py

# Run dbt transformations
cd dbt_project
dbt run
dbt test
```

### 6. Configure dbt profile

Create `~/.dbt/profiles.yml`:
```yaml
dbt_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: binance_pipeline
      password: your_password
      dbname: binance_db
      schema: public
      threads: 4
```

### 7. Open Grafana

Navigate to `http://localhost:3000` and connect PostgreSQL
as a data source using your credentials.

---

## Automated Pipeline (Cron)

The pipeline runs automatically every hour via cron:
```bash
# Extract → Load → Transform
0  * * * * python extract/binance_extract.py
5  * * * * python load/load_to_postgres.py
10 * * * * cd dbt_project && dbt run
```

---

## dbt Documentation

Generate and serve interactive pipeline documentation:
```bash
cd dbt_project
dbt docs generate
dbt docs serve
```

Opens at `http://localhost:8080` — includes full lineage graph.

---

## What I Learned

- Designing and implementing a production ELT pipeline from scratch
- Working with REST APIs and handling real-world data quality issues
- PostgreSQL schema design for analytical workloads
- dbt data modeling — staging, intermediate, and mart layers
- Writing idempotent data loads using `ON CONFLICT DO NOTHING`
- SQL window functions for moving averages and volatility calculations
- Connecting Grafana to PostgreSQL for live dashboard visualization
- Scheduling pipelines with cron jobs

---

## Author

**Dennis Kirimi**
Risk Data Engineer
[LinkedIn](https://www.linkedin.com/in/denniskirimi1999/) ·
[Portfolio](https://denniskirimi.netlify.app/) ·
[GitHub](https://github.com/theedataengineer)

---

## License

MIT License — feel free to fork and build on this project.