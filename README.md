
# 🪙 Crypto Market Intelligence Pipeline

A production-grade **Databricks + Delta Lake** data pipeline that ingests live cryptocurrency data from the **CoinGecko API**, processes it through a **Medallion Architecture** (Bronze → Silver → Gold), and serves a real-time **Crypto Market Intelligence Dashboard**.

---

## 📐 Architecture Overview
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/2fd47a57-e643-419f-ac81-8c3ed0bd39b1" />


## 📁 Project Structure

```
crypto_pipeline/
│
├── bronze/
│   ├── cluster_init.py          # Cluster initialisation: DB setup, library installs, global config
│   ├── 01_ingest_market_data.py # Fetch live market data (prices, market cap, volume, rankings)
│   ├── 02_ingest_ohlc_data.py   # Fetch 4-hour OHLC candlestick data (30-day history)
│   └── ingest_trending_data.py  # Fetch top trending coins & write to Bronze + Gold
│
├── silver/
│   ├── 03a_silver_dim_tables.py # Build Star Schema dimensions: dim_coin, dim_date, dim_date_hour
│   ├── 03b_silver_fact_market.py# Build silver_market_metrics fact table (DQ, anomaly, quarantine)
│   └── 03c_silver_fact_ohlc.py  # Build silver_ohlc_metrics fact table (candle validation, ABC)
│
├── gold/
│   └── 05_gold_trader_view.py   # Build all Gold tables: snapshots, OHLC, performance, summary
│
└── Crypto Market Intelligence.lvdash.json  # Databricks Lakeview dashboard definition
```

---

## 🔑 Key Features

- **Medallion Architecture** — structured Bronze → Silver → Gold data flow on Delta Lake
- **Star Schema (Silver)** — dim/fact separation for efficient BI slicing and joining
- **Exponential Backoff Retry** — CoinGecko API calls are fault-tolerant with configurable retries
- **Idempotent MERGE Operations** — all writes use Delta MERGE to prevent duplicate rows
- **Data Quality & Quarantine** — invalid/anomalous rows are quarantined and logged, never silently dropped
- **Ingestion Logging** — every pipeline run appends to `ingestion_status_log` with status, row counts, and error messages
- **Trading Signals** — Gold layer computes signals (RSI, moving averages, momentum) for trader views
- **30-Day OHLC History** — 4-hour candles with Bollinger Bands, ATR, and support/resistance levels
- **Lakeview Dashboard** — pre-built Databricks dashboard JSON for immediate import

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Compute | Databricks (Apache Spark) |
| Storage | Delta Lake (managed tables) |
| Language | Python (PySpark) |
| Data Source | CoinGecko API (`pycoingecko`) |
| Dashboard | Databricks Lakeview |
| Data Format | Delta (Parquet + transaction log) |

---

## ⚙️ Setup & Prerequisites

### 1. Databricks Workspace
You need access to a Databricks workspace with:
- A running cluster (DBR 12.x or later recommended)
- Unity Catalog **or** Hive Metastore enabled

### 2. Python Libraries
The notebooks install dependencies automatically via `%pip install`. Libraries used:

```
pycoingecko
requests
pandas
pyspark (included in Databricks runtime)
```

### 3. CoinGecko API
- The pipeline uses the **free CoinGecko Demo API** (no key required for basic endpoints).
- Rate limit is respected via a `1.2s` delay between calls (`RATE_LIMIT_S = 1.2`).
- For higher throughput, configure a CoinGecko Pro API key and update `CoinGeckoAPI()` accordingly.

---

## 🚀 Running the Pipeline

Run the notebooks **in order** from your Databricks workspace:

```
Step 1 — Cluster Init
  bronze/cluster_init.py
  → Creates the crypto_db database and installs all dependencies

Step 2 — Bronze Ingestion (can run in parallel)
  bronze/01_ingest_market_data.py   → Live market prices & rankings
  bronze/02_ingest_ohlc_data.py     → 30-day 4h OHLC candles
  bronze/ingest_trending_data.py    → Today's top trending coins

Step 3 — Silver Transformation (run sequentially)
  silver/03a_silver_dim_tables.py   → Dimension tables (run first)
  silver/03b_silver_fact_market.py  → Market fact table
  silver/03c_silver_fact_ohlc.py    → OHLC fact table

Step 4 — Gold Aggregation
  gold/05_gold_trader_view.py       → All Gold tables + trading signals
```

> **Tip:** Schedule these as a Databricks Job with task dependencies to automate the pipeline end-to-end.

---

## 📊 Delta Tables Reference

### Bronze
| Table | Description | Key Columns |
|---|---|---|
| `bronze_market_data` | Raw market snapshot per coin | `id`, `ingestion_date`, `current_price`, `market_cap` |
| `bronze_market_data_archive` | Historical archive of all snapshots | Same as above |
| `bronze_ohlc_data` | Raw 4h OHLC candles | `coin_id`, `timestamp_ms`, `open`, `high`, `low`, `close` |
| `bronze_trending_data` | Top trending coins from CoinGecko | `coin_id`, `trending_rank`, `score` |

### Silver (Star Schema)
| Table | Type | Grain |
|---|---|---|
| `dim_coin` | Dimension | 1 row per coin (SCD Type-1) |
| `dim_date` | Dimension | 1 row per calendar date |
| `dim_date_hour` | Dimension | 1 row per date + hour |
| `silver_market_metrics` | Fact | 1 row per (coin_id, ingestion_date) |
| `silver_ohlc_metrics` | Fact | 1 row per (coin_id, timestamp_ms) |

### Gold
| Table | Powers |
|---|---|
| `gold_market_snapshot` | Latest per-coin price, signals, performance label |
| `gold_ohlc_history` | 30-day candlestick chart |
| `gold_performance_metrics` | Top gainers / losers |
| `gold_market_summary` | Market-wide KPIs (total market cap, dominance) |
| `gold_trending_coins` | Trending coins enriched with price & trading signals |

### Operational
| Table | Description |
|---|---|
| `ingestion_status_log` | Audit log for every pipeline run |
| `silver_market_quarantine` | Rows that failed DQ checks |
| `silver_ohlc_quarantine` | OHLC rows that failed candle validation |
| `silver_data_quality_log` | Per-column DQ metric history |

---

## 📈 Dashboard
<img width="881" height="573" alt="Screenshot 2026-04-21 at 12 30 41 PM" src="https://github.com/user-attachments/assets/883ac8a6-31fb-461d-aebb-21b943b8c1b0" />

<img width="886" height="724" alt="Screenshot 2026-04-21 at 12 30 21 PM" src="https://github.com/user-attachments/assets/85db93fe-8172-4c78-a7f3-7abddda6ff0c" />

<img width="886" height="814" alt="Screenshot 2026-04-21 at 12 29 56 PM" src="https://github.com/user-attachments/assets/1268bb10-78e8-471f-a1e0-6abf32cb9ba4" />

<img width="915" height="801" alt="Screenshot 2026-04-21 at 12 29 11 PM" src="https://github.com/user-attachments/assets/b709c2f2-9c3e-4485-a44e-0dc4103fe0ce" />

## 📌 Job & Pipelines 

Jobs : 
<img width="927" height="400" alt="Screenshot 2026-04-21 at 12 46 04 PM" src="https://github.com/user-attachments/assets/321bd6af-5d7d-4214-b570-0a2c39734591" />

Alert : 

<img width="703" height="547" alt="Screenshot 2026-04-21 at 12 46 57 PM" src="https://github.com/user-attachments/assets/07aa41a3-c403-4eb6-9a41-7987d8dc2909" />



## 🔧 Configuration

Key constants are defined at the top of each notebook. Common ones to adjust:

| Parameter | Default | Description |
|---|---|---|
| `DB_NAME` | `crypto_db` | Databricks database name |
| `MAX_RETRIES` | `4` | API retry attempts before failure |
| `BASE_BACKOFF_S` | `2` | Initial retry wait (doubles each attempt) |
| `MAX_BACKOFF_S` | `60` | Maximum retry wait in seconds |
| `RATE_LIMIT_S` | `1.2` | Delay between API calls |
| `LOOKBACK_DAYS` | `30` | Days of OHLC history to fetch |
| `CANDLE_HOURS` | `4` | OHLC candle size in hours |

---

## 📌 Notes

- All Delta tables use **idempotent MERGE** — re-running any notebook is safe and will not create duplicates.
- The Silver layer quarantines bad data rather than dropping it — check `silver_market_quarantine` if row counts look low.
- The pipeline is designed for **daily scheduled runs** but can be triggered on-demand at any time.
- `ingestion_status_log` is your first stop for debugging any pipeline issues.

---

## 📄 License

This project is for educational and personal use. CoinGecko data is subject to [CoinGecko's Terms of Service](https://www.coingecko.com/en/terms).
