# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS crypto_db")
spark.sql("USE crypto_db")
print("Database ready")

# COMMAND ----------

# ── Install libraries ──────────────────────────────────────────────
%pip install pycoingecko requests pandas

# COMMAND ----------

# ── Confirm installs ───────────────────────────────────────────────
from pycoingecko import CoinGeckoAPI
import requests, pandas as pd
print("All libraries loaded ✓")

# COMMAND ----------

# ── Global config  ───────────────────────────

# Database and table names — don't change these
DB_NAME         = "crypto_db"
BRONZE_MARKET   = f"{DB_NAME}.bronze_market_data"
BRONZE_OHLC     = f"{DB_NAME}.bronze_ohlc_data"
SILVER_MARKET   = f"{DB_NAME}.silver_market_data"
SILVER_OHLC     = f"{DB_NAME}.silver_ohlc_metrics"
GOLD_TRADER     = f"{DB_NAME}.gold_trader_signals"
GOLD_INVESTOR   = f"{DB_NAME}.gold_investor_signals"
GOLD_MARKET_SNAP= f"{DB_NAME}.gold_market_snapshot"
TOP_N_COINS = 20
# Delta table paths (stored inside Databricks managed storage)
DELTA_BASE = "dbfs:/user/hive/warehouse/crypto_db.db"

print("Config loaded ✓")
print(f"Tables will be stored at: {DELTA_BASE}")

# COMMAND ----------


