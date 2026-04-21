# Databricks notebook source
# MAGIC %pip install pycoingecko --quiet
# MAGIC
# MAGIC from pycoingecko import CoinGeckoAPI
# MAGIC import pandas as pd
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.types import StructType, StructField, StringType
# MAGIC from datetime import datetime, timezone, timedelta
# MAGIC import time
# MAGIC import traceback
# MAGIC
# MAGIC cg = CoinGeckoAPI()
# MAGIC DB_NAME       = "crypto_db"
# MAGIC BRONZE_OHLC = f"{DB_NAME}.bronze_ohlc_data"

# COMMAND ----------

# ── Config ─────────────────────────────────────────────────────────────────
cg = CoinGeckoAPI()

DB_NAME             = "crypto_db"
BRONZE_OHLC         = f"{DB_NAME}.bronze_ohlc_data"
BRONZE_OHLC_ARCHIVE = f"{DB_NAME}.bronze_ohlc_data_archive"
INGESTION_LOG       = f"{DB_NAME}.ingestion_status_log"

MAX_RETRIES    = 4
BASE_BACKOFF_S = 2
MAX_BACKOFF_S  = 60
RATE_LIMIT_S   = 1.2   # CoinGecko Demo: 30 req/min

# COMMAND ----------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')

def write_ingestion_log(source: str, status: str, rows: int,
                        error_msg: str = "", retry_count: int = 0):
    log_row = [{
        "log_timestamp":  _now_utc(),
        "ingestion_date": _today_utc(),
        "source":         source,
        "status":         status,
        "rows_written":   rows,
        "retry_count":    retry_count,
        "error_message":  error_msg[:2000] if error_msg else "",
    }]
    log_sdf = spark.createDataFrame(log_row).select(
        F.col("log_timestamp").cast("timestamp"),
        F.col("ingestion_date"),
        F.col("source"),
        F.col("status"),
        F.col("rows_written").cast("int"),
        F.col("retry_count").cast("int"),
        F.col("error_message"),
    )
    exists = spark.catalog.tableExists(INGESTION_LOG)
    (log_sdf.write.format("delta")
        .mode("append" if exists else "overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(INGESTION_LOG))
    print(f"[LOG] {status} | source={source} | rows={rows} | retries={retry_count}")


# COMMAND ----------

# ── Get top 20 coin IDs (reuse market endpoint) ───────────────────
def get_top20_ids_with_retry() -> list:
    """Fetch coin IDs with retry; fall back to saved IDs if available."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            markets = cg.get_coins_markets(
                vs_currency='usd', order='market_cap_desc',
                per_page=20, page=1, sparkline=False)
            ids = [c['id'] for c in markets]
            print(f"✓ Got {len(ids)} coin IDs on attempt {attempt}")
            return ids
        except Exception as exc:
            last_exc = exc
            wait = min(BASE_BACKOFF_S * (2 ** (attempt - 1)), MAX_BACKOFF_S)
            print(f"⚠ Attempt {attempt}/{MAX_RETRIES}: {exc}. Retry in {wait}s")
            time.sleep(wait)

    # Fallback: use distinct coin_ids already in bronze OHLC
    if spark.catalog.tableExists(BRONZE_OHLC):
        ids = [r[0] for r in
               spark.table(BRONZE_OHLC).select("coin_id").distinct().collect()]
        print(f"↩ Fallback: {len(ids)} IDs from existing bronze table")
        return ids

    raise RuntimeError(f"Cannot retrieve coin IDs. Last error: {last_exc}")



# COMMAND ----------

# ── Per-coin OHLC fetch with exponential-backoff retry ───────────────────

def fetch_ohlc_single_coin(coin_id: str, days: int = 30) -> list:
    """
    Return list of raw rows for one coin.
    Raises on exhausted retries.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ohlc = cg.get_coin_ohlc_by_id(
                id=coin_id, vs_currency='usd', days=days)
            if ohlc is None:
                raise ValueError("Empty OHLC response")
            return ohlc
        except Exception as exc:
            last_exc = exc
            wait = min(BASE_BACKOFF_S * (2 ** (attempt - 1)), MAX_BACKOFF_S)
            print(f"  ⚠ {coin_id} attempt {attempt}/{MAX_RETRIES}: {exc}. Wait {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"{coin_id}: all retries failed — {last_exc}")


def fetch_ohlc_all_coins(coin_ids: list, days: int = 30,
                         run_id: str = "") -> pd.DataFrame:
    """
    Loop through all coins, collect OHLC rows, skip coins that fully fail.
    Returns a combined Pandas DataFrame.
    """
    all_records = []
    failed_coins = []

    for coin_id in coin_ids:
        try:
            ohlc_data = fetch_ohlc_single_coin(coin_id, days)
            now_ts = _now_utc()
            for row in ohlc_data:
                all_records.append({
                    'coin_id':             coin_id,
                    'timestamp_ms':        row[0],
                    'open':                row[1],
                    'high':                row[2],
                    'low':                 row[3],
                    'close':               row[4],
                    'ingestion_timestamp': now_ts,
                    'ingestion_date':      _today_utc(),
                    'days_requested':      days,
                    'source_api':          f'/coins/{coin_id}/ohlc',
                    'pipeline_run_id':     run_id,
                })
            print(f"  ✓ {coin_id}: {len(ohlc_data)} candles")
            time.sleep(RATE_LIMIT_S)

        except RuntimeError as exc:
            print(f"  ✗ {coin_id} skipped: {exc}")
            failed_coins.append(coin_id)
            time.sleep(2)

    if failed_coins:
        print(f"\n⚠ Failed coins ({len(failed_coins)}): {failed_coins}")

    pdf = pd.DataFrame(all_records)
    print(f"\n✓ Total OHLC records: {len(pdf)}")
    return pdf, failed_coins


# COMMAND ----------

# ── Enrich DataFrame before write ─────────────────────────────────────────

def enrich_ohlc_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf['candle_datetime'] = pd.to_datetime(pdf['timestamp_ms'], unit='ms', utc=True)
    pdf['candle_date']     = pdf['candle_datetime'].dt.strftime('%Y-%m-%d')
    return pdf


# COMMAND ----------

# ── Schema Evolution — Bronze OHLC Data ─────────────────────────────────────────

SCHEMA_REGISTRY_TABLE = f"{DB_NAME}.schema_evolution_log"

def log_schema_change(table_name: str, change_type: str,
                       column_name: str, old_dtype: str = '',
                       new_dtype: str = '', run_id: str = ''):
    """
    Persist schema change events to schema_evolution_log.
    change_type in {SCHEMA_INITIALISED, COLUMN_ADDED, COLUMN_DROPPED, TYPE_CHANGED}
    """
    row = [{
        'event_timestamp': _now_utc(),
        'run_id':          run_id,
        'table_name':      table_name,
        'change_type':     change_type,
        'column_name':     column_name,
        'old_dtype':       old_dtype,
        'new_dtype':       new_dtype,
    }]
    sdf = spark.createDataFrame(row)
    reg_exists = spark.catalog.tableExists(SCHEMA_REGISTRY_TABLE)
    (sdf.write.format('delta')
        .mode('append' if reg_exists else 'overwrite')
        .option('mergeSchema', 'true')
        .saveAsTable(SCHEMA_REGISTRY_TABLE))
    print(f'[SCHEMA] {change_type} | {table_name}.{column_name} {old_dtype} -> {new_dtype}')


def evolve_schema(new_sdf, target_table: str, run_id: str):
    """
    Reconcile incoming DataFrame schema against the live Delta table.
    Rules applied in order:
      1. New columns   -> mergeSchema handles write; event logged.
      2. Dropped cols  -> NULL-filled in new_sdf so MERGE does not fail.
      3. Type conflict -> widen both sides to StringType; logged for review.
    Returns a reconciled sdf safe to MERGE.
    """
    from pyspark.sql.functions import lit, col as scol
    from pyspark.sql.types import StringType

    if not spark.catalog.tableExists(target_table):
        # First run — log initial schema
        for field in new_sdf.schema.fields:
            log_schema_change(target_table, 'SCHEMA_INITIALISED',
                              field.name, new_dtype=str(field.dataType),
                              run_id=run_id)
        return new_sdf

    existing = {f.name: f.dataType for f in spark.table(target_table).schema.fields}
    incoming = {f.name: f.dataType for f in new_sdf.schema.fields}
    reconciled = new_sdf

    # 1. New columns in incoming batch
    for col_name, dtype in incoming.items():
        if col_name not in existing:
            print(f'[SCHEMA] New column: {col_name} ({dtype}) -> will be added via mergeSchema')
            log_schema_change(target_table, 'COLUMN_ADDED',
                              col_name, new_dtype=str(dtype), run_id=run_id)

    # 2. Columns present in target but absent from incoming -> NULL-fill
    for col_name, dtype in existing.items():
        if col_name not in incoming:
            print(f'[SCHEMA] Column missing in new batch: {col_name} -> back-filling NULL')
            reconciled = reconciled.withColumn(col_name, lit(None).cast(dtype))
            log_schema_change(target_table, 'COLUMN_DROPPED',
                              col_name, old_dtype=str(dtype), run_id=run_id)

    # 3. Type conflicts -> widen to StringType
    for col_name in set(incoming) & set(existing):
        if type(incoming[col_name]) != type(existing[col_name]):
            print(f'[SCHEMA] Type conflict on {col_name}: '
                  f'{existing[col_name]} -> {incoming[col_name]} -- widening to StringType')
            reconciled = reconciled.withColumn(col_name, scol(col_name).cast(StringType()))
            log_schema_change(target_table, 'TYPE_CHANGED', col_name,
                              old_dtype=str(existing[col_name]),
                              new_dtype='StringType', run_id=run_id)

    return reconciled


print('Schema evolution helpers loaded for Bronze OHLC Data')


# COMMAND ----------

# ── Idempotent write via MERGE ────────────────────────────────────────────

def write_bronze_ohlc(pdf, run_id: str) -> int:
    """
    1. Schema evolution check  (new/dropped cols, type conflicts)
    2. MERGE on (coin_id, timestamp_ms) — no duplicates on re-runs
    3. APPEND archive snapshot with run_id
    """
    if pdf.empty:
        print('No OHLC data to write — skipping')
        return 0

    pdf = enrich_ohlc_pdf(pdf)
    sdf = spark.createDataFrame(pdf)

    # ── Schema evolution ─────────────────────────────────────────────────
    sdf = evolve_schema(sdf, BRONZE_OHLC, run_id)

    # ── Current table: MERGE ────────────────────────────────────────────
    exists = spark.catalog.tableExists(BRONZE_OHLC)
    if not exists:
        (sdf.write.format('delta')
            .mode('overwrite')
            .option('overwriteSchema', 'true')
            .partitionBy('coin_id', 'candle_date')
            .saveAsTable(BRONZE_OHLC))
        print(f'Created {BRONZE_OHLC}')
    else:
        sdf.createOrReplaceTempView('_new_ohlc_data')
        spark.sql(f"""
            MERGE INTO {BRONZE_OHLC} AS tgt
            USING _new_ohlc_data AS src
              ON  tgt.coin_id      = src.coin_id
              AND tgt.timestamp_ms = src.timestamp_ms
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f'Merged into {BRONZE_OHLC}')

    # ── Archive: always append, absorbs new cols via mergeSchema ─────────
    sdf_arc = sdf.withColumn('archive_run_id', F.lit(run_id))
    arc_exists = spark.catalog.tableExists(BRONZE_OHLC_ARCHIVE)
    (sdf_arc.write.format('delta')
        .mode('append' if arc_exists else 'overwrite')
        .option('mergeSchema', 'true')
        .partitionBy('coin_id', 'ingestion_date')
        .saveAsTable(BRONZE_OHLC_ARCHIVE))
    print(f'Archive appended — run_id={run_id}')

    current_cnt = spark.table(BRONZE_OHLC).count()
    archive_cnt = spark.table(BRONZE_OHLC_ARCHIVE).count()
    print(f'  Current rows : {current_cnt}')
    print(f'  Archive rows : {archive_cnt}')
    return current_cnt


# COMMAND ----------

# ── Delta optimisation ────────────────────────────────────────────────────

def optimise_bronze_ohlc():
    try:
        spark.sql(f"OPTIMIZE {BRONZE_OHLC} ZORDER BY (timestamp_ms)")
        print(f"✓ OPTIMIZE + ZORDER done on {BRONZE_OHLC}")
    except Exception as exc:
        print(f"⚠ OPTIMIZE skipped: {exc}")

# COMMAND ----------

# ── Backfill detector ────────────────────────────────────────────────────

def detect_missing_ohlc_days(coin_ids: list, lookback_days: int = 7):
    """
    Check whether each expected candle_date exists for each coin.
    Logs BACKFILL_REQUIRED for any gap (Demo API cannot retrieve historical OHLC).
    """
    if not spark.catalog.tableExists(BRONZE_OHLC):
        return
    existing = {
    (r['coin_id'], r['candle_date'])
    for r in spark.table(BRONZE_OHLC)
        .select("coin_id","candle_date").distinct().collect()
}
    today = datetime.now(timezone.utc).date()
    missing = []
    for coin_id in coin_ids:
        for offset in range(1, lookback_days + 1):
            d = (today - timedelta(days=offset)).strftime('%Y-%m-%d')
            if (coin_id, d) not in existing:
                missing.append(f"{coin_id}@{d}")
    if missing:
        print(f"⚠ Missing OHLC candles ({len(missing)}): {missing[:10]} …")
        write_ingestion_log("bronze_ohlc_data", "BACKFILL_REQUIRED",
                            0, f"Gaps: {missing[:5]}", 0)
    else:
        print("✓ No OHLC gaps in lookback window")


# COMMAND ----------

# ── Fallback: reuse last archive snapshot for failed coins ────────────────

def fallback_for_failed_coins(failed_ids: list, run_id: str):
    """
    If any coins failed entirely, copy their most-recent rows from the archive
    into the current table so the silver layer is never starved.
    """
    if not failed_ids or not spark.catalog.tableExists(BRONZE_OHLC_ARCHIVE):
        return
    print(f"\n↩ Applying archive fallback for {len(failed_ids)} failed coins …")
    df_fallback = (spark.table(BRONZE_OHLC_ARCHIVE)
        .filter(F.col("coin_id").isin(failed_ids))
        .withColumn("rn", F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.partitionBy("coin_id","timestamp_ms")
            .orderBy(F.col("archive_run_id").desc())))
        .filter(F.col("rn") == 1).drop("rn")
        .withColumn("ingestion_date",      F.lit(_today_utc()))
        .withColumn("ingestion_timestamp", F.lit(_now_utc()).cast("timestamp"))
        .withColumn("pipeline_run_id",     F.lit(run_id))
        .withColumn("is_fallback",         F.lit(True))
    )
    if df_fallback.count() == 0:
        print("  ↩ No archive rows available for failed coins")
        return
    df_fallback.createOrReplaceTempView("_fallback_ohlc")
    spark.sql(f"""
        MERGE INTO {BRONZE_OHLC} AS tgt
        USING _fallback_ohlc AS src
          ON  tgt.coin_id      = src.coin_id
          AND tgt.timestamp_ms = src.timestamp_ms
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  ✓ Fallback rows inserted for: {failed_ids}")
    write_ingestion_log("bronze_ohlc_data", "FALLBACK",
                        df_fallback.count(), f"coins={failed_ids}", 0)

# COMMAND ----------

from pyspark.sql.functions import desc# ── Main orchestrator ─────────────────────────────────────────────────────


def run_ohlc_ingestion(days: int = 30):
    run_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    print(f"\n{'='*60}")
    print(f" OHLC Ingestion  run_id={run_id}  {_now_utc()}")
    print(f"{'='*60}")

    try:
        top20_ids = get_top20_ids_with_retry()
    except RuntimeError as exc:
        write_ingestion_log("bronze_ohlc_data", "FAILURE", 0, str(exc), MAX_RETRIES)
        raise

    pdf_ohlc,failed_ids = fetch_ohlc_all_coins(top20_ids, days=days, run_id=run_id)

    try:
        rows = write_bronze_ohlc(pdf_ohlc, run_id)
        fallback_for_failed_coins(failed_ids, run_id)
        optimise_bronze_ohlc()
        detect_missing_ohlc_days(top20_ids, lookback_days=7)
        write_ingestion_log("bronze_ohlc_data", "SUCCESS", rows)

        # ── BTC preview ─────────────────────────────────────────────────
        print("\n=== OHLC Preview (BTC, last 3 candles) ===")
        spark.table(BRONZE_OHLC).filter(F.col("coin_id") == "bitcoin").orderBy(
            F.col("timestamp_ms").desc()).select(
            "coin_id","candle_date","open","high","low","close"
        ).show(3, truncate=False)

    except Exception as exc:
        tb = traceback.format_exc()
        write_ingestion_log("bronze_ohlc_data", "FAILURE", 0, tb[:2000], 0)
        raise

run_ohlc_ingestion(days=30)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS crypto_space.crypto_db.bronze_ohlc_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from crypto_space.crypto_db.bronze_ohlc_data;

# COMMAND ----------


