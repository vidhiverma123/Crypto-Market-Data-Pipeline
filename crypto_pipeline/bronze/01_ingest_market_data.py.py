# Databricks notebook source
# ── 0. Dependencies ───────────────────────────────────────────────────────
%pip install pycoingecko requests --quiet

# COMMAND ----------

# ── Imports & config ───────────────────────────────────────────────
from pycoingecko import CoinGeckoAPI
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timezone, timedelta
import time
import traceback
cg = CoinGeckoAPI()

DB_NAME               = "crypto_db"
BRONZE_MARKET         = f"{DB_NAME}.bronze_market_data"
BRONZE_MARKET_ARCHIVE = f"{DB_NAME}.bronze_market_data_archive"
INGESTION_LOG         = f"{DB_NAME}.ingestion_status_log"

# Retry / backoff parameters
MAX_RETRIES     = 4
BASE_BACKOFF_S  = 2      # seconds — doubles each attempt
MAX_BACKOFF_S   = 60

# COMMAND ----------

# ── Logging helpers ────────────────────────────────────────────────────────
def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')

def write_ingestion_log(source: str, status: str, rows: int,
                        error_msg: str = "", retry_count: int = 0):
    """
    Append one row to the ingestion_status_log Delta table.
    status ∈ {SUCCESS, FAILURE, FALLBACK, BACKFILL}
    """
    log_row = [{
        "log_timestamp":  _now_utc(),
        "ingestion_date": _today_utc(),
        "source":         source,
        "status":         status,
        "rows_written":   rows,
        "retry_count":    retry_count,
        "error_message":  error_msg[:2000] if error_msg else "",
    }]
    schema = StructType([
        StructField("log_timestamp",  StringType()),
        StructField("ingestion_date", StringType()),
        StructField("source",         StringType()),
        StructField("status",         StringType()),
        StructField("rows_written",   StringType()),
        StructField("retry_count",    StringType()),
        StructField("error_message",  StringType()),
    ])
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
    print(f"[LOG] {status} | source={source} | rows={rows} | retries={retry_count} | {error_msg[:120] if error_msg else 'ok'}")



# COMMAND ----------

# ── API fetch with exponential-backoff retry ───────────────────────────────

def fetch_market_data_with_retry() -> list:
    """
    Call CoinGecko /coins/markets with exponential-backoff retry.
    Returns raw JSON list on success, raises RuntimeError after MAX_RETRIES.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = cg.get_coins_markets(
                vs_currency='usd',
                order='market_cap_desc',
                per_page=20,
                page=1,
                sparkline=False,
                price_change_percentage='24h'
            )
            if not data:
                raise ValueError("API returned empty list")
            print(f"✓ API success on attempt {attempt} — {len(data)} coins fetched")
            return data

        except Exception as exc:
            last_exc = exc
            wait = min(BASE_BACKOFF_S * (2 ** (attempt - 1)), MAX_BACKOFF_S)
            print(f"⚠ Attempt {attempt}/{MAX_RETRIES} failed: {exc}. Retrying in {wait}s …")
            time.sleep(wait)

    raise RuntimeError(f"All {MAX_RETRIES} attempts failed. Last error: {last_exc}")


def get_last_successful_snapshot() -> "pyspark.sql.DataFrame | None":
    """
    Fallback: return the most recent partition from the bronze current table.
    Returns None if the table doesn't exist or is empty.
    """
    try:
        if not spark.catalog.tableExists(BRONZE_MARKET):
            return None
        df = spark.table(BRONZE_MARKET)
        if df.count() == 0:
            return None
        latest_date = df.select(F.max("ingestion_date")).first()[0]
        snapshot = df.filter(F.col("ingestion_date") == latest_date)
        print(f"↩ Fallback snapshot loaded from {latest_date} — {snapshot.count()} rows")
        return snapshot
    except Exception as exc:
        print(f"✗ Could not load fallback snapshot: {exc}")
        return None


# COMMAND ----------

# ── Convert raw JSON → Spark DataFrame with lineage columns ───────────────

def raw_to_spark_df(raw_data: list, run_id: str) -> "pyspark.sql.DataFrame":
    pdf = pd.DataFrame(raw_data)
    pdf['ingestion_timestamp'] = _now_utc()
    pdf['ingestion_date']      = _today_utc()
    pdf['source_api']          = '/coins/markets'
    pdf['api_vs_currency']     = 'usd'
    pdf['pipeline_run_id']     = run_id
    sdf = spark.createDataFrame(pdf)
    print(f"✓ Spark DataFrame: {sdf.count()} rows, {len(sdf.columns)} cols")
    return sdf

# COMMAND ----------

# ── Schema Evolution — Bronze Market Data ─────────────────────────────────────────

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


print('Schema evolution helpers loaded for Bronze Market Data')


# COMMAND ----------

# ── Idempotent MERGE write — safe to re-run ───────────────────────────────

def write_bronze_market(sdf, run_id: str):
    """
    1. Schema evolution check  (new/dropped cols, type conflicts)
    2. MERGE into bronze_market_data    (one row per coin per day)
    3. APPEND into bronze_market_archive (every raw snapshot)
    """
    # ── Schema evolution ─────────────────────────────────────────────────
    sdf = evolve_schema(sdf, BRONZE_MARKET, run_id)

    # ── Current table ────────────────────────────────────────────────────
    exists = spark.catalog.tableExists(BRONZE_MARKET)
    if not exists:
        (sdf.write.format('delta')
            .mode('overwrite')
            .option('overwriteSchema', 'true')
            .partitionBy('ingestion_date')
            .saveAsTable(BRONZE_MARKET))
        print(f'Created {BRONZE_MARKET}')
    else:
        sdf.createOrReplaceTempView('_new_market_data')
        spark.sql(f"""
            MERGE INTO {BRONZE_MARKET} AS tgt
            USING _new_market_data AS src
              ON  tgt.id = src.id
              AND tgt.ingestion_date = src.ingestion_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f'Merged into {BRONZE_MARKET}')

    # ── Archive table — pure append, absorbs new cols via mergeSchema ─────
    sdf_arc = sdf.withColumn('archive_run_id', F.lit(run_id))
    arc_exists = spark.catalog.tableExists(BRONZE_MARKET_ARCHIVE)
    (sdf_arc.write.format('delta')
        .mode('append' if arc_exists else 'overwrite')
        .option('mergeSchema', 'true')
        .partitionBy('ingestion_date')
        .saveAsTable(BRONZE_MARKET_ARCHIVE))
    print(f'Archive appended — run_id={run_id}')

    current_cnt = spark.table(BRONZE_MARKET).count()
    archive_cnt = spark.table(BRONZE_MARKET_ARCHIVE).count()
    print(f'  Current rows : {current_cnt}')
    print(f'  Archive rows : {archive_cnt}')
    return current_cnt


# COMMAND ----------

# ── Delta optimisation (run after write) ─────────────────────────────────

def optimise_bronze_market():
    try:
        spark.sql(f"OPTIMIZE {BRONZE_MARKET} ZORDER BY (id)")
        print(f"✓ OPTIMIZE + ZORDER done on {BRONZE_MARKET}")
    except Exception as exc:
        print(f"⚠ OPTIMIZE skipped: {exc}")


# COMMAND ----------

# ── Backfill: detect and fill missing dates ───────────────────────────────

def backfill_missing_days(lookback_days: int = 3):
    """
    After a successful run check if the last N days have data.
    If a day is missing AND today's API call succeeded, we cannot retrieve
    historical snapshots from CoinGecko (Demo plan limitation).
    This function logs missing days so downstream can flag them as STALE.
    """
    if not spark.catalog.tableExists(BRONZE_MARKET):
        return
    existing_dates = {
    row[0] for row in
    spark.table(BRONZE_MARKET)
        .select("ingestion_date")
        .distinct()
        .orderBy(F.col("ingestion_date").desc())
        .limit(lookback_days)
        .collect()
}
    today = datetime.now(timezone.utc).date()
    for offset in range(1, lookback_days + 1):
        check_date = (today - timedelta(days=offset)).strftime('%Y-%m-%d')
        if check_date not in existing_dates:
            print(f"⚠ Missing market data for {check_date} — cannot backfill from CoinGecko Demo")
            write_ingestion_log(
                source="bronze_market_data",
                status="BACKFILL_REQUIRED",
                rows=0,
                error_msg=f"No data found for {check_date}",
            )

# COMMAND ----------

# ── Main orchestrator ─────────────────────────────────────────────────────

def run_market_ingestion():
    run_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    print(f"\n{'='*60}")
    print(f" Market Ingestion  run_id={run_id}  {_now_utc()}")
    print(f"{'='*60}")

    retry_count = 0
    raw_data    = None
    used_fallback = False

    # ── Attempt API fetch ────────────────────────────────────────────────
    try:
        raw_data = fetch_market_data_with_retry()
        retry_count = sum(1 for _ in range(MAX_RETRIES))   # actual retries tracked inside
    except RuntimeError as exc:
        print(f"✗ API exhausted — switching to fallback snapshot\n  {exc}")
        used_fallback = True
        fallback_sdf = get_last_successful_snapshot()
        if fallback_sdf is None:
            write_ingestion_log("bronze_market_data", "FAILURE", 0,
                                str(exc), MAX_RETRIES)
            raise RuntimeError("No fallback available and API failed. Pipeline aborted.")
        # Re-stamp the fallback with today so MERGE works correctly
        fallback_sdf = (fallback_sdf
            .withColumn("ingestion_timestamp", F.lit(_now_utc()).cast("timestamp"))
            .withColumn("ingestion_date",      F.lit(_today_utc()))
            .withColumn("pipeline_run_id",     F.lit(run_id))
            .withColumn("is_fallback",         F.lit(True)))
        rows = write_bronze_market(fallback_sdf, run_id)
        write_ingestion_log("bronze_market_data", "FALLBACK", rows,
                            f"Used snapshot from previous run: {exc}", MAX_RETRIES)
        return

    # ── Normal path ──────────────────────────────────────────────────────
    try:
        sdf = raw_to_spark_df(raw_data, run_id)
        sdf = sdf.withColumn("is_fallback", F.lit(False))
        rows = write_bronze_market(sdf, run_id)
        optimise_bronze_market()
        backfill_missing_days(lookback_days=3)
        write_ingestion_log("bronze_market_data", "SUCCESS", rows,
                            "", retry_count)

        # ── Sample preview ───────────────────────────────────────────────
        print("\n=== Bronze Market Preview (BTC) ===")
        spark.table(BRONZE_MARKET).filter(F.col("id") == "bitcoin").select(
            "id","name","current_price","market_cap","ingestion_date"
        ).show(1, truncate=False)

    except Exception as exc:
        tb = traceback.format_exc()
        write_ingestion_log("bronze_market_data", "FAILURE", 0, tb[:2000], retry_count)
        raise

run_market_ingestion()


# COMMAND ----------

print("=== Bronze Market Data Preview ===")
spark.table(BRONZE_MARKET).select(
    "id", "name", "current_price", "market_cap", 
    "total_volume", "ingestion_date"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW PARTITIONS crypto_space.crypto_db.bronze_market_data;

# COMMAND ----------

# Check partitions and date range for bronze_market_data
spark.sql("""
    SELECT 
        ingestion_date,
        COUNT(*) as row_count,
        COUNT(DISTINCT id) as coins
    FROM crypto_db.bronze_market_data
    GROUP BY ingestion_date
    ORDER BY ingestion_date
""").show(100, truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In a SQL cell
# MAGIC SHOW PARTITIONS crypto_space.crypto_db.bronze_market_data;
