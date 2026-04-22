# Databricks notebook source
# ── 0. Dependencies ───────────────────────────────────────────────────────
%pip install pycoingecko requests --quiet

# COMMAND ----------

# ── Imports & config ───────────────────────────────────────────────────────
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
BRONZE_MARKET_QUAR    = f"{DB_NAME}.bronze_market_data_quarantine"   # NEW: severe partial loads
INGESTION_LOG         = f"{DB_NAME}.ingestion_status_log"
 
# Retry / backoff parameters
MAX_RETRIES     = 4
BASE_BACKOFF_S  = 2
MAX_BACKOFF_S   = 60
 
# ── Partial load detection constants (Change Request 1) ──────────────────
EXPECTED_COINS     = 20
FETCH_BUFFER       = 25      # fetch more to absorb dupes
WARN_THRESHOLD     = 0.80    # <80%  → PARTIAL_LOAD
CRITICAL_THRESHOLD = 0.50    # <50%  → PARTIAL_LOAD_CRITICAL
ABORT_THRESHOLD    = 0.10    # <10%  → FAILURE
 
# Known coin IDs used for stale reconciliation
ALL_EXPECTED_IDS = [
    'bitcoin', 'ethereum', 'tether', 'ripple', 'binancecoin',
    'usd-coin', 'solana', 'tron', 'figure-heloc', 'dogecoin',
    'whitebit', 'usds', 'leo-token', 'hyperliquid', 'cardano',
    'bitcoin-cash', 'memecore', 'monero', 'chainlink', 'stellar',
]
 

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
    status ∈ {SUCCESS, PARTIAL_LOAD, PARTIAL_LOAD_CRITICAL, PARTIAL_LOAD_SEVERE,
              FAILURE, FALLBACK, BACKFILL}
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

# ── Partial load quality helpers (Change Request 1) ──────────────────────
 
def evaluate_load_quality(received: int, expected: int) -> str:
    """Returns one of five status strings based on received/expected ratio."""
    ratio = received / expected
    if ratio >= 1.0:
        return 'SUCCESS'
    elif ratio >= WARN_THRESHOLD:
        return 'PARTIAL_LOAD'
    elif ratio >= CRITICAL_THRESHOLD:
        return 'PARTIAL_LOAD_CRITICAL'
    elif ratio >= ABORT_THRESHOLD:
        return 'PARTIAL_LOAD_SEVERE'
    else:
        return 'FAILURE'
 
 
def dedupe_with_conflict_check(raw_data: list, run_id: str):
    """
    Groups by coin id; if all rows identical → keep first.
    If prices differ → keep first but log DATA_QUALITY_WARNING.
    Returns: (deduped_list, conflict_coin_ids)
    """
    seen = {}
    conflict_ids = []
    for item in raw_data:
        coin_id = item.get('id')
        if coin_id not in seen:
            seen[coin_id] = item
        else:
            # Check for price conflict
            if item.get('current_price') != seen[coin_id].get('current_price'):
                conflict_ids.append(coin_id)
                print(f"[DATA_QUALITY_WARNING] Price conflict on {coin_id} — keeping first occurrence")
    return list(seen.values()), conflict_ids
 
 
def refetch_missing_slots(deduped: list, expected: int, run_id: str) -> list:
    """
    Called when deduping leaves us below EXPECTED_COINS.
    Fetches page 2 with a small buffer — single extra API call.
    """
    missing_count = expected - len(deduped)
    existing_ids  = {c['id'] for c in deduped}
    print(f"  ↩ Refetching {missing_count * 2} candidates from page 2 to fill {missing_count} slots …")
    try:
        extra = cg.get_coins_markets(
            vs_currency='usd',
            order='market_cap_desc',
            per_page=missing_count * 2,
            page=2,
            sparkline=False,
            price_change_percentage='24h'
        )
        fills = [c for c in extra if c['id'] not in existing_ids]
        result = deduped + fills[:missing_count]
        print(f"  ↩ After refetch: {len(result)} coins")
        return result
    except Exception as exc:
        print(f"  ⚠ Refetch failed: {exc} — proceeding with {len(deduped)} coins")
        return deduped
 
 
def reconcile_partial_load(sdf_incoming, run_id: str):
    """
    After any partial load, carry forward last known rows for missing coins
    stamped as load_status='STALE'. Prevents silent stale data in silver.
    """
    incoming_ids = {r[0] for r in sdf_incoming.select('id').collect()}
    missing_ids  = set(ALL_EXPECTED_IDS) - incoming_ids
    if not missing_ids:
        return None
    print(f"  ↩ Reconciling {len(missing_ids)} missing coins as STALE: {list(missing_ids)[:5]} …")
    if not spark.catalog.tableExists(BRONZE_MARKET):
        return None
    last_known = (spark.table(BRONZE_MARKET)
        .filter(F.col('id').isin(list(missing_ids)))
        .withColumn('_rn', F.row_number().over(
            __import__('pyspark.sql.window', fromlist=['Window'])
            .Window.partitionBy('id').orderBy(F.col('ingestion_date').desc())))
        .filter(F.col('_rn') == 1).drop('_rn')
        .withColumn('ingestion_date',      F.lit(_today_utc()))
        .withColumn('ingestion_timestamp', F.lit(_now_utc()))
        .withColumn('pipeline_run_id',     F.lit(run_id))
        .withColumn('load_status',         F.lit('STALE'))
        .withColumn('is_fallback',         F.lit(True))
        .withColumn('coins_expected',      F.lit(EXPECTED_COINS))
        .withColumn('coins_received',      F.lit(len(incoming_ids)))
    )
    return last_known

# COMMAND ----------

# ── API fetch with partial load logic (Change Request 1) ─────────────────
 
def fetch_market_data_with_retry() -> tuple:
    """
    Call CoinGecko /coins/markets with FETCH_BUFFER, dedupe, refetch if needed.
    Returns (data_list, load_status_string).
    Raises RuntimeError after MAX_RETRIES if fetch completely fails.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            raw = cg.get_coins_markets(
                vs_currency='usd',
                order='market_cap_desc',
                per_page=FETCH_BUFFER,    # fetch buffer > expected
                page=1,
                sparkline=False,
                price_change_percentage='24h'
            )
            if not raw:
                raise ValueError("API returned empty list")
 
            run_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
 
            # Dedupe
            data, conflict_ids = dedupe_with_conflict_check(raw, run_id)
            if conflict_ids:
                print(f"⚠ Price conflicts on: {conflict_ids}")
 
            # Refetch if still short
            if len(data) < EXPECTED_COINS:
                data = refetch_missing_slots(data, EXPECTED_COINS, run_id)
 
            # Trim to expected
            data = data[:EXPECTED_COINS]
 
            status = evaluate_load_quality(len(data), EXPECTED_COINS)
            print(f"✓ API success on attempt {attempt} — {len(data)} coins | status={status}")
            return data, status
 
        except Exception as exc:
            last_exc = exc
            wait = min(BASE_BACKOFF_S * (2 ** (attempt - 1)), MAX_BACKOFF_S)
            print(f"⚠ Attempt {attempt}/{MAX_RETRIES} failed: {exc}. Retrying in {wait}s …")
            time.sleep(wait)
 
    raise RuntimeError(f"All {MAX_RETRIES} attempts failed. Last error: {last_exc}")
 
 
def get_last_successful_snapshot() -> "pyspark.sql.DataFrame | None":
    """Fallback: return the most recent partition from the bronze current table."""
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
 
def raw_to_spark_df(raw_data: list, run_id: str, load_status: str) -> "pyspark.sql.DataFrame":
    pdf = pd.DataFrame(raw_data)
    pdf['ingestion_timestamp'] = _now_utc()
    pdf['ingestion_date']      = _today_utc()
    pdf['source_api']          = '/coins/markets'
    pdf['api_vs_currency']     = 'usd'
    pdf['pipeline_run_id']     = run_id
    pdf['load_status']         = load_status          # NEW: partial load tier
    pdf['coins_expected']      = EXPECTED_COINS        # NEW
    pdf['coins_received']      = len(raw_data)         # NEW
    sdf = spark.createDataFrame(pdf)
    print(f"✓ Spark DataFrame: {sdf.count()} rows, {len(sdf.columns)} cols")
    return sdf
 

# COMMAND ----------

# ── Schema Evolution — Bronze Market Data ─────────────────────────────────────────
 
SCHEMA_REGISTRY_TABLE = f"{DB_NAME}.schema_evolution_log"
 
def log_schema_change(table_name: str, change_type: str,
                       column_name: str, old_dtype: str = '',
                       new_dtype: str = '', run_id: str = ''):
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
    from pyspark.sql.functions import lit, col as scol
    from pyspark.sql.types import StringType
 
    if not spark.catalog.tableExists(target_table):
        for field in new_sdf.schema.fields:
            log_schema_change(target_table, 'SCHEMA_INITIALISED',
                              field.name, new_dtype=str(field.dataType),
                              run_id=run_id)
        return new_sdf
 
    existing = {f.name: f.dataType for f in spark.table(target_table).schema.fields}
    incoming = {f.name: f.dataType for f in new_sdf.schema.fields}
    reconciled = new_sdf
 
    for col_name, dtype in incoming.items():
        if col_name not in existing:
            print(f'[SCHEMA] New column: {col_name} ({dtype}) -> will be added via mergeSchema')
            log_schema_change(target_table, 'COLUMN_ADDED',
                              col_name, new_dtype=str(dtype), run_id=run_id)
 
    for col_name, dtype in existing.items():
        if col_name not in incoming:
            print(f'[SCHEMA] Column missing in new batch: {col_name} -> back-filling NULL')
            reconciled = reconciled.withColumn(col_name, lit(None).cast(dtype))
            log_schema_change(target_table, 'COLUMN_DROPPED',
                              col_name, old_dtype=str(dtype), run_id=run_id)
 
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

# ── Idempotent MERGE write — with quarantine routing (Change Requests 1+2) ─
 
def write_bronze_market(sdf, run_id: str, load_status: str):
    """
    1. Schema evolution check
    2. Route PARTIAL_LOAD_SEVERE rows to quarantine table instead of main
    3. MERGE into bronze_market_data (main)
    4. APPEND into archive
    5. Reconcile stale rows for missing coins (partial loads only)
    """
    # ── Schema evolution ─────────────────────────────────────────────────
    sdf = evolve_schema(sdf, BRONZE_MARKET, run_id)
 
    # ── PARTIAL_LOAD_SEVERE → quarantine only (Change Request 1, section 2.6) ─
    if load_status == 'PARTIAL_LOAD_SEVERE':
        quar_exists = spark.catalog.tableExists(BRONZE_MARKET_QUAR)
        (sdf.write.format('delta')
            .mode('append' if quar_exists else 'overwrite')
            .option('mergeSchema', 'true')
            .partitionBy('ingestion_date')
            .saveAsTable(BRONZE_MARKET_QUAR))
        print(f"⚠ PARTIAL_LOAD_SEVERE — rows written to quarantine: {BRONZE_MARKET_QUAR}")
        # Use fallback data for main table
        fallback_sdf = get_last_successful_snapshot()
        if fallback_sdf is None:
            print("  ✗ No fallback available — main table unchanged")
            return 0
        sdf = (fallback_sdf
            .withColumn('ingestion_timestamp', F.lit(_now_utc()).cast('timestamp'))
            .withColumn('ingestion_date',      F.lit(_today_utc()))
            .withColumn('pipeline_run_id',     F.lit(run_id))
            .withColumn('is_fallback',         F.lit(True))
            .withColumn('load_status',         F.lit('PARTIAL_LOAD_SEVERE'))
        )
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
 
    # ── Stale reconciliation for partial loads (Change Request 2.7) ──────
    if load_status in ('PARTIAL_LOAD', 'PARTIAL_LOAD_CRITICAL', 'PARTIAL_LOAD_SEVERE'):
        stale_sdf = reconcile_partial_load(sdf, run_id)
        if stale_sdf is not None and stale_sdf.count() > 0:
            stale_sdf.createOrReplaceTempView('_stale_market_data')
            spark.sql(f"""
                MERGE INTO {BRONZE_MARKET} AS tgt
                USING _stale_market_data AS src
                  ON  tgt.id = src.id
                  AND tgt.ingestion_date = src.ingestion_date
                WHEN NOT MATCHED THEN INSERT *
            """)
            print(f"  ✓ Stale rows reconciled: {stale_sdf.count()}")
 
    # ── Archive table ─────────────────────────────────────────────────────
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
 
    retry_count  = 0
    raw_data     = None
    used_fallback = False
 
    # ── Attempt API fetch ────────────────────────────────────────────────
    try:
        raw_data, load_status = fetch_market_data_with_retry()
        retry_count = MAX_RETRIES
    except RuntimeError as exc:
        print(f"✗ API exhausted — switching to fallback snapshot\n  {exc}")
        used_fallback = True
        load_status   = 'FAILURE'
        fallback_sdf  = get_last_successful_snapshot()
        if fallback_sdf is None:
            write_ingestion_log("bronze_market_data", "FAILURE", 0,
                                str(exc), MAX_RETRIES)
            raise RuntimeError("No fallback available and API failed. Pipeline aborted.")
        fallback_sdf = (fallback_sdf
            .withColumn("ingestion_timestamp", F.lit(_now_utc()).cast("timestamp"))
            .withColumn("ingestion_date",      F.lit(_today_utc()))
            .withColumn("pipeline_run_id",     F.lit(run_id))
            .withColumn("is_fallback",         F.lit(True))
            .withColumn("load_status",         F.lit("FAILURE"))
        )
        rows = write_bronze_market(fallback_sdf, run_id, 'FAILURE')
        write_ingestion_log("bronze_market_data", "FALLBACK", rows,
                            f"Used snapshot from previous run: {exc}", MAX_RETRIES)
        return
 
    # ── Normal / partial load path ────────────────────────────────────────
    try:
        sdf = raw_to_spark_df(raw_data, run_id, load_status)
        sdf = sdf.withColumn("is_fallback", F.lit(False))
        rows = write_bronze_market(sdf, run_id, load_status)
        optimise_bronze_market()
        backfill_missing_days(lookback_days=3)
        write_ingestion_log("bronze_market_data", load_status, rows,
                            "", retry_count)
 
        # ── Sample preview ───────────────────────────────────────────────
        print("\n=== Bronze Market Preview (BTC) ===")
        spark.table(BRONZE_MARKET).filter(F.col("id") == "bitcoin").select(
            "id", "name", "current_price", "market_cap", "ingestion_date",
            "load_status", "coins_expected", "coins_received"
        ).show(1, truncate=False)
 
    except Exception as exc:
        tb = traceback.format_exc()
        write_ingestion_log("bronze_market_data", "FAILURE", 0, tb[:2000], retry_count)
        raise
 
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
run_market_ingestion()

# COMMAND ----------

from datetime import datetime, timezone
print(datetime.now(timezone.utc))


# COMMAND ----------

from pyspark.sql.functions import max

latest_date = spark.table(BRONZE_MARKET) \
    .selectExpr("max(ingestion_date) as max_date") \
    .collect()[0]["max_date"]

spark.table(BRONZE_MARKET) \
    .filter(F.col("ingestion_date") == latest_date) \
    .show(5, truncate=False)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW PARTITIONS crypto_space.crypto_db.bronze_market_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In a SQL cell
# MAGIC SHOW PARTITIONS crypto_space.crypto_db.bronze_market_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from crypto_space.crypto_db.bronze_market_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN crypto_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM crypto_db.bronze_market_data;
