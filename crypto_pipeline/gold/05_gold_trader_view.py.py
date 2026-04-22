# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, FloatType
from datetime import datetime, timezone
import traceback
 
DB_NAME = 'crypto_db'
 
# ── Source
SILVER_MARKET  = f'{DB_NAME}.silver_market_metrics'
SILVER_OHLC    = f'{DB_NAME}.silver_ohlc_metrics'
 
# ── Gold targets
GOLD_SNAPSHOT    = f'{DB_NAME}.gold_market_snapshot'
GOLD_OHLC        = f'{DB_NAME}.gold_ohlc_history'
GOLD_PERFORMANCE = f'{DB_NAME}.gold_performance_metrics'
GOLD_SUMMARY     = f'{DB_NAME}.gold_market_summary'
INGESTION_LOG    = f'{DB_NAME}.ingestion_status_log'
 
# ── Rounding precision
PRICE_P = 4
PCT_P   = 3
RATIO_P = 6
SCORE_P = 4   # recommendation_score
 
def _now_utc():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
 
def _today_utc():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')
 
def round_numeric_cols(df, price_cols=[], pct_cols=[], ratio_cols=[]):
    """Round all double/float columns by category."""
    for c, dtype in df.dtypes:
        if dtype in ('double', 'float'):
            if c in price_cols:
                df = df.withColumn(c, F.round(F.col(c), PRICE_P))
            elif c in pct_cols:
                df = df.withColumn(c, F.round(F.col(c), PCT_P))
            elif c in ratio_cols:
                df = df.withColumn(c, F.round(F.col(c), RATIO_P))
            else:
                df = df.withColumn(c, F.round(F.col(c), PCT_P))
    return df
 
def write_log(table, status, rows, error=''):
    row = [{'log_timestamp': _now_utc(), 'ingestion_date': _today_utc(),
            'source': table, 'status': status,
            'rows_written': rows, 'retry_count': 0,
            'error_message': error[:2000]}]
    sdf = spark.createDataFrame(row).select(
        F.col('log_timestamp').cast('timestamp'), 'ingestion_date',
        'source', 'status',
        F.col('rows_written').cast('int'),
        F.col('retry_count').cast('int'), 'error_message')
    ex = spark.catalog.tableExists(INGESTION_LOG)
    (sdf.write.format('delta')
        .mode('append' if ex else 'overwrite')
        .option('mergeSchema', 'true')
        .saveAsTable(INGESTION_LOG))
 
def merge_into(df, table, pk_cols, partition_col=None):
    """Generic idempotent MERGE — drops internal cols before write."""
    df = df.drop(*[c for c in df.columns if c.startswith('_')])
    view = '_gold_tmp_' + table.split('.')[-1]
    df.createOrReplaceTempView(view)
    on_clause = ' AND '.join([f'tgt.{c} = src.{c}' for c in pk_cols])
    if not spark.catalog.tableExists(table):
        w = (df.write.format('delta')
               .mode('overwrite')
               .option('overwriteSchema', 'true'))
        if partition_col:
            w = w.partitionBy(partition_col)
        w.saveAsTable(table)
        print(f'  Created  {table}')
    else:
        spark.sql(f'''
            MERGE INTO {table} tgt USING {view} src
            ON {on_clause}
            WHEN MATCHED     THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        ''')
        print(f'  Merged → {table}')
 
print('Config loaded.')
for t in [GOLD_SNAPSHOT, GOLD_OHLC, GOLD_PERFORMANCE, GOLD_SUMMARY]:
    print(f'  {t}')

# COMMAND ----------

# ── Load Silver Tables ───────────────────────────────────────────

sm = spark.table(SILVER_MARKET)
so = spark.table(SILVER_OHLC)
 
print(f'Silver Market : {sm.count()} rows | {sm.select("ingestion_date").distinct().count()} dates')
print(f'Silver OHLC   : {so.count()} rows | {so.select("coin_id").distinct().count()} coins')
 

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════
# GOLD TABLE 1 — gold_market_snapshot
# Grain    : 1 row per coin — latest state only
# Dashboards: Market Dynamics Comparison + Performance Metrics
# ══════════════════════════════════════════════════════════════════════════

 
def build_market_snapshot():
 
    # ── Latest row per coin from silver_market ────────────────────────────
    w_mkt = Window.partitionBy('id').orderBy(F.col('ingestion_date').desc())
    sm_latest = (sm
        .withColumn('_rn', F.row_number().over(w_mkt))
        .filter('_rn = 1').drop('_rn')
    )
 
    # ── Latest candle per coin from silver_ohlc ───────────────────────────
    w_ohlc = Window.partitionBy('coin_id').orderBy(F.col('candle_datetime').desc())
    so_latest = (so
        .withColumn('_rn', F.row_number().over(w_ohlc))
        .filter('_rn = 1').drop('_rn')
        .select(
            F.col('coin_id'),
            F.col('open').alias('ohlc_open'),
            F.col('close').alias('ohlc_close'),
            F.col('candle_direction'),
            F.col('candle_date').alias('latest_candle_date'),
            F.col('ma_7d'), F.col('ma_14d'), F.col('ma_30d'),
            F.col('volatility_7d'), F.col('volatility_14d'), F.col('volatility_30d'),
            F.col('volatility_cluster'),
            F.col('momentum_7d'),
            F.col('trend_strength'),
            F.col('price_level_signal'),
            # CARRIED: continuous score from silver_ohlc (no label)
            F.col('recommendation_score'),
            F.col('support_14d'), F.col('resistance_14d'),
            F.col('support_30d'), F.col('resistance_30d'),
        )
    )

    # ── Join ──────────────────────────────────────────────────────────────
    df = (sm_latest
            .join(so_latest, sm_latest['id'] == so_latest['coin_id'], 'left')
            .drop('coin_id')
        )
 
    # ── Gold-derived columns ──────────────────────────────────────────────
    df = (df
        # ATH drawdown
        .withColumn('ath_drawdown_pct',
            F.when(F.col('ath') > 0,
                (F.col('current_price') - F.col('ath')) / F.col('ath') * 100
            ).otherwise(F.lit(None)))
 
        # Price vs 30d MA
        .withColumn('price_vs_ma30_pct',
            F.when(F.col('ma_30d').isNotNull() & (F.col('ma_30d') > 0),
                (F.col('current_price') - F.col('ma_30d')) / F.col('ma_30d') * 100
            ).otherwise(F.lit(None)))
 
        # Performance label — based on price change only (not a recommendation)
        .withColumn('performance_label',
            F.when(F.col('price_change_percentage_24h') >= 5,           'TOP_GAINER')
             .when(F.col('price_change_percentage_24h') <= -5,          'TOP_LOSER')
             .when(F.col('price_change_percentage_24h').between(-1, 1), 'STABLE')
             .otherwise('MOVER'))
 
        # ── ADDED: gold_recommendation_score ─────────────────────────────
        # Blended score at the gold layer combining:
        #   - recommendation_score from OHLC (60%): multi-factor signal
        #   - market_trend_score from market data (40%): directional encoding
        #     of short_term_trend_signal (BULLISH=0.70, NEUTRAL=0.50, BEARISH=0.30)
        # Result is in [0, 1]. No threshold applied — callers define their own.
        .withColumn('_market_trend_score',
            F.when(F.col('short_term_trend_signal') == 'BULLISH', F.lit(0.70))
             .when(F.col('short_term_trend_signal') == 'BEARISH', F.lit(0.30))
             .otherwise(F.lit(0.50))
        )
        .withColumn('gold_recommendation_score',
            F.round(
                F.when(
                    F.col('recommendation_score').isNotNull(),
                    F.col('recommendation_score') * 0.60
                    + F.col('_market_trend_score') * 0.40
                ).otherwise(F.col('_market_trend_score')),
                SCORE_P
            ).cast(DoubleType())
        )
        .drop('_market_trend_score')
 
        .withColumn('snapshot_date',    F.lit(_today_utc()))
        .withColumn('gold_updated_at',  F.lit(_now_utc()).cast('timestamp'))
    )
 
    # ── Round all numerics ────────────────────────────────────────────────
    price_cols = ['current_price','high_24h','low_24h','price_change_24h',
                  'ath','atl','ohlc_open','ohlc_close',
                  'ma_7d','ma_14d','ma_30d','ma_3',
                  'support_14d','resistance_14d','support_30d','resistance_30d']
    pct_cols   = ['price_change_percentage_24h','price_change_pct_24h_capped',
                  'ath_change_percentage','atl_change_percentage',
                  'market_cap_change_percentage_24h',
                  'price_movement_metric','daily_return_pct','price_3d_change_pct',
                  'price_range_volatility','rank_movement',
                  'ath_drawdown_pct','price_vs_ma30_pct',
                  'momentum_7d','data_freshness_hours']
    ratio_cols = ['liquidity_strength','volatility_7d','volatility_14d','volatility_30d']
 
    df = round_numeric_cols(df, price_cols, pct_cols, ratio_cols)
 
    # ── Final column selection ────────────────────────────────────────────
    return df.select(
        # Identity
        'id','symbol','name','market_cap_rank','market_segment',
        # Price
        'current_price','high_24h','low_24h',
        'price_change_24h','price_change_percentage_24h','price_change_pct_24h_capped',
        # Market size
        'market_cap','fully_diluted_valuation','total_volume',
        'circulating_supply','total_supply','max_supply',
        # Market signals (silver) — directional features only, no labels
        'price_movement_metric','daily_return_pct','price_3d_change_pct',
        'short_term_trend_signal','price_range_volatility',
        'liquidity_strength','rank_movement','ma_3',
        # ATH/ATL
        'ath','ath_change_percentage','ath_drawdown_pct',
        'atl','atl_change_percentage',
        # OHLC signals
        'ohlc_open','ohlc_close','candle_direction','latest_candle_date',
        'ma_7d','ma_14d','ma_30d','price_vs_ma30_pct',
        'volatility_7d','volatility_14d','volatility_30d','volatility_cluster',
        'momentum_7d','support_14d','resistance_14d',
        'trend_strength','price_level_signal',
        # Scores (continuous, threshold-agnostic)
        'recommendation_score',          # raw OHLC-based score
        'gold_recommendation_score',     # blended gold-layer score
        # Performance
        'performance_label',
        # Anomaly
        'anomaly_flag','anomaly_reason',
        # Meta
        'data_freshness_hours','ingestion_date','snapshot_date','gold_updated_at',
    )
 
 
try:
    df_snap = build_market_snapshot()
    merge_into(df_snap, GOLD_SNAPSHOT, ['id'])
    cnt = spark.table(GOLD_SNAPSHOT).count()
    write_log(GOLD_SNAPSHOT, 'SUCCESS', cnt)
    print(f'  Rows: {cnt}')
except Exception as e:
    write_log(GOLD_SNAPSHOT, 'FAILURE', 0, traceback.format_exc())
    raise
 
 
# COMMAND ----------


# COMMAND ----------


# ══════════════════════════════════════════════════════════════════════════
# GOLD TABLE 2 — gold_ohlc_history
# Grain    : 1 row per (coin_id, candle_date) — 4h candles → daily OHLC
# Dashboard: OHLC Candlestick 30-day chart
#
# KEY CHANGES:
#   - trading_signal REMOVED from daily_close select and final output.
#   - recommendation_score aggregations ADDED:
#       avg_recommendation_score  — daily average score
#       p25_recommendation_score  — 25th percentile (conservative threshold)
#       p75_recommendation_score  — 75th percentile (aggressive threshold)
#   - Callers can decide their own threshold to interpret the scores.
# ══════════════════════════════════════════════════════════════════════════
 
def build_ohlc_history():
 
    w_asc  = Window.partitionBy('coin_id','candle_date').orderBy('candle_datetime')
    w_desc = Window.partitionBy('coin_id','candle_date').orderBy(F.col('candle_datetime').desc())
 
    so_tagged = (so
        .withColumn('_rn_asc',  F.row_number().over(w_asc))
        .withColumn('_rn_desc', F.row_number().over(w_desc))
    )
 
    # First candle of day → daily open
    daily_open = (so_tagged.filter('_rn_asc = 1')
        .select('coin_id','candle_date',
                F.col('open').alias('open')))
 
    # Last candle of day → daily close + carry latest signals (no trading_signal)
    daily_close = (so_tagged.filter('_rn_desc = 1')
        .select('coin_id','candle_date',
                F.col('close').alias('close'),
                'ma_7d','ma_14d','ma_30d',
                'volatility_7d','volatility_14d','volatility_30d',
                'volatility_cluster',
                'momentum_7d',
                'trend_strength','price_level_signal',
                'support_14d','resistance_14d',
                'support_30d','resistance_30d',
                # ADDED: last candle's score as closing score
                F.col('recommendation_score').alias('close_recommendation_score'),
        ))
 
    # Aggregate intraday stats including score distribution
    daily_agg = (so
        .groupBy('coin_id','candle_date')
        .agg(
            F.max('high').alias('high'),
            F.min('low').alias('low'),
            F.avg('daily_range_pct').alias('avg_range_pct'),
            F.count('*').alias('candle_count'),
            F.sum(F.when(F.col('candle_direction') == 'BULL', 1).otherwise(0))
                .alias('bull_candles'),
            F.sum(F.when(F.col('candle_direction') == 'BEAR', 1).otherwise(0))
                .alias('bear_candles'),
            F.sum(F.when(F.col('anomaly_flag').isNotNull(), 1).otherwise(0))
                .alias('anomaly_candle_count'),
            # ADDED: score aggregations for the day
            F.round(F.avg('recommendation_score'), SCORE_P)
                .alias('avg_recommendation_score'),
            F.round(
                F.expr('percentile_approx(recommendation_score, 0.25)'), SCORE_P
            ).alias('p25_recommendation_score'),
            F.round(
                F.expr('percentile_approx(recommendation_score, 0.75)'), SCORE_P
            ).alias('p75_recommendation_score'),
        )
    )
 
    df = (daily_agg
        .join(daily_open,  ['coin_id','candle_date'], 'left')
        .join(daily_close, ['coin_id','candle_date'], 'left')
    )
 
    df = (df
        .withColumn('daily_candle_direction',
            F.when(F.col('close') > F.col('open'), 'BULL')
             .when(F.col('close') < F.col('open'), 'BEAR')
             .otherwise('DOJI'))
 
        .withColumn('daily_body_pct',
            F.when(F.col('open') > 0,
                F.abs(F.col('close') - F.col('open')) / F.col('open') * 100
            ).otherwise(F.lit(None)))
 
        .withColumn('daily_range_pct',
            F.when(F.col('open') > 0,
                (F.col('high') - F.col('low')) / F.col('open') * 100
            ).otherwise(F.lit(None)))
 
        .withColumn('bull_bear_ratio',
            F.when(F.col('bear_candles') > 0,
                F.col('bull_candles') / F.col('bear_candles')
            ).otherwise(F.lit(None)))
 
        .withColumn('gold_updated_at', F.lit(_now_utc()).cast('timestamp'))
    )
 
    # Round
    price_cols = ['open','high','low','close',
                  'ma_7d','ma_14d','ma_30d',
                  'support_14d','resistance_14d','support_30d','resistance_30d']
    pct_cols   = ['avg_range_pct','daily_body_pct','daily_range_pct',
                  'momentum_7d','bull_bear_ratio']
    ratio_cols = ['volatility_7d','volatility_14d','volatility_30d']
 
    df = round_numeric_cols(df, price_cols, pct_cols, ratio_cols)
 
    return df.select(
        'coin_id','candle_date',
        'open','high','low','close',
        'daily_candle_direction','daily_body_pct','daily_range_pct',
        'avg_range_pct','candle_count','bull_candles','bear_candles',
        'bull_bear_ratio','anomaly_candle_count',
        'ma_7d','ma_14d','ma_30d',
        'volatility_7d','volatility_14d','volatility_30d','volatility_cluster',
        'momentum_7d',
        'support_14d','resistance_14d','support_30d','resistance_30d',
        'price_level_signal','trend_strength',
        # ADDED: score columns, REMOVED: trading_signal
        'avg_recommendation_score',
        'p25_recommendation_score',
        'p75_recommendation_score',
        'close_recommendation_score',
        'gold_updated_at',
    )
 
 
try:
    df_ohlc = build_ohlc_history()
    merge_into(df_ohlc, GOLD_OHLC, ['coin_id','candle_date'], partition_col='coin_id')
    cnt = spark.table(GOLD_OHLC).count()
    write_log(GOLD_OHLC, 'SUCCESS', cnt)
    print(f'  Rows: {cnt}')
except Exception as e:
    write_log(GOLD_OHLC, 'FAILURE', 0, traceback.format_exc())
    raise
 
 


# COMMAND ----------

 
# ══════════════════════════════════════════════════════════════════════════
# GOLD TABLE 3 — gold_performance_metrics
# Grain    : 1 row per (coin_id, ingestion_date) — full history
# Dashboard: Top Gainers / Losers leaderboard + performance over time
#
# KEY CHANGES:
#   - avg_recommendation_score ADDED — daily average score per coin,
#     joined from silver_ohlc_metrics. Enables score-ranked leaderboards.
#   - No categorical recommendation labels stored.
# ══════════════════════════════════════════════════════════════════════════
 
def build_performance_metrics():
 
    w_gain = Window.partitionBy('ingestion_date').orderBy(F.col('price_change_percentage_24h').desc())
    w_loss = Window.partitionBy('ingestion_date').orderBy(F.col('price_change_percentage_24h').asc())
    w_vol  = Window.partitionBy('ingestion_date').orderBy(F.col('total_volume').desc())
    w_pct  = Window.partitionBy('ingestion_date').orderBy('price_change_percentage_24h')
    w_coin = Window.partitionBy('id').orderBy('ingestion_date')
 
    # ── Pre-compute per-coin daily score from silver_ohlc ─────────────────
    ohlc_daily_score = (so
        .groupBy(
            F.col('coin_id').alias('id'),
            F.col('ingestion_date')
        )
        .agg(
            F.round(F.avg('recommendation_score'), SCORE_P)
                .alias('avg_recommendation_score')
        )
    )
 
    df = (sm.select(
            'id','symbol','name','ingestion_date',
            'market_cap_rank','market_segment',
            'current_price','market_cap','total_volume',
            'price_change_24h','price_change_percentage_24h',
            'price_change_pct_24h_capped',
            'price_movement_metric','daily_return_pct','price_3d_change_pct',
            'short_term_trend_signal','price_range_volatility',
            'liquidity_strength','rank_movement','anomaly_flag',
        )
 
        # Rank within each day
        .withColumn('gainer_rank',  F.rank().over(w_gain))
        .withColumn('loser_rank',   F.rank().over(w_loss))
        .withColumn('volume_rank',  F.rank().over(w_vol))
 
        # Percentile within day (0=worst, 100=best)
        .withColumn('performance_percentile',
            F.round(F.percent_rank().over(w_pct) * 100, 1))
 
        # Performance label — price-based only, not a recommendation
        .withColumn('performance_label',
            F.when(F.col('price_change_percentage_24h') >= 5,           'TOP_GAINER')
             .when(F.col('price_change_percentage_24h') <= -5,          'TOP_LOSER')
             .when(F.col('price_change_percentage_24h').between(-1, 1), 'STABLE')
             .otherwise('MOVER'))
 
        # Consecutive gain/loss streak
        .withColumn('_is_gain',
            F.when(F.col('daily_return_pct') > 0, 1).otherwise(0))
        .withColumn('_streak_grp',
            F.sum(
                F.when(F.col('_is_gain') != F.lag('_is_gain', 1).over(w_coin), 1)
                 .otherwise(0)
            ).over(w_coin))
        .withColumn('streak_days',
            F.count('*').over(
                Window.partitionBy('id','_streak_grp').orderBy('ingestion_date')
                      .rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn('streak_direction',
            F.when(F.col('_is_gain') == 1, 'GAINING').otherwise('LOSING'))
 
        .withColumn('gold_updated_at', F.lit(_now_utc()).cast('timestamp'))
    )
 
    # ADDED: join avg_recommendation_score from OHLC silver
    df = df.join(ohlc_daily_score, on=['id', 'ingestion_date'], how='left')
 
    # Round
    price_cols = ['current_price','price_change_24h']
    pct_cols   = ['price_change_percentage_24h','price_change_pct_24h_capped',
                  'price_movement_metric','daily_return_pct','price_3d_change_pct',
                  'price_range_volatility','rank_movement','performance_percentile']
    ratio_cols = ['liquidity_strength']
 
    df = round_numeric_cols(df, price_cols, pct_cols, ratio_cols)
 
    return df.select(
        'id','symbol','name','ingestion_date',
        'market_cap_rank','market_segment',
        'current_price','market_cap','total_volume',
        'price_change_24h','price_change_percentage_24h','price_change_pct_24h_capped',
        'price_movement_metric','daily_return_pct','price_3d_change_pct',
        'short_term_trend_signal','price_range_volatility','liquidity_strength',
        'rank_movement',
        'gainer_rank','loser_rank','volume_rank',
        'performance_percentile','performance_label',
        'streak_days','streak_direction',
        # ADDED: continuous score — callers apply their own threshold
        'avg_recommendation_score',
        'anomaly_flag','gold_updated_at',
    )
 
 
try:
    df_perf = build_performance_metrics()
    merge_into(df_perf, GOLD_PERFORMANCE, ['id','ingestion_date'],
               partition_col='ingestion_date')
    cnt = spark.table(GOLD_PERFORMANCE).count()
    write_log(GOLD_PERFORMANCE, 'SUCCESS', cnt)
    print(f'  Rows: {cnt}')
except Exception as e:
    write_log(GOLD_PERFORMANCE, 'FAILURE', 0, traceback.format_exc())
    raise


# COMMAND ----------

 
# COMMAND ----------
 
# ══════════════════════════════════════════════════════════════════════════
# GOLD TABLE 4 — gold_market_summary
# Grain    : 1 row per ingestion_date — market-wide headline KPIs
# Dashboard: Overview card, market sentiment, BTC dominance
#
# KEY CHANGES:
#   - avg_recommendation_score ADDED — market-wide daily average score.
#   - pct_high_signal ADDED — fraction of coins with score above the
#     market-wide 75th percentile (a relative threshold, not hardcoded).
#   - No categorical recommendation label counts stored.
# ══════════════════════════════════════════════════════════════════════════
 
def build_market_summary():
 
    w_gain = Window.partitionBy('ingestion_date').orderBy(F.col('price_change_percentage_24h').desc())
    w_loss = Window.partitionBy('ingestion_date').orderBy(F.col('price_change_percentage_24h').asc())
    w_vol  = Window.partitionBy('ingestion_date').orderBy(F.col('total_volume').desc())
    w_liq  = Window.partitionBy('ingestion_date').orderBy(F.col('liquidity_strength').desc())
 
    sm_tagged = (sm
        .withColumn('_rg', F.row_number().over(w_gain))
        .withColumn('_rl', F.row_number().over(w_loss))
        .withColumn('_rv', F.row_number().over(w_vol))
        .withColumn('_rq', F.row_number().over(w_liq))
    )
 
    top_gainer = (sm_tagged.filter('_rg = 1').select(
        'ingestion_date',
        F.col('id').alias('top_gainer_coin'),
        F.col('name').alias('top_gainer_name'),
        F.round(F.col('price_change_percentage_24h'), PCT_P).alias('top_gainer_pct')))
 
    top_loser = (sm_tagged.filter('_rl = 1').select(
        'ingestion_date',
        F.col('id').alias('top_loser_coin'),
        F.col('name').alias('top_loser_name'),
        F.round(F.col('price_change_percentage_24h'), PCT_P).alias('top_loser_pct')))
 
    highest_vol = (sm_tagged.filter('_rv = 1').select(
        'ingestion_date',
        F.col('id').alias('highest_volume_coin'),
        F.round(F.col('total_volume'), 0).alias('highest_volume_usd')))
 
    most_liquid = (sm_tagged.filter('_rq = 1').select(
        'ingestion_date',
        F.col('id').alias('most_liquid_coin'),
        F.round(F.col('liquidity_strength'), RATIO_P).alias('top_liquidity_ratio')))
 
    btc = (sm.filter(F.col('id') == 'bitcoin')
           .select('ingestion_date',
                   F.col('market_cap').alias('btc_market_cap')))
 
    # ── Per-date score aggregations from silver_ohlc ──────────────────────
    ohlc_scores = (so
        .groupBy(F.col('ingestion_date'))
        .agg(
            F.round(F.avg('recommendation_score'), SCORE_P)
                .alias('avg_recommendation_score'),
            F.round(
                F.expr('percentile_approx(recommendation_score, 0.75)'), SCORE_P
            ).alias('p75_score'),
        )
    )
 
    # pct_high_signal: fraction of coins whose daily avg score > market p75
    coin_daily_score = (so
        .groupBy(F.col('coin_id'), F.col('ingestion_date'))
        .agg(F.avg('recommendation_score').alias('coin_avg_score'))
    )
    coin_score_with_p75 = coin_daily_score.join(
        ohlc_scores.select('ingestion_date', 'p75_score'),
        on='ingestion_date', how='left'
    )
    pct_high = (coin_score_with_p75
        .groupBy('ingestion_date')
        .agg(
            F.round(
                F.sum(F.when(F.col('coin_avg_score') > F.col('p75_score'), 1).otherwise(0))
                / F.count('*') * 100,
                PCT_P
            ).alias('pct_high_signal')
        )
    )
 
    summary = (sm.groupBy('ingestion_date').agg(
        F.round(F.sum('market_cap'), 0).alias('total_market_cap'),
        F.round(F.sum('total_volume'), 0).alias('total_volume'),
        F.round(F.avg('price_change_percentage_24h'), PCT_P).alias('avg_price_change_pct'),
        F.round(F.expr('percentile_approx(price_change_percentage_24h, 0.5)'), PCT_P)
            .alias('median_price_change_pct'),
        F.sum(F.when(F.col('price_change_percentage_24h') > 0,  1).otherwise(0)).alias('gainers_count'),
        F.sum(F.when(F.col('price_change_percentage_24h') < 0,  1).otherwise(0)).alias('losers_count'),
        F.sum(F.when(F.col('price_change_percentage_24h') == 0, 1).otherwise(0)).alias('stable_count'),
        F.round(F.max('price_change_percentage_24h'), PCT_P).alias('max_gain_pct'),
        F.round(F.min('price_change_percentage_24h'), PCT_P).alias('max_loss_pct'),
        F.round(F.avg('liquidity_strength'), RATIO_P).alias('avg_liquidity_strength'),
        F.round(F.avg('price_range_volatility'), PCT_P).alias('avg_price_range_volatility'),
        F.sum(F.when(F.col('anomaly_flag') == True, 1).otherwise(0)).alias('anomaly_coins_count'),
        F.count('id').alias('total_coins'),
    ))
 
    df = (summary
        .join(top_gainer,  'ingestion_date', 'left')
        .join(top_loser,   'ingestion_date', 'left')
        .join(highest_vol, 'ingestion_date', 'left')
        .join(most_liquid, 'ingestion_date', 'left')
        .join(btc,         'ingestion_date', 'left')
        .join(ohlc_scores, 'ingestion_date', 'left')
        .join(pct_high,    'ingestion_date', 'left')
        .withColumn('btc_dominance_pct',
            F.when(F.col('total_market_cap') > 0,
                F.round(F.col('btc_market_cap') / F.col('total_market_cap') * 100, PCT_P)
            ).otherwise(F.lit(None)))
        .withColumn('market_sentiment',
            F.when(F.col('gainers_count') > F.col('losers_count') * 2, 'VERY_BULLISH')
             .when(F.col('gainers_count') > F.col('losers_count'),      'BULLISH')
             .when(F.col('losers_count')  > F.col('gainers_count') * 2, 'VERY_BEARISH')
             .when(F.col('losers_count')  > F.col('gainers_count'),     'BEARISH')
             .otherwise('NEUTRAL'))
        .withColumn('gold_updated_at', F.lit(_now_utc()).cast('timestamp'))
        .drop('btc_market_cap', 'p75_score')
    )
 
    return df.select(
        'ingestion_date',
        'total_market_cap','total_volume','total_coins',
        'gainers_count','losers_count','stable_count',
        'avg_price_change_pct','median_price_change_pct',
        'max_gain_pct','max_loss_pct',
        'avg_liquidity_strength','avg_price_range_volatility',
        'anomaly_coins_count',
        'btc_dominance_pct','market_sentiment',
        # ADDED: market-wide score KPIs (threshold-agnostic)
        'avg_recommendation_score',
        'pct_high_signal',
        'top_gainer_coin','top_gainer_name','top_gainer_pct',
        'top_loser_coin','top_loser_name','top_loser_pct',
        'highest_volume_coin','highest_volume_usd',
        'most_liquid_coin','top_liquidity_ratio',
        'gold_updated_at',
    )
 
 
try:
    df_summ = build_market_summary()
    merge_into(df_summ, GOLD_SUMMARY, ['ingestion_date'])
    cnt = spark.table(GOLD_SUMMARY).count()
    write_log(GOLD_SUMMARY, 'SUCCESS', cnt)
    print(f'  Rows: {cnt}')
except Exception as e:
    write_log(GOLD_SUMMARY, 'FAILURE', 0, traceback.format_exc())
    raise

# COMMAND ----------

 
# ── Verification ─────────────────────────────────────────────────────────
 
print('\n' + '='*60)
print(' GOLD LAYER VERIFICATION')
print('='*60)
 
print('\n--- Snapshot: Top 5 by rank ---')
spark.table(GOLD_SNAPSHOT).orderBy('market_cap_rank').select(
    'id','name','market_cap_rank','current_price','market_cap',
    'total_volume','performance_label',
    'recommendation_score','gold_recommendation_score',
).show(5, truncate=False)
 
print('\n--- Top 5 by recommendation_score (today) ---')
# Example of threshold-agnostic query — dashboard applies its own threshold
spark.table(GOLD_SNAPSHOT) \
    .filter(F.col('gold_recommendation_score').isNotNull()) \
    .orderBy(F.col('gold_recommendation_score').desc()) \
    .select('id','name','gold_recommendation_score','recommendation_score',
            'trend_strength','price_level_signal','short_term_trend_signal') \
    .show(5, truncate=False)
 
print('\n--- Top 5 Gainers (today) ---')
spark.table(GOLD_PERFORMANCE).filter(F.col('ingestion_date') == _today_utc()) \
    .orderBy('gainer_rank') \
    .select('id','name','price_change_percentage_24h','gainer_rank',
            'performance_percentile','performance_label',
            'avg_recommendation_score',
            'streak_days','streak_direction') \
    .show(5, truncate=False)
 
print('\n--- Top 5 Losers (today) ---')
spark.table(GOLD_PERFORMANCE).filter(F.col('ingestion_date') == _today_utc()) \
    .orderBy('loser_rank') \
    .select('id','name','price_change_percentage_24h','loser_rank',
            'performance_percentile','performance_label',
            'avg_recommendation_score') \
    .show(5, truncate=False)
 
print('\n--- OHLC History: BTC last 5 days ---')
spark.table(GOLD_OHLC).filter(F.col('coin_id') == 'bitcoin') \
    .orderBy(F.col('candle_date').desc()) \
    .select('coin_id','candle_date','open','high','low','close',
            'daily_candle_direction','daily_range_pct','daily_body_pct',
            'ma_7d','ma_14d','ma_30d','trend_strength',
            'avg_recommendation_score','close_recommendation_score') \
    .show(5, truncate=False)
 
print('\n--- Market Summary ---')
spark.table(GOLD_SUMMARY).orderBy(F.col('ingestion_date').desc()) \
    .select('ingestion_date','total_market_cap','total_volume',
            'gainers_count','losers_count','market_sentiment',
            'btc_dominance_pct',
            'avg_recommendation_score','pct_high_signal',
            'top_gainer_coin','top_gainer_pct',
            'top_loser_coin','top_loser_pct') \
    .show(3, truncate=False)
 
print('\n--- Row Counts ---')
for t in [GOLD_SNAPSHOT, GOLD_OHLC, GOLD_PERFORMANCE, GOLD_SUMMARY]:
    print(f'  {t}: {spark.table(t).count()}')
 
 
# COMMAND ----------
 
# ── Gold Layer Validation ────────────────────────────────────────────────
 
def validate_gold():
    results = {}
 
    snap = spark.table(GOLD_SNAPSHOT)
    results['snapshot_required_cols'] = all(c in snap.columns for c in [
        'id','name','current_price','market_cap','total_volume','market_cap_rank',
        'market_segment','performance_label',
        'trend_strength',
        'ma_7d','ma_14d','ma_30d','momentum_7d',
        'support_14d','resistance_14d',
        # CHANGED: was trading_signal / combined_signal
        'recommendation_score',
        'gold_recommendation_score',
    ])
    results['snapshot_unique_coin'] = snap.count() == snap.select('id').distinct().count()
    # CHANGED: score range check instead of categorical value check
    results['snapshot_score_in_range'] = snap.filter(
        F.col('gold_recommendation_score').isNotNull() &
        ((F.col('gold_recommendation_score') < 0) |
         (F.col('gold_recommendation_score') > 1))
    ).count() == 0
    # trading_signal must NOT exist in this table
    results['snapshot_no_trading_signal'] = 'trading_signal' not in snap.columns
    results['snapshot_no_combined_signal'] = 'combined_signal' not in snap.columns
 
    ohlc = spark.table(GOLD_OHLC)
    results['ohlc_required_cols'] = all(c in ohlc.columns for c in [
        'coin_id','candle_date','open','high','low','close',
        'daily_candle_direction','daily_range_pct',
        'ma_7d','ma_14d','ma_30d','trend_strength',
        # CHANGED: was trading_signal
        'avg_recommendation_score',
    ])
    results['ohlc_unique_grain'] = ohlc.count() == ohlc.select('coin_id','candle_date').distinct().count()
    results['ohlc_hl_valid']     = ohlc.filter(F.col('high') < F.col('low')).count() == 0
    results['ohlc_no_trading_signal'] = 'trading_signal' not in ohlc.columns
 
    perf = spark.table(GOLD_PERFORMANCE)
    results['perf_required_cols'] = all(c in perf.columns for c in [
        'id','name','ingestion_date','price_change_percentage_24h',
        'gainer_rank','loser_rank','performance_label',
        'performance_percentile','streak_days','streak_direction',
        # ADDED
        'avg_recommendation_score',
    ])
    results['perf_unique_grain'] = perf.count() == perf.select('id','ingestion_date').distinct().count()
 
    summ = spark.table(GOLD_SUMMARY)
    results['summary_required_cols'] = all(c in summ.columns for c in [
        'ingestion_date','total_market_cap','total_volume',
        'gainers_count','losers_count','market_sentiment',
        'btc_dominance_pct','top_gainer_coin','top_loser_coin',
        # ADDED
        'avg_recommendation_score', 'pct_high_signal',
    ])
    results['summary_unique_date'] = summ.count() == summ.select('ingestion_date').distinct().count()
 
    print('\n=== GOLD VALIDATION ===')
    all_pass = True
    for k, v in results.items():
        if not v: all_pass = False
        print(f'  {k:45} : {"PASS" if v else "FAIL"}')
    print(f'\n  Overall : {"ALL PASS" if all_pass else "FAILURES DETECTED"}')
    return results
 
 
validate_gold()

# COMMAND ----------


