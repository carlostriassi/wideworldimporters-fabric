# Fabric notebook source


# MARKDOWN ********************

# # Bronze: Azure Cosmos DB ingestion
# Registry-driven ingestion from Azure Cosmos DB (SQL/Core API) using the
# Azure Cosmos DB Spark connector v4 (`com.azure.cosmos.spark`).
# Supports incremental reads via Change Feed or `_ts` watermark, and
# full container reads.
# 
# Library required: `com.azure.cosmos.spark:azure-cosmos-spark_3-4_2-12:<version>`
# (add to cluster Maven libraries).
# 
# Registry entry example:
# ```python
# 'cosmos_events': {
#     'connector_type':    'cosmosdb',
#     'kv_endpoint':       'cosmos-account-endpoint', 
#     'kv_account_key':    'cosmos-account-key',
#     'cosmos_database':   'operational_db',
#     'cosmos_container':  'events',
#     'use_change_feed':   True,
#     'change_feed_start': 'Beginning',   # Beginning | Now | <continuation_token>
#     'bronze_table':      'cosmos/events',
#     'load_type':         'incremental',
#     'watermark_col':     '_ts',
#     'primary_keys':      ['id'],
# }
# ```

# CELL ********************

# CELL 1 — Imports + Spark config
import sys, os

# Copy config from BronzeLH to local driver filesystem.
# Avoids relying on the /lakehouse/ FUSE mount, which is not guaranteed
# when notebooks run as pipeline TridentNotebook activities.
_lh        = mssparkutils.lakehouse.get("BronzeLH")
_cfg_src   = f"abfss://{_lh.workspaceId}@onelake.dfs.fabric.microsoft.com/{_lh.id}/Files/config"
_cfg_local = "/tmp/nb_config"
os.makedirs(_cfg_local, exist_ok=True)
for _f in mssparkutils.fs.ls(_cfg_src):
    if _f.name.endswith(".py"):
        with open(f"{_cfg_local}/{_f.name}", "w") as _fh:
            _fh.write(mssparkutils.fs.head(_f.path, 1_000_000))
import importlib
for _mod in ("workspace_config", "source_registry"):
    sys.modules.pop(_mod, None)
sys.path.insert(0, _cfg_local)
importlib.invalidate_caches()

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, md5, concat_ws, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime
import json, logging

from workspace_config import (
    BRONZE_TABLES, BRONZE_FILES, WATERMARK_PATH, SCHEMA_PATH, QUARANTINE_PATH,
    apply_spark_settings, get_secret
)
from source_registry import get_sources_by_type

spark = SparkSession.builder.getOrCreate()
apply_spark_settings(spark)
log = logging.getLogger('bronze_cosmosdb')

# CELL ********************

# CELL 2 — Watermark helpers
WATERMARK_SCHEMA = StructType([
    StructField('table_name',     StringType(),    False),
    StructField('last_watermark', StringType(),    True),
    StructField('last_run_at',    TimestampType(), True),
    StructField('rows_loaded',    LongType(),      True),
    StructField('load_status',    StringType(),    True),
])

def read_watermark(table_name):
    try:
        df_wm = spark.read.format('delta').load(WATERMARK_PATH)
        rows = df_wm.filter(col('table_name') == table_name) \
                    .filter(col('load_status') == 'SUCCESS') \
                    .orderBy(col('last_run_at').desc()).limit(1).collect()
        if rows:
            return rows[0]['last_watermark']
    except Exception:
        pass
    return '0'

def write_watermark(table_name, watermark_val, rows_loaded, status='SUCCESS'):
    df_new = spark.createDataFrame([{
        'table_name':     table_name,
        'last_watermark': watermark_val,
        'last_run_at':    datetime.utcnow(),
        'rows_loaded':    rows_loaded,
        'load_status':    status,
    }], schema=WATERMARK_SCHEMA)
    if DeltaTable.isDeltaTable(spark, WATERMARK_PATH):
        DeltaTable.forPath(spark, WATERMARK_PATH).alias('t').merge(
            df_new.alias('s'), 't.table_name = s.table_name'
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df_new.write.format('delta').mode('overwrite').save(WATERMARK_PATH)

# CELL ********************

# CELL 3 — Cosmos DB reader builder
COSMOS_FORMAT = 'cosmos.oltp'
COSMOS_CF_FORMAT = 'cosmos.oltp.changeFeed'

def build_cosmos_options(cfg):
    return {
        'spark.cosmos.accountEndpoint':      get_secret(cfg['kv_endpoint']),
        'spark.cosmos.accountKey':           get_secret(cfg['kv_account_key']),
        'spark.cosmos.database':             cfg['cosmos_database'],
        'spark.cosmos.container':            cfg['cosmos_container'],
        'spark.cosmos.read.inferSchema.enabled': 'true',
        'spark.cosmos.read.partitioning.strategy': 'Default',
    }

def build_change_feed_options(cfg):
    checkpoint = f"{BRONZE_FILES}/_checkpoints/{cfg['cosmos_container']}_cf"
    opts = build_cosmos_options(cfg)
    opts.update({
        'spark.cosmos.changeFeed.startFrom':       cfg.get('change_feed_start', 'Beginning'),
        'spark.cosmos.changeFeed.mode':            'Incremental',
        'spark.cosmos.changeFeed.itemCountPerTriggerHint': '1000',
        'checkpointLocation':                      checkpoint,
    })
    return opts

def read_cosmos(cfg, watermark_val):
    opts = build_cosmos_options(cfg)
    if cfg.get('use_change_feed') and cfg['load_type'] == 'incremental':
        return spark.readStream.format(COSMOS_CF_FORMAT).options(**opts).load(), True
    # Batch read with _ts filter
    df = spark.read.format(COSMOS_FORMAT).options(**opts).load()
    if cfg['load_type'] == 'incremental' and watermark_val != '0':
        df = df.filter(col('_ts') > int(watermark_val))
    return df, False

# CELL ********************

# CELL 4 — Schema drift detection
def infer_and_store_schema(df, table_name):
    schema_file  = f'{SCHEMA_PATH}/{table_name}_schema.json'
    history_file = f'{SCHEMA_PATH}/{table_name}_schema_history.json'
    current_fields = {f.name: str(f.dataType) for f in df.schema.fields}
    drift_detected = False

    try:
        stored        = json.loads(mssparkutils.fs.head(schema_file, 65536))
        stored_fields = {f['name']: f['type'] for f in stored['fields']}
        new_cols     = set(current_fields) - set(stored_fields)
        dropped_cols = set(stored_fields)  - set(current_fields)
        type_changes = {
            c for c in current_fields
            if c in stored_fields and current_fields[c] != stored_fields[c]
        }

        if new_cols:
            log.warning(f'[SCHEMA DRIFT] {table_name}: NEW columns: {new_cols}')
            drift_detected = True

        if type_changes:
            for col_name in type_changes:
                log.warning(
                    f"[SCHEMA DRIFT] {table_name}: TYPE CHANGE on '{col_name}': "
                    f"{stored_fields[col_name]} -> {current_fields[col_name]}"
                )
            drift_detected = True

        if dropped_cols:
            log.error(
                f'[SCHEMA DRIFT] {table_name}: DROPPED columns: {dropped_cols}. '
                f'Silver transforms will fail. Pipeline halted.'
            )
            mssparkutils.fs.put(
                f'{SCHEMA_PATH}/{table_name}_DROP_ALERT.json',
                json.dumps({
                    'table':        table_name,
                    'dropped_cols': list(dropped_cols),
                    'detected_at':  str(datetime.utcnow()),
                    'action':       'PIPELINE_HALTED'
                }, indent=2),
                overwrite=True
            )
            raise ValueError(
                f'[SCHEMA DRIFT] Dropped columns detected on {table_name}: {dropped_cols}. '
                f'Review Silver transforms before re-running.'
            )

    except Exception as e:
        if 'SCHEMA DRIFT' in str(e) or 'DROPPED' in str(e):
            raise
        log.info(f'[SCHEMA] First run for {table_name} — storing baseline.')

    try:
        history_raw = mssparkutils.fs.head(history_file, 1_000_000)
        history     = json.loads(history_raw)
    except Exception:
        history     = {'table': table_name, 'versions': []}

    history['versions'].append({
        'observed_at': str(datetime.utcnow()),
        'fields':      [{'name': n, 'type': t} for n, t in current_fields.items()],
        'drift':       drift_detected
    })
    mssparkutils.fs.put(history_file, json.dumps(history, indent=2), overwrite=True)

    mssparkutils.fs.put(
        schema_file,
        json.dumps({
            'fields':          [{'name': n, 'type': t} for n, t in current_fields.items()],
            'updated_at':      str(datetime.utcnow()),
            'drift_on_update': drift_detected
        }, indent=2),
        overwrite=True
    )
    return df.schema

# CELL ********************

# CELL 5 — Column renames, quarantine + audit columns
def apply_column_renames(df, cfg):
    for src_col, tgt_col in cfg.get('column_renames', {}).items():
        if src_col in df.columns:
            df = df.withColumnRenamed(src_col, tgt_col)
    return df

def quarantine_bad_rows(df, table_name, cfg):
    """
    Registry-driven Bronze quarantine.
    Builds an OR-chain from DQ_RULES_REGISTRY for the table:
      - not_null_cols : null checks (falls back to cfg primary_keys if no entry)
      - enum_rules    : invalid categorical values (string-safe pre-cast)
    Range/business rules are deferred to Silver (require type casting first).
    Records the first matching rule name in _quarantine_reason per row.
    """
    from dq_rules_registry import DQ_RULES_REGISTRY
    from pyspark.sql.functions import when

    rules      = DQ_RULES_REGISTRY.get(table_name, {})
    not_null   = rules.get('not_null_cols', cfg.get('primary_keys', []))
    enum_rules = rules.get('enum_rules', {})

    bad    = lit(False)
    reason = lit('')

    # Rule 1: null checks
    for c in not_null:
        if c in df.columns:
            bad    = bad | col(c).isNull()
            reason = when(col(c).isNull(), lit(f'null:{c}')).otherwise(reason)

    # Rule 2: enum checks (string-safe pre-cast)
    for c, valid_vals in enum_rules.items():
        if c in df.columns:
            enum_bad = col(c).isNotNull() & ~col(c).isin(valid_vals)
            bad      = bad | enum_bad
            reason   = when(enum_bad, lit(f'enum:{c}')).otherwise(reason)

    df_bad   = df.filter(bad)                  .withColumn('_quarantine_reason', reason)                  .withColumn('_quarantine_at',     current_timestamp())
    df_clean = df.filter(~bad)

    bad_count = df_bad.count()
    if bad_count > 0:
        df_bad.write.format('delta').mode('append')               .option('mergeSchema', 'true')               .save(f'{QUARANTINE_PATH}/{table_name}')
        log.warning(f'[Q] {table_name}: {bad_count} rows quarantined')
    return df_clean
def add_audit_cols(df, cfg, watermark_val):
    pks = cfg['primary_keys']
    return df \
        .withColumn('_ingested_at',       current_timestamp()) \
        .withColumn('_source_system',     lit(f"CosmosDB_{cfg['cosmos_database']}")) \
        .withColumn('_source_table',      lit(cfg['cosmos_container'])) \
        .withColumn('_load_type',         lit(cfg['load_type'])) \
        .withColumn('_watermark_applied', lit(watermark_val)) \
        .withColumn('_row_hash',          md5(concat_ws('|', *[col(c) for c in pks])))

# CELL ********************

# CELL 6 — Bronze micro-batch writer (streaming) and batch writer
def write_micro_batch(batch_df, batch_id, bronze_path, table_name, cfg):
    if batch_df.isEmpty():
        return
    batch_df  = apply_column_renames(batch_df, cfg)
    df_clean  = quarantine_bad_rows(batch_df, table_name, cfg)
    df_bronze = add_audit_cols(df_clean, cfg, 'change_feed')
    df_bronze.write.format('delta').mode('append') \
             .option('mergeSchema', 'true').save(bronze_path)
    log.info(f'[BRONZE] {table_name} batch {batch_id}: {df_bronze.count()} rows written')

# CELL ********************

# CELL 7 — Master ingestion loop
cosmos_sources = get_sources_by_type('cosmosdb')
summary = []

for table_name, cfg in cosmos_sources.items():
    log.info(f'\n=== {table_name} ===')
    wm = None
    try:
        wm = read_watermark(table_name)
        bronze_path = f"{BRONZE_TABLES}/{cfg['bronze_table']}"
        df_or_stream, is_streaming = read_cosmos(cfg, wm)

        if is_streaming:
            checkpoint = f"{BRONZE_FILES}/_checkpoints/{cfg['cosmos_container']}_cf"
            query = (
                df_or_stream.writeStream
                .foreachBatch(lambda bdf, bid: write_micro_batch(bdf, bid, bronze_path, table_name, cfg))
                .option('checkpointLocation', checkpoint)
                .trigger(availableNow=True)
                .start()
            )
            query.awaitTermination()
        else:
            infer_and_store_schema(df_or_stream, table_name)
            df_or_stream = apply_column_renames(df_or_stream, cfg)
            df_clean  = quarantine_bad_rows(df_or_stream, table_name, cfg)
            df_bronze = add_audit_cols(df_clean, cfg, wm)
            rows = df_bronze.count()
            if rows > 0:
                df_bronze.write.format('delta').mode('append') \
                         .option('mergeSchema', 'true').save(bronze_path)
            new_wm = str(df_clean.agg({'_ts': 'max'}).collect()[0][0]) if rows > 0 else wm
            write_watermark(table_name, new_wm, rows, 'SUCCESS')

        write_watermark(table_name, str(datetime.utcnow()), -1, 'SUCCESS')
        summary.append({'table': table_name, 'rows': -1, 'status': 'SUCCESS'})
    except Exception as e:
        log.error(f'[ERROR] {table_name}: {e}')
        write_watermark(table_name, wm or '0', 0, 'FAILED')
        summary.append({'table': table_name, 'rows': 0, 'status': 'FAILED', 'error': str(e)})

failed = [s for s in summary if s['status'] == 'FAILED']
mssparkutils.notebook.exit(json.dumps({
    'status':  'FAILED' if failed else 'SUCCESS',
    'tables':  len(summary),
    'failed':  [s['table'] for s in failed],
}))
