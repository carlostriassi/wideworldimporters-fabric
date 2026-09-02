# Fabric notebook source


# MARKDOWN ********************

# # Bronze: MongoDB ingestion
# Registry-driven ingestion from MongoDB (Atlas or self-hosted) using the
# MongoDB Spark Connector v10 (`org.mongodb.spark:mongo-spark-connector`).
# Supports incremental loads via `_id` / timestamp field watermarks and
# full collection reads. Nested documents are preserved as structs.
# 
# Library required: `org.mongodb.spark:mongo-spark-connector_2.12:10.x.x`
# (add to cluster Maven libraries).
# 
# Registry entry example:
# ```python
# 'mongo_orders': {
#     'connector_type': 'mongodb',
#     'kv_conn_string': 'mongo-atlas-conn-string',
#     'mongo_database': 'ecommerce',
#     'mongo_collection': 'orders',
#     'pipeline':       [],               # optional aggregation pipeline
#     'partition_key':  '_id',
#     'num_partitions': 8,
#     'bronze_table':   'mongodb/orders',
#     'load_type':      'incremental',
#     'watermark_col':  'updatedAt',
#     'primary_keys':   ['_id'],
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
from pyspark.sql.functions import current_timestamp, lit, col, md5, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime
import json, logging

from workspace_config import (
    BRONZE_TABLES, WATERMARK_PATH, SCHEMA_PATH, QUARANTINE_PATH,
    apply_spark_settings, get_secret
)
from source_registry import get_sources_by_type

spark = SparkSession.builder.getOrCreate()
apply_spark_settings(spark)
log = logging.getLogger('bronze_mongodb')

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
    return '1900-01-01 00:00:00'

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

# CELL 3 — MongoDB Spark Connector reader builder
MONGO_FORMAT = 'mongodb'

def build_mongo_options(cfg, watermark_val):
    wm_col  = cfg.get('watermark_col')
    is_incr = cfg['load_type'] == 'incremental'
    conn_str = get_secret(cfg['kv_conn_string'])

    opts = {
        'spark.mongodb.read.connection.uri':     conn_str,
        'spark.mongodb.read.database':           cfg['mongo_database'],
        'spark.mongodb.read.collection':         cfg['mongo_collection'],
        'spark.mongodb.read.partitioner':        'com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner',
        'spark.mongodb.read.partitioner.options.partition.key':   cfg.get('partition_key', '_id'),
        'spark.mongodb.read.partitioner.options.samples.per.partition': '200',
        'spark.mongodb.read.readPreference.name': 'secondaryPreferred',
    }

    pipeline = list(cfg.get('pipeline', []))
    if is_incr and wm_col and watermark_val != '1900-01-01 00:00:00':
        pipeline.insert(0, {'$match': {wm_col: {'$gt': watermark_val}}})
    if pipeline:
        opts['spark.mongodb.read.aggregation.pipeline'] = json.dumps(pipeline)

    return opts

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

def _resolve_col(cfg, df, name):
    """Resolve a registry column name onto the DataFrame's actual column name.

    `apply_column_renames()` runs before quarantine, audit stamping and the
    watermark max(), so a name declared in source casing (e.g. 'LastEditedWhen')
    is already 'last_edited_when' on the DataFrame — and vice versa when the
    registry declares the post-rename name. Accept either form.
    """
    cols    = set(df.columns)
    if name in cols:
        return name
    renames = cfg.get('column_renames') or {}
    if renames.get(name) in cols:
        return renames[name]
    for src, tgt in renames.items():          # registry used the renamed name
        if tgt == name and src in cols:
            return src
    raise KeyError(
        f"column '{name}' not found on the DataFrame after column renames. "
        f"Check primary_keys / watermark_col / column_renames in "
        f"source_registry.py. Available columns: {sorted(cols)}"
    )

def _resolve_pks(cfg, df):
    """Map registry `primary_keys` onto the DataFrame's post-rename names."""
    return [_resolve_col(cfg, df, pk) for pk in cfg.get('primary_keys', [])]

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
    not_null   = rules.get('not_null_cols', _resolve_pks(cfg, df))
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
    pks = _resolve_pks(cfg, df)
    return df \
        .withColumn('_ingested_at',       current_timestamp()) \
        .withColumn('_source_system',     lit(f"MongoDB_{cfg['mongo_database']}")) \
        .withColumn('_source_table',      lit(cfg['mongo_collection'])) \
        .withColumn('_load_type',         lit(cfg['load_type'])) \
        .withColumn('_watermark_applied', lit(watermark_val)) \
        .withColumn('_row_hash',          md5(concat_ws('|', *[col(c).cast('string') for c in pks])))

# CELL ********************

# CELL 6 — Bronze writer
def write_bronze(df, cfg, table_name):
    bronze_path = f"{BRONZE_TABLES}/{cfg['bronze_table']}"
    row_count   = df.count()
    if row_count == 0:
        log.info(f'[BRONZE] {table_name}: 0 new rows — skipping write')
        return 0
    mode = 'overwrite' if cfg['load_type'] == 'full' else 'append'
    df.write.format('delta').mode(mode) \
      .option('mergeSchema', 'true').save(bronze_path)
    log.info(f'[BRONZE] {table_name}: {row_count} rows → {bronze_path}')
    return row_count

# CELL ********************

# CELL 7 — Master ingestion loop
mongo_sources = get_sources_by_type('mongodb')
summary = []

for table_name, cfg in mongo_sources.items():
    log.info(f'\n=== {table_name} ===')
    wm = None
    try:
        wm   = read_watermark(table_name) if cfg['load_type'] == 'incremental' else None
        opts = build_mongo_options(cfg, wm)
        df_raw = spark.read.format(MONGO_FORMAT).options(**opts).load()

        # Cast ObjectId _id to string for Delta compatibility
        if '_id' in df_raw.columns:
            df_raw = df_raw.withColumn('_id', col('_id').cast('string'))

        infer_and_store_schema(df_raw, table_name)
        df_raw    = apply_column_renames(df_raw, cfg)
        df_clean  = quarantine_bad_rows(df_raw, table_name, cfg)
        df_bronze = add_audit_cols(df_clean, cfg, wm or 'full_load')
        rows = write_bronze(df_bronze, cfg, table_name)
        new_wm = df_clean.agg({_resolve_col(cfg, df_clean, cfg['watermark_col']): 'max'}).collect()[0][0] \
                 if cfg.get('watermark_col') and rows > 0 else str(datetime.utcnow())
        write_watermark(table_name, str(new_wm), rows, 'SUCCESS')
        summary.append({'table': table_name, 'rows': rows, 'status': 'SUCCESS'})
    except Exception as e:
        log.error(f'[ERROR] {table_name}: {e}')
        write_watermark(table_name, wm or '', 0, 'FAILED')
        summary.append({'table': table_name, 'rows': 0, 'status': 'FAILED', 'error': str(e)})

failed = [s for s in summary if s['status'] == 'FAILED']
mssparkutils.notebook.exit(json.dumps({
    'status':  'FAILED' if failed else 'SUCCESS',
    'tables':  len(summary),
    'failed':  [s['table'] for s in failed],
}))
