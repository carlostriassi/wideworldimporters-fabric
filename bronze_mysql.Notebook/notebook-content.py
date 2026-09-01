# Fabric notebook source


# MARKDOWN ********************

# # Bronze: MySQL / MariaDB ingestion
# Registry-driven JDBC ingestion from MySQL or MariaDB with watermark, schema
# drift detection, parallel partitioning, quarantine, and Delta append.
# 
# Driver JAR required: `mysql:mysql-connector-java:8.x.x` (add to cluster libraries).
# For MariaDB: `org.mariadb.jdbc:mariadb-java-client:3.x.x`.
# 
# Registry entry example:
# ```python
# 'mysql_customers': {
#     'connector_type': 'mysql',
#     'kv_user_key':    'mysql-app-user',
#     'kv_pass_key':    'mysql-app-pass',
#     'jdbc_host':      'prod-mysql.company.com',
#     'jdbc_port':      3306,
#     'jdbc_db':        'ecommerce',
#     'source_schema':  'ecommerce',
#     'source_table':   'customers',
#     'bronze_table':   'mysql/customers',
#     'load_type':      'incremental',
#     'watermark_col':  'updated_at',
#     'partition_col':  'customer_id',
#     'lower_bound':    1,
#     'upper_bound':    50000000,
#     'num_partitions': 8,
#     'primary_keys':   ['customer_id'],
#     'use_ssl':        True,
#     'driver_variant': 'mysql',    # mysql | mariadb
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
log = logging.getLogger('bronze_mysql')

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

# CELL 3 — MySQL/MariaDB JDBC connection builder
DRIVER_MAP = {
    'mysql':   'com.mysql.cj.jdbc.Driver',
    'mariadb': 'org.mariadb.jdbc.Driver',
}

def build_jdbc_url(cfg):
    variant = cfg.get('driver_variant', 'mysql')
    host    = cfg['jdbc_host']
    port    = cfg.get('jdbc_port', 3306)
    db      = cfg['jdbc_db']
    use_ssl = 'true' if cfg.get('use_ssl', True) else 'false'
    if variant == 'mariadb':
        return f'jdbc:mariadb://{host}:{port}/{db}?useSSL={use_ssl}&allowPublicKeyRetrieval=true'
    return (f'jdbc:mysql://{host}:{port}/{db}'
            f'?useSSL={use_ssl}&allowPublicKeyRetrieval=true'
            f'&serverTimezone=UTC&characterEncoding=UTF-8')

def build_jdbc_options(cfg, watermark_val):
    schema   = cfg['source_schema']
    table    = cfg['source_table']
    wm_col   = cfg.get('watermark_col')
    is_incr  = cfg['load_type'] == 'incremental'
    variant  = cfg.get('driver_variant', 'mysql')

    if is_incr and wm_col and watermark_val != '1900-01-01 00:00:00':
        pushdown = f"(SELECT * FROM `{schema}`.`{table}` WHERE `{wm_col}` > '{watermark_val}') AS src"
    else:
        pushdown = f'`{schema}`.`{table}`'

    opts = {
        'url':              build_jdbc_url(cfg),
        'dbtable':          pushdown,
        'user':             get_secret(cfg['kv_user_key']),
        'password':         get_secret(cfg['kv_pass_key']),
        'driver':           DRIVER_MAP.get(variant, DRIVER_MAP['mysql']),
        'pushDownPredicate':'true',
        'fetchsize':        '10000',
    }
    if cfg.get('partition_col'):
        opts.update({
            'partitionColumn': cfg['partition_col'],
            'lowerBound':      str(cfg['lower_bound']),
            'upperBound':      str(cfg['upper_bound']),
            'numPartitions':   str(cfg['num_partitions']),
        })
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
    pks     = cfg['primary_keys']
    variant = cfg.get('driver_variant', 'mysql').upper()
    return df \
        .withColumn('_ingested_at',       current_timestamp()) \
        .withColumn('_source_system',     lit(f"{variant}_{cfg['jdbc_db']}")) \
        .withColumn('_source_table',      lit(f"{cfg['source_schema']}.{cfg['source_table']}")) \
        .withColumn('_load_type',         lit(cfg['load_type'])) \
        .withColumn('_watermark_applied', lit(watermark_val)) \
        .withColumn('_row_hash',          md5(concat_ws('|', *[col(c) for c in pks])))

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
mysql_sources = get_sources_by_type('mysql')
summary = []

for table_name, cfg in mysql_sources.items():
    log.info(f'\n=== {table_name} ===')
    wm = None
    try:
        wm   = read_watermark(table_name) if cfg['load_type'] == 'incremental' else None
        opts = build_jdbc_options(cfg, wm)
        df_raw = spark.read.format('jdbc').options(**opts).load()
        infer_and_store_schema(df_raw, table_name)
        df_raw    = apply_column_renames(df_raw, cfg)
        df_clean  = quarantine_bad_rows(df_raw, table_name, cfg)
        df_bronze = add_audit_cols(df_clean, cfg, wm or 'full_load')
        rows = write_bronze(df_bronze, cfg, table_name)
        new_wm = df_clean.agg({cfg['watermark_col']: 'max'}).collect()[0][0] \
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
