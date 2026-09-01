# Fabric notebook source


# MARKDOWN ********************

# # Bronze: Azure Event Hub ingestion (Structured Streaming)
# Registry-driven Structured Streaming ingestion from Azure Event Hub using the
# Kafka protocol with SASL_SSL authentication. Parses JSON payloads, applies
# watermarks for late data, and writes micro-batches to Bronze Delta (append-only).
# 
# **No code changes needed to add a new event hub** — add an entry to
# `config/source_registry.py` with `connector_type: eventhub`.
# 
# Registry entry example (flat payload — one JSON object per event, e.g. IoT telemetry
# or an application publishing domain events with no upstream DB table):
# ```python
# 'iot_telemetry': {
#     'connector_type':     'eventhub',
#     'kv_eh_conn_str':     'eh-iot-connection-string',
#     'eh_namespace':       'mynamespace.servicebus.windows.net',
#     'eh_name':            'iot-telemetry',
#     'consumer_group':     '$Default',
#     'starting_offsets':   'latest',
#     'max_offsets_trigger': 100000,
#     'checkpoint_path':    '{BRONZE_FILES}/_checkpoints/iot_telemetry',
#     'payload_schema':     <StructType>,
#     'watermark_col':      'event_time',
#     'watermark_delay':    '10 minutes',
#     'bronze_table':       'iot/iot_telemetry',
#     'load_type':          'streaming',
#     'primary_keys':       ['device_id', 'event_time'],
# }
# ```
#
# Registry entry example (CDC envelope — the common case when the event hub mirrors an
# existing relational table via a CDC connector such as Debezium, Qlik Replicate, or
# Fabric Mirroring's CDC feed. `payload_schema` describes the OUTER envelope, whose
# `after`/`before` fields are themselves StructTypes matching the table's own row shape —
# derived from sql_discovery_results Section 2 exactly as for a JDBC source):
# ```python
# 'healthcare_patients_stream': {
#     'connector_type':     'eventhub',
#     'kv_eh_conn_str':     'eh-healthcare-patients-connection-string',
#     'eh_namespace':       'mynamespace.servicebus.windows.net',
#     'eh_name':            'healthcare-patients-cdc',
#     'consumer_group':     '$Default',
#     'starting_offsets':   'latest',
#     'max_offsets_trigger': 50000,
#     'checkpoint_path':    '{BRONZE_FILES}/_checkpoints/healthcare_patients_stream',
#     'payload_schema':     <StructType with fields: op (string), ts_ms (long),
#                            before (StructType matching table row), after (StructType matching table row)>,
#     'cdc_envelope':       True,           # default False — enables envelope parsing below
#     'cdc_op_col':         'op',           # default 'op'
#     'cdc_after_col':      'after',        # default 'after'
#     'cdc_before_col':     'before',       # default 'before'
#     'watermark_col':      'modified_at',  # must exist on the table's row schema
#     'watermark_delay':    '10 minutes',
#     'bronze_table':       'healthcare/healthcare_patients_stream',
#     'load_type':          'streaming',
#     'primary_keys':       ['patient_id'],
# }
# ```
# CDC caveat: `_cdc_deleted` is captured in Bronze (append-only, so a delete event lands
# as a new row flagged True) but Silver's MERGE has no delete clause today — same
# limitation documented in CLAUDE.md for `cdc_mode: "cdf"`. Add a Silver business_rule
# that filters/soft-deletes on `_cdc_deleted` before treating this as a solved problem.

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

import shutil, importlib
for _mod_name in ("workspace_config", "source_registry"):
    sys.modules.pop(_mod_name, None)
shutil.rmtree(f"{_cfg_local}/__pycache__", ignore_errors=True)

sys.path.insert(0, _cfg_local)
# On a reused Spark session (Fabric may keep a live session across runs), Python's
# sys.path_importer_cache can keep serving a stale directory listing for _cfg_local
# even after these files are rewritten with new content — popping sys.modules alone
# does not force a re-scan. Without this, a session that already imported these
# modules once can silently keep using an older config on every later run.
importlib.invalidate_caches()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, col, md5, concat_ws,
    from_json, from_unixtime, to_timestamp, when
)
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
log = logging.getLogger('bronze_eventhub')

# CELL ********************

# CELL 2 — Kafka/Event Hub reader builder
def build_eh_read_options(cfg):
    conn_str = get_secret(cfg['kv_eh_conn_str'])
    namespace = cfg['eh_namespace']
    eh_name   = cfg['eh_name']

    # Kafka SASL_SSL connection string format for Event Hub
    sasl_config = (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn_str}";'
    )
    return {
        'kafka.bootstrap.servers':                  f'{namespace}:9093',
        'subscribe':                                 eh_name,
        'kafka.security.protocol':                  'SASL_SSL',
        'kafka.sasl.mechanism':                     'PLAIN',
        'kafka.sasl.jaas.config':                   sasl_config,
        'kafka.request.timeout.ms':                 '60000',
        'kafka.session.timeout.ms':                 '30000',
        'startingOffsets':                          cfg.get('starting_offsets', 'latest'),
        'maxOffsetsPerTrigger':                     str(cfg.get('max_offsets_trigger', 50000)),
        'failOnDataLoss':                           'false',
    }

# CELL ********************

# CELL 3 — Payload parser + audit columns
def parse_and_enrich(df_raw, cfg):
    payload_schema = cfg['payload_schema']
    wm_col         = cfg.get('watermark_col', 'event_time')
    wm_delay       = cfg.get('watermark_delay', '10 minutes')
    pks            = cfg['primary_keys']
    cdc_envelope   = cfg.get('cdc_envelope', False)

    df_payload = df_raw.select(
        from_json(col('value').cast('string'), payload_schema).alias('payload'),
        col('topic').alias('_eh_topic'),
        col('partition').alias('_eh_partition'),
        col('offset').cast('long').alias('_eh_offset'),
        col('timestamp').alias('_eh_enqueue_time'),
    )

    if cdc_envelope:
        # CDC envelope (Debezium-style): {op, ts_ms, before, after}. Deletes carry no
        # 'after' image, so the row is taken from 'before' instead — Bronze is
        # append-only, so a delete lands as a normal row flagged _cdc_deleted=True.
        op_col     = cfg.get('cdc_op_col', 'op')
        after_col  = cfg.get('cdc_after_col', 'after')
        before_col = cfg.get('cdc_before_col', 'before')

        df_parsed = (
            df_payload
            .withColumn('_cdc_op', col(f'payload.{op_col}'))
            .withColumn(
                '_cdc_row',
                when(col('_cdc_op') == 'd', col(f'payload.{before_col}'))
                .otherwise(col(f'payload.{after_col}'))
            )
            .withColumn('_cdc_deleted', col('_cdc_op') == lit('d'))
            .select(
                '_cdc_row.*', '_cdc_op', '_cdc_deleted',
                '_eh_topic', '_eh_partition', '_eh_offset', '_eh_enqueue_time'
            )
        )
    else:
        df_parsed = (
            df_payload
            .withColumn('_cdc_op', lit(None).cast('string'))
            .withColumn('_cdc_deleted', lit(False))
            .select(
                'payload.*', '_cdc_op', '_cdc_deleted',
                '_eh_topic', '_eh_partition', '_eh_offset', '_eh_enqueue_time'
            )
        )

    df_wm = df_parsed.withWatermark(wm_col, wm_delay)

    return df_wm \
        .withColumn('_ingested_at',       current_timestamp()) \
        .withColumn('_source_system',     lit(f"EventHub_{cfg['eh_namespace'].split('.')[0]}")) \
        .withColumn('_source_table',      lit(cfg['eh_name'])) \
        .withColumn('_load_type',         lit('streaming')) \
        .withColumn('_watermark_applied', lit(wm_delay)) \
        .withColumn('_row_hash',          md5(concat_ws('|', *[col(c) for c in pks])))

# CELL ********************

# CELL 4 — Schema drift detection (applied once on first micro-batch)
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

# CELL 5 — Column renames + micro-batch writer (called per batch by foreachBatch)
def apply_column_renames(df, cfg):
    for src_col, tgt_col in cfg.get('column_renames', {}).items():
        if src_col in df.columns:
            df = df.withColumnRenamed(src_col, tgt_col)
    return df

def write_micro_batch(batch_df, batch_id, bronze_path, table_name):
    if batch_df.isEmpty():
        return
    row_count = batch_df.count()
    batch_df.write.format('delta').mode('append') \
            .option('mergeSchema', 'true').save(bronze_path)
    log.info(f'[BRONZE] {table_name} batch {batch_id}: {row_count} rows written')

# CELL ********************

# CELL 6 — Master streaming loop (availableNow trigger for pipeline runs)
eh_sources = get_sources_by_type('eventhub')
summary = []

for table_name, cfg in eh_sources.items():
    log.info(f'\n=== {table_name} ===')
    try:
        read_opts    = build_eh_read_options(cfg)
        checkpoint   = cfg['checkpoint_path'].format(BRONZE_FILES=BRONZE_FILES)
        bronze_path  = f"{BRONZE_TABLES}/{cfg['bronze_table']}"

        df_raw = spark.readStream.format('kafka').options(**read_opts).load()
        df_enriched = parse_and_enrich(df_raw, cfg)

        # Schema drift check on first batch only
        schema_checked = [False]
        def batch_writer(batch_df, batch_id):
            if not schema_checked[0] and not batch_df.isEmpty():
                infer_and_store_schema(batch_df, table_name)
                schema_checked[0] = True
            batch_df = apply_column_renames(batch_df, cfg)
            write_micro_batch(batch_df, batch_id, bronze_path, table_name)

        query = (
            df_enriched.writeStream
            .format('delta')
            .foreachBatch(batch_writer)
            .option('checkpointLocation', checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        summary.append({'table': table_name, 'rows': -1, 'status': 'SUCCESS'})
    except Exception as e:
        log.error(f'[ERROR] {table_name}: {e}')
        summary.append({'table': table_name, 'rows': 0, 'status': 'FAILED', 'error': str(e)})

failed = [s for s in summary if s['status'] == 'FAILED']
mssparkutils.notebook.exit(json.dumps({
    'status':  'FAILED' if failed else 'SUCCESS',
    'tables':  len(summary),
    'failed':  [s['table'] for s in failed],
}))
