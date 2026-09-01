# Fabric notebook source


# MARKDOWN ********************

# # Bronze: Generic REST API ingestion
# Registry-driven ingestion from any JSON REST API. Supports OAuth2 (client
# credentials), API Key, and Basic authentication. Handles cursor-based,
# offset-based, and link-header pagination. Flattens nested JSON responses.
# 
# Registry entry example:
# ```python
# 'hubspot_contacts': {
#     'connector_type':  'rest_api',
#     'base_url':        'https://api.hubapi.com',
#     'endpoint':        '/crm/v3/objects/contacts',
#     'auth_type':       'api_key',    # api_key | oauth2_cc | basic
#     'kv_api_key':      'hubspot-api-key',
#     'auth_header':     'Authorization',
#     'auth_prefix':     'Bearer',
#     'query_params':    {'limit': 100, 'properties': 'firstname,lastname,email,hs_lastmodifieddate'},
#     'response_root':   'results',   # JSON path to the records array
#     'next_page_key':   'paging.next.after',  # cursor field in response
#     'next_param_name': 'after',     # query param name to pass the cursor
#     'max_pages':       1000,
#     'bronze_table':    'hubspot/contacts',
#     'load_type':       'incremental',
#     'watermark_col':   'hs_lastmodifieddate',
#     'primary_keys':    ['id'],
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
import json, logging, requests, base64, time
from functools import reduce

from workspace_config import (
    BRONZE_TABLES, WATERMARK_PATH, SCHEMA_PATH, QUARANTINE_PATH,
    apply_spark_settings, get_secret
)
from source_registry import get_sources_by_type

spark = SparkSession.builder.getOrCreate()
apply_spark_settings(spark)
log = logging.getLogger('bronze_rest_api')

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
    return '1900-01-01T00:00:00Z'

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

# CELL 3 — Auth builders
def build_headers(cfg):
    auth_type = cfg.get('auth_type', 'api_key')
    headers   = {'Accept': 'application/json', 'Content-Type': 'application/json'}

    if auth_type == 'api_key':
        api_key = get_secret(cfg['kv_api_key'])
        prefix  = cfg.get('auth_prefix', 'Bearer')
        header  = cfg.get('auth_header', 'Authorization')
        headers[header] = f'{prefix} {api_key}' if prefix else api_key

    elif auth_type == 'oauth2_cc':
        token_url = cfg['token_url']
        client_id = get_secret(cfg['kv_client_id'])
        client_secret = get_secret(cfg['kv_client_secret'])
        scope = cfg.get('scope', '')
        resp = requests.post(token_url, data={
            'grant_type': 'client_credentials',
            'client_id': client_id, 'client_secret': client_secret,
            'scope': scope
        }, timeout=30)
        resp.raise_for_status()
        token = resp.json()['access_token']
        headers['Authorization'] = f'Bearer {token}'

    elif auth_type == 'basic':
        user = get_secret(cfg['kv_username'])
        pw   = get_secret(cfg['kv_password'])
        cred = base64.b64encode(f'{user}:{pw}'.encode()).decode()
        headers['Authorization'] = f'Basic {cred}'

    return headers

def get_nested(data, path):
    """Resolve dotted path like 'paging.next.after' in a dict."""
    parts = path.split('.')
    return reduce(lambda d, k: d.get(k, {}) if isinstance(d, dict) else None, parts, data)

def fetch_all_records(cfg, watermark_val):
    headers     = build_headers(cfg)
    base_url    = cfg['base_url'].rstrip('/')
    endpoint    = cfg['endpoint']
    params      = dict(cfg.get('query_params', {}))
    response_root  = cfg.get('response_root', 'results')
    next_page_key  = cfg.get('next_page_key')
    next_param_name = cfg.get('next_param_name', 'page')
    max_pages   = cfg.get('max_pages', 500)
    wm_col      = cfg.get('watermark_col')
    wm_param    = cfg.get('watermark_param')

    if wm_param and watermark_val != '1900-01-01T00:00:00Z':
        params[wm_param] = watermark_val

    all_records, page = [], 0
    while page < max_pages:
        resp = requests.get(f'{base_url}{endpoint}', headers=headers, params=params, timeout=60)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', 10))
            log.warning(f'[REST] rate limited — sleeping {retry_after}s')
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        data    = resp.json()
        records = get_nested(data, response_root) if response_root else data
        if not isinstance(records, list):
            records = [records]
        if not records:
            break
        all_records.extend(records)
        page += 1
        log.info(f'[REST] page {page}: {len(all_records)} total records')

        if next_page_key:
            cursor = get_nested(data, next_page_key)
            if not cursor:
                break
            params[next_param_name] = cursor
        else:
            break

    return all_records

def flatten_record(record, prefix=''):
    flat = {}
    for k, v in record.items():
        key = f'{prefix}{k}' if prefix else k
        if isinstance(v, dict):
            flat.update(flatten_record(v, prefix=f'{key}_'))
        elif isinstance(v, list):
            flat[key] = json.dumps(v)
        else:
            flat[key] = v
    return flat

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
    pks  = cfg['primary_keys']
    host = cfg['base_url'].replace('https://', '').replace('http://', '').split('/')[0]
    return df \
        .withColumn('_ingested_at',       current_timestamp()) \
        .withColumn('_source_system',     lit(f'REST_{host}')) \
        .withColumn('_source_table',      lit(cfg['endpoint'])) \
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
api_sources = get_sources_by_type('rest_api')
summary = []

for table_name, cfg in api_sources.items():
    log.info(f'\n=== {table_name} ===')
    wm = None
    try:
        wm = read_watermark(table_name) if cfg['load_type'] == 'incremental' else '1900-01-01T00:00:00Z'
        records = fetch_all_records(cfg, wm)
        if not records:
            log.info(f'[BRONZE] {table_name}: 0 new records')
            write_watermark(table_name, str(datetime.utcnow()), 0, 'SUCCESS')
            summary.append({'table': table_name, 'rows': 0, 'status': 'SUCCESS'})
            continue

        flat_records = [flatten_record(r) for r in records]
        flat_records = [{k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v)
                         for k, v in r.items()} for r in flat_records]
        df_raw = spark.createDataFrame(flat_records)
        infer_and_store_schema(df_raw, table_name)
        df_raw    = apply_column_renames(df_raw, cfg)
        df_clean  = quarantine_bad_rows(df_raw, table_name, cfg)
        df_bronze = add_audit_cols(df_clean, cfg, wm)
        rows = write_bronze(df_bronze, cfg, table_name)

        wm_col = cfg.get('watermark_col')
        new_wm = df_clean.agg({wm_col: 'max'}).collect()[0][0] \
                 if wm_col and rows > 0 else str(datetime.utcnow())
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
