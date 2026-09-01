# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Silver: DQ, deduplication, standardisation
# Registry-driven Silver pipeline. All rules come from `config/dq_rules_registry.py`.
# No code changes needed to add DQ rules for a new table.

# CELL ********************

# CELL 1 — Imports + config
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

# Drop any previously-imported config modules so a re-upload of these files
# in BronzeLH/Files/config/ is picked up without restarting Spark. Also
# remove their .pyc bytecode caches in case mssparkutils.fs.head didn't
# touch the .py mtime in a way Python's import cache trusts.
import shutil, importlib
for _mod_name in (
    "workspace_config", "source_registry", "dq_rules_registry",
    "star_schema_registry", "security_registry",
):
    sys.modules.pop(_mod_name, None)
shutil.rmtree(f"{_cfg_local}/__pycache__", ignore_errors=True)

sys.path.insert(0, _cfg_local)
# On a reused Spark session (Fabric may keep a live session across runs), Python's
# sys.path_importer_cache can keep serving a stale directory listing for _cfg_local
# even after these files are rewritten with new content — popping sys.modules alone
# does not force a re-scan. Without this, a session that already imported these
# modules once can silently keep using an older config on every later run.
importlib.invalidate_caches()

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, sha2, concat_ws, coalesce,
    trim, upper, lower, regexp_replace, regexp_extract, when, row_number,
    length, expr, array, array_remove, array_union, datediff,
    stddev as spark_stddev, mean as spark_mean,
    max as spark_max, avg as spark_avg,
)
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import json, logging

from workspace_config import (
    BRONZE_TABLES, SILVER_TABLES, SILVER_FILES, SILVER_QUARANTINE,
    SILVER_METRICS, apply_spark_settings, PIPELINE_TIER
)
from dq_rules_registry import DQ_RULES_REGISTRY, get_dq_rules
from source_registry import SOURCE_REGISTRY

spark = SparkSession.builder.getOrCreate()
apply_spark_settings(spark)
log = logging.getLogger('silver_dq')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2 — DQMetrics class
class DQMetrics:
    def __init__(self, table_name: str, run_id: str):
        self.table_name = table_name
        self.run_id     = run_id
        self.run_at     = datetime.utcnow()
        self.stages     = {}

    def record(self, stage: str, df: DataFrame) -> DataFrame:
        self.stages[stage] = df.count()
        log.info(f'[DQ] {self.table_name}.{stage}: {self.stages[stage]:,}')
        return df

    def rejection_rate(self) -> float:
        raw   = self.stages.get('bronze_read', 1)
        final = self.stages.get('silver_write', 0)
        return round((raw - final) / raw * 100, 2)

    def persist(self):
        rows = [{'run_id': self.run_id, 'table_name': self.table_name,
                 'run_at': self.run_at, 'stage': k, 'row_count': v}
                for k, v in self.stages.items()]
        spark.createDataFrame(rows).write.format('delta').mode('append') \
             .option('mergeSchema', 'true').save(SILVER_METRICS)

    def assert_threshold(self, max_rejection_pct: float = 5.0):
        rate = self.rejection_rate()
        if rate > max_rejection_pct:
            raise ValueError(
                f'[DQ GATE] {self.table_name}: {rate}% rejection exceeds {max_rejection_pct}%'
            )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3 — Stage 1: null enforcement + type casting
def enforce_nulls_and_types(df, table_name, rules, metrics):
    for c, t in rules.get('cast_rules', {}).items():
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(t))

    null_cond = lit(False)
    for c in rules.get('not_null_cols', []):
        if c in df.columns:
            null_cond = null_cond | col(c).isNull()

    df_bad = df.filter(null_cond) \
               .withColumn('_dq_stage',  lit('null_or_cast')) \
               .withColumn('_dq_run_id', lit(metrics.run_id)) \
               .withColumn('_dq_at',     current_timestamp())
    df_clean = df.filter(~null_cond)

    if df_bad.count() > 0:
        df_bad.write.format('delta').mode('append').option('mergeSchema','true') \
              .save(f'{SILVER_QUARANTINE}/{table_name}')

    return metrics.record('after_null_type', df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4 — Stage 2: range clipping + enum validation
def apply_range_and_enum(df, table_name, rules, metrics):
    for c, r in rules.get('range_rules', {}).items():
        if c not in df.columns:
            continue
        lo, hi = r.get('min'), r.get('max')
        orig_type = df.schema[c].dataType
        # Observability: capture rows that would be clipped, BEFORE mutation.
        # Soft-clip is preserved (rows are not removed) — this is data for triage,
        # not enforcement. Each violated (row, col) is written separately so the
        # triage script can mine per-column distributions for range proposals.
        clip_cond = lit(False)
        if lo is not None:
            clip_cond = clip_cond | (col(c) < lit(lo))
        if hi is not None:
            clip_cond = clip_cond | (col(c) > lit(hi))
        df_clipped = df.filter(col(c).isNotNull() & clip_cond)
        clip_ct = df_clipped.count()
        if clip_ct > 0:
            lo_lit = lit(str(lo)) if lo is not None else lit(None).cast('string')
            hi_lit = lit(str(hi)) if hi is not None else lit(None).cast('string')
            df_clipped \
                .withColumn('_dq_stage',        lit(f'range_clip:{c}')) \
                .withColumn('_dq_run_id',       lit(metrics.run_id)) \
                .withColumn('_dq_at',           current_timestamp()) \
                .withColumn('_clip_col',        lit(c)) \
                .withColumn('_clip_orig_value', col(c).cast('string')) \
                .withColumn('_clip_lower',      lo_lit) \
                .withColumn('_clip_upper',      hi_lit) \
                .write.format('delta').mode('append').option('mergeSchema', 'true') \
                .save(f'{SILVER_QUARANTINE}/{table_name}_clipped')
            log.warning(f'[RANGE] {table_name}.{c}: {clip_ct} rows clipped to [{lo},{hi}]')
        if lo is not None:
            df = df.withColumn(c, when(col(c) < lo, lit(lo)).otherwise(col(c)))
        if hi is not None:
            df = df.withColumn(c, when(col(c) > hi, lit(hi)).otherwise(col(c)))
        # Restore declared dtype: when().otherwise() with Python-float bounds
        # promotes decimal columns to double, which breaks the Silver MERGE
        # (DELTA_FAILED_TO_MERGE_FIELDS) on subsequent runs.
        df = df.withColumn(c, col(c).cast(orig_type))

    bad_enum = lit(False)
    for c, vals in rules.get('enum_rules', {}).items():
        if c in df.columns:
            bad_enum = bad_enum | (~col(c).isin(vals) & col(c).isNotNull())

    df_bad   = df.filter(bad_enum) \
                 .withColumn('_dq_stage',  lit('enum_fail')) \
                 .withColumn('_dq_run_id', lit(metrics.run_id)) \
                 .withColumn('_dq_at',     current_timestamp())
    df_clean = df.filter(~bad_enum)

    if df_bad.count() > 0:
        df_bad.write.format('delta').mode('append').option('mergeSchema','true') \
              .save(f'{SILVER_QUARANTINE}/{table_name}')

    return metrics.record('after_range_enum', df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5 — Stage 3: exact deduplication
def exact_dedup(df, table_name, rules, metrics):
    key_cols    = rules['dedup_key']
    tiebreak    = rules['dedup_tiebreak']
    w = Window.partitionBy(*key_cols).orderBy(col(tiebreak).desc())
    df_ranked = df.withColumn('_rank', row_number().over(w))
    df_dupes  = df_ranked.filter(col('_rank') > 1).drop('_rank') \
                         .withColumn('_dq_stage',  lit('exact_duplicate')) \
                         .withColumn('_dq_run_id', lit(metrics.run_id)) \
                         .withColumn('_dq_at',     current_timestamp())
    df_clean  = df_ranked.filter(col('_rank') == 1).drop('_rank')
    if df_dupes.count() > 0:
        df_dupes.write.format('delta').mode('append').option('mergeSchema','true') \
                .save(f'{SILVER_QUARANTINE}/{table_name}_dupes')
    return metrics.record('after_dedup', df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 6 — Stage 4: standardisation
def standardise(df):
    if 'phone' in df.columns:
        df = df.withColumn('_orig_phone', col('phone')) \
               .withColumn('phone_digits', regexp_replace(col('phone'), r'[^\d]', '')) \
               .withColumn('phone_e164',
                   when(length(col('phone_digits')) == 10, expr("concat('+1', phone_digits)"))
                   .when(length(col('phone_digits')) == 11, expr("concat('+', phone_digits)"))
                   .otherwise(lit(None).cast('string'))) \
               .drop('phone_digits')
    if 'email' in df.columns:
        df = df.withColumn('_orig_email', col('email')) \
               .withColumn('email', lower(trim(col('email'))))
    if 'status_cd' in df.columns:
        status_map = {'comp':'COMPLETED','complete':'COMPLETED','completed':'COMPLETED',
                      'canc':'CANCELLED','cancel':'CANCELLED','pend':'PENDING','ship':'SHIPPED'}
        s = col('status_cd')
        for raw, can in status_map.items():
            s = when(lower(trim(col('status_cd'))) == raw, lit(can)).otherwise(s)
        df = df.withColumn('status_cd', s)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 7 — Stage 5: business rules
def enforce_business_rules(df, table_name, rules, metrics):
    warn_exprs = []
    for r in rules.get('business_rules', []):
        rule_name = r['name']
        expr_str  = r['expression']
        severity  = r['severity']
        condition  = eval(expr_str)  # safe — rules come from our own registry; expression is a PASS condition (TRUE = valid row)
        fail_count = df.filter(~condition).count()
        if fail_count == 0:
            continue
        if severity == 'ERROR':
            df.filter(~condition) \
              .withColumn('_dq_stage',  lit(f'biz:{rule_name}')) \
              .withColumn('_dq_run_id', lit(metrics.run_id)) \
              .withColumn('_dq_at',     current_timestamp()) \
              .write.format('delta').mode('append') \
              .option('mergeSchema','true').save(f'{SILVER_QUARANTINE}/{table_name}')
            df = df.filter(condition)
            log.warning(f'[BIZ] {table_name}.{rule_name}: {fail_count} quarantined')
        elif severity == 'WARN':
            warn_exprs.append(when(~condition, lit(rule_name)).otherwise(lit(None)))
            log.warning(f'[BIZ] {table_name}.{rule_name}: {fail_count} flagged')
    if warn_exprs:
        df = df.withColumn('_dq_warnings', array_remove(array(*warn_exprs), None))
    return metrics.record('after_business_rules', df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2b — Pre-stage: freshness and volume checks (advisory — no row filtering)
def check_freshness(df, table_name, rules, source_cfg):
    sla_hours = rules.get('freshness_sla_hours')
    if not sla_hours:
        return
    wm_col = source_cfg.get('watermark_col')
    if not wm_col or wm_col not in df.columns:
        return
    max_ts = df.agg(spark_max(col(wm_col))).collect()[0][0]
    if max_ts is None:
        log.warning(f'[FRESHNESS] {table_name}: watermark column {wm_col} is all NULLs')
        return
    max_ts_dt = max_ts.toPython() if hasattr(max_ts, 'toPython') else max_ts
    threshold = datetime.utcnow() - timedelta(hours=sla_hours)
    if max_ts_dt < threshold:
        age_h = round((datetime.utcnow() - max_ts_dt).total_seconds() / 3600, 1)
        log.warning(
            f'[FRESHNESS] {table_name}: newest row is {age_h}h old — SLA is {sla_hours}h'
        )

def check_volume_rules(bronze_count, table_name, rules):
    vol = rules.get('volume_rules', {})
    if not vol:
        return
    if not DeltaTable.isDeltaTable(spark, SILVER_METRICS):
        log.info(f'[VOLUME] {table_name}: no metrics baseline yet — skipping')
        return
    baseline = spark.read.format('delta').load(SILVER_METRICS) \
        .filter(
            (col('table_name') == table_name) &
            (col('stage') == 'bronze_read') &
            (datediff(current_date(), col('run_at').cast('date')) <= 30)
        )
    if baseline.count() < 3:
        log.info(f'[VOLUME] {table_name}: fewer than 3 baseline runs — skipping volume check')
        return
    avg_count = baseline.agg(spark_avg('row_count')).collect()[0][0]
    if not avg_count or avg_count == 0:
        return
    pct = (bronze_count / avg_count) * 100
    mn  = vol.get('min_rows_pct_of_baseline', 50.0)
    mx  = vol.get('max_rows_pct_of_baseline', 300.0)
    if pct < mn:
        log.warning(
            f'[VOLUME] {table_name}: {bronze_count} rows = {pct:.1f}% of 30-day avg '
            f'({avg_count:.0f}) — below {mn}% threshold'
        )
    elif pct > mx:
        log.warning(
            f'[VOLUME] {table_name}: {bronze_count} rows = {pct:.1f}% of 30-day avg '
            f'({avg_count:.0f}) — above {mx}% threshold'
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3b — Stage 2b: format / regex validation
def apply_format_rules(df, table_name, rules, metrics):
    fmt = rules.get('format_rules', {})
    if not fmt:
        return metrics.record('after_format', df)
    bad = lit(False)
    for c, pattern in fmt.items():
        if c not in df.columns:
            continue
        invalid = col(c).isNotNull() & (regexp_extract(col(c), pattern, 0) == '')
        bad = bad | invalid
    df_bad   = df.filter(bad) \
                 .withColumn('_dq_stage',  lit('format_fail')) \
                 .withColumn('_dq_run_id', lit(metrics.run_id)) \
                 .withColumn('_dq_at',     current_timestamp())
    df_clean = df.filter(~bad)
    if df_bad.count() > 0:
        df_bad.write.format('delta').mode('append').option('mergeSchema', 'true') \
              .save(f'{SILVER_QUARANTINE}/{table_name}')
        log.warning(f'[FORMAT] {table_name}: {df_bad.count()} rows quarantined')
    return metrics.record('after_format', df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5b — Post-Stage 3: referential integrity (FK-style)
def apply_referential_rules(df, table_name, rules, metrics):
    ref_rules = rules.get('referential_rules', [])
    if not ref_rules:
        return metrics.record('after_referential', df)
    df_clean = df
    for r in ref_rules:
        rule_name       = r['name']
        silver_path_rel = r['silver_path']
        join_col        = r['join_col']
        # ref_join_col supports logical FKs where the referenced table's own
        # column has a different name (e.g. patients.primary_provider_id ->
        # providers.provider_id) — mirrors star_schema_registry's dim_join_col
        # pattern for the same reason (see CLAUDE.md adjuster_id/agent_id note).
        # Defaults to join_col when the names already match.
        ref_join_col    = r.get('ref_join_col', join_col)
        severity        = r['severity']
        if join_col not in df_clean.columns:
            continue
        silver_path = f'{SILVER_TABLES}/{silver_path_rel}'
        if not DeltaTable.isDeltaTable(spark, silver_path):
            log.info(
                f'[REF] {table_name}.{rule_name}: {silver_path_rel} not in Silver yet — skipping'
            )
            continue
        ref_table_df = spark.read.format('delta').load(silver_path)
        if ref_join_col not in ref_table_df.columns:
            log.error(
                f"[REF] {table_name}.{rule_name}: ref_join_col '{ref_join_col}' not found in "
                f"{silver_path_rel} — available columns: {sorted(ref_table_df.columns)}. "
                f"If the FK column name on {table_name} ('{join_col}') differs from the "
                f"referenced table's own key column, set \"ref_join_col\" in this rule to the "
                f"correct column name on {silver_path_rel}. Skipping this rule."
            )
            continue
        ref_keys = (
            ref_table_df
            .select(ref_join_col).distinct()
            .withColumnRenamed(ref_join_col, join_col)
        )
        orphans   = df_clean.join(ref_keys, on=join_col, how='leftanti')
        orphan_ct = orphans.count()
        if orphan_ct > 0:
            orphans.withColumn('_dq_stage',  lit(f'ref:{rule_name}')) \
                   .withColumn('_dq_run_id', lit(metrics.run_id)) \
                   .withColumn('_dq_at',     current_timestamp()) \
                   .write.format('delta').mode('append').option('mergeSchema', 'true') \
                   .save(f'{SILVER_QUARANTINE}/{table_name}')
            if severity == 'ERROR':
                df_clean = df_clean.join(ref_keys, on=join_col, how='inner')
                log.warning(f'[REF] {table_name}.{rule_name}: {orphan_ct} quarantined (ERROR)')
            else:
                log.warning(f'[REF] {table_name}.{rule_name}: {orphan_ct} orphans (WARN — kept)')
    return metrics.record('after_referential', df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5c — Post-Stage 3: column fill rate monitoring (advisory — no row filtering)
def check_fill_rates(df, table_name, rules):
    fill = rules.get('fill_rate_rules', {})
    if not fill:
        return
    total = df.count()
    if total == 0:
        return
    for c, min_pct in fill.items():
        if c not in df.columns:
            continue
        filled   = df.filter(col(c).isNotNull()).count()
        fill_pct = round((filled / total) * 100, 1)
        if fill_pct < min_pct:
            log.warning(
                f'[FILL] {table_name}.{c}: {fill_pct}% filled — below {min_pct}% threshold'
            )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 7b — Stage 5b: conditional field rules
def apply_conditional_rules(df, table_name, rules, metrics):
    cond_rules = rules.get('conditional_rules', [])
    if not cond_rules:
        return df
    warn_exprs = []
    for r in cond_rules:
        rule_name     = r['name']
        condition_str = r['condition']
        required_col  = r['required_col']
        severity      = r['severity']
        if required_col not in df.columns:
            continue
        condition = eval(condition_str)
        failing   = condition & col(required_col).isNull()
        fail_ct   = df.filter(failing).count()
        if fail_ct == 0:
            continue
        if severity == 'ERROR':
            df.filter(failing) \
              .withColumn('_dq_stage',  lit(f'cond:{rule_name}')) \
              .withColumn('_dq_run_id', lit(metrics.run_id)) \
              .withColumn('_dq_at',     current_timestamp()) \
              .write.format('delta').mode('append').option('mergeSchema', 'true') \
              .save(f'{SILVER_QUARANTINE}/{table_name}')
            df = df.filter(~failing)
            log.warning(f'[COND] {table_name}.{rule_name}: {fail_ct} quarantined')
        else:
            warn_exprs.append(when(failing, lit(rule_name)).otherwise(lit(None)))
            log.warning(f'[COND] {table_name}.{rule_name}: {fail_ct} flagged (WARN)')
    if warn_exprs:
        new_warns = array_remove(array(*warn_exprs), None)
        if '_dq_warnings' in df.columns:
            df = df.withColumn('_dq_warnings', array_union(col('_dq_warnings'), new_warns))
        else:
            df = df.withColumn('_dq_warnings', new_warns)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 7c — Post-Stage 5: statistical outlier detection (z-score)
def apply_statistical_rules(df, table_name, rules, metrics):
    stat_rules = rules.get('statistical_rules', {})
    if not stat_rules:
        return metrics.record('after_statistical', df)
    bad = lit(False)
    for c, params in stat_rules.items():
        if c not in df.columns:
            continue
        max_z = params.get('max_zscore', 4.0)
        stats = df.select(
            spark_mean(col(c)).alias('mu'),
            spark_stddev(col(c)).alias('sigma')
        ).collect()[0]
        mu, sigma = stats['mu'], stats['sigma']
        if sigma is None or sigma == 0.0:
            continue
        outlier = col(c).isNotNull() & (((col(c) - lit(mu)) / lit(sigma)) > lit(max_z)) | \
                  col(c).isNotNull() & (((col(c) - lit(mu)) / lit(sigma)) < lit(-max_z))
        ct = df.filter(outlier).count()
        if ct > 0:
            log.warning(
                f'[STAT] {table_name}.{c}: {ct} outliers (μ={mu:.2f} σ={sigma:.2f} '
                f'threshold=±{max_z}σ)'
            )
        bad = bad | outlier
    df_bad   = df.filter(bad) \
                 .withColumn('_dq_stage',  lit('statistical_outlier')) \
                 .withColumn('_dq_run_id', lit(metrics.run_id)) \
                 .withColumn('_dq_at',     current_timestamp())
    df_clean = df.filter(~bad)
    if df_bad.count() > 0:
        df_bad.write.format('delta').mode('append').option('mergeSchema', 'true') \
              .save(f'{SILVER_QUARANTINE}/{table_name}')
    return metrics.record('after_statistical', df_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 8 — Stage 6: audit columns
def attach_audit_cols(df, source_system, natural_key_cols, change_detect_cols):
    return df \
        .withColumn('_silver_processed_at', current_timestamp()) \
        .withColumn('_source_system',       lit(source_system)) \
        .withColumn('_natural_key_hash',
            sha2(concat_ws('|', *[col(c) for c in natural_key_cols]), 256)) \
        .withColumn('_row_hash',
            sha2(concat_ws('|', *[
                coalesce(col(c).cast('string'), lit('__null__'))
                for c in change_detect_cols
            ]), 256))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 9 — Silver MERGE writer
def write_silver_merge(df, silver_path, join_col, metrics, table_name):
    if not DeltaTable.isDeltaTable(spark, silver_path):
        df.write.format('delta').mode('overwrite') \
          .option('mergeSchema','true').save(silver_path)
    else:
        DeltaTable.forPath(spark, silver_path).alias('t').merge(
            df.alias('s'), f't.{join_col} = s.{join_col}'
        ).whenMatchedUpdate(
            condition='t._row_hash <> s._row_hash',
            set={c: f's.{c}' for c in df.columns}
        ).whenNotMatchedInsertAll().execute()
    metrics.record('silver_write', df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 9b — DISCARD suppression filter
# Rows whose natural key appears in a resolved DISCARD decision are dropped
# before any DQ stage runs, so they never land in the quarantine queue again.
# Only T4 stages (biz: / cond: prefixes) are suppressed — T1/T2/T3 decisions
# are not stored in _resolutions and are unaffected.
def apply_discard_suppressions(df: DataFrame, table_name: str, dedup_key: str) -> DataFrame:
    res_path = f"{SILVER_FILES}/_resolutions/{table_name}"
    if not DeltaTable.isDeltaTable(spark, res_path):
        return df
    discarded_keys = (
        spark.read.format("delta").load(res_path)
        .filter(
            (col("decision") == "RESOLVED") &
            (
                col("dq_stage").startswith("biz:") |
                col("dq_stage").startswith("cond:")
            )
        )
        .select("natural_key")
        .distinct()
    )
    if discarded_keys.rdd.isEmpty():
        return df
    n_suppressed = df.join(
        discarded_keys,
        df[dedup_key].cast("string") == discarded_keys["natural_key"],
        "inner",
    ).count()
    if n_suppressed:
        log.info(f'[DISCARD] {table_name}: suppressing {n_suppressed} row(s) with resolved DISCARD decisions')
    return df.join(
        discarded_keys,
        df[dedup_key].cast("string") == discarded_keys["natural_key"],
        "left_anti",
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 10 — Master loop: tables present in both SOURCE_REGISTRY and DQ_RULES_REGISTRY
# Only process tables that have Bronze data (SOURCE_REGISTRY) AND DQ rules defined.
# Paths come from SOURCE_REGISTRY['bronze_table'] so they match what bronze_blob wrote
# (e.g. "financial/loan_payments") rather than deriving from the table name.
RUN_ID    = datetime.utcnow().strftime('%Y-%m-%d-%H%M')
all_metrics = []

tables_to_process = {
    name: DQ_RULES_REGISTRY[name]
    for name in SOURCE_REGISTRY
    if name in DQ_RULES_REGISTRY
}

for table_name, rules in tables_to_process.items():
    try:
        table_path  = SOURCE_REGISTRY[table_name]['bronze_table']
        bronze_path = f'{BRONZE_TABLES}/{table_path}'
        silver_path = f'{SILVER_TABLES}/{table_path}'

        if not DeltaTable.isDeltaTable(spark, bronze_path):
            log.info(f'[SILVER] {table_name}: Bronze table not found — skipping')
            continue

        source_cfg = SOURCE_REGISTRY[table_name]
        m = DQMetrics(table_name, RUN_ID)

        df = m.record('bronze_read', spark.read.format('delta').load(bronze_path))

        # Pre-stage advisory checks (table-level — no row filtering)
        check_freshness(df, table_name, rules, source_cfg)
        check_volume_rules(m.stages['bronze_read'], table_name, rules)

        df = apply_discard_suppressions(df, table_name, rules['dedup_key'][0])  # DISCARD gate
        df = enforce_nulls_and_types(df, table_name, rules, m)     # Stage 1
        if PIPELINE_TIER != "starter":
            df = apply_range_and_enum(df, table_name, rules, m)        # Stage 2
            df = apply_format_rules(df, table_name, rules, m)          # Stage 2b — Gap 1
            df = exact_dedup(df, table_name, rules, m)                 # Stage 3a
            df = apply_referential_rules(df, table_name, rules, m)     # Gap 5
            check_fill_rates(df, table_name, rules)                    # Gap 4 (advisory)
            df = standardise(df)                                        # Stage 4
            df = enforce_business_rules(df, table_name, rules, m)      # Stage 5
            df = apply_conditional_rules(df, table_name, rules, m)     # Gap 6
            df = apply_statistical_rules(df, table_name, rules, m)     # Gap 3
        df = attach_audit_cols(
            df,
            source_system=table_name,
            natural_key_cols=rules['dedup_key'],
            change_detect_cols=[c for c in df.columns if not c.startswith('_')]
        )
        m.record('after_audit_cols', df)
        write_silver_merge(df, silver_path, rules['dedup_key'][0], m, table_name)
        if PIPELINE_TIER != "starter":
            m.persist()
            m.assert_threshold(rules.get('max_rejection_pct', 5.0))
        all_metrics.append({'table': table_name, 'rate': m.rejection_rate(), 'status': 'SUCCESS'})
    except Exception as e:
        log.error(f'[SILVER] {table_name}: {e}')
        all_metrics.append({'table': table_name, 'status': 'FAILED', 'error': str(e)})

failed = [x for x in all_metrics if x['status'] == 'FAILED']
mssparkutils.notebook.exit(json.dumps({
    'status':     'DQ_GATE_FAILED' if failed else 'SUCCESS',
    'tier':       PIPELINE_TIER,
    'stages_run': 1 if PIPELINE_TIER == 'starter' else 6,
    'failed':     [x['table'] for x in failed],
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
