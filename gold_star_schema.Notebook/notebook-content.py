# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Gold: Star schema build
# Registry-driven Gold layer. Dimensions and facts built from the
# `config/star_schema_registry.py`. No code changes for new schemas. 

# CELL ********************

# CELL 1 — Imports
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
for _mod in ("workspace_config", "star_schema_registry", "security_registry"):
    sys.modules.pop(_mod, None)
sys.path.insert(0, _cfg_local)
importlib.invalidate_caches()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, conv,
    abs as spark_abs,
    year, quarter, month, dayofmonth, dayofweek,
    date_format, when, coalesce, expr, sha2, concat_ws,
    length, substring, concat, substring_index
)
from delta.tables import DeltaTable 
import json, logging

from workspace_config import SILVER_TABLES, GOLD_TABLES, apply_spark_settings, PIPELINE_TIER
from star_schema_registry import STAR_SCHEMA_REGISTRY, get_all_domains
from security_registry import SECURITY_USER_SEED

spark = SparkSession.builder.getOrCreate()
apply_spark_settings(spark)
log = logging.getLogger('gold')

def _stable_sk(key_expr):
    """Deterministic hash-based surrogate key — stable across reruns and partition changes.
    Takes first 15 hex chars of SHA-256 (60 bits), converts to long, ensures positive.
    Same natural key always produces the same SK regardless of Spark partition layout.
    """
    return spark_abs(
        conv(
            sha2(coalesce(key_expr.cast("string"), lit("__null__")), 256).substr(1, 15),
            16, 10
        ).cast("long")
    )

if PIPELINE_TIER != "advanced":
    mssparkutils.notebook.exit(json.dumps({
        'status': 'SKIPPED',
        'tier':   PIPELINE_TIER,
        'reason': "Gold layer requires 'advanced' tier — skipping",
    }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 1b — Security user dimension (dim_security_user)
# Reads SECURITY_USER_SEED from security_registry and builds/upserts
# the shared/dim_security_user Gold Delta table. Attribute columns and
# their Spark types are derived dynamically from the seed so adding a
# new attribute (e.g. store_code, territory_id) only requires editing
# security_registry.py — no notebook change. Schema drift triggers a
# one-time overwriteSchema rebuild that drops columns no longer present
# in the seed (e.g. legacy branch_code / loan_officer_id / agent_id).

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    IntegerType, BooleanType
)

# Fixed columns present for every domain — everything else is an attribute
# discovered from the seed.
_FIXED_CORE_COLS = ["user_principal_name", "display_name", "domain", "role"]
_FIXED_TAIL_COLS = ["is_active"]
_RESERVED_COLS   = set(_FIXED_CORE_COLS) | set(_FIXED_TAIL_COLS) | {
    "user_sk", "_created_at", "_updated_at"
}

def _infer_attr_type(col_name, seed):
    # First non-None value wins; default to StringType for all-None columns.
    for u in seed:
        v = u.get(col_name)
        if v is None:
            continue
        if isinstance(v, bool):
            return BooleanType()
        if isinstance(v, int):
            return IntegerType()
        return StringType()
    return StringType()

def _discover_attr_cols(seed):
    seen, out = set(_RESERVED_COLS), []
    for u in seed:
        for k in u.keys():
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out

_ATTR_COLS  = _discover_attr_cols(SECURITY_USER_SEED)
_ATTR_TYPES = {c: _infer_attr_type(c, SECURITY_USER_SEED) for c in _ATTR_COLS}

_ROW_SCHEMA = StructType(
    [StructField("user_sk",             LongType(),    False),
     StructField("user_principal_name", StringType(),  False),
     StructField("display_name",        StringType(),  False),
     StructField("domain",              StringType(),  False),
     StructField("role",                StringType(),  False)]
    + [StructField(c, _ATTR_TYPES[c], True) for c in _ATTR_COLS]
    + [StructField("is_active",         BooleanType(), False)]
)

def build_dim_security_user():
    gold_path = f"{GOLD_TABLES}/shared/dim_security_user"

    rows = [
        (
            i,
            u["user_principal_name"],
            u["display_name"],
            u["domain"],
            u["role"],
        )
        + tuple(u.get(c) for c in _ATTR_COLS)
        + (bool(u.get("is_active", True)),)
        for i, u in enumerate(SECURITY_USER_SEED)
    ]
    df = spark.createDataFrame(rows, schema=_ROW_SCHEMA) \
      .withColumn("_created_at", current_timestamp()) \
      .withColumn("_updated_at", current_timestamp())

    if DeltaTable.isDeltaTable(spark, gold_path):
        existing_cols = set(spark.read.format("delta").load(gold_path).columns)
        new_cols      = set(df.columns)
        if existing_cols != new_cols:
            df.write.format("delta") \
              .mode("overwrite").option("overwriteSchema", "true") \
              .save(gold_path)
            log.info(
                f"[GOLD] dim_security_user: schema rebuild "
                f"(added={sorted(new_cols - existing_cols)}, "
                f"dropped={sorted(existing_cols - new_cols)}), "
                f"{df.count()} rows"
            )
        else:
            dt = DeltaTable.forPath(spark, gold_path)
            update_set = {c: f"s.{c}" for c in _ATTR_COLS}
            update_set["role"]        = "s.role"
            update_set["is_active"]   = "s.is_active"
            update_set["_updated_at"] = "current_timestamp()"
            dt.alias("t").merge(
                df.alias("s"),
                "t.user_principal_name = s.user_principal_name AND t.domain = s.domain"
            ).whenMatchedUpdate(set=update_set) \
             .whenNotMatchedInsertAll().execute()
            log.info("[GOLD] dim_security_user: upsert complete")
    else:
        df.write.format("delta").mode("overwrite").save(gold_path)
        log.info(f"[GOLD] dim_security_user: initial write, {df.count()} rows")

if not SECURITY_USER_SEED:
    log.info("[GOLD] dim_security_user: SECURITY_USER_SEED empty — skipping (Phase 4 not wired)")
else:
    build_dim_security_user()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2 — Date dimension generator (shared across all domains)
def build_dim_date(cfg):
    gold_path  = f"{GOLD_TABLES}/{cfg['gold_path']}"
    start_date = cfg['start_date']
    end_date   = cfg['end_date']
    df = spark.sql(f"""
        SELECT
            CAST(date_format(d,'yyyyMMdd') AS INT) AS date_key,
            d                                       AS full_date,
            year(d)                                 AS year,
            quarter(d)                              AS quarter,
            month(d)                                AS month_num,
            date_format(d,'MMMM')                   AS month_name,
            day(d)                                  AS day_of_month,
            dayofweek(d)                            AS day_of_week,
            date_format(d,'EEEE')                   AS day_name,
            CASE WHEN dayofweek(d) IN (1,7) THEN true ELSE false END AS is_weekend
        FROM (
            SELECT explode(sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )) AS d
        )
    """)
    # SQL NULL cast avoids Python None → DateType conversion errors in Fabric runtime.
    unknown = spark.sql("""
        SELECT
            -1                 AS date_key,
            CAST(NULL AS DATE) AS full_date,
            -1                 AS year,
            -1                 AS quarter,
            -1                 AS month_num,
            'Unknown'          AS month_name,
            -1                 AS day_of_month,
            -1                 AS day_of_week,
            'Unknown'          AS day_name,
            false              AS is_weekend
    """)
    df = df.union(unknown)
    df.write.format('delta').mode('overwrite').save(gold_path)
    log.info(f'[GOLD] dim_date: {df.count()} rows (includes date_key=-1 sentinel)')
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3 — Dimension builder (type1 and scd2)
def resolve_columns(columns_cfg, exclude=()):
    """
    Returns (source_name, target_name) pairs from a columns list.
    Each entry is either a plain string (src == tgt) or
    {"source": "src_col", "target": "Display Name"} for a rename.
    Entries whose source or target is in `exclude` are omitted.
    """
    exclude = set(exclude)
    pairs = []
    for c in columns_cfg:
        if isinstance(c, dict):
            src, tgt = c['source'], c['target']
        else:
            src = tgt = c
        if src not in exclude and tgt not in exclude:
            pairs.append((src, tgt))
    return pairs

def _apply_masked_columns(dim_name, dim_cfg, gold_path):
    """Read the Delta table, add registry-defined masked columns, overwrite in place."""
    masked = dim_cfg.get('masked_columns')
    if not masked:
        return
    df = spark.read.format('delta').load(gold_path)
    for col_name, sql_expr in masked.items():
        df = df.withColumn(col_name, expr(sql_expr))
    df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(gold_path)
    log.info(f'[GOLD] {dim_name}: masked columns applied: {list(masked)}')

def build_dimension(dim_name, dim_cfg):
    dim_type  = dim_cfg['type']
    gold_path = f"{GOLD_TABLES}/{dim_cfg['gold_path']}"

    # generated and static have no surrogate_key/natural_key — handle before accessing them
    if dim_type == 'generated':
        return build_dim_date(dim_cfg)

    if dim_type == 'static':
        df = spark.createDataFrame(dim_cfg['static_rows'])
        df.write.format('delta').mode('overwrite').save(gold_path)
        return df

    sk_col = dim_cfg['surrogate_key']
    nk_col = dim_cfg['natural_key']

    silver_path = f"{SILVER_TABLES}/{dim_cfg['silver_source']}"
    df_src = spark.read.format('delta').load(silver_path)

    if dim_type == 'type1':
        # sk_col doesn't exist in the source — exclude it from select, add via withColumn
        col_pairs = resolve_columns(dim_cfg['columns'], exclude={sk_col})
        df_dim = df_src.select([col(s).alias(t) for s, t in col_pairs]) \
                       .withColumn(sk_col, _stable_sk(col(nk_col)))
        df_dim.write.format('delta').mode('overwrite').save(gold_path)
        log.info(f'[GOLD] {dim_name}: {df_dim.count()} rows (type1)')
        _apply_masked_columns(dim_name, dim_cfg, gold_path)
        return spark.read.format('delta').load(gold_path)

    if dim_type == 'scd2':
        from pyspark.sql.functions import current_date
        scd2_exclude  = {sk_col, 'eff_start_date', 'eff_end_date', 'is_current'}
        col_pairs     = resolve_columns(dim_cfg['columns'], exclude=scd2_exclude)
        scd2_src_cols = [s for s, _ in col_pairs]
        track_cols    = dim_cfg.get('scd2_track_cols', scd2_src_cols)

        # Hash only the business-meaningful columns defined in scd2_track_cols.
        # This avoids creating new SCD2 versions for non-tracked changes (e.g. name typo fixes).
        df_src = df_src.withColumn('_scd2_hash',
            sha2(concat_ws('|', *[
                coalesce(col(c).cast('string'), lit('__null__')) for c in track_cols
            ]), 256)
        )

        if not DeltaTable.isDeltaTable(spark, gold_path):
            df_dim = df_src.select([col(s).alias(t) for s, t in col_pairs] + [col('_scd2_hash')]) \
                .withColumn(sk_col, _stable_sk(col(nk_col))) \
                .withColumn('eff_start_date', current_date()) \
                .withColumn('eff_end_date',   lit(None).cast('date')) \
                .withColumn('is_current',     lit(True))
            df_dim.write.format('delta').mode('overwrite').save(gold_path)
        else:
            dt = DeltaTable.forPath(spark, gold_path)
            if '_scd2_hash' not in dt.toDF().columns:
                # Existing table predates _scd2_hash — rebuild once to add the column.
                df_dim = df_src.select([col(s).alias(t) for s, t in col_pairs] + [col('_scd2_hash')]) \
                    .withColumn(sk_col, _stable_sk(col(nk_col))) \
                    .withColumn('eff_start_date', current_date()) \
                    .withColumn('eff_end_date',   lit(None).cast('date')) \
                    .withColumn('is_current',     lit(True))
                df_dim.write.format('delta').mode('overwrite').save(gold_path)
                log.info(f'[GOLD] {dim_name}: rebuilt with _scd2_hash (migration)')
                _apply_masked_columns(dim_name, dim_cfg, gold_path)
                return spark.read.format('delta').load(gold_path)
            dt.alias('t').merge(
                df_src.alias('s'),
                f't.{nk_col} = s.{nk_col} AND t.is_current = true AND t._scd2_hash <> s._scd2_hash'
            ).whenMatchedUpdate(set={
                'is_current':    lit(False),
                'eff_end_date':  current_date()
            }).execute()
            # Join only on nk_col from existing Gold to avoid duplicate columns
            df_new = df_src.join(
                dt.toDF().filter('is_current = false').select(nk_col), on=nk_col, how='inner'
            ).select([col(s).alias(t) for s, t in col_pairs] + [col('_scd2_hash')]) \
             .withColumn(sk_col, _stable_sk(concat_ws("|", col(nk_col).cast("string"), col("_scd2_hash")))) \
             .withColumn('eff_start_date', current_date()) \
             .withColumn('eff_end_date',   lit(None).cast('date')) \
             .withColumn('is_current',     lit(True))
            df_new.write.format('delta').mode('append').save(gold_path)
        log.info(f'[GOLD] {dim_name}: SCD2 merge complete')
        _apply_masked_columns(dim_name, dim_cfg, gold_path)
        return spark.read.format('delta').load(gold_path)

    if dim_type == 'type4':
        from pyspark.sql.functions import current_date
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, desc as _desc

        col_pairs  = resolve_columns(dim_cfg['columns'], exclude={sk_col})
        track_cols = dim_cfg.get('scd2_track_cols', [s for s, _ in col_pairs])

        # Hash on source columns (before rename) so track_cols names match the Silver schema
        df_src = df_src.withColumn('_type4_hash',
            sha2(concat_ws('|', *[
                coalesce(col(c).cast('string'), lit('__null__')) for c in track_cols
            ]), 256)
        )
        df_dim = df_src.select([col(s).alias(t) for s, t in col_pairs] + [col('_type4_hash')]) \
                       .withColumn(sk_col, _stable_sk(col(nk_col)))

        # ── Main table: Type 1 overwrite (always current state, no history rows) ──
        df_main = df_dim.drop('_type4_hash')
        df_main.write.format('delta').mode('overwrite').save(gold_path)
        log.info(f'[GOLD] {dim_name}: {df_main.count()} rows (type4 main)')

        # ── History table: append-only, one row per detected change ──
        history_path = f"{gold_path}_history"
        df_hist = df_dim.withColumn('eff_start_date', current_date())

        if DeltaTable.isDeltaTable(spark, history_path):
            existing   = spark.read.format('delta').load(history_path)
            w          = Window.partitionBy(nk_col).orderBy(_desc('eff_start_date'))
            latest     = existing.withColumn('_rn', row_number().over(w)) \
                                 .filter(col('_rn') == 1) \
                                 .select(
                                     col(nk_col).alias('_h_nk'),
                                     col('_type4_hash').alias('_h_hash')
                                 )
            # Append only rows that are new (NK not seen before) or changed (hash differs)
            df_changed = df_hist \
                .join(latest, df_hist[nk_col] == col('_h_nk'), how='left') \
                .filter(col('_h_nk').isNull() | (df_hist['_type4_hash'] != col('_h_hash'))) \
                .select(df_hist.columns)
            n_new = df_changed.count()
            if n_new > 0:
                df_changed.write.format('delta').mode('append').save(history_path)
            log.info(f'[GOLD] {dim_name}: {n_new} row(s) appended to history (type4)')
        else:
            df_hist.write.format('delta').mode('overwrite').save(history_path)
            log.info(f'[GOLD] {dim_name}: type4 history initial write, {df_hist.count()} rows')

        _apply_masked_columns(dim_name, dim_cfg, gold_path)
        return spark.read.format('delta').load(gold_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4 — Fact builder
_gold_ri_warnings = []  # collects RI near-miss warnings written to gold_warnings.json at run end

def _resolve_join_source(dim_name, schema):
    """Return the registry entry for a dimension_joins target, whether it is a
    dimension or another fact referenced as a degenerate dimension."""
    return (schema.get('dimensions', {}).get(dim_name)
            or schema.get('facts', {}).get(dim_name))


def build_fact(fact_name, fact_cfg, dim_dfs, schema):
    gold_path = f"{GOLD_TABLES}/{fact_cfg['gold_path']}"
    sources   = fact_cfg['silver_source']

    if isinstance(sources, list):
        df_a = spark.read.format('delta').load(f"{SILVER_TABLES}/{sources[0]}")
        second = sources[1]
        if isinstance(second, dict):
            # {"join": "<silver_path>", "on": "<child_join_col>",
            #  "right_on": "<parent_join_col>" (optional, defaults to "on"),
            #  "how": "<left|inner|...>"}
            # Lets a child-grain fact (e.g. fact_diagnoses) pull carry columns —
            # like a date column that lives only on the parent — via a LEFT join,
            # instead of the legacy inner-only two-string shape below. right_on
            # covers the case where the parent's key column isn't named the same
            # as the child's FK column (same logical-FK naming mismatch as
            # dimension_joins.dim_join_col / referential_rules.ref_join_col).
            b_path   = second['join']
            join_key = second['on']
            right_on = second.get('right_on', join_key)
            how      = second.get('how', 'left')
        else:
            # Legacy shape: plain path string + top-level join_key, inner join only.
            b_path   = second
            join_key = fact_cfg['join_key']
            right_on = join_key
            how      = 'inner'
        df_b = spark.read.format('delta').load(f"{SILVER_TABLES}/{b_path}")
        if right_on != join_key:
            df_b = df_b.withColumnRenamed(right_on, join_key)
        # Drop parent-side columns that duplicate child columns (other than the
        # join key) so the child grain wins on shared columns like created_at /
        # modified_at — prevents AMBIGUOUS_REFERENCE on later select.
        dup_in_b = (set(df_a.columns) & set(df_b.columns)) - {join_key}
        df = df_a.join(df_b.drop(*dup_in_b), on=join_key, how=how)
    else:
        df = spark.read.format('delta').load(f"{SILVER_TABLES}/{sources}")

    # parent_join: bring carry_cols (e.g. admit_date, patient_id) from a parent
    # Silver table when the child fact has no direct date/FK column of its own.
    # Only the join key + declared carry_cols are selected from the parent to
    # avoid AMBIGUOUS_REFERENCE on shared audit columns.
    if 'parent_join' in fact_cfg:
        pj         = fact_cfg['parent_join']
        pj_key     = pj['join_key']
        carry_cols = pj['carry_cols']
        df_parent  = spark.read.format('delta').load(f"{SILVER_TABLES}/{pj['silver_path']}")
        df_parent  = df_parent.select([pj_key] + carry_cols)
        df = df.join(df_parent, on=pj_key, how='left')

    date_col = fact_cfg['date_key_source']
    if date_col is not None:
        df = df.withColumn('date_key',
            coalesce(date_format(col(date_col), 'yyyyMMdd').cast('integer'), lit(-1)))

    for dim_join in fact_cfg['dimension_joins']:
        dim_name = dim_join['dim']
        if dim_name not in dim_dfs:
            # A missing join target always ends in UNRESOLVED_COLUMN at the final
            # select (sk_col never gets added), so fail here with the actual cause.
            # Most common case: dimension_joins names another fact as a degenerate
            # dimension, but that fact is defined AFTER this one in the registry.
            raise ValueError(
                f'[GOLD] {fact_name}: dimension_joins target "{dim_name}" has not been '
                f'built. Built so far: {sorted(dim_dfs)}. If "{dim_name}" is a fact used '
                f'as a degenerate dimension, move it BEFORE "{fact_name}" in the domain\'s '
                f'"facts" dict in star_schema_registry.py — facts build in insertion order.'
            )
        dim_df   = dim_dfs[dim_name]
        if dim_join.get('filter'):
            dim_df = dim_df.filter(dim_join['filter'])
        join_col     = dim_join['join_col']
        dim_join_col = dim_join.get('dim_join_col', join_col)
        sk_col       = dim_join['sk_col']
        if join_col == sk_col:
            # join_col IS the SK (e.g. date_key already computed in fact).
            # Select only the key column to avoid a duplicate-column select.
            dim_select = dim_df.select(col(dim_join_col).alias(join_col))
        else:
            # Resolve the SK column as it actually exists on the referenced table,
            # then alias it to the registry's sk_col. A dimension usually exposes
            # sk_col verbatim; a fact referenced as a degenerate dimension exposes
            # ITS OWN surrogate_key (e.g. fact_encounters_sk) rather than the
            # analytics-friendly name the referencing fact wants (encounter_sk).
            # Aliasing keeps sk_col the single contract for the fact's "columns"
            # list and for Power BI relationships — no registry edit required.
            src_cfg = _resolve_join_source(dim_name, schema) or {}
            src_sk  = sk_col if sk_col in dim_df.columns else src_cfg.get('surrogate_key')
            if src_sk not in dim_df.columns:
                raise ValueError(
                    f'[GOLD] {fact_name} → {dim_name}: cannot resolve sk_col "{sk_col}". '
                    f'Neither "{sk_col}" nor {dim_name}\'s surrogate_key '
                    f'"{src_cfg.get("surrogate_key")}" exists on {dim_name}. '
                    f'Available columns: {sorted(dim_df.columns)}.'
                )
            dim_select = dim_df.select(
                col(dim_join_col).alias(join_col), col(src_sk).alias(sk_col)
            )
        df = df.join(dim_select, on=join_col, how='left')

        max_null_pct = dim_join.get('max_null_sk_pct')
        if max_null_pct is not None:
            total      = df.count()
            null_count = df.filter(col(sk_col).isNull()).count()
            null_pct   = (null_count / total * 100) if total > 0 else 0.0
            ri_msg     = (f'[GOLD] {fact_name} → {dim_name}: '
                          f'{null_count}/{total} NULL {sk_col} ({null_pct:.1f}%)')
            if null_pct > max_null_pct:
                raise ValueError(
                    f'{ri_msg} — exceeds max_null_sk_pct={max_null_pct}. '
                    f'Fix referential integrity in source before re-running.'
                )
            elif null_count > 0:
                log.warning(ri_msg)
                _gold_ri_warnings.append({
                    "table":                      fact_name,
                    "dimension":                  dim_name,
                    "null_sk_count":              null_count,
                    "null_sk_pct":                round(null_pct, 4),
                    "configured_max_null_sk_pct": float(max_null_pct),
                    "status":                     "WARNING",
                })
            else:
                log.info(ri_msg)

    for derived_col, expr_str in fact_cfg.get('derived_cols', {}).items():
        df = df.withColumn(derived_col, expr(expr_str))

    df = df.withColumn(fact_cfg['surrogate_key'], _stable_sk(col(fact_cfg['natural_key'])))

    # Select only registry-defined columns — drops audit columns, _dq_warnings (ARRAY),
    # and any other Silver columns not part of the Gold output schema.
    fact_col_pairs = resolve_columns(fact_cfg['columns'])
    df = df.select([col(s).alias(t) for s, t in fact_col_pairs])

    nk          = fact_cfg['natural_key']
    sk_col_f    = fact_cfg['surrogate_key']
    dim_sk_cols = [dj['sk_col'] for dj in fact_cfg.get('dimension_joins', [])]
    update_cols = [sk_col_f] + dim_sk_cols
    missing_sk  = [c for c in update_cols if c not in df.columns]
    if missing_sk:
        raise ValueError(
            f'[GOLD] {fact_name}: surrogate/SK columns {missing_sk} are missing from '
            f'the Gold DataFrame — add them to the fact\'s "columns" list in '
            f'star_schema_registry.py and re-run.'
        )
    update_set  = {c: f"s.{c}" for c in update_cols}
    # Repair stale SK columns on existing rows (migration from monotonically_increasing_id).
    # Hash is deterministic — condition is false after first successful run, making it a no-op.
    sk_changed  = " OR ".join(f"t.{c} != s.{c}" for c in update_cols)

    if DeltaTable.isDeltaTable(spark, gold_path):
        DeltaTable.forPath(spark, gold_path).alias('t').merge(
            df.alias('s'), f't.{nk} = s.{nk}'
        ).whenMatchedUpdate(condition=sk_changed, set=update_set
        ).whenNotMatchedInsertAll().execute()
    else:
        df.write.format('delta').mode('overwrite').save(gold_path)

    log.info(f'[GOLD] {fact_name}: {df.count()} rows')
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5 — Master Gold build loop
results = []

for domain in get_all_domains():
    schema = STAR_SCHEMA_REGISTRY[domain]
    dim_dfs = {}

    # Build dimensions first
    for dim_name, dim_cfg in schema['dimensions'].items():
        try:
            dim_dfs[dim_name] = build_dimension(dim_name, dim_cfg)
            results.append({'layer': 'dim', 'name': dim_name, 'status': 'SUCCESS'})
        except Exception as e:
            log.error(f'[GOLD] {dim_name}: {e}')
            results.append({'layer': 'dim', 'name': dim_name, 'status': 'FAILED', 'error': str(e)})

    # Build facts. Store each fact's built dataframe into dim_dfs (keyed by
    # fact_name) as it completes — a later fact's dimension_joins can name an
    # earlier fact as a degenerate "dim" (e.g. fact_procedures -> fact_encounters)
    # to inherit its surrogate key. Registry order MUST build the referenced
    # fact before the fact that joins to it, same as dimensions.
    for fact_name, fact_cfg in schema['facts'].items():
        try:
            dim_dfs[fact_name] = build_fact(fact_name, fact_cfg, dim_dfs, schema)
            results.append({'layer': 'fact', 'name': fact_name, 'status': 'SUCCESS'})
        except Exception as e:
            log.error(f'[GOLD] {fact_name}: {e}')
            results.append({'layer': 'fact', 'name': fact_name, 'status': 'FAILED', 'error': str(e)})

failed = [r for r in results if r['status'] == 'FAILED']
print('\nGOLD BUILD SUMMARY')
for r in results:
    icon   = '✓' if r['status'] == 'SUCCESS' else '✗'
    detail = f"\n       ERROR: {r['error']}" if r.get('error') else ''
    print(f'  {icon}  [{r["layer"]}] {r["name"]} — {r["status"]}{detail}')

# Write RI warnings (or empty array) to a shared flat path in GoldLH so the
# Triage tab can surface permissive max_null_sk_pct thresholds.
# Written flat (not per-run) because gold and triage run_ids are not aligned.
_gold_lh       = mssparkutils.lakehouse.get("GoldLH")
_gold_files    = (f"abfss://{_gold_lh.workspaceId}@onelake.dfs.fabric.microsoft.com"
                  f"/{_gold_lh.id}/Files")
_warnings_path = f"{_gold_files}/_triage/gold_warnings.json"
mssparkutils.fs.put(_warnings_path, json.dumps(_gold_ri_warnings, indent=2), overwrite=True)
log.info(
    f"[GOLD] {len(_gold_ri_warnings)} RI warning(s) written to _triage/gold_warnings.json"
    if _gold_ri_warnings
    else "[GOLD] no RI warnings — wrote empty _triage/gold_warnings.json"
)

mssparkutils.notebook.exit(json.dumps({
    'status':  'FAILED' if failed else 'SUCCESS',
    'failed':  [r['name'] for r in failed],
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
