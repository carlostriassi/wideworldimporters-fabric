# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Silver Quarantine Resolver
#
# Reads the `_resolutions` Delta table and acts on each decision:
#   REINSERT             → re-MERGE the original quarantine row into Silver,
#                          applying override_values, attaching resolver audit columns
#   DISCARD              → mark RESOLVED; row stays out of Silver
#   SOURCE_FIX_PENDING   → no-op (source data steward owns; row stays quarantined)
#   REGISTRY_UPDATED     → no-op (next pipeline run re-ingests cleanly; mark RESOLVED)
#
# Idempotent: rows already at status RESOLVED are skipped. Safe to re-run.
#
# Trigger: manual, after a human has populated `_resolutions` rows — either by
# hand in a Fabric notebook, or by the Streamlit workflow app, which submits its
# decisions through the `inbox_json` parameter below (it has no Spark session of
# its own and so cannot write Delta directly).
# NOT part of the master pipeline.

# PARAMETERS CELL ********************

# ── Parameters ──
# Bound by the Fabric "Run On Demand Item Job" REST API. The defaults make a plain
# manual run behave exactly as it always has, so nothing changes for hand-driven use.
#
# inbox_json  — JSON array of decision objects to append to `_resolutions` before
#               resolving. Used by the Streamlit workflow app, which runs outside
#               Fabric and has no Spark session, so it cannot write Delta itself.
#               Each object requires table_name, dq_run_id, natural_key, dq_stage,
#               decision; override_values / resolver_upn / resolver_note are optional.
# record_only — "true" writes the inbox rows and stops without resolving them. This
#               is what the app's "Submit decision" button sends; "Submit & run
#               resolver now" sends "false".

inbox_json  = "[]"
record_only = "false"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 1 — Imports + config copy
import sys, os, json
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, coalesce,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType,
)
from delta.tables import DeltaTable

_lh        = mssparkutils.lakehouse.get("BronzeLH")
_cfg_src   = f"abfss://{_lh.workspaceId}@onelake.dfs.fabric.microsoft.com/{_lh.id}/Files/config"
_cfg_local = "/tmp/nb_config"
os.makedirs(_cfg_local, exist_ok=True)
for _f in mssparkutils.fs.ls(_cfg_src):
    if _f.name.endswith(".py"):
        with open(f"{_cfg_local}/{_f.name}", "w") as _fh:
            _fh.write(mssparkutils.fs.head(_f.path, 1_000_000))

import shutil, importlib
for _mod in ("workspace_config", "source_registry", "dq_rules_registry"):
    sys.modules.pop(_mod, None)
shutil.rmtree(f"{_cfg_local}/__pycache__", ignore_errors=True)
sys.path.insert(0, _cfg_local)
# On a reused Spark session (Fabric may keep a live session across runs), Python's
# sys.path_importer_cache can keep serving a stale directory listing for _cfg_local
# even after these files are rewritten with new content — popping sys.modules alone
# does not force a re-scan. Without this, a session that already imported these
# modules once can silently keep using an older config on every later run.
importlib.invalidate_caches()

from workspace_config import (
    SILVER_TABLES, SILVER_FILES, SILVER_QUARANTINE,
    apply_spark_settings,
)
from source_registry   import SOURCE_REGISTRY
from dq_rules_registry import DQ_RULES_REGISTRY

spark = SparkSession.builder.getOrCreate()
apply_spark_settings(spark)

RESOLUTIONS_ROOT       = f"{SILVER_FILES}/_resolutions"
RESOLUTION_METRICS     = f"{SILVER_FILES}/_resolution_metrics"
RUN_ID                 = datetime.utcnow().strftime("%Y-%m-%d-%H%M")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2 — _resolutions schema (defined here; created on first write per table)

RESOLUTIONS_SCHEMA = StructType([
    StructField("dq_run_id",       StringType(),                  False),
    StructField("table_name",      StringType(),                  False),
    StructField("natural_key",     StringType(),                  False),
    StructField("dq_stage",        StringType(),                  False),
    StructField("decision",        StringType(),                  False),
    StructField("override_values", MapType(StringType(), StringType()), True),
    StructField("resolver_upn",    StringType(),                  True),
    StructField("resolver_note",   StringType(),                  True),
    StructField("resolved_at",     TimestampType(),               True),
])

# Decisions the resolver acts on. Anything else is treated as "no-op, mark RESOLVED".
ACT_REINSERT = "REINSERT"
NOOP_DECISIONS = {"DISCARD", "REGISTRY_UPDATED", "SOURCE_FIX_PENDING"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2b — Inbox: append externally-submitted decisions to _resolutions
#
# The Streamlit workflow app (scripts/generate_replit_app.py --template workflow)
# runs on Replit with no Spark session, so it cannot write a Delta row itself. It
# triggers this notebook and passes its decisions as `inbox_json`. Performing the
# write here keeps RESOLUTIONS_SCHEMA above as the single definition of the
# `_resolutions` row shape, rather than duplicating it in the app.
#
# The write is a MERGE on the same composite key the resolver acts on, so
# re-submitting a row updates its decision instead of queueing a duplicate. Rows
# already at decision RESOLVED are left alone — reopening a settled row would
# re-REINSERT it. To revise a resolved decision, edit `_resolutions` directly.

_REQUIRED_INBOX_KEYS = ("table_name", "dq_run_id", "natural_key", "dq_stage", "decision")
_VALID_DECISIONS     = {ACT_REINSERT} | NOOP_DECISIONS


def _parse_inbox(raw: str) -> list[tuple]:
    """Validate inbox_json into RESOLUTIONS_SCHEMA-ordered tuples.

    Raises on anything malformed rather than skipping it: a silent skip would let
    the submitting app report "decision recorded" while the decision was dropped,
    which is the exact failure this cell exists to eliminate.
    """
    if not raw or not str(raw).strip():
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"inbox_json is not valid JSON: {e}") from e
    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        raise ValueError(
            f"inbox_json must be a JSON array or object, got {type(parsed).__name__}"
        )

    out: list[tuple] = []
    for i, item in enumerate(parsed):
        if not isinstance(item, dict):
            raise ValueError(
                f"inbox_json[{i}] must be an object, got {type(item).__name__}"
            )
        missing = [k for k in _REQUIRED_INBOX_KEYS if not item.get(k)]
        if missing:
            raise ValueError(
                f"inbox_json[{i}] missing required key(s): {', '.join(missing)}"
            )
        decision = str(item["decision"]).strip().upper()
        if decision not in _VALID_DECISIONS:
            raise ValueError(
                f"inbox_json[{i}] decision {decision!r} is not one of "
                f"{sorted(_VALID_DECISIONS)}"
            )
        overrides = item.get("override_values") or None
        if overrides is not None:
            if not isinstance(overrides, dict):
                raise ValueError(
                    f"inbox_json[{i}] override_values must be an object, got "
                    f"{type(overrides).__name__}"
                )
            # MapType(String, String): stringify values so a JSON number does not
            # fail the Spark conversion.
            overrides = {
                str(k): (None if v is None else str(v)) for k, v in overrides.items()
            }
        if decision != ACT_REINSERT and overrides:
            raise ValueError(
                f"inbox_json[{i}] has override_values but decision is {decision!r} — "
                "overrides are only applied on REINSERT and would be silently ignored"
            )
        out.append((
            str(item["dq_run_id"]),
            str(item["table_name"]),
            str(item["natural_key"]),
            str(item["dq_stage"]),
            decision,
            overrides,
            str(item.get("resolver_upn") or "external-submission"),
            str(item.get("resolver_note") or ""),
            None,                       # resolved_at — stamped below
        ))
    return out


_inbox       = _parse_inbox(inbox_json)
_RECORD_ONLY = str(record_only).strip().lower() in ("true", "1", "yes")

if _inbox:
    print(f"Inbox: {len(_inbox)} externally-submitted decision(s)")
    for _tbl in sorted({r[1] for r in _inbox}):
        _rows  = [r for r in _inbox if r[1] == _tbl]
        _df_in = (
            spark.createDataFrame(_rows, RESOLUTIONS_SCHEMA)
                 .withColumn("resolved_at", current_timestamp())
        )
        _path = f"{RESOLUTIONS_ROOT}/{_tbl}"
        if DeltaTable.isDeltaTable(spark, _path):
            (
                DeltaTable.forPath(spark, _path).alias("t")
                .merge(
                    _df_in.alias("s"),
                    "t.dq_run_id   = s.dq_run_id   AND "
                    "t.table_name  = s.table_name  AND "
                    "t.natural_key = s.natural_key AND "
                    "t.dq_stage    = s.dq_stage",
                )
                .whenMatchedUpdateAll(condition="t.decision <> 'RESOLVED'")
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            _df_in.write.format("delta").mode("append").save(_path)
        print(f"  {_tbl}: {len(_rows)} decision(s) written to _resolutions")
else:
    print("Inbox: empty — resolving whatever is already in _resolutions")

if _RECORD_ONLY:
    _msg = (
        f"Recorded {len(_inbox)} decision(s) to _resolutions. record_only=true, "
        "so nothing was resolved — re-run this notebook without record_only to act "
        "on them."
    )
    print(_msg)
    mssparkutils.notebook.exit(json.dumps({
        "status":   "SUCCESS",
        "mode":     "record_only",
        "recorded": len(_inbox),
        "resolved": 0,
        "message":  _msg,
    }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3 — Helpers

def _safe_read(path: str):
    try:
        if DeltaTable.isDeltaTable(spark, path):
            return spark.read.format("delta").load(path)
    except Exception:
        pass
    return None


def _natural_key_expr(rules: dict):
    """Match silver_quarantine_triage's natural-key construction."""
    keys = rules.get("dedup_key") or []
    if not keys:
        return lit(None).cast("string")
    if len(keys) == 1:
        return col(keys[0]).cast("string")
    return F.concat_ws("|", *[col(k).cast("string") for k in keys])


def _silver_audit_cols(df: DataFrame, table_name: str, natural_key_cols: list[str]) -> DataFrame:
    """Mirrors silver_dq_dedup.attach_audit_cols so resolved rows are
    indistinguishable from pipeline-produced rows."""
    change_detect_cols = [c for c in df.columns if not c.startswith("_") and not c.startswith("_qq_")]
    return (
        df
        .withColumn("_silver_processed_at", current_timestamp())
        .withColumn("_source_system",       lit(table_name))
        .withColumn("_natural_key_hash",
            sha2(concat_ws("|", *[col(c) for c in natural_key_cols]), 256))
        .withColumn("_row_hash",
            sha2(concat_ws("|", *[
                coalesce(col(c).cast("string"), lit("__null__"))
                for c in change_detect_cols
            ]), 256))
    )


def _list_resolutions_tables() -> list[str]:
    """Return every table that has a _resolutions directory on disk."""
    try:
        entries = mssparkutils.fs.ls(RESOLUTIONS_ROOT)
    except Exception:
        return []
    out = []
    for e in entries:
        if e.isDir:
            out.append(e.name.rstrip("/"))
    return out

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4 — Resolver master loop

resolution_tables = _list_resolutions_tables()
print(f"_resolutions tables found: {len(resolution_tables)}")

results: list[dict] = []

for table_name in resolution_tables:
    rules     = DQ_RULES_REGISTRY.get(table_name)
    source    = SOURCE_REGISTRY.get(table_name)
    if rules is None or source is None:
        print(f"  [SKIP] {table_name}: not in registries")
        continue

    res_path  = f"{RESOLUTIONS_ROOT}/{table_name}"
    df_res    = _safe_read(res_path)
    if df_res is None:
        continue
    pending = df_res.filter(col("decision") != "RESOLVED")
    if pending.rdd.isEmpty():
        print(f"  {table_name}: 0 pending decisions")
        continue

    silver_path = f"{SILVER_TABLES}/{source['bronze_table']}"
    qpath       = f"{SILVER_QUARANTINE}/{table_name}"
    df_q        = _safe_read(qpath)
    nat_key_e   = _natural_key_expr(rules)

    reinserted = 0
    discarded  = 0
    errored    = 0
    resolved_ids: list[tuple[str, str, str, str]] = []   # (run_id, table, key, stage)

    rows_pending = pending.collect()
    print(f"  {table_name}: {len(rows_pending)} pending decision row(s)")

    rebuilt_frames: list[DataFrame] = []

    for r in rows_pending:
        run_id_v   = r["dq_run_id"]
        nat_key_v  = r["natural_key"]
        stage_v    = r["dq_stage"]
        decision   = r["decision"]
        overrides  = dict(r["override_values"] or {})

        if decision in NOOP_DECISIONS:
            resolved_ids.append((run_id_v, table_name, nat_key_v, stage_v))
            if decision == "DISCARD":
                discarded += 1
            continue

        if decision != ACT_REINSERT:
            # Unrecognised decision — leave for human, don't mark RESOLVED.
            continue

        if df_q is None:
            print(f"    [ERROR] {table_name}: quarantine missing — cannot REINSERT")
            errored += 1
            continue

        match = (
            df_q
            .filter(col("_dq_run_id") == run_id_v)
            .filter(col("_dq_stage")  == stage_v)
            .filter(nat_key_e         == nat_key_v)
        )
        if match.rdd.isEmpty():
            print(f"    [ERROR] {table_name}: no quarantine row for "
                  f"(run_id={run_id_v}, key={nat_key_v}, stage={stage_v})")
            errored += 1
            continue

        # Apply overrides one column at a time. Cast to string→target column type
        # only when the existing column type is known; otherwise we pass through
        # the string lit and let Spark coerce on MERGE.
        df_one = match
        for fld, val in overrides.items():
            if fld not in df_one.columns:
                continue
            df_one = df_one.withColumn(fld, lit(val).cast(df_one.schema[fld].dataType))

        # Resolver audit columns
        df_one = (
            df_one
            .withColumn("_qq_resolved_at",  current_timestamp())
            .withColumn("_qq_resolver_upn", lit(r["resolver_upn"]))
            .withColumn("_qq_decision",     lit(ACT_REINSERT))
            .withColumn("_qq_run_id",       lit(RUN_ID))
        )

        rebuilt_frames.append(df_one)
        resolved_ids.append((run_id_v, table_name, nat_key_v, stage_v))
        reinserted += 1

    # MERGE all reinserts for this table in one operation
    if rebuilt_frames:
        df_merge = rebuilt_frames[0]
        for f in rebuilt_frames[1:]:
            df_merge = df_merge.unionByName(f, allowMissingColumns=True)

        # Drop quarantine-only fields so target Silver schema isn't polluted.
        for c in ("_dq_stage", "_dq_run_id", "_dq_at"):
            if c in df_merge.columns:
                df_merge = df_merge.drop(c)

        # Attach Silver audit cols so resolved rows match the pipeline shape.
        df_merge = _silver_audit_cols(df_merge, table_name, rules["dedup_key"])

        join_col = rules["dedup_key"][0]
        if DeltaTable.isDeltaTable(spark, silver_path):
            DeltaTable.forPath(spark, silver_path).alias("t").merge(
                df_merge.alias("s"), f"t.{join_col} = s.{join_col}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df_merge.write.format("delta").mode("overwrite") \
                    .option("mergeSchema", "true").save(silver_path)

    # Flip resolved rows in _resolutions to status RESOLVED
    if resolved_ids:
        resolved_df = spark.createDataFrame(
            [(r, t, k, s) for (r, t, k, s) in resolved_ids],
            "dq_run_id string, table_name string, natural_key string, dq_stage string",
        ).withColumn("decision",   lit("RESOLVED")) \
         .withColumn("resolved_at", current_timestamp())

        DeltaTable.forPath(spark, res_path).alias("t").merge(
            resolved_df.alias("s"),
            "t.dq_run_id = s.dq_run_id AND t.table_name = s.table_name AND "
            "t.natural_key = s.natural_key AND t.dq_stage = s.dq_stage"
        ).whenMatchedUpdate(set={
            "decision":    "s.decision",
            "resolved_at": "s.resolved_at",
        }).execute()

    results.append({
        "table":      table_name,
        "reinserted": reinserted,
        "discarded":  discarded,
        "errored":    errored,
    })
    print(f"    reinserted={reinserted}  discarded={discarded}  errored={errored}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5 — Persist metrics + notebook exit

if results:
    metrics_rows = [
        {
            "resolver_run_id": RUN_ID,
            "run_at":          datetime.utcnow(),
            "table_name":      r["table"],
            "reinserted":      r["reinserted"],
            "discarded":       r["discarded"],
            "errored":         r["errored"],
        }
        for r in results
    ]
    spark.createDataFrame(metrics_rows).write.format("delta") \
         .mode("append").option("mergeSchema", "true") \
         .save(RESOLUTION_METRICS)

failed = [r for r in results if r["errored"] > 0]
mssparkutils.notebook.exit(json.dumps({
    "status":           "SUCCESS" if not failed else "PARTIAL_FAILURE",
    "resolver_run_id":  RUN_ID,
    "tables_processed": len(results),
    "rows_reinserted":  sum(r["reinserted"] for r in results),
    "rows_discarded":   sum(r["discarded"]  for r in results),
    "rows_errored":     sum(r["errored"]    for r in results),
    "failed":           [r["table"] for r in failed],
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
