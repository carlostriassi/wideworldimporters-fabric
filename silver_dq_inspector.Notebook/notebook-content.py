# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Silver DQ Inspector — Extended
# Read-only diagnostics notebook. All outputs written to `GoldLH/Files/_dq_reports/<timestamp>/`.
# Run after every Silver pipeline execution to assess data quality before Power BI reports go live.

# MARKDOWN ********************

# # CELL 1 — Imports + config copy

# CELL ********************

import sys, os, io
from datetime import datetime, timedelta

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, desc, count, countDistinct,
    round as spark_round, length as spark_length,
    explode, split, trim,
    max as spark_max, min as spark_min,
    to_timestamp,
)
from delta.tables import DeltaTable

# Config copy pattern — immune to FUSE mount issues in pipeline TridentNotebook runs
_lh        = mssparkutils.lakehouse.get("BronzeLH")
_cfg_src   = f"abfss://{_lh.workspaceId}@onelake.dfs.fabric.microsoft.com/{_lh.id}/Files/config"
_cfg_local = "/tmp/nb_config"
os.makedirs(_cfg_local, exist_ok=True)
for _f in mssparkutils.fs.ls(_cfg_src):
    if _f.name.endswith(".py"):
        with open(f"{_cfg_local}/{_f.name}", "w") as _fh:
            _fh.write(mssparkutils.fs.head(_f.path, 1_000_000))

import importlib
for _mod in ("workspace_config", "source_registry", "dq_rules_registry", "star_schema_registry"):
    sys.modules.pop(_mod, None)
sys.path.insert(0, _cfg_local)
importlib.invalidate_caches()

from workspace_config import (
    QUARANTINE_PATH as BRONZE_QUARANTINE_PATH,
    SILVER_QUARANTINE, SILVER_METRICS,
    BRONZE_TABLES, SILVER_TABLES,
    GOLD_TABLES, GOLD_FILES,
    apply_spark_settings,
)
from source_registry      import SOURCE_REGISTRY
from dq_rules_registry    import DQ_RULES_REGISTRY
from star_schema_registry import STAR_SCHEMA_REGISTRY

spark = SparkSession.builder.getOrCreate()
apply_spark_settings(spark)

TABLES = list(SOURCE_REGISTRY.keys())
print(f"Source registry: {len(TABLES)} tables")
for t in TABLES:
    print(f"  • {t}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 2 — Runtime setup: GOLD_LH_NAME variable, report dir, helpers

# CELL ********************

# ── Gold Lakehouse reference ──────────────────────────────────────────────────
# Single variable — change here if the lakehouse is renamed in the workspace.
# Paths (GOLD_TABLES, GOLD_FILES) come from workspace_config and use the item ID,
# so they are immune to name changes; this variable is used for display only.
GOLD_LH_NAME = "GoldLH"

# ── Report output directory ───────────────────────────────────────────────────
RUN_TS     = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
REPORT_DIR = f"{GOLD_FILES}/_dq_reports/{RUN_TS}"
mssparkutils.fs.mkdirs(REPORT_DIR)
print(f"Run timestamp   : {RUN_TS}")
print(f"Report directory: {REPORT_DIR}")

# accumulates all findings for the master action plan in Cell 13
_action_log = []

def _log(severity, table, check, finding, action):
    _action_log.append({"severity": severity, "table": table,
                        "check": check, "finding": finding, "action": action})

# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_read(path):
    """Return a Delta DataFrame, or None if the table does not exist."""
    try:
        if DeltaTable.isDeltaTable(spark, path):
            return spark.read.format("delta").load(path)
    except Exception:
        pass
    return None

def write_csv(df_or_pdf, filename, max_rows=None):
    """Convert to pandas and write as CSV to REPORT_DIR via mssparkutils.fs.put."""
    if df_or_pdf is None:
        return None
    if hasattr(df_or_pdf, "toPandas"):
        pdf = df_or_pdf.limit(max_rows).toPandas() if max_rows else df_or_pdf.toPandas()
    else:
        pdf = df_or_pdf.head(max_rows) if max_rows else df_or_pdf.copy()
    if len(pdf) == 0:
        return None
    buf = io.StringIO()
    pdf.to_csv(buf, index=False)
    dest = f"{REPORT_DIR}/{filename}"
    mssparkutils.fs.put(dest, buf.getvalue(), True)
    return dest

def section(title):
    bar = "═" * 80
    print(f"\n{bar}\n  ◆ {title}\n{bar}")

def subsection(title):
    print(f"\n  ── {title} ──")

def fmt_table(rows, headers, max_w=38):
    """Print a clean bordered ASCII table with auto-sized columns."""
    if not rows:
        print("  (no rows)")
        return
    str_rows = [[str(v)[:max_w] if v is not None else "" for v in r] for r in rows]
    widths = [
        max(len(str(h)), max((len(r[i]) for r in str_rows), default=0))
        for i, h in enumerate(headers)
    ]
    sep = "  +" + "+".join("-" * (w + 2) for w in widths) + "+"
    def row_line(vals):
        return "  |" + "|".join(f" {str(v)[:widths[i]]:<{widths[i]}} " for i, v in enumerate(vals)) + "|"
    print(sep)
    print(row_line(headers))
    print("  +" + "+".join("=" * (w + 2) for w in widths) + "+")
    for r in str_rows:
        print(row_line(r))
    print(sep)
    print(f"  {len(rows):,} row(s)")

# ── Violation guidance: stage → (description, fix action, Power BI impact) ───
GUIDANCE = {
    "null_or_cast":        ("NULL value or type-cast failure",
                            "Column arrives as NULL or incompatible type. Fix insert validation at source.",
                            "Missing SK columns hide fact rows from ALL Power BI visuals."),
    "enum_fail":           ("Invalid enum value",
                            "Value is outside the allowed set. Add the value to enum_rules or fix at source.",
                            "Rejected rows are absent from dimension filter lists in reports."),
    "format_fail":         ("Format pattern mismatch",
                            "Value does not match the expected regex (e.g. POL-######). Fix at source.",
                            "No direct visibility impact but data appears inconsistent in reports."),
    "exact_duplicate":     ("Exact duplicate on natural key",
                            "Source system is emitting duplicate records. Investigate upstream CDC/ETL.",
                            "Without dedup, all aggregate measures (SUM, COUNT) are overstated."),
    "statistical_outlier": ("Statistical outlier — high z-score",
                            "Extreme value detected. Confirm with business: legitimate or data-entry error.",
                            "Skews chart axes, averages, and KPI cards if not resolved."),
    "biz_error":           ("Business rule violation — ERROR (row quarantined)",
                            "A cross-column rule fired and the row was rejected (e.g. paid > approved).",
                            "Fact row is absent from Silver. Aggregates are understated."),
    "warn_biz":            ("Business rule warning — WARN (row kept in Silver)",
                            "Anomaly detected but row was kept. Review with business owner.",
                            "Anomalous data reaches Power BI and may distort measures."),
    "ref_orphan":          ("FK orphan — child references a missing parent",
                            "Fix referential integrity at source before next pipeline run.",
                            "CRITICAL: fact rows with NULL SK are invisible to dimension-filtered visuals."),
    "null_sk":             ("NULL surrogate key in Gold fact table",
                            "Source FK had no match in the dimension. Fix source RI or the Silver join.",
                            "CRITICAL: invisible to any report user filtering or slicing by that dimension."),
}

print("Helpers ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 3 — Gold table discovery (dynamic — no hardcoded names)

# CELL ********************

section(f"GOLD TABLE DISCOVERY  [{GOLD_LH_NAME}]")
print("Tables are discovered by listing GOLD_TABLES from workspace_config.")
print("No table names are hardcoded in this notebook.\n")

gold_schema_tables = {}   # { schema: [table, ...] }
gold_all_paths     = {}   # { "schema/table": abfss_path }

try:
    for schema_entry in mssparkutils.fs.ls(GOLD_TABLES):
        if not schema_entry.isDir:
            continue
        schema_name = schema_entry.name.rstrip("/")
        tables_in_schema = []
        for tbl_entry in mssparkutils.fs.ls(schema_entry.path):
            if tbl_entry.isDir:
                tbl_name = tbl_entry.name.rstrip("/")
                tables_in_schema.append(tbl_name)
                gold_all_paths[f"{schema_name}/{tbl_name}"] = tbl_entry.path
        if tables_in_schema:
            gold_schema_tables[schema_name] = tables_in_schema
except Exception as e:
    print(f"[WARN] Could not list {GOLD_LH_NAME}/Tables: {e}")

gold_all_tables  = list(gold_all_paths.keys())
gold_fact_tables = [t for t in gold_all_tables if "/fact_" in t]

discovery_rows = [
    (schema, tbl, "FACT" if "/fact_" in f"{schema}/{tbl}" else ("SHARED" if schema == "shared" else "DIM"))
    for schema, tables in sorted(gold_schema_tables.items())
    for tbl in sorted(tables)
]
fmt_table(discovery_rows, ["Schema", "Table", "Role"])
print(f"\nFact tables identified: {gold_fact_tables}")

df_disc = pd.DataFrame(discovery_rows, columns=["schema", "table", "role"])
display(df_disc)
write_csv(df_disc, "00_gold_table_discovery.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 4 — Bronze quarantine: summary across all tables

# CELL ********************

section("BRONZE QUARANTINE — Summary by table")
print("Rows caught at Bronze ingestion (NULL PKs, enum violations).")
print("Each row here never reached Silver or Gold.\n")

b_summary = []
for table_name in TABLES:
    path = f"{BRONZE_QUARANTINE_PATH}/{table_name}"
    df   = safe_read(path)
    if df is None:
        b_summary.append((table_name, 0, "—", "✅ Clean"))
        continue
    # count(lit(1)) — counts every row regardless of NULL values in any column
    cnt = df.agg(count(lit(1)).alias("n")).collect()[0]["n"]
    if cnt == 0:
        b_summary.append((table_name, 0, "—", "✅ Clean"))
        continue
    # distinct() to list violation type labels — not to count rows
    viol_types = sorted(
        r["_quarantine_reason"]
        for r in df.select("_quarantine_reason").distinct().collect()
        if r["_quarantine_reason"]
    )
    b_summary.append((table_name, cnt, ", ".join(viol_types), "❌ Action needed"))
    _log("ERROR", table_name, "bronze_quarantine",
         f"{cnt:,} rows quarantined ({', '.join(viol_types)})",
         "Fix NULL PKs or invalid enum values at source before next pipeline run.")

fmt_table(b_summary, ["Table", "Quarantined Rows", "Violation Types", "Status"])

df_bs = pd.DataFrame(b_summary, columns=["table_name", "quarantined_rows", "violation_types", "status"])
display(df_bs)
dest = write_csv(df_bs, "01_bronze_quarantine_summary.csv")
print(f"\n→ {dest}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 5 — Bronze quarantine: per-table violation detail

# CELL ********************

section("BRONZE QUARANTINE — Per-table violation detail")
print("Only tables with quarantined rows are shown. Full rows written to CSV.\n")

written = []
for table_name in TABLES:
    path = f"{BRONZE_QUARANTINE_PATH}/{table_name}"
    df   = safe_read(path)
    if df is None:
        continue
    cnt = df.agg(count(lit(1)).alias("n")).collect()[0]["n"]
    if cnt == 0:
        continue

    subsection(f"{table_name}  ({cnt:,} quarantined rows)")

    # Breakdown by violation type — count(lit(1)) per group, not count(column)
    by_type = (
        df.groupBy("_quarantine_reason")
          .agg(count(lit(1)).alias("row_count"))
          .orderBy(desc("row_count"))
    )
    print("  Breakdown by violation type:")
    display(by_type)

    print(f"  Sample rows (up to 1,000 most recent):")
    display(df.orderBy(desc("_quarantine_at")).limit(1_000))

    dest = write_csv(df, f"02_bronze_quarantine_{table_name}.csv", max_rows=10_000)
    written.append(dest)
    print(f"  → {dest}")

if not written:
    print("  No Bronze quarantine rows found for any table. ✅")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 6 — Silver quarantine: summary across all tables

# CELL ********************

section("SILVER QUARANTINE — Summary by table and DQ stage")
print("All six Silver DQ stages: null/cast, range/enum, format, dedup,")
print("referential integrity, business rules, statistical outliers.\n")

s_summary = []
for table_name in TABLES:
    path = f"{SILVER_QUARANTINE}/{table_name}"
    df   = safe_read(path)

    if df is None:
        s_summary.append((table_name, 0, 0, "—", "✅ Clean"))
        continue

    cnt = df.agg(count(lit(1)).alias("n")).collect()[0]["n"]

    # duplicate quarantine (separate _dupes path)
    df_d     = safe_read(f"{SILVER_QUARANTINE}/{table_name}_dupes")
    dupe_cnt = df_d.agg(count(lit(1)).alias("n")).collect()[0]["n"] if df_d else 0

    if cnt == 0 and dupe_cnt == 0:
        s_summary.append((table_name, 0, 0, "—", "✅ Clean"))
        continue

    # stage breakdown — count(lit(1)) per group
    stage_agg = (
        df.groupBy("_dq_stage")
          .agg(count(lit(1)).alias("rows"))
          .orderBy(desc("rows"))
          .collect()
    )
    stage_str = " | ".join(f"{r['_dq_stage']}:{r['rows']:,}" for r in stage_agg)

    stages_set   = {r["_dq_stage"] for r in stage_agg}
    hard_error   = bool({"null_or_cast", "enum_fail"} & stages_set) or \
                   any(s.startswith("biz:") for s in stages_set)
    status = "❌ Action needed" if hard_error else "⚠️  Review"

    s_summary.append((table_name, cnt, dupe_cnt, stage_str[:70], status))
    _log("ERROR" if hard_error else "WARN", table_name, "silver_quarantine",
         f"{cnt:,} quarantined + {dupe_cnt:,} dupes  [{stage_str[:60]}]",
         "Investigate the stage breakdown below; trace root cause to source data.")

fmt_table(s_summary, ["Table", "Quarantined", "Dupes", "Stage Breakdown", "Status"])

df_ss = pd.DataFrame(s_summary, columns=["table_name", "quarantined_rows", "dupes",
                                          "stage_breakdown", "status"])
display(df_ss)
dest = write_csv(df_ss, "03_silver_quarantine_summary.csv")
print(f"\n→ {dest}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 7 — Silver quarantine: per-table stage breakdown + sample rows

# CELL ********************

section("SILVER QUARANTINE — Per-table stage breakdown and sample rows")
print("Each stage name maps to a DQ check. Stages with drops need source investigation.\n")

STAGE_GUIDE = {
    "null_or_cast":        "NULL value or type-cast failure — check not_null_cols and cast_rules",
    "enum_fail":           "Value outside allowed set — check enum_rules in dq_rules_registry",
    "format_fail":         "Regex format mismatch — check format_rules",
    "exact_duplicate":     "Duplicate on natural key — check source CDC / dedup_key",
    "statistical_outlier": "Extreme z-score — check statistical_rules",
    "ref:":                "FK orphan — child row has no matching parent row in Silver",
    "biz:":                "Business rule ERROR — cross-column validation failed",
    "cond:":               "Conditional rule — required column NULL given a specific status value",
}

written = []
for table_name in TABLES:
    path = f"{SILVER_QUARANTINE}/{table_name}"
    df   = safe_read(path)
    if df is None:
        continue
    cnt = df.agg(count(lit(1)).alias("n")).collect()[0]["n"]
    if cnt == 0:
        continue

    subsection(f"{table_name}  ({cnt:,} quarantined rows)")

    # Count by stage — count(lit(1)) avoids skipping NULLs in any column
    by_stage = (
        df.groupBy("_dq_stage")
          .agg(count(lit(1)).alias("row_count"))
          .orderBy(desc("row_count"))
    )
    print("  Count by DQ stage:")
    display(by_stage)

    # Print guidance for each stage that has violations
    print("  Stage interpretation:")
    for row in by_stage.collect():
        stage = row["_dq_stage"]
        guide = next((v for k, v in STAGE_GUIDE.items() if stage.startswith(k)), "—")
        print(f"    {stage:<35} {row['row_count']:>6,} rows  →  {guide}")

    # Count by run_id to show trend
    print("\n  Count by pipeline run:")
    by_run = (
        df.groupBy("_dq_run_id")
          .agg(count(lit(1)).alias("row_count"))
          .orderBy(desc("_dq_run_id"))
    )
    display(by_run)

    # Sample rows — most recent violations, full columns visible
    print(f"\n  Sample rows (up to 500 most recent):")
    display(df.orderBy(desc("_dq_at")).limit(500))

    dest = write_csv(df, f"04_silver_quarantine_{table_name}.csv", max_rows=10_000)
    written.append(dest)
    print(f"  → {dest}")

    # Duplicate quarantine
    df_d = safe_read(f"{SILVER_QUARANTINE}/{table_name}_dupes")
    if df_d:
        dupe_cnt = df_d.agg(count(lit(1)).alias("n")).collect()[0]["n"]
        if dupe_cnt > 0:
            subsection(f"{table_name} — duplicates  ({dupe_cnt:,} rows)")
            display(df_d.orderBy(desc("_dq_at")).limit(500))
            dest_d = write_csv(df_d, f"04_silver_dupes_{table_name}.csv", max_rows=10_000)
            print(f"  → {dest_d}")

if not written:
    print("  No Silver quarantine rows found. ✅")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 8 — DQ stage funnel: all tables, latest pipeline run

# CELL ********************

section("DQ STAGE FUNNEL — All tables (latest pipeline run each)")
print("Shows how many rows were dropped at each Silver DQ stage.")
print("Every drop is potential data missing from Power BI reports.\n")

STAGE_ORDER = [
    "bronze_read", "after_null_type", "after_range_enum",
    "after_format", "after_dedup", "after_referential",
    "after_business_rules", "after_statistical",
    "after_audit_cols", "silver_write",
]

df_metrics = safe_read(SILVER_METRICS)
funnel_out = []

if df_metrics is None:
    print("[WARN] No DQ metrics table found — Silver has not run yet.")
else:
    # Remove duplicate metric records for the same (table, run, stage)
    # before any aggregation; prevents inflated row counts on joined pivots
    df_metrics = df_metrics.dropDuplicates(["table_name", "run_id", "stage"])

    for table_name in TABLES:
        df_tbl = df_metrics.filter(col("table_name") == table_name)
        if df_tbl.agg(count(lit(1)).alias("n")).collect()[0]["n"] == 0:
            continue

        # latest run: max(run_id) — deterministic, no .first() on unsorted data
        latest_run = df_tbl.agg(spark_max("run_id").alias("rid")).collect()[0]["rid"]
        stage_data = {
            r["stage"]: r["row_count"]
            for r in df_tbl.filter(col("run_id") == latest_run).collect()
        }
        if not stage_data:
            continue

        subsection(f"{table_name}  (run: {latest_run})")
        print(f"  {'Stage':<32} {'Rows':>10} {'Dropped':>10} {'Drop %':>8}  Note")
        print(f"  {'-'*32} {'-'*10} {'-'*10} {'-'*8}  ----")

        prev = None
        for stage in STAGE_ORDER:
            if stage not in stage_data:
                continue
            rows    = stage_data[stage]
            dropped = (prev - rows) if prev is not None else 0
            pct     = round(dropped / prev * 100, 1) if prev and prev > 0 else 0.0
            note    = "◄ violations" if dropped > 0 else ""
            print(f"  {stage:<32} {rows:>10,} {dropped:>10,} {pct:>7.1f}%  {note}")
            if dropped > 0 and stage != "bronze_read":
                funnel_out.append((table_name, latest_run, stage, rows, dropped, pct))
            prev = rows

    if funnel_out:
        df_f = pd.DataFrame(funnel_out,
                            columns=["table_name", "run_id", "stage",
                                     "rows_after", "dropped", "drop_pct"])
        print("\n  All drops across all tables:")
        display(df_f)
        dest = write_csv(df_f, "05_dq_stage_funnel.csv")
        print(f"\n→ {dest}")
    else:
        print("\n  No rows dropped at any stage. ✅")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 9 — DQ metrics: rejection rate vs configured threshold (all runs)

# CELL ********************

section("DQ METRICS — Rejection rate vs configured threshold")
print("Tables approaching or exceeding max_rejection_pct will block future pipeline runs.")
print("Rate = (bronze_rows - silver_rows) / bronze_rows × 100\n")

df_metrics = safe_read(SILVER_METRICS)
rej_out = []

if df_metrics is None:
    print("[WARN] No DQ metrics table found. Run Silver first.")
else:
    df_metrics = df_metrics.dropDuplicates(["table_name", "run_id", "stage"])

    for table_name in TABLES:
        threshold = DQ_RULES_REGISTRY.get(table_name, {}).get("max_rejection_pct", 5.0)
        df_tbl    = df_metrics.filter(col("table_name") == table_name)

        if df_tbl.agg(count(lit(1)).alias("n")).collect()[0]["n"] == 0:
            continue

        # one row per run_id for bronze_read and silver_write stages
        # dropDuplicates(["run_id"]) on each side prevents cartesian inflation on join
        bronze = (
            df_tbl.filter(col("stage") == "bronze_read")
                  .dropDuplicates(["run_id"])
                  .select("run_id", col("row_count").alias("bronze_rows"), "run_at")
        )
        silver = (
            df_tbl.filter(col("stage") == "silver_write")
                  .dropDuplicates(["run_id"])
                  .select("run_id", col("row_count").alias("silver_rows"))
        )

        trend = (
            bronze.join(silver, on="run_id", how="left")
                  .withColumn("rejected",
                              col("bronze_rows") - col("silver_rows"))
                  .withColumn("rejection_pct",
                              spark_round(
                                  when(col("bronze_rows") > 0,
                                       (col("bronze_rows") - col("silver_rows"))
                                       / col("bronze_rows") * 100
                                  ).otherwise(0.0), 2
                              ))
                  .withColumn("threshold",  lit(round(threshold, 1)))
                  .withColumn("status",
                              when(col("rejection_pct") > threshold,
                                   lit("❌ EXCEEDS"))
                             .when(col("rejection_pct") > threshold * 0.80,
                                   lit("⚠️  NEAR"))
                             .otherwise(lit("✅ OK")))
                  .orderBy(desc("run_at"))
        )

        latest = trend.first()
        if latest:
            rej_out.append((
                table_name,
                str(latest["run_at"])[:19] if latest["run_at"] else "—",
                int(latest["bronze_rows"] or 0),
                int(latest["silver_rows"] or 0),
                int(latest["rejected"]   or 0),
                f"{latest['rejection_pct'] or 0:.1f}%",
                f"{threshold:.1f}%",
                latest["status"],
            ))
            if latest["status"] != "✅ OK":
                _log("ERROR" if "EXCEEDS" in latest["status"] else "WARN",
                     table_name, "rejection_rate",
                     f"Rejection {latest['rejection_pct']:.1f}% vs threshold {threshold:.1f}%",
                     "Check DQ stage funnel (Cell 8) to find which stage drops the most rows.")

    if rej_out:
        headers = ["Table", "Latest Run", "Bronze", "Silver",
                   "Rejected", "Rate", "Threshold", "Status"]
        fmt_table(rej_out, headers)
        df_rej = pd.DataFrame(rej_out, columns=headers)
        display(df_rej)
        dest = write_csv(df_rej, "06_rejection_rates.csv")
        print(f"\n→ {dest}")
    else:
        print("  No metrics data available yet.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 10 — Business rule warnings: rows that passed to Silver with flags

# CELL ********************

section("BUSINESS RULE WARNINGS — Anomalous rows that reached Silver")
print("These rows triggered WARN-level business rules but were NOT quarantined.")
print("They are present in Silver and will appear in Power BI — review with the business team.\n")

warn_summary = []
for table_name in TABLES:
    reg         = SOURCE_REGISTRY.get(table_name, {})
    silver_path = f"{SILVER_TABLES}/{reg.get('bronze_table', table_name)}"
    df_s        = safe_read(silver_path)
    if df_s is None or "_dq_warnings" not in df_s.columns:
        warn_summary.append((table_name, 0, "—", "✅ No warnings"))
        continue

    df_warned = df_s.filter(
        col("_dq_warnings").isNotNull() & (F.size(col("_dq_warnings")) > 0)
    )
    warn_cnt = df_warned.agg(count(lit(1)).alias("n")).collect()[0]["n"]

    if warn_cnt == 0:
        warn_summary.append((table_name, 0, "—", "✅ No warnings"))
        continue

    df_flags = (
        df_warned
        .select(explode(col("_dq_warnings")).alias("flag"))
        .filter(col("flag").isNotNull() & (spark_length(col("flag")) > 0))
        .groupBy("flag")
        .agg(count(lit(1)).alias("row_count"))
        .orderBy(desc("row_count"))
    )
    top_flags = ", ".join(r["flag"] for r in df_flags.collect()[:3])
    warn_summary.append((table_name, warn_cnt, top_flags, "⚠️  Review with business"))

    _log("WARN", table_name, "business_rule_warnings",
         f"{warn_cnt:,} rows with warn flags: {top_flags}",
         GUIDANCE["warn_biz"][1])

    subsection(f"{table_name}  ({warn_cnt:,} warned rows)")
    print("  Flag breakdown:")
    display(df_flags)
    print(f"\n  Sample warned rows (up to 500):")
    display(df_warned.limit(500))
    dest = write_csv(df_warned, f"07_biz_warn_{table_name}.csv", max_rows=10_000)
    print(f"  → {dest}")

fmt_table(warn_summary, ["Table", "Warned Rows", "Top Flags", "Status"])
df_ws = pd.DataFrame(warn_summary, columns=["table_name", "warned_rows", "top_flags", "status"])
display(df_ws)
dest = write_csv(df_ws, "07_biz_warn_summary.csv")
print(f"\n→ {dest}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 11 — Silver referential integrity: FK orphan rates

# CELL ********************

section("SILVER REFERENTIAL INTEGRITY — FK orphan rates")
print("FK orphan rows in Silver produce NULL surrogate keys in Gold fact tables.")
print("Fact rows with NULL SKs are INVISIBLE to Power BI visuals filtered by that dimension.\n")

ri_rows = []
for table_name in TABLES:
    rules    = DQ_RULES_REGISTRY.get(table_name, {})
    ref_rules = rules.get("referential_rules", [])
    if not ref_rules:
        continue

    reg         = SOURCE_REGISTRY.get(table_name, {})
    silver_path = f"{SILVER_TABLES}/{reg.get('bronze_table', table_name)}"
    df_s        = safe_read(silver_path)
    if df_s is None:
        continue

    # total Silver rows — count(*) once, reuse
    total = df_s.agg(count(lit(1)).alias("n")).collect()[0]["n"]

    for (rule_name, dim_table_path, fk_col, severity) in ref_rules:
        if fk_col not in df_s.columns:
            ri_rows.append((table_name, rule_name, fk_col, "—", "—", "—", "⚠️  col absent"))
            continue

        # count(*) rows where FK is NULL
        null_fk  = df_s.filter(col(fk_col).isNull()) \
                        .agg(count(lit(1)).alias("n")).collect()[0]["n"]
        null_pct = round(null_fk / total * 100, 2) if total > 0 else 0.0

        # count orphan rows: child rows whose FK value has no match in the parent table
        orphan_rows = 0
        df_dim = safe_read(f"{SILVER_TABLES}/{dim_table_path}")
        if df_dim is not None:
            pk_col = rule_name.replace("fk_", "")
            if pk_col in df_dim.columns:
                # Spark anti-join: child rows with no matching parent row
                orphan_rows = (
                    df_s.filter(col(fk_col).isNotNull())
                        .join(df_dim.select(col(pk_col).alias("_pk")),
                              col(fk_col) == col("_pk"), "left_anti")
                        .agg(count(lit(1)).alias("n")).collect()[0]["n"]
                )

        orphan_pct = round(orphan_rows / total * 100, 2) if total > 0 else 0.0
        status = "❌ Fix required" if (orphan_rows > 0 or null_fk > 0) else "✅ Clean"
        ri_rows.append((
            table_name, rule_name, fk_col,
            f"{null_fk:,} ({null_pct:.1f}%)",
            f"{orphan_rows:,} ({orphan_pct:.1f}%)",
            severity, status,
        ))

        if orphan_rows > 0 or null_fk > 0:
            _log("ERROR" if severity == "ERROR" else "WARN",
                 table_name, f"silver_ri_{rule_name}",
                 f"{orphan_rows:,} orphan rows + {null_fk:,} NULL FKs on {fk_col}",
                 GUIDANCE["ref_orphan"][1])

headers = ["Table", "Rule", "FK Column", "NULL FKs", "Orphan Rows", "Severity", "Status"]
fmt_table(ri_rows, headers)
df_ri = pd.DataFrame(ri_rows, columns=headers)
display(df_ri)
dest = write_csv(df_ri, "08_silver_referential_integrity.csv")
print(f"\n→ {dest}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 12 — Fill rate and freshness SLA

# CELL ********************

section("FILL RATE & FRESHNESS SLA")
print("fill_rate_rules: minimum % of non-NULL values required per column.")
print("freshness_sla_hours: maximum age of the latest Silver record.\n")

fill_rows    = []
fresh_rows   = []

for table_name in TABLES:
    rules       = DQ_RULES_REGISTRY.get(table_name, {})
    reg         = SOURCE_REGISTRY.get(table_name, {})
    silver_path = f"{SILVER_TABLES}/{reg.get('bronze_table', table_name)}"
    df_s        = safe_read(silver_path)
    if df_s is None:
        continue

    # total rows — count(*) once
    total = df_s.agg(count(lit(1)).alias("n")).collect()[0]["n"]

    # ── Fill rate ─────────────────────────────────────────────────────────
    for col_name, min_pct in rules.get("fill_rate_rules", {}).items():
        if col_name not in df_s.columns:
            fill_rows.append((table_name, col_name, "—", f"{min_pct:.1f}%", "—", "⚠️  col absent"))
            continue

        # count(col_name) intentionally skips NULLs — correct for fill-rate calculation
        non_null   = df_s.agg(count(col(col_name)).alias("n")).collect()[0]["n"]
        actual_pct = round(non_null / total * 100, 1) if total > 0 else 0.0
        gap        = actual_pct - min_pct

        if actual_pct >= min_pct:
            status = "✅ OK"
        elif actual_pct >= min_pct * 0.90:
            status = "⚠️  Near threshold"
        else:
            status = "❌ Below threshold"

        fill_rows.append((table_name, col_name,
                          f"{actual_pct:.1f}%", f"{min_pct:.1f}%",
                          f"{gap:+.1f}%", status))

        if actual_pct < min_pct:
            _log("WARN", table_name, f"fill_rate_{col_name}",
                 f"'{col_name}' fill {actual_pct:.1f}% < required {min_pct:.1f}%",
                 f"Enrich '{col_name}' at source or lower the threshold if acceptable.")

    # ── Freshness SLA ─────────────────────────────────────────────────────
    sla_hours = rules.get("freshness_sla_hours")
    if sla_hours and "_silver_processed_at" in df_s.columns:
        latest_ts = df_s.agg(spark_max("_silver_processed_at").alias("ts")).collect()[0]["ts"]
        if latest_ts:
            age_h = (datetime.utcnow() - latest_ts.replace(tzinfo=None)).total_seconds() / 3600
            fresh_status = (
                f"❌ {age_h:.0f}h old (SLA: {sla_hours}h)" if age_h > sla_hours
                else f"✅ {age_h:.1f}h old (SLA: {sla_hours}h)"
            )
            fresh_rows.append((table_name, f"{age_h:.1f}h", f"{sla_hours}h", fresh_status))
            if age_h > sla_hours:
                _log("WARN", table_name, "freshness_sla",
                     f"Last processed {age_h:.0f}h ago (SLA: {sla_hours}h)",
                     "Schedule the Bronze → Silver pipeline to run more frequently.")

subsection("Fill Rate")
if fill_rows:
    fmt_table(fill_rows, ["Table", "Column", "Actual", "Min Required", "Gap", "Status"])
    df_fill = pd.DataFrame(fill_rows, columns=["table_name", "column", "actual_pct",
                                                "min_required", "gap", "status"])
    display(df_fill)
    dest = write_csv(df_fill, "09_fill_rate.csv")
    print(f"\n→ {dest}")
else:
    print("  No fill_rate_rules configured.")

subsection("Freshness SLA")
if fresh_rows:
    fmt_table(fresh_rows, ["Table", "Age", "SLA", "Status"])
    df_fresh = pd.DataFrame(fresh_rows, columns=["table_name", "age", "sla", "status"])
    display(df_fresh)
    dest = write_csv(df_fresh, "09_freshness_sla.csv")
    print(f"\n→ {dest}")
else:
    print("  No freshness_sla_hours configured or _silver_processed_at column absent.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 13 — Gold referential integrity: NULL surrogate keys in fact tables

# CELL ********************

section(f"GOLD REFERENTIAL INTEGRITY — NULL surrogate keys in fact tables  [{GOLD_LH_NAME}]")
print("A NULL surrogate key means a fact row had no matching dimension row.")
print("CRITICAL: these rows are completely INVISIBLE to any Power BI visual filtered")
print("by that dimension. RLS roles that filter through the dimension also miss them.\n")

null_sk_rows = []

for domain, domain_cfg in STAR_SCHEMA_REGISTRY.items():
    for fact_name, fact_cfg in domain_cfg.get("facts", {}).items():
        gold_path = f"{GOLD_TABLES}/{fact_cfg['gold_path']}"
        df_fact   = safe_read(gold_path)
        if df_fact is None:
            print(f"  [SKIP] {fact_cfg['gold_path']} not found in {GOLD_LH_NAME}")
            continue

        # total fact rows — count(*) once per fact table
        total = df_fact.agg(count(lit(1)).alias("n")).collect()[0]["n"]
        if total == 0:
            continue

        for join in fact_cfg.get("dimension_joins", []):
            sk_col    = join["sk_col"]
            dim_name  = join["dim"]
            threshold = join.get("max_null_sk_pct", 0.0)

            if sk_col not in df_fact.columns:
                null_sk_rows.append((domain, fact_name, dim_name, sk_col,
                                     "—", "—", f"≤{threshold:.1f}%", "⚠️  SK col absent"))
                continue

            # count(*) WHERE sk IS NULL — count(lit(1)) on filtered DF
            null_cnt = (
                df_fact.filter(col(sk_col).isNull())
                       .agg(count(lit(1)).alias("n")).collect()[0]["n"]
            )
            null_pct = round(null_cnt / total * 100, 2)

            if null_pct <= threshold:
                status = "✅ OK"
            elif null_pct <= threshold + 2.0:
                status = "⚠️  Approaching limit"
            else:
                status = "❌ Exceeds threshold"

            null_sk_rows.append((
                domain, fact_name, dim_name, sk_col,
                f"{null_cnt:,}", f"{null_pct:.2f}%",
                f"≤{threshold:.1f}%", status,
            ))

            if null_pct > threshold:
                _log("ERROR", fact_name, f"gold_null_sk_{sk_col}",
                     f"{null_cnt:,} rows ({null_pct:.2f}%) have NULL {sk_col} "
                     f"— threshold ≤{threshold:.1f}%",
                     GUIDANCE["null_sk"][1]
                     + f" Verify Silver join on '{join.get('join_col', sk_col)}'.")

if null_sk_rows:
    headers = ["Domain", "Fact Table", "Dimension", "SK Column",
               "NULL Rows", "NULL %", "Threshold", "Status"]
    fmt_table(null_sk_rows, headers)
    df_sk = pd.DataFrame(null_sk_rows, columns=headers)
    display(df_sk)
    dest = write_csv(df_sk, "10_gold_null_sk_rates.csv")
    print(f"\n→ {dest}")
else:
    print("  No Gold fact tables found or no dimension joins configured.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 14 — Power BI readiness dashboard + action plan

# CELL ********************

section("POWER BI READINESS DASHBOARD — Consolidated findings and action plan")
print("Fix all ❌ items before enabling live reports. ⚠️  items affect report accuracy.\n")

errors = [a for a in _action_log if a["severity"] == "ERROR"]
warns  = [a for a in _action_log if a["severity"] == "WARN"]

print(f"  ❌ Errors (must fix before go-live) : {len(errors)}")
print(f"  ⚠️  Warnings (affect report accuracy): {len(warns)}")
print()

# ── Per-table readiness scorecard ─────────────────────────────────────────────
score_rows = []
for table_name in TABLES:
    t_err  = sum(1 for a in _action_log if a["table"] == table_name and a["severity"] == "ERROR")
    t_warn = sum(1 for a in _action_log if a["table"] == table_name and a["severity"] == "WARN")
    ready  = "❌ NOT READY" if t_err else ("⚠️  REVIEW" if t_warn else "✅ READY")
    score_rows.append((table_name, t_err, t_warn, ready))

# include Gold fact tables in the scorecard
for gt in gold_fact_tables:
    fact_short = gt.split("/")[-1]
    t_err  = sum(1 for a in _action_log if a["table"] == fact_short and a["severity"] == "ERROR")
    t_warn = sum(1 for a in _action_log if a["table"] == fact_short and a["severity"] == "WARN")
    if t_err or t_warn:
        ready = "❌ NOT READY" if t_err else "⚠️  REVIEW"
        score_rows.append((gt, t_err, t_warn, ready))

fmt_table(score_rows, ["Table / Fact", "Errors", "Warnings", "Readiness"])
df_scores = pd.DataFrame(score_rows, columns=["table_name", "error_count", "warn_count", "readiness"])
display(df_scores)

# ── Full action plan ──────────────────────────────────────────────────────────
if _action_log:
    subsection("Action Plan — all findings ordered by severity then table")
    action_rows = sorted(_action_log, key=lambda x: (x["severity"], x["table"]))
    action_display = [
        (a["severity"], a["table"], a["check"], a["finding"][:70], a["action"][:90])
        for a in action_rows
    ]
    fmt_table(action_display,
              ["Sev", "Table", "Check", "Finding", "Recommended Action"],
              max_w=70)
    df_actions = pd.DataFrame(
        [(a["severity"], a["table"], a["check"], a["finding"], a["action"])
         for a in action_rows],
        columns=["severity", "table", "check", "finding", "action"]
    )
    display(df_actions)
    dest_act   = write_csv(df_actions, "11_action_plan.csv")
    dest_score = write_csv(df_scores,  "12_powerbi_readiness.csv")
    print(f"\n→ Action plan : {dest_act}")
    print(f"→ Readiness   : {dest_score}")
else:
    print("  ✅ No issues found across any check. Pipeline is clean and ready for Power BI.")
    write_csv(df_scores, "12_powerbi_readiness.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CELL 15 — Write master index to Gold Files

# CELL ********************

section("REPORT INDEX")

n_err  = len([a for a in _action_log if a["severity"] == "ERROR"])
n_warn = len([a for a in _action_log if a["severity"] == "WARN"])

index_lines = [
    "=" * 70,
    "  DQ INSPECTOR — RUN REPORT",
    "=" * 70,
    f"  Run timestamp : {RUN_TS}",
    f"  Source tables : {len(TABLES)}",
    f"  Gold LH       : {GOLD_LH_NAME}",
    f"  Report dir    : {REPORT_DIR}",
    "",
    "  ACTION SUMMARY",
    f"    ❌ Errors   : {n_err}  (must fix before enabling live reports)",
    f"    ⚠️  Warnings : {n_warn}  (affect report accuracy)",
    "",
    "  FILES PRODUCED",
    "    00_gold_table_discovery.csv       — Gold tables discovered dynamically",
    "    01_bronze_quarantine_summary.csv  — Bronze quarantine row counts per table",
    "    02_bronze_quarantine_<table>.csv  — Bronze quarantine rows per affected table",
    "    03_silver_quarantine_summary.csv  — Silver quarantine counts + stage breakdown",
    "    04_silver_quarantine_<table>.csv  — Silver quarantine rows per affected table",
    "    04_silver_dupes_<table>.csv       — Duplicate-quarantine rows per affected table",
    "    05_dq_stage_funnel.csv            — Row drops per stage per table (latest run)",
    "    06_rejection_rates.csv            — Rejection % vs max_rejection_pct threshold",
    "    07_biz_warn_<table>.csv           — WARN-level rows that passed to Silver",
    "    07_biz_warn_summary.csv           — Business rule warning summary",
    "    08_silver_referential_integrity   — FK orphan counts in Silver",
    "    09_fill_rate.csv                  — Column fill % vs required minimum",
    "    09_freshness_sla.csv              — Data age vs freshness SLA hours",
    "    10_gold_null_sk_rates.csv         — NULL SK rates in Gold fact tables",
    "    11_action_plan.csv                — All findings with recommended actions",
    "    12_powerbi_readiness.csv          — Per-table readiness scorecard",
    "    00_index.txt                      — This file",
    "",
    "  GUIDANCE",
    "    Bronze quarantine  → fix at source; re-run pipeline after correction",
    "    Silver quarantine  → check DQ stage funnel; trace violation to source column",
    "    Business rule WARN → validate anomalous rows with the business team",
    "    Silver FK orphans  → resolve source referential integrity before Gold rebuild",
    "    Gold NULL SKs      → rows invisible in Power BI; fix source FK or Silver join",
    "    Fill rate below %  → enrich sparse columns at source or adjust threshold",
    "    Freshness SLA      → schedule pipeline to run within the configured window",
    "=" * 70,
]

index_content = "\n".join(index_lines)
print(index_content)

mssparkutils.fs.put(f"{REPORT_DIR}/00_index.txt", index_content, True)
print(f"\n✅ All reports written to:")
print(f"   {REPORT_DIR}/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
