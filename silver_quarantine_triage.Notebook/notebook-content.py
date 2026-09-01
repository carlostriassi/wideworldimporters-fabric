# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Silver Quarantine Triage
#
# Classifies every quarantine row from a target run_id into one of four
# resolution tiers, mines actionable proposals where possible, and writes
# routed outputs that downstream actors can act on.
#
# Tiers:
#   T1  exact_duplicate                              → auto-archive (no re-insert)
#   T2  null_or_cast | ref:<name>                    → null/cast registry attribution report
#   T3  enum_fail | format_fail | statistical_outlier | range_clip:<col>
#                                                    → propose registry diff
#   T4  biz:<name> | cond:<name>                     → append to _pending_review for human review
#
# Outputs (under GoldLH/Files/_triage/<run_id>/):
#   triage_report.md
#   t3_proposed_registry_diff.py
#   source_fixes/<source>__<table>.csv  (one per group)
#
# Delta sink:
#   SilverLH/Files/_pending_review/<table>/  (T4 reviewer queue)
#
# Read-only against Silver tables. Never writes to dq_rules_registry.py,
# never modifies source CSVs, never re-inserts into Silver.

# CELL ********************

# CELL 1 — Imports + config copy (same pattern as silver_dq_dedup / inspector)
import sys, os, io, json
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, count, countDistinct, current_timestamp,
    desc, asc, when, expr,
    max as spark_max, min as spark_min,
    mean as spark_mean, stddev as spark_stddev,
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
importlib.invalidate_caches()

from workspace_config import (
    SILVER_FILES, SILVER_QUARANTINE, GOLD_FILES,
    apply_spark_settings,
)
from source_registry   import SOURCE_REGISTRY
from dq_rules_registry import DQ_RULES_REGISTRY

spark = SparkSession.builder.getOrCreate()
apply_spark_settings(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2 — Parameters

# Override these at the top of a re-run or pass via mssparkutils.notebook.run
RUN_ID       = "latest"   # "latest" picks the most recent _dq_run_id seen in quarantine
TABLE_FILTER = None       # set to a string to restrict to one table; None = all

PENDING_REVIEW_ROOT = f"{SILVER_FILES}/_pending_review"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3 — Helpers + tier classifier

def _safe_read(path: str):
    try:
        if DeltaTable.isDeltaTable(spark, path):
            return spark.read.format("delta").load(path)
    except Exception:
        pass
    return None


def _classify(dq_stage: str) -> str:
    """Map a _dq_stage value to a tier label (T1/T2/T3/T4).

    T1 — exact duplicate          → auto-archived, no action
    T2 — null_or_cast / ref:*     → null/cast registry fix (cast_rules / not_null_cols)
    T3 — range_clip / enum / fmt  → registry bounds/enum fix (t3_proposed_registry_diff.py)
    T4 — biz: / cond:             → pending human review (Resolve tab)
    """
    if dq_stage == "exact_duplicate":
        return "T1"
    if dq_stage == "null_or_cast":
        return "T2"
    if dq_stage.startswith("ref:"):
        return "T2"
    if dq_stage in {"enum_fail", "format_fail", "statistical_outlier"}:
        return "T3"
    if dq_stage.startswith("range_clip:"):
        return "T3"
    if dq_stage.startswith("biz:") or dq_stage.startswith("cond:"):
        return "T4"
    return "T?"   # unknown — surface in report so the classifier can be extended


def _quarantine_paths_for_table(table_name: str) -> list[tuple[str, str]]:
    """All quarantine sinks that may exist for one source table.
    Returns [(label, path), ...] — label distinguishes main vs _dupes vs _clipped."""
    return [
        ("main",    f"{SILVER_QUARANTINE}/{table_name}"),
        ("dupes",   f"{SILVER_QUARANTINE}/{table_name}_dupes"),
        ("clipped", f"{SILVER_QUARANTINE}/{table_name}_clipped"),
    ]


def _natural_key_expr(rules: dict):
    """Build a SQL expression that renders the natural key as a single string.
    Composite keys are pipe-joined (matches _natural_key_hash convention)."""
    keys = rules.get("dedup_key") or []
    if not keys:
        return lit(None).cast("string")
    if len(keys) == 1:
        return col(keys[0]).cast("string")
    return F.concat_ws("|", *[col(k).cast("string") for k in keys])


print("Helpers ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4 — Resolve RUN_ID + discover available quarantine

tables_to_scan = [
    t for t in DQ_RULES_REGISTRY
    if (TABLE_FILTER is None or t == TABLE_FILTER) and t in SOURCE_REGISTRY
]

# Build a union of (table_name, kind, _dq_run_id, _dq_stage, _dq_at) across all
# quarantine sinks so we can resolve "latest" and slice by run.
union_rows: list[DataFrame] = []
for t in tables_to_scan:
    for kind, path in _quarantine_paths_for_table(t):
        df = _safe_read(path)
        if df is None or "_dq_run_id" not in df.columns:
            continue
        union_rows.append(
            df.select(
                lit(t).alias("table_name"),
                lit(kind).alias("kind"),
                col("_dq_run_id"),
                col("_dq_stage"),
                col("_dq_at"),
            )
        )

if not union_rows:
    print("No quarantine data found for any registered table. Nothing to triage.")
    mssparkutils.notebook.exit(json.dumps({
        "status": "EMPTY_QUARANTINE",
        "run_id": None,
        "tables_scanned": len(tables_to_scan),
    }))

df_index = union_rows[0]
for d in union_rows[1:]:
    df_index = df_index.unionByName(d, allowMissingColumns=True)

if RUN_ID == "latest":
    latest_row = (
        df_index.select("_dq_run_id")
        .orderBy(desc("_dq_run_id"))
        .limit(1)
        .collect()
    )
    if not latest_row:
        print("Quarantine paths exist but contain no _dq_run_id rows. Exiting.")
        mssparkutils.notebook.exit(json.dumps({"status": "EMPTY_QUARANTINE"}))
    RESOLVED_RUN_ID = latest_row[0]["_dq_run_id"]
else:
    RESOLVED_RUN_ID = RUN_ID

print(f"Triaging run_id: {RESOLVED_RUN_ID}")
print(f"Tables in scope: {len(tables_to_scan)}")

REPORT_DIR = f"{GOLD_FILES}/_triage/{RESOLVED_RUN_ID}"
mssparkutils.fs.mkdirs(REPORT_DIR)
mssparkutils.fs.mkdirs(f"{REPORT_DIR}/source_fixes")
print(f"Report dir: {REPORT_DIR}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5 — Stage counts per (table, kind, stage) for the resolved run_id

stage_counts = (
    df_index
    .filter(col("_dq_run_id") == RESOLVED_RUN_ID)
    .groupBy("table_name", "kind", "_dq_stage")
    .agg(count(lit(1)).alias("row_count"))
    .orderBy("table_name", "kind", "_dq_stage")
).collect()

if not stage_counts:
    print(f"No quarantine rows for run_id={RESOLVED_RUN_ID}. Exiting.")
    mssparkutils.notebook.exit(json.dumps({
        "status": "EMPTY_RUN",
        "run_id": RESOLVED_RUN_ID,
    }))

# Aggregate by tier per table for the top summary
tier_summary: dict[tuple[str, str], int] = {}
unknown_stages: set[str] = set()
for r in stage_counts:
    tier = _classify(r["_dq_stage"])
    if tier == "T?":
        unknown_stages.add(r["_dq_stage"])
    tier_summary[(r["table_name"], tier)] = (
        tier_summary.get((r["table_name"], tier), 0) + r["row_count"]
    )

print(f"Stage groups: {len(stage_counts)}")
print(f"Tables with quarantine: {len({r['table_name'] for r in stage_counts})}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 6 — T3 rule mining: enum_fail, format_fail, range_clip, statistical_outlier
# Builds proposals the user reviews and applies to dq_rules_registry.py.

proposals: list[str] = [
    f'"""',
    f"Proposed registry changes — run_id {RESOLVED_RUN_ID}",
    f"Generated: {datetime.utcnow().isoformat()}Z",
    f"",
    f"This file is a REVIEW DOCUMENT, not a patch. Each block below shows:",
    f"  1. the current value in config/dq_rules_registry.py",
    f"  2. observation comments (counts, percentiles, sample values)",
    f"  3. one or more copy-pasteable Python snippets you can paste",
    f"     into config/dq_rules_registry.py to apply a proposal.",
    f"",
    f"Workflow:",
    f"  - Read each block",
    f"  - Decide: apply a proposal, change the source data, or do nothing",
    f"  - Paste accepted snippets into config/dq_rules_registry.py",
    f"  - Re-upload to BronzeLH/Files/config/ and re-run the pipeline",
    f'"""',
    "",
]

SECTION_BAR = "# " + "═" * 72


def _fmt_python_list(values: list, indent: str = "    ", per_line: int = 4) -> str:
    """Format a Python list with per_line entries per line, indented."""
    if not values:
        return "[]"
    quoted = [repr(v) for v in values]
    lines = ["["]
    for i in range(0, len(quoted), per_line):
        chunk = ", ".join(quoted[i:i + per_line])
        suffix = "," if i + per_line < len(quoted) else ","
        lines.append(f"{indent}{chunk}{suffix}")
    lines.append("]")
    return "\n".join(lines)


def _mine_enum(table_name: str, rules: dict, df_main: DataFrame) -> list[str]:
    out = []
    enum_rules = rules.get("enum_rules", {})
    if not enum_rules:
        return out
    df_e = df_main.filter(col("_dq_stage") == "enum_fail")
    if df_e.rdd.isEmpty():
        return out
    for c, allowed in enum_rules.items():
        if c not in df_e.columns:
            continue
        seen = (
            df_e.select(col(c).cast("string").alias("v"))
                .filter(col("v").isNotNull() & ~col("v").isin(allowed))
                .groupBy("v")
                .agg(count(lit(1)).alias("n"))
                .orderBy(desc("n"))
                .limit(20)
                .collect()
        )
        if not seen:
            continue
        new_values = [r["v"] for r in seen]
        counts     = {r["v"]: r["n"] for r in seen}

        out.append(SECTION_BAR)
        out.append(f"# {table_name}.enum_rules[{c!r}]")
        out.append(SECTION_BAR)
        out.append(f"# Current values:")
        out.append(f"#   {_fmt_python_list(list(allowed), indent='#       ')}")
        out.append(f"#")
        out.append(f"# Observed unknown values during run {RESOLVED_RUN_ID}:")
        for v in new_values:
            out.append(f"#   {v!r:30s}  count={counts[v]}")
        out.append(f"#")
        out.append(f"# ── PROPOSAL — extend enum_rules[{c!r}] with new entries below ──")
        out.append(f"# Paste this over the current entry. Each NEW line has a comment with")
        out.append(f"# its observed count — DELETE any value you do not actually want to accept.")
        out.append(f"{c!r}: [")
        existing_quoted = ", ".join(repr(v) for v in allowed)
        out.append(f"    {existing_quoted},")
        for v in new_values:
            out.append(f"    {v!r},  # NEW — observed {counts[v]}x; confirm with business")
        out.append(f"],")
        out.append("")
    return out


def _mine_format(table_name: str, rules: dict, df_main: DataFrame) -> list[str]:
    out = []
    fmt_rules = rules.get("format_rules", {})
    if not fmt_rules:
        return out
    df_f = df_main.filter(col("_dq_stage") == "format_fail")
    if df_f.rdd.isEmpty():
        return out
    for c, pattern in fmt_rules.items():
        if c not in df_f.columns:
            continue
        seen = (
            df_f.select(col(c).cast("string").alias("v"))
                .filter(col("v").isNotNull())
                .groupBy("v").agg(count(lit(1)).alias("n"))
                .orderBy(desc("n")).limit(10).collect()
        )
        if not seen:
            continue
        out.append(SECTION_BAR)
        out.append(f"# {table_name}.format_rules[{c!r}]")
        out.append(SECTION_BAR)
        out.append(f"# Current regex: {pattern!r}")
        out.append(f"#")
        out.append(f"# Top non-matching values:")
        for r in seen:
            out.append(f"#   {r['v']!r:30s}  count={r['n']}")
        out.append(f"#")
        out.append(f"# No machine-generated proposal — regex changes need human design.")
        out.append(f"# Consider: relax pattern, fix source data, or accept the legacy format")
        out.append(f"# via standardisation in silver_dq_dedup.standardise().")
        out.append("")
    return out


def _mine_statistical(table_name: str, rules: dict, df_main: DataFrame) -> list[str]:
    out = []
    stat_rules = rules.get("statistical_rules", {})
    if not stat_rules:
        return out
    df_s = df_main.filter(col("_dq_stage") == "statistical_outlier")
    if df_s.rdd.isEmpty():
        return out
    for c, params in stat_rules.items():
        if c not in df_s.columns:
            continue
        thresh = params.get("max_zscore", 4.0)
        agg = df_s.agg(
            count(lit(1)).alias("n"),
            spark_min(col(c)).alias("mn"),
            spark_max(col(c)).alias("mx"),
            spark_mean(col(c)).alias("mu"),
        ).collect()[0]
        if agg["n"] == 0:
            continue
        out.append(SECTION_BAR)
        out.append(f"# {table_name}.statistical_rules[{c!r}]")
        out.append(SECTION_BAR)
        out.append(f"# Current: {dict(params)}")
        out.append(f"#")
        out.append(f"# Observed outliers during run {RESOLVED_RUN_ID}:")
        out.append(f"#   n={agg['n']}  min={agg['mn']}  max={agg['mx']}  mean={agg['mu']}")
        out.append(f"#")
        out.append(f"# ── OPTION A — raise threshold to catch only more extreme values ──")
        out.append(f"{c!r}: {{'max_zscore': {round(thresh + 2.0, 1)}}},")
        out.append(f"")
        out.append(f"# ── OPTION B — keep threshold, route outliers to _resolutions (no change) ──")
        out.append(f"# (leave current entry as-is)")
        out.append("")
    return out


def _mine_range_clip(table_name: str, rules: dict, df_clipped: DataFrame) -> list[str]:
    """Mines _clipped sink for per-column distribution of original (pre-clip) values."""
    out = []
    if df_clipped is None:
        return out
    range_rules = rules.get("range_rules", {})
    if not range_rules:
        return out
    if "_clip_col" not in df_clipped.columns:
        return out
    for c, bounds in range_rules.items():
        if not isinstance(bounds, dict):
            continue   # legacy shape — registry contract forbids it, skip defensively
        lo, hi = bounds.get("min"), bounds.get("max")
        df_c = df_clipped.filter(col("_clip_col") == c)
        if df_c.rdd.isEmpty():
            continue
        vals = df_c.select(col("_clip_orig_value").cast("double").alias("v")).filter(col("v").isNotNull())
        if vals.rdd.isEmpty():
            continue
        agg = vals.agg(
            count(lit(1)).alias("n"),
            spark_min(col("v")).alias("mn"),
            spark_max(col("v")).alias("mx"),
            spark_mean(col("v")).alias("mu"),
        ).collect()[0]
        try:
            p95, p99 = vals.approxQuantile("v", [0.95, 0.99], 0.01)
        except Exception:
            p95, p99 = None, None

        out.append(SECTION_BAR)
        out.append(f"# {table_name}.range_rules[{c!r}]")
        out.append(SECTION_BAR)
        out.append(f"# Current bounds: ({lo}, {hi})")
        out.append(f"#")
        out.append(f"# Clipped during run {RESOLVED_RUN_ID}:")
        out.append(f"#   n={agg['n']}  observed_min={agg['mn']}  observed_max={agg['mx']}  mean={agg['mu']}")
        if p95 is not None:
            out.append(f"#   pre-clip percentiles  p95={p95}  p99={p99}")
        out.append(f"#")
        if p99 is not None:
            new_lo = round(min(lo if lo is not None else agg['mn'], agg['mn']), 4)
            new_hi = round(max(hi if hi is not None else agg['mx'], p99), 4)
            out.append(f"# ── OPTION A — widen to observed p99 (caps extreme outliers) ──")
            out.append(f"{c!r}: ({new_lo}, {new_hi}),")
            out.append(f"")
            wide_lo = round(min(lo if lo is not None else agg['mn'], agg['mn']), 4)
            wide_hi = round(agg['mx'], 4)
            out.append(f"# ── OPTION B — widen to observed min/max (accepts all observed) ──")
            out.append(f"{c!r}: ({wide_lo}, {wide_hi}),")
            out.append(f"")
        out.append(f"# ── OPTION C — leave bounds, fix at source (no change) ──")
        out.append(f"# (leave current entry as-is)")
        out.append("")
    return out


for table_name in {r["table_name"] for r in stage_counts}:
    rules = DQ_RULES_REGISTRY.get(table_name, {})
    df_main    = _safe_read(f"{SILVER_QUARANTINE}/{table_name}")
    df_clipped = _safe_read(f"{SILVER_QUARANTINE}/{table_name}_clipped")
    if df_main is not None:
        df_main = df_main.filter(col("_dq_run_id") == RESOLVED_RUN_ID)
        proposals.extend(_mine_enum(table_name, rules, df_main))
        proposals.extend(_mine_format(table_name, rules, df_main))
        proposals.extend(_mine_statistical(table_name, rules, df_main))
    if df_clipped is not None:
        df_clipped = df_clipped.filter(col("_dq_run_id") == RESOLVED_RUN_ID)
        proposals.extend(_mine_range_clip(table_name, rules, df_clipped))

proposals_text = "\n".join(proposals) + "\n"
mssparkutils.fs.put(f"{REPORT_DIR}/t3_proposed_registry_diff.py", proposals_text, True)
print(f"Wrote t3_proposed_registry_diff.py ({len(proposals)} lines)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 6b — T2 null/cast registry analysis
# For each table with null_or_cast / ref: quarantine rows, identifies which not_null_cols,
# cast_rules, and referential_rules entries are responsible and emits t2_cast_null_report.md.

def _mine_null_cast(table_name: str, rules: dict, df_main: DataFrame) -> str:
    """Mine null_or_cast T3 rows: which not_null_cols / cast_rules entries are firing."""
    df_t3 = df_main.filter(col("_dq_stage") == "null_or_cast")
    if df_t3.rdd.isEmpty():
        return ""

    total = df_t3.count()
    out_lines = [f"### null_or_cast — {table_name}  ({total} row(s))\n"]

    not_null_cols = rules.get("not_null_cols", [])
    cast_rules    = rules.get("cast_rules", {})

    table_rows = []

    for c in not_null_cols:
        if c not in df_t3.columns:
            continue
        n = df_t3.filter(col(c).isNull()).count()
        if n > 0:
            table_rows.append((
                f"`{c}`",
                "null",
                n,
                "`not_null_cols`",
                f"Remove `'{c}'` from `not_null_cols` or fix source data",
            ))

    for c, target_type in cast_rules.items():
        if c not in df_t3.columns:
            continue
        try:
            n = df_t3.filter(
                col(c).isNotNull() & col(c).cast(target_type).isNull()
            ).count()
        except Exception:
            continue
        if n > 0:
            table_rows.append((
                f"`{c}`",
                f"cast → `{target_type}`",
                n,
                "`cast_rules`",
                f"Fix source format or relax `cast_rules['{c}']` target type",
            ))

    if not table_rows:
        out_lines.append("_No column-level attribution found — quarantine rows may lack original column data._\n")
        return "\n".join(out_lines)

    table_rows.sort(key=lambda x: -x[2])
    headers = ["column", "failure", "rows", "registry entry", "suggested action"]
    out_lines.append("| " + " | ".join(headers) + " |\n")
    out_lines.append("|" + "|".join(["---"] * len(headers)) + "|\n")
    for row in table_rows:
        out_lines.append("| " + " | ".join(str(v) for v in row) + " |\n")
    out_lines.append("")
    return "\n".join(out_lines)


def _mine_ref(table_name: str, rules: dict, df_main: DataFrame) -> str:
    """Mine ref:* T3 rows: which referential_rules entries are firing and how many rows."""
    df_ref = df_main.filter(col("_dq_stage").startswith("ref:"))
    if df_ref.rdd.isEmpty():
        return ""

    total = df_ref.count()
    out_lines = [f"### referential failures — {table_name}  ({total} row(s))\n"]

    # Count rows per rule name (the part after "ref:")
    from pyspark.sql.functions import regexp_extract
    stage_counts_ref = (
        df_ref
        .groupBy("_dq_stage")
        .agg(count(lit(1)).alias("n"))
        .orderBy(col("n").desc())
        .collect()
    )

    ref_rules = rules.get("referential_rules", [])
    rule_names = {r["name"] if isinstance(r, dict) else (r[0] if isinstance(r, (list, tuple)) else r) for r in ref_rules} if ref_rules else set()

    table_rows = []
    for row in stage_counts_ref:
        rule_key = row["_dq_stage"].replace("ref:", "", 1)
        in_registry = "yes" if rule_key in rule_names else "not found — add to `referential_rules`"
        table_rows.append((
            f"`{rule_key}`",
            row["n"],
            "`referential_rules`",
            f"Fix FK source data or remove rule `'{rule_key}'` from `referential_rules`",
            in_registry,
        ))

    headers = ["rule name", "rows", "registry entry", "suggested action", "in registry?"]
    out_lines.append("| " + " | ".join(headers) + " |\n")
    out_lines.append("|" + "|".join(["---"] * len(headers)) + "|\n")
    for row in table_rows:
        out_lines.append("| " + " | ".join(str(v) for v in row) + " |\n")
    out_lines.append("")
    return "\n".join(out_lines)


t3_report_parts = [
    f"# T2 — Null / Cast / Referential Failure Registry Analysis\n",
    f"_run_id `{RESOLVED_RUN_ID}`_\n\n",
    "For each table and column, this shows how many quarantined rows fail "
    "`not_null_cols`, `cast_rules`, or `referential_rules` checks. "
    "Apply fixes to `config/dq_rules_registry.py`, "
    "then repeat Step 3 (upload config files to BronzeLH and re-run master_pipeline).\n\n",
]

t2_tables = {r["table_name"] for r in stage_counts if _classify(r["_dq_stage"]) == "T2"}
t3_any = False
for table_name in sorted(t2_tables):
    rules   = DQ_RULES_REGISTRY.get(table_name, {})
    df_main = _safe_read(f"{SILVER_QUARANTINE}/{table_name}")
    if df_main is None:
        continue
    df_main = df_main.filter(col("_dq_run_id") == RESOLVED_RUN_ID)
    for mine_fn in (_mine_null_cast, _mine_ref):
        section = mine_fn(table_name, rules, df_main)
        if section:
            t3_report_parts.append(section)
            t3_any = True

if not t3_any:
    t3_report_parts.append("_(no T2 null / cast / referential failures in this run)_\n")

mssparkutils.fs.put(
    f"{REPORT_DIR}/t2_cast_null_report.md",
    "\n".join(t3_report_parts),
    True,
)
print(f"Wrote t2_cast_null_report.md ({len(t2_tables)} table(s) with T2 rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 6c — T3 detail CSVs: downloadable export of range_clip/enum_fail/format_fail/statistical_outlier rows
# T3 rows span TWO quarantine sinks:
#   {table}_clipped  → range_clip:<col> (soft clip — row still written to Silver with adjusted value)
#   {table} main     → enum_fail, format_fail, statistical_outlier
# Cell 6 reads both when generating t3_proposed_registry_diff.py; we must do the same here.

t3_sf_written: list[tuple[str, str, int]] = []

for table_name in {r["table_name"] for r in stage_counts if _classify(r["_dq_stage"]) == "T3"}:
    dfs_t3 = []

    # enum_fail / format_fail / statistical_outlier live in the main quarantine
    df_main = _safe_read(f"{SILVER_QUARANTINE}/{table_name}")
    if df_main is not None:
        df_main = df_main.filter(col("_dq_run_id") == RESOLVED_RUN_ID)
        df_t3_main = df_main.filter(
            col("_dq_stage").isin(["enum_fail", "format_fail", "statistical_outlier"])
        )
        if not df_t3_main.rdd.isEmpty():
            dfs_t3.append(df_t3_main)

    # range_clip:<col> rows live in the _clipped sink
    df_clipped = _safe_read(f"{SILVER_QUARANTINE}/{table_name}_clipped")
    if df_clipped is not None:
        df_clipped = df_clipped.filter(col("_dq_run_id") == RESOLVED_RUN_ID)
        if not df_clipped.rdd.isEmpty():
            dfs_t3.append(df_clipped)

    if not dfs_t3:
        continue

    df_t3_exp = dfs_t3[0] if len(dfs_t3) == 1 \
                else dfs_t3[0].unionByName(dfs_t3[1], allowMissingColumns=True)

    if "_source_system" not in df_t3_exp.columns:
        df_t3_exp = df_t3_exp.withColumn("_source_system", lit("UNKNOWN"))

    sources = [r["_source_system"] for r in df_t3_exp.select("_source_system").distinct().collect()]
    for src in sources:
        src_label = (src or "UNKNOWN").replace("/", "_").replace(":", "_")
        df_one = df_t3_exp.filter(col("_source_system") == src) if src is not None \
                 else df_t3_exp.filter(col("_source_system").isNull())
        pdf = df_one.toPandas()
        if pdf.empty:
            continue
        dest = f"{REPORT_DIR}/t3_source_fixes/{src_label}__{table_name}.csv"
        buf = io.StringIO()
        pdf.to_csv(buf, index=False)
        mssparkutils.fs.put(dest, buf.getvalue(), True)
        t3_sf_written.append((src_label, table_name, len(pdf)))

print(f"T3 detail CSVs: {len(t3_sf_written)} file(s)")
for s, t, n in t3_sf_written:
    print(f"  - {s}__{t}.csv  ({n} rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 7 — T2 source-fix CSVs: one per (_source_system, table)
# Quarantine rows from Bronze carry _source_system (set by every connector).

import pandas as pd

t2_files_written: list[tuple[str, str, int]] = []  # (source_system, table_name, row_count)

for table_name in {r["table_name"] for r in stage_counts}:
    rules = DQ_RULES_REGISTRY.get(table_name, {})
    df_main = _safe_read(f"{SILVER_QUARANTINE}/{table_name}")
    if df_main is None:
        continue
    df_main = df_main.filter(col("_dq_run_id") == RESOLVED_RUN_ID)
    # T2 stages: null_or_cast + ref:<name>
    df_t2 = df_main.filter(
        (col("_dq_stage") == "null_or_cast") | col("_dq_stage").startswith("ref:")
    )
    if df_t2.rdd.isEmpty():
        continue
    nat_key = _natural_key_expr(rules).alias("natural_key")
    source_col = col("_source_system") if "_source_system" in df_t2.columns else lit("UNKNOWN").alias("_source_system")
    df_t2 = df_t2.select(
        source_col.alias("_source_system"),
        nat_key,
        col("_dq_stage"),
        col("_dq_at"),
    )
    # write one CSV per source_system
    sources = [r["_source_system"] for r in df_t2.select("_source_system").distinct().collect()]
    for src in sources:
        src_label = (src or "UNKNOWN").replace("/", "_").replace(":", "_")
        df_one = df_t2.filter(col("_source_system") == src) if src is not None \
                 else df_t2.filter(col("_source_system").isNull())
        pdf = df_one.toPandas()
        if pdf.empty:
            continue
        dest = f"{REPORT_DIR}/source_fixes/{src_label}__{table_name}.csv"
        buf = io.StringIO()
        pdf.to_csv(buf, index=False)
        mssparkutils.fs.put(dest, buf.getvalue(), True)
        t2_files_written.append((src_label, table_name, len(pdf)))

print(f"T2 source-fix CSVs: {len(t2_files_written)} file(s)")
for s, t, n in t2_files_written:
    print(f"  - {s}__{t}.csv  ({n} rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 8 — T4 pending review: append to SilverLH/Files/_pending_review/<table>/
# Each row carries the original quarantine record + a triage-side _qq_status.
# Idempotency: composite key (_dq_run_id, table_name, natural_key, _dq_stage).
# A separate resolver notebook later reads _resolutions (filled in by humans)
# and re-MERGEs approved rows back into Silver.

t4_appended: dict[str, int] = {}

for table_name in {r["table_name"] for r in stage_counts}:
    rules = DQ_RULES_REGISTRY.get(table_name, {})
    df_main = _safe_read(f"{SILVER_QUARANTINE}/{table_name}")
    if df_main is None:
        continue
    df_main = df_main.filter(col("_dq_run_id") == RESOLVED_RUN_ID)
    df_t4 = df_main.filter(
        col("_dq_stage").startswith("biz:") | col("_dq_stage").startswith("cond:")
    )
    if df_t4.rdd.isEmpty():
        continue

    nat_key_col = _natural_key_expr(rules)
    df_pending = (
        df_t4
        .withColumn("_qq_natural_key", nat_key_col)
        .withColumn("_qq_table_name",  lit(table_name))
        .withColumn("_qq_status",      lit("PENDING"))
        .withColumn("_qq_queued_at",   current_timestamp())
    )

    pending_path = f"{PENDING_REVIEW_ROOT}/{table_name}"
    if DeltaTable.isDeltaTable(spark, pending_path):
        # Idempotent: don't double-queue the same (run_id, key, stage).
        DeltaTable.forPath(spark, pending_path).alias("t").merge(
            df_pending.alias("s"),
            (
                "t._dq_run_id    = s._dq_run_id   AND "
                "t._qq_table_name = s._qq_table_name AND "
                "t._qq_natural_key = s._qq_natural_key AND "
                "t._dq_stage     = s._dq_stage"
            ),
        ).whenNotMatchedInsertAll().execute()
    else:
        df_pending.write.format("delta").mode("overwrite") \
                  .option("mergeSchema", "true").save(pending_path)

    ct = df_t4.count()
    t4_appended[table_name] = ct

print(f"T4 pending_review appends: {sum(t4_appended.values())} row(s) across {len(t4_appended)} table(s)")
for k, v in t4_appended.items():
    print(f"  - {k}: +{v}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 8b — T4 source-fix CSVs: downloadable export of biz:/cond: bad rows
# Mirrors Cell 7 but for T4 rows. Written to {REPORT_DIR}/t4_source_fixes/
# so the UI "T4 source fixes" tab can serve them as downloads.

t4_sf_written: list[tuple[str, str, int]] = []

for table_name in {r["table_name"] for r in stage_counts if _classify(r["_dq_stage"]) == "T4"}:
    rules = DQ_RULES_REGISTRY.get(table_name, {})
    df_main = _safe_read(f"{SILVER_QUARANTINE}/{table_name}")
    if df_main is None:
        continue
    df_main = df_main.filter(col("_dq_run_id") == RESOLVED_RUN_ID)
    df_t4_exp = df_main.filter(
        col("_dq_stage").startswith("biz:") | col("_dq_stage").startswith("cond:")
    )
    if df_t4_exp.rdd.isEmpty():
        continue
    nat_key = _natural_key_expr(rules).alias("natural_key")
    source_col = col("_source_system") if "_source_system" in df_t4_exp.columns else lit("UNKNOWN").alias("_source_system")
    df_t4_exp = df_t4_exp.select(
        source_col.alias("_source_system"),
        nat_key,
        col("_dq_stage"),
        col("_dq_at"),
        *[col(c) for c in df_t4_exp.columns
          if c not in {"_source_system", "_dq_stage", "_dq_at"}],
    )
    sources = [r["_source_system"] for r in df_t4_exp.select("_source_system").distinct().collect()]
    for src in sources:
        src_label = (src or "UNKNOWN").replace("/", "_").replace(":", "_")
        df_one = df_t4_exp.filter(col("_source_system") == src) if src is not None \
                 else df_t4_exp.filter(col("_source_system").isNull())
        pdf = df_one.toPandas()
        if pdf.empty:
            continue
        dest = f"{REPORT_DIR}/t4_source_fixes/{src_label}__{table_name}.csv"
        buf = io.StringIO()
        pdf.to_csv(buf, index=False)
        mssparkutils.fs.put(dest, buf.getvalue(), True)
        t4_sf_written.append((src_label, table_name, len(pdf)))

print(f"T4 source-fix CSVs: {len(t4_sf_written)} file(s)")
for s, t, n in t4_sf_written:
    print(f"  - {s}__{t}.csv  ({n} rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 9 — Write triage_report.md + exit JSON

def _md_table(headers, rows):
    if not rows:
        return "_(none)_\n"
    line = "| " + " | ".join(headers) + " |\n"
    line += "|" + "|".join(["---"] * len(headers)) + "|\n"
    for r in rows:
        line += "| " + " | ".join(str(x) for x in r) + " |\n"
    return line


lines: list[str] = []
lines.append(f"# Quarantine triage — run_id `{RESOLVED_RUN_ID}`\n")
lines.append(f"_Generated: {datetime.utcnow().isoformat()}Z_\n")
lines.append("")

# Tier summary (rows per tier per table)
table_names = sorted({r["table_name"] for r in stage_counts})
tier_rows = []
for t in table_names:
    tier_rows.append([
        t,
        tier_summary.get((t, "T1"), 0),
        tier_summary.get((t, "T2"), 0),
        tier_summary.get((t, "T3"), 0),
        tier_summary.get((t, "T4"), 0),
        tier_summary.get((t, "T?"), 0),
    ])
lines.append("## Tier summary\n")
lines.append(_md_table(
    ["table", "T1 archive", "T2 null/cast", "T3 registry", "T4 review", "T? unknown"],
    tier_rows,
))
lines.append("")

# Stage breakdown (table, kind, stage, count)
stage_rows = [
    [r["table_name"], r["kind"], r["_dq_stage"], _classify(r["_dq_stage"]), r["row_count"]]
    for r in stage_counts
]
lines.append("## Stage breakdown\n")
lines.append(_md_table(["table", "kind", "_dq_stage", "tier", "row_count"], stage_rows))
lines.append("")

# T2 source-fix CSVs
lines.append("## T2 — Source-fix CSVs\n")
if t2_files_written:
    rows = [[s, t, n] for s, t, n in t2_files_written]
    lines.append(_md_table(["source_system", "table", "rows"], rows))
    lines.append(f"\nFiles under: `{REPORT_DIR}/source_fixes/`\n")
else:
    lines.append("_(none)_\n")
lines.append("")

# T4 queue
lines.append("## T4 — Pending review\n")
if t4_appended:
    rows = [[t, c] for t, c in sorted(t4_appended.items())]
    lines.append(_md_table(["table", "rows queued"], rows))
    lines.append(f"\nReviewer queue: `{PENDING_REVIEW_ROOT}/<table>/`\n")
    lines.append("Reviewer workflow: fill in `_resolutions` for each row, then run "
                 "`silver_quarantine_resolver` notebook.\n")
else:
    lines.append("_(none)_\n")
lines.append("")

# Unknown stages — surface so the classifier can be extended
if unknown_stages:
    lines.append("## Unknown stages (extend `_classify` in this notebook)\n")
    lines.append("\n".join(f"- `{s}`" for s in sorted(unknown_stages)))
    lines.append("")

# Pointers
lines.append("## Next steps\n")
lines.append(f"1. Review `{REPORT_DIR}/t3_proposed_registry_diff.py`; apply approved diffs to `config/dq_rules_registry.py`.\n")
lines.append(f"2. Send each `{REPORT_DIR}/source_fixes/<source>__<table>.csv` to the appropriate source data steward.\n")
lines.append(f"3. For T4 rows, fill `{SILVER_FILES}/_resolutions/<table>/` and run `silver_quarantine_resolver`.\n")
lines.append("4. Re-run Bronze → Silver after fixes have landed; re-run this notebook to confirm reduction.\n")

mssparkutils.fs.put(f"{REPORT_DIR}/triage_report.md", "\n".join(lines), True)
print(f"Wrote {REPORT_DIR}/triage_report.md")

mssparkutils.notebook.exit(json.dumps({
    "status":         "SUCCESS",
    "run_id":         RESOLVED_RUN_ID,
    "tables":         len(table_names),
    "t1_rows":        sum(v for (_t, tier), v in tier_summary.items() if tier == "T1"),
    "t2_rows":        sum(v for (_t, tier), v in tier_summary.items() if tier == "T2"),
    "t3_rows":        sum(v for (_t, tier), v in tier_summary.items() if tier == "T3"),
    "t4_rows":        sum(v for (_t, tier), v in tier_summary.items() if tier == "T4"),
    "unknown_stages": sorted(unknown_stages),
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
