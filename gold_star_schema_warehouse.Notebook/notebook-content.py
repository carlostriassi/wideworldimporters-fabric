# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Gold: Star schema build — Fabric Warehouse (GoldWH)
# Parallel of gold_star_schema.Notebook. Writes physical T-SQL tables to GoldWH
# instead of Delta tables to GoldLH. Gated by workspace_config.WAREHOUSE_ENABLED
# and pipeline parameter gold_destination ('warehouse' | 'both').
#
# Use cases:
#   - HIPAA / regulated workloads needing SQL Server Audit on PHI access
#   - Heavy T-SQL aggregations against fact tables (Warehouse columnstore)
#   - T-SQL CHECK / NOT NULL constraints enforced at write time
#
# Default deployments leave WAREHOUSE_ENABLED=False and never invoke this notebook.

# CELL ********************

# CELL 0 — Install dependencies
# pyodbc is pre-installed on Fabric Spark runtime; install only if missing.
# %pip is disabled in Fabric — use notebookutils.library.install instead.
try:
    import pyodbc
except ImportError:
    notebookutils.library.install("pyodbc")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 1 — Imports, gate, pyodbc connection
import sys, os, json, logging, struct, time

# Copy config from BronzeLH to local driver filesystem
# (matches the LH notebook's cell 1 pattern — see gold_star_schema.Notebook).
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
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StringType, LongType, IntegerType, ShortType, ByteType,
    DoubleType, FloatType, DecimalType, BooleanType, DateType, TimestampType,
)

import workspace_config as _wc
from workspace_config import (
    WORKSPACE_GUID, ONELAKE_HOST, PIPELINE_TIER,
    WAREHOUSE_ENABLED, WAREHOUSE_ITEM_ID, WAREHOUSE_SQL_ENDPOINT,
    SILVER_TABLES, GOLD_TABLES, GOLD_ITEM_ID,
)

# Warehouse display name doubles as the SQL catalog. getattr keeps older
# client configs (written before GOLD_WH_NAME existed) working unchanged.
GOLD_WH_NAME = getattr(_wc, "GOLD_WH_NAME", "GoldWH")
from star_schema_registry import STAR_SCHEMA_REGISTRY, get_all_domains
from security_registry    import SECURITY_USER_SEED

spark = SparkSession.builder.getOrCreate()

log = logging.getLogger("gold_wh")
logging.basicConfig(level=logging.INFO)

# Gate 1: tier
if PIPELINE_TIER != "advanced":
    mssparkutils.notebook.exit(json.dumps({
        "status": "SKIPPED",
        "tier":   PIPELINE_TIER,
        "reason": "Gold layer requires 'advanced' tier — skipping",
    }))

# Gate 2: warehouse enabled
if not WAREHOUSE_ENABLED:
    mssparkutils.notebook.exit(json.dumps({
        "status": "SKIPPED",
        "reason": "WAREHOUSE_ENABLED is False in workspace_config — skipping GoldWH build",
    }))

if not WAREHOUSE_ITEM_ID:
    mssparkutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error":  "WAREHOUSE_ITEM_ID is empty — run scripts/bootstrap_warehouse.py first",
    }))

# Resolve SQL endpoint
_endpoint = WAREHOUSE_SQL_ENDPOINT or f"{WORKSPACE_GUID}-{WAREHOUSE_ITEM_ID}.datawarehouse.fabric.microsoft.com"

import pyodbc

def _wh_token() -> bytes:
    """Get Entra token for SQL resource, packed for ODBC SQL_COPT_SS_ACCESS_TOKEN."""
    token = mssparkutils.credentials.getToken("https://database.windows.net")
    tok_bytes = token.encode("utf-8") if isinstance(token, str) else token
    enc = b""
    for c in tok_bytes:
        enc += bytes([c]) + bytes(1)
    return struct.pack("=i", len(enc)) + enc

def _warehouse_conn():
    """
    Returns a pyodbc connection to GoldWH using an Entra-issued access token.
    Reopened per call so cells can recover from token expiry without manual
    intervention.
    """
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{_endpoint},1433;"
        "Encrypt=yes;TrustServerCertificate=no;"
        f"Database={GOLD_WH_NAME};"
    )
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: _wh_token()})

def _exec(sql: str, params=None):
    """Execute a single statement; commits and returns affected rowcount (or -1)."""
    with _warehouse_conn() as conn:
        cur = conn.cursor()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        rc = cur.rowcount
        conn.commit()
        return rc

def _query(sql: str, params=None) -> list:
    """Execute a query; returns list of tuples."""
    with _warehouse_conn() as conn:
        cur = conn.cursor()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur.fetchall()

def _insert_df_to_wh(df, wh_table: str):
    """Batch-insert a PySpark DataFrame into a WH physical table via pyodbc executemany."""
    import pandas as pd
    pdf  = df.toPandas()
    pdf  = pdf.where(pd.notna(pdf), other=None)
    cols = list(pdf.columns)
    sql  = (f"INSERT INTO {wh_table} ({', '.join(f'[{c}]' for c in cols)}) "
            f"VALUES ({', '.join('?' * len(cols))})")
    rows = [tuple(r) for r in pdf.itertuples(index=False, name=None)]
    with _warehouse_conn() as conn:
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(sql, rows)
        conn.commit()
    log.info(f"[GOLD-WH] {len(rows)} rows → {wh_table}")

# Quick connectivity check — fails fast if the warehouse is unreachable
try:
    _query("SELECT 1")
    log.info(f"[GOLD-WH] Connected to {_endpoint}")
except Exception as exc:
    mssparkutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error":  f"Cannot reach GoldWH endpoint {_endpoint}: {exc}",
    }))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 1b — Security user dimension (shared.dim_security_user)
# T-SQL equivalent of build_dim_security_user() in the Lakehouse notebook.
# Attribute columns are discovered dynamically from SECURITY_USER_SEED so a
# new attribute requires only a registry change — no notebook edit.

_FIXED_CORE_COLS = ["user_principal_name", "display_name", "domain", "role"]
_FIXED_TAIL_COLS = ["is_active"]
_RESERVED_COLS   = set(_FIXED_CORE_COLS) | set(_FIXED_TAIL_COLS) | {
    "user_sk", "_created_at", "_updated_at"
}

def _infer_sql_type(col_name, seed):
    for u in seed:
        v = u.get(col_name)
        if v is None:
            continue
        if isinstance(v, bool):
            return "BIT"
        if isinstance(v, int):
            return "INT"
        return "VARCHAR(100)"
    return "VARCHAR(100)"

def _discover_attr_cols(seed):
    seen, out = set(_RESERVED_COLS), []
    for u in seed:
        for k in u.keys():
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out

def build_dim_security_user_wh():
    if not SECURITY_USER_SEED:
        log.info("[GOLD-WH] dim_security_user: SECURITY_USER_SEED empty — skipping")
        return

    attr_cols = _discover_attr_cols(SECURITY_USER_SEED)
    attr_ddl  = ",\n    ".join(
        f"[{c}] {_infer_sql_type(c, SECURITY_USER_SEED)} NULL" for c in attr_cols
    )

    _exec("IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'shared') EXEC('CREATE SCHEMA [shared]');")

    # Drop the shortcut-backed view from security_ddl.sql if present, since
    # GoldWH physical mode replaces it with a real table.
    _exec("""
        IF OBJECT_ID('shared.dim_security_user', 'V') IS NOT NULL
            DROP VIEW shared.dim_security_user;
    """)

    _exec(f"""
        IF OBJECT_ID('shared.dim_security_user', 'U') IS NULL
        CREATE TABLE shared.dim_security_user (
            user_sk             BIGINT IDENTITY NOT NULL,
            user_principal_name VARCHAR(255) NOT NULL,
            display_name        VARCHAR(255) NOT NULL,
            [domain]            VARCHAR(50)  NOT NULL,
            [role]              VARCHAR(100) NOT NULL,
            {attr_ddl},
            is_active           BIT           NOT NULL,
            _created_at         DATETIME2(6)     NOT NULL,
            _updated_at         DATETIME2(6)     NOT NULL
        );
    """)

    # Build inline VALUES rows — avoids temp tables (not supported across execute() calls
    # in Fabric Warehouse). SECURITY_USER_SEED is always small so this is safe.
    def _sql_lit(v):
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, int):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"

    value_rows = []
    for u in SECURITY_USER_SEED:
        vals = [
            _sql_lit(u["user_principal_name"]),
            _sql_lit(u["display_name"]),
            _sql_lit(u["domain"]),
            _sql_lit(u["role"]),
            *[_sql_lit(u.get(c)) for c in attr_cols],
            _sql_lit(bool(u.get("is_active", True))),
        ]
        value_rows.append(f"        ({', '.join(vals)})")

    values_sql      = ",\n".join(value_rows)
    alias_cols      = ", ".join(
        ["user_principal_name", "display_name", "[domain]", "[role]"]
        + [f"[{c}]" for c in attr_cols]
        + ["is_active"]
    )
    attr_update     = ",\n            ".join(
        [f"t.[{c}] = s.[{c}]" for c in attr_cols]
    ) or "t.is_active = s.is_active"
    insert_attr_cols = (", " + ", ".join(f"[{c}]" for c in attr_cols)) if attr_cols else ""
    insert_attr_vals = (", " + ", ".join(f"s.[{c}]" for c in attr_cols)) if attr_cols else ""

    with _warehouse_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            MERGE shared.dim_security_user AS t
            USING (
                VALUES
{values_sql}
            ) AS s({alias_cols})
              ON t.user_principal_name = s.user_principal_name AND t.[domain] = s.[domain]
            WHEN MATCHED THEN UPDATE SET
                t.display_name = s.display_name,
                t.[role]       = s.[role],
                {attr_update},
                t.is_active    = s.is_active,
                t._updated_at  = SYSUTCDATETIME()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (user_principal_name, display_name, [domain], [role]{insert_attr_cols}, is_active, _created_at, _updated_at)
                VALUES (s.user_principal_name, s.display_name, s.[domain], s.[role]{insert_attr_vals}, s.is_active, SYSUTCDATETIME(), SYSUTCDATETIME());
        """)
        conn.commit()

    n = _query("SELECT COUNT(*) FROM shared.dim_security_user")[0][0]
    log.info(f"[GOLD-WH] dim_security_user: {n} rows after MERGE")

build_dim_security_user_wh()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2 — Date dimension generator (shared across all domains)
# T-SQL equivalent of build_dim_date() using a recursive CTE. The CTE is
# capped by MAXRECURSION to support multi-decade ranges (Fabric Warehouse
# allows up to 32767, set to 32767 to cover ~90 years).

def build_dim_date_wh(cfg, domain: str):
    import datetime as _dt
    schema     = domain
    table      = cfg["gold_path"].split("/")[-1]   # e.g. "dim_date"
    start_date = cfg["start_date"]
    end_date   = cfg["end_date"]
    print(f"[dim_date] START  schema={schema} table={table} range={start_date} → {end_date}")

    print("[dim_date] dropping view if present …")
    _exec(f"IF OBJECT_ID('{schema}.{table}', 'V') IS NOT NULL DROP VIEW {schema}.{table};")

    print("[dim_date] dropping + creating table …")
    _exec(f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL DROP TABLE {schema}.{table};")
    _exec(f"""
        CREATE TABLE {schema}.{table} (
            date_key      INT          NOT NULL,
            full_date     DATE         NULL,
            [year]        INT          NOT NULL,
            [quarter]     INT          NOT NULL,
            month_num     INT          NOT NULL,
            month_name    VARCHAR(20) NOT NULL,
            day_of_month  INT          NOT NULL,
            day_of_week   INT          NOT NULL,
            day_name      VARCHAR(20) NOT NULL,
            is_weekend    BIT          NOT NULL
        );
    """)
    print("[dim_date] table created OK")

    # Generate rows in Python — Fabric Warehouse does not support OPTION(MAXRECURSION).
    # Pass full_date as an ISO string (not datetime.date) to avoid pyodbc
    # executemany hanging on DATE-typed parameters over the Fabric ODBC driver.
    start = _dt.date.fromisoformat(start_date)
    end   = _dt.date.fromisoformat(end_date)
    delta = (end - start).days
    print(f"[dim_date] generating {delta + 1} date rows in Python …")
    rows  = []
    for i in range(delta + 1):
        d      = start + _dt.timedelta(days=i)
        py_wd  = d.weekday()           # 0=Mon … 6=Sun
        sql_wd = 1 + (py_wd + 1) % 7  # 1=Sun … 7=Sat (SQL DATEFIRST 7 default)
        rows.append((
            int(d.strftime("%Y%m%d")),
            d.isoformat(),             # string avoids DATE-type hang in pyodbc
            d.year,
            (d.month - 1) // 3 + 1,
            d.month,
            d.strftime("%B"),
            d.day,
            sql_wd,
            d.strftime("%A"),
            1 if py_wd >= 5 else 0,    # Sat=5, Sun=6
        ))

    # Sentinel row for unknown dates (date_key = -1) — matches LH notebook
    rows.append((-1, None, -1, -1, -1, "Unknown", -1, -1, "Unknown", 0))
    print(f"[dim_date] {len(rows)} rows built (including sentinel); building SQL literals …")

    def _dv(v):
        """Render a Python value as a T-SQL literal — no parameter binding needed."""
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, int):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"

    insert_prefix = (
        f"INSERT INTO {schema}.{table} "
        "(date_key,full_date,[year],[quarter],month_num,month_name,"
        "day_of_month,day_of_week,day_name,is_weekend) VALUES "
    )
    batch_size = 500
    total_batches = (len(rows) + batch_size - 1) // batch_size

    with _warehouse_conn() as conn:
        print("[dim_date] connection acquired; inserting via SQL literals …")
        cur = conn.cursor()
        for b in range(total_batches):
            batch = rows[b * batch_size : (b + 1) * batch_size]
            value_clauses = ",".join(
                "(" + ",".join(_dv(c) for c in row) + ")" for row in batch
            )
            cur.execute(insert_prefix + value_clauses)
            print(f"[dim_date]   batch {b + 1}/{total_batches} inserted ({len(batch)} rows)")
        conn.commit()
    print("[dim_date] commit OK")

    n = _query(f"SELECT COUNT(*) FROM {schema}.{table}")[0][0]
    log.info(f"[GOLD-WH] {schema}.{table}: {n} rows (includes date_key=-1 sentinel)")
    print(f"[dim_date] DONE  {n} rows in {schema}.{table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3 — Dimension builder (type1 and scd2)
# Reads Silver via cross-database query against the Silver shortcut schema
# (assumes Silver tables are shortcutted into GoldWH under SilverLH.<domain>).

def _spark_to_sql_type(spark_type) -> str:
    """Map a PySpark DataType to the narrowest appropriate T-SQL type."""
    if isinstance(spark_type, (LongType,)):
        return "BIGINT"
    if isinstance(spark_type, (IntegerType, ShortType, ByteType)):
        return "INT"
    if isinstance(spark_type, DecimalType):
        return f"DECIMAL({spark_type.precision},{spark_type.scale})"
    if isinstance(spark_type, (DoubleType, FloatType)):
        return "FLOAT"
    if isinstance(spark_type, BooleanType):
        return "BIT"
    if isinstance(spark_type, DateType):
        return "DATE"
    if isinstance(spark_type, TimestampType):
        return "DATETIME2(6)"
    return "VARCHAR(255)"

def _silver_col_types(silver_source) -> dict:
    """
    Returns {col_name: sql_type_str} by reading the Silver Delta schema.
    Accepts a single path string, or a list of two sources (multi-source facts) —
    the second entry may be a plain path string or a {"join": path, "on", "how"}
    dict (see build_fact_wh's two-table join handling). When two sources are
    provided, the first non-StringType wins on conflicts.
    """
    sources = silver_source if isinstance(silver_source, list) else [silver_source]
    sources = [s["join"] if isinstance(s, dict) else s for s in sources]
    result = {}
    for src in sources:
        schema = spark.read.format("delta").load(f"{SILVER_TABLES}/{src}").schema
        for f in schema.fields:
            if f.name not in result:
                result[f.name] = _spark_to_sql_type(f.dataType)
    return result

def _apply_masked_columns_wh(schema: str, table: str, dim_cfg: dict):
    """
    T-SQL equivalent of _apply_masked_columns() in the LH notebook.
    For each entry in dim_cfg['masked_columns'] {col_name: sql_expr}:
      1. ADD the column if it doesn't exist (VARCHAR(255) — display-only).
      2. UPDATE every row with the expression (full table scan, acceptable for dims).
    """
    masked = dim_cfg.get("masked_columns")
    if not masked:
        return
    existing_cols = {
        row[0].lower()
        for row in _query(
            f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
        )
    }
    for col_name, sql_expr in masked.items():
        if col_name.lower() not in existing_cols:
            _exec(f"ALTER TABLE {schema}.{table} ADD [{col_name}] VARCHAR(255) NULL;")
        _exec(f"UPDATE {schema}.{table} SET [{col_name}] = {sql_expr};")
    log.info(f"[GOLD-WH] {schema}.{table}: masked columns applied: {list(masked)}")

def resolve_columns(columns_cfg, exclude=()):
    """Same as LH version — returns (source_name, target_name) pairs."""
    exclude = set(exclude)
    pairs = []
    for c in columns_cfg:
        if isinstance(c, dict):
            src, tgt = c["source"], c["target"]
        else:
            src = tgt = c
        if src not in exclude and tgt not in exclude:
            pairs.append((src, tgt))
    return pairs

def _silver_three_part(silver_source: str) -> str:
    """
    Converts a silver_source like 'healthcare/healthcare_patients' to a
    three-part SQL name: 'SilverLH.healthcare.healthcare_patients'.
    Assumes Silver shortcuts exist in GoldWH under the SilverLH database name.
    """
    domain, table = silver_source.split("/", 1)
    return f"SilverLH.{domain}.{table}"

def _drop_view_if_exists(schema: str, table: str):
    _exec(f"IF OBJECT_ID('{schema}.{table}', 'V') IS NOT NULL DROP VIEW {schema}.{table};")

def _drop_dependent_policies(schema: str, table: str):
    """Drop RLS security policies referencing this table so DROP TABLE can proceed.
    The security notebook recreates them on its next run."""
    rows = _query(f"""
        SELECT QUOTENAME(OBJECT_SCHEMA_NAME(sp.object_id))
               + '.' + QUOTENAME(OBJECT_NAME(sp.object_id))
        FROM sys.security_policies AS sp
        INNER JOIN sys.security_predicates AS pred ON pred.object_id = sp.object_id
        INNER JOIN sys.tables AS t ON t.object_id = pred.target_object_id
        WHERE SCHEMA_NAME(t.schema_id) = '{schema}' AND t.name = '{table}'
    """)
    for (pol_name,) in rows:
        _exec(f"DROP SECURITY POLICY {pol_name};")
        log.info(f"[GOLD-WH] dropped security policy {pol_name} before rebuilding {schema}.{table}")

def build_dimension_wh(dim_name: str, dim_cfg: dict, domain: str):
    dim_type = dim_cfg["type"]
    schema   = domain
    table    = dim_cfg["gold_path"].split("/")[-1]

    if dim_type == "generated":
        build_dim_date_wh(dim_cfg, domain)
        return table

    if dim_type == "static":
        _drop_view_if_exists(schema, table)
        # Caller is expected to define static_rows + columns shape; for
        # parity with the LH notebook we keep this minimal — extend if a
        # registry adopts static dims.
        raise NotImplementedError("Static dimensions are not yet implemented for GoldWH.")

    sk_col = dim_cfg["surrogate_key"]
    nk_col = dim_cfg["natural_key"]
    silver = _silver_three_part(dim_cfg["silver_source"])

    col_pairs    = resolve_columns(dim_cfg["columns"], exclude={sk_col})
    src_cols_sql = ",\n            ".join(f"[{s}] AS [{t}]" for s, t in col_pairs)
    silver_types = _silver_col_types(dim_cfg["silver_source"])
    tgt_col_defs = ",\n            ".join(
        f"[{t}] {silver_types.get(s, 'VARCHAR(255)')} NULL" for s, t in col_pairs
    )
    tgt_col_list = ", ".join(f"[{t}]" for _, t in col_pairs)

    _drop_view_if_exists(schema, table)

    if dim_type == "type1":
        _drop_view_if_exists(schema, table)
        _drop_dependent_policies(schema, table)
        _exec(f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL DROP TABLE {schema}.{table};")
        _exec(f"""
            CREATE TABLE {schema}.{table} (
                {sk_col} BIGINT NOT NULL,
                {tgt_col_defs}
            );
        """)
        # Read from GoldLH — hash-based SKs already assigned by gold_star_schema.Notebook.
        # Avoids SK drift vs LH fact tables that re-deriving from Silver would cause.
        df_gold   = spark.read.format("delta").load(f"{GOLD_TABLES}/{dim_cfg['gold_path']}")
        gold_cols = [sk_col] + [t for _, t in col_pairs]
        _insert_df_to_wh(df_gold.select(gold_cols), f"{schema}.{table}")
        n = _query(f"SELECT COUNT(*) FROM {schema}.{table}")[0][0]
        log.info(f"[GOLD-WH] {schema}.{table}: {n} rows (type1)")
        _apply_masked_columns_wh(schema, table, dim_cfg)
        return table

    if dim_type == "scd2":
        _drop_view_if_exists(schema, table)
        _drop_dependent_policies(schema, table)
        _exec(f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL DROP TABLE {schema}.{table};")
        _exec(f"""
            CREATE TABLE {schema}.{table} (
                {sk_col} BIGINT NOT NULL,
                {tgt_col_defs},
                [_scd2_hash]     VARCHAR(64) NULL,
                [eff_start_date] DATE        NULL,
                [eff_end_date]   DATE        NULL,
                [is_current]     BIT         NULL
            );
        """)
        # GoldLH is authoritative for SCD2 history and hash-based SKs.
        # Mirror the full LH SCD2 table (all versions) into WH to ensure SK alignment.
        df_gold    = spark.read.format("delta").load(f"{GOLD_TABLES}/{dim_cfg['gold_path']}")
        scd2_extra = ["_scd2_hash", "eff_start_date", "eff_end_date", "is_current"]
        gold_cols  = [sk_col] + [t for _, t in col_pairs] + [c for c in scd2_extra if c in df_gold.columns]
        _insert_df_to_wh(df_gold.select(gold_cols), f"{schema}.{table}")
        n = _query(f"SELECT COUNT(*) FROM {schema}.{table}")[0][0]
        log.info(f"[GOLD-WH] {schema}.{table}: {n} rows (scd2)")
        _apply_masked_columns_wh(schema, table, dim_cfg)
        return table

    raise ValueError(f"Unknown dimension type: {dim_type}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4 — Fact builder (with RI gate matching LH semantics)
_gold_wh_ri_warnings = []  # written to _triage/gold_warnings.json at run end (mirrors LH notebook)

def _resolve_join_source_wh(domain: str, dim_name: str) -> dict:
    """Return the registry entry for a dimension_joins target, whether it is a
    dimension or another fact referenced as a degenerate dimension. Mirrors
    _resolve_join_source() in gold_star_schema.Notebook."""
    dom = STAR_SCHEMA_REGISTRY[domain]
    return dom.get("dimensions", {}).get(dim_name) or dom.get("facts", {}).get(dim_name)


def build_fact_wh(fact_name: str, fact_cfg: dict, domain: str, built_dims: set):
    schema      = domain
    table       = fact_cfg["gold_path"].split("/")[-1]
    sources     = fact_cfg["silver_source"]
    date_source = fact_cfg["date_key_source"]
    sk_col      = fact_cfg["surrogate_key"]
    nk_col      = fact_cfg["natural_key"]
    derived     = fact_cfg.get("derived_cols", {})
    fact_pairs  = resolve_columns(fact_cfg["columns"])

    # Every dimension_joins target must already be built: the SELECT list below
    # emits `<alias>.[sk] AS [...]` for each join unconditionally, so skipping an
    # unbuilt target leaves an unbound alias and fails with an opaque SQL binding
    # error. Fail here with the actual cause instead (mirrors the LH notebook).
    for _dj in fact_cfg["dimension_joins"]:
        if _dj["dim"] not in built_dims:
            raise ValueError(
                f'[GOLD-WH] {fact_name}: dimension_joins target "{_dj["dim"]}" has not been '
                f'built. Built so far: {sorted(built_dims)}. If "{_dj["dim"]}" is a fact used '
                f'as a degenerate dimension, move it BEFORE "{fact_name}" in the domain\'s '
                f'"facts" dict in star_schema_registry.py — facts build in insertion order.'
            )

    # Pre-read Silver schema for typed DDL; also pull parent_join types if present
    silver_types = _silver_col_types(sources)
    if "parent_join" in fact_cfg:
        pj_types = _silver_col_types(fact_cfg["parent_join"]["silver_path"])
        silver_types.update({k: v for k, v in pj_types.items() if k not in silver_types})
    _sk_cols = {dj["sk_col"] for dj in fact_cfg["dimension_joins"]}

    def _fact_col_sql_type(src_name: str, tgt_name: str) -> str:
        if tgt_name == "date_key":
            return "INT"
        if tgt_name in _sk_cols:
            return "BIGINT"  # dim SK from LEFT JOIN — always BIGINT, nullable
        if tgt_name in derived:
            return "VARCHAR(255)"  # derived expression — type not inferable from Silver
        return silver_types.get(src_name, "VARCHAR(255)")

    # Resolve source: single Silver table, two-table join, or parent_join carry
    if isinstance(sources, list):
        second = sources[1]
        if isinstance(second, dict):
            # {"join": "<silver_path>", "on": "<child_join_col>",
            #  "right_on": "<parent_join_col>" (optional, defaults to "on"),
            #  "how": "<left|inner|...>"}
            # Mirrors gold_star_schema.Notebook's two-table join spec. right_on
            # covers the case where the parent's key column isn't named the same
            # as the child's FK column.
            b_path   = second["join"]
            join_key = second["on"]
            right_on = second.get("right_on", join_key)
            how_sql  = {"left": "LEFT", "inner": "INNER"}.get(second.get("how", "left"), "LEFT")
        else:
            # Legacy shape: plain path string + top-level join_key, inner join only.
            b_path   = second
            join_key = fact_cfg["join_key"]
            right_on = join_key
            how_sql  = "INNER"
        a_path = sources[0]
        a = _silver_three_part(a_path)
        b = _silver_three_part(b_path)
        # T-SQL has no equivalent of Spark's join(on=...) column-dedup — a bare
        # SELECT * keeps both a.[join_key] and b.[right_on] as separate output
        # columns of the same name and 8156s on SELECT * INTO. Build an explicit
        # column list instead: child (a) wins on any name collision (mirrors
        # dup_in_b in gold_star_schema.Notebook), and right_on is dropped from b
        # entirely since a's join_key already carries that value.
        a_cols = [f.name for f in spark.read.format("delta").load(f"{SILVER_TABLES}/{a_path}").schema.fields]
        b_cols = [f.name for f in spark.read.format("delta").load(f"{SILVER_TABLES}/{b_path}").schema.fields]
        dup_in_b = (set(a_cols) & set(b_cols)) - {right_on}
        b_select_cols = [c for c in b_cols if c not in dup_in_b and c != right_on]
        select_list = ", ".join(f"a.[{c}]" for c in a_cols)
        if b_select_cols:
            select_list += ", " + ", ".join(f"b.[{c}]" for c in b_select_cols)
        src_sql = f"{a} AS a {how_sql} JOIN {b} AS b ON a.[{join_key}] = b.[{right_on}]"
        src_alias = f"(SELECT {select_list} FROM {src_sql}) AS src"
    elif "parent_join" in fact_cfg:
        # parent_join: child fact has no direct date/FK column — carry them from
        # a parent Silver table. Only the join key + declared carry_cols are
        # projected from the parent to avoid ambiguous column names.
        pj        = fact_cfg["parent_join"]
        pj_key    = pj["join_key"]
        carry     = pj["carry_cols"]
        child     = _silver_three_part(sources)
        parent    = _silver_three_part(pj["silver_path"])
        carry_sql = ", ".join(f"p.[{c}]" for c in carry)
        src_alias = (
            f"(SELECT c.*, {carry_sql} "
            f"FROM {child} AS c "
            f"LEFT JOIN {parent} AS p ON c.[{pj_key}] = p.[{pj_key}]) AS src"
        )
    else:
        src_alias = f"{_silver_three_part(sources)} AS src"

    # Build SELECT list with derived columns and dim SK lookups
    select_parts = []
    join_clauses = []
    ri_checks    = []

    for s, t in fact_pairs:
        if t == "date_key":
            # Computed from date_source — coalesce to -1 sentinel
            select_parts.append(
                f"COALESCE(CAST(FORMAT(src.[{date_source}], 'yyyyMMdd') AS INT), -1) AS [date_key]"
            )
            continue
        if t == sk_col:
            # Hash-based surrogate key — mirrors _stable_sk() in the LH notebook.
            # ABS(first 8 bytes of SHA-256(nk) cast to BIGINT) is deterministic across runs.
            select_parts.append(
                f"ABS(CAST(SUBSTRING(HASHBYTES('SHA2_256', "
                f"ISNULL(CAST(src.[{nk_col}] AS VARCHAR(255)), '__null__')), 1, 8) AS BIGINT))"
                f" AS [{sk_col}]"
            )
            continue
        if t in derived:
            select_parts.append(f"({derived[t]}) AS [{t}]")
            continue

        # Look up if this target is a dim SK from one of the joins
        sk_join = None
        for dim_join in fact_cfg["dimension_joins"]:
            if dim_join["sk_col"] == t:
                sk_join = dim_join
                break
        if sk_join:
            _dkey = sk_join['dim']
            dim_alias = _dkey if _dkey.startswith("dim_") else f"dim_{_dkey}"
            # Resolve the SK column as it actually exists on the referenced table,
            # then alias it to the registry's sk_col. A dimension usually exposes
            # sk_col verbatim; a fact referenced as a degenerate dimension exposes
            # ITS OWN surrogate_key (e.g. fact_encounters_sk) rather than the
            # analytics-friendly name the referencing fact wants (encounter_sk).
            _src_cfg = _resolve_join_source_wh(domain, _dkey) or {}
            _src_sk  = _src_cfg.get("surrogate_key", sk_join["sk_col"])
            select_parts.append(f"{dim_alias}.[{_src_sk}] AS [{t}]")
            continue

        # Plain source column passthrough
        select_parts.append(f"src.[{s}] AS [{t}]")

    # Build LEFT JOIN clauses for each dimension
    for dim_join in fact_cfg["dimension_joins"]:
        dim_name      = dim_join["dim"]
        join_col      = dim_join["join_col"]
        dim_join_col  = dim_join.get("dim_join_col", join_col)
        sk            = dim_join["sk_col"]
        # Resolve from dimensions OR facts — a degenerate-dimension target is a
        # fact, so indexing ["dimensions"] directly raises KeyError on its name.
        _src_cfg      = _resolve_join_source_wh(domain, dim_name)
        if _src_cfg is None:
            raise ValueError(
                f'[GOLD-WH] {fact_name}: dimension_joins target "{dim_name}" is not defined '
                f'as a dimension or a fact in domain "{domain}" of star_schema_registry.py.'
            )
        dim_table     = _src_cfg["gold_path"].split("/")[-1]
        dim_alias     = dim_name if dim_name.startswith("dim_") else f"dim_{dim_name}"
        # Facts carry no "type" key — only SCD2 dimensions need the is_current filter.
        _dim_type_wh = _src_cfg.get("type")
        _scd2_filter = f" AND {dim_alias}.[is_current] = 1" if _dim_type_wh == "scd2" else ""
        if join_col == "date_key":
            # date_key is computed from date_source in the fact; LEFT JOIN on the computed expression
            join_expr = f"COALESCE(CAST(FORMAT(src.[{date_source}], 'yyyyMMdd') AS INT), -1)"
            join_clauses.append(
                f"LEFT JOIN {schema}.{dim_table} AS {dim_alias} "
                f"ON {dim_alias}.[{dim_join_col}] = {join_expr}{_scd2_filter}"
            )
        else:
            join_clauses.append(
                f"LEFT JOIN {schema}.{dim_table} AS {dim_alias} "
                f"ON {dim_alias}.[{dim_join_col}] = src.[{join_col}]{_scd2_filter}"
            )
        max_pct = dim_join.get("max_null_sk_pct")
        if max_pct is not None:
            ri_checks.append((dim_name, sk, max_pct, dim_alias))

    # Drop view shortcut if it exists
    _drop_view_if_exists(schema, table)

    select_clause = ",\n            ".join(select_parts)
    join_clause   = "\n        ".join(join_clauses) if join_clauses else ""

    # Use a permanent staging table — Fabric Warehouse does not support #temp tables
    # across separate execute() calls.  Named with a leading underscore so it is
    # easy to identify as transient.  Dropped in a finally block so it is always
    # cleaned up even when an RI gate raises.
    stage_table = f"{schema}._{table}_stage"

    target_col_defs = ",\n            ".join(
        f"[{t}] BIGINT NOT NULL" if t == sk_col else f"[{t}] {_fact_col_sql_type(s, t)} NULL"
        for s, t in fact_pairs
    )
    target_col_list = ", ".join(f"[{t}]" for _, t in fact_pairs)

    # Drop any remnant from a prior failed run
    _exec(f"IF OBJECT_ID('{stage_table}', 'U') IS NOT NULL DROP TABLE {stage_table};")

    # Materialise the joined Silver → dim SK result into the permanent staging table
    _exec(f"""
        SELECT
            {select_clause}
        INTO {stage_table}
        FROM {src_alias}
        {join_clause};
    """)

    try:
        # RI gate — count NULL SKs per joined dim, fail if over threshold
        total = _query(f"SELECT COUNT(*) FROM {stage_table}")[0][0]
        for dim_name, sk, max_pct, _ in ri_checks:
            nulls    = _query(f"SELECT COUNT(*) FROM {stage_table} WHERE [{sk}] IS NULL")[0][0]
            null_pct = (nulls / total * 100) if total else 0.0
            msg = f"[GOLD-WH] {fact_name} → {dim_name}: {nulls}/{total} NULL {sk} ({null_pct:.1f}%)"
            if null_pct > max_pct:
                raise ValueError(
                    f"{msg} — exceeds max_null_sk_pct={max_pct}. "
                    f"Fix referential integrity in source before re-running."
                )
            if nulls > 0:
                log.warning(msg)
                _gold_wh_ri_warnings.append({
                    "table":                      fact_name,
                    "dimension":                  dim_name,
                    "null_sk_count":              nulls,
                    "null_sk_pct":                round(null_pct, 4),
                    "configured_max_null_sk_pct": float(max_pct),
                    "status":                     "WARNING",
                })
            else:
                log.info(msg)

        # Create fact table + CCI if it doesn't exist yet; migrate schema if it does
        table_exists = bool(_query(
            f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
        ))
        if not table_exists:
            _exec(f"""
                CREATE TABLE {schema}.{table} (
                    {target_col_defs}
                );
            """)
            # Fabric Warehouse creates all tables as Clustered Columnstore by default — no explicit CCI needed.
        else:
            # Schema migration: add columns present in the registry but absent from the warehouse table.
            # Triggered when columns are added to the fact's 'columns' list after the table was first created.
            existing_cols = {
                r[0].lower()
                for r in _query(
                    f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
                )
            }
            for s, t in fact_pairs:
                if t.lower() not in existing_cols:
                    # Always NULL in ALTER TABLE — SQL Server rejects NOT NULL without a DEFAULT on tables with rows.
                    sql_type = f"{_fact_col_sql_type(s, t)} NULL"
                    _exec(f"ALTER TABLE {schema}.{table} ADD [{t}] {sql_type};")
                    log.info(f"[GOLD-WH] {fact_name}: schema migration — added column [{t}] {sql_type}")

        # Idempotent upsert — inserts new rows and repairs all SK columns for existing rows.
        # (migration-aware: first run fixes stale hash/monotonically_increasing_id values;
        #  hash is deterministic so the WHEN MATCHED condition is false on subsequent runs)
        _all_sk_list    = [sk_col] + [dj["sk_col"] for dj in fact_cfg["dimension_joins"]]
        _update_set_sql = ",\n                ".join(f"t.[{c}] = s.[{c}]" for c in _all_sk_list)
        _sk_changed     = " OR ".join(
            f"ISNULL(t.[{c}], -1) <> ISNULL(s.[{c}], -1)" for c in _all_sk_list
        )
        _exec(f"""
            MERGE {schema}.{table} AS t
            USING {stage_table} AS s
              ON t.[{nk_col}] = s.[{nk_col}]
            WHEN MATCHED AND ({_sk_changed}) THEN
                UPDATE SET {_update_set_sql}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({target_col_list})
                VALUES ({", ".join(f"s.[{t}]" for _, t in fact_pairs)});
        """)

    finally:
        # Always clean up the staging table, even on RI gate failure
        _exec(f"IF OBJECT_ID('{stage_table}', 'U') IS NOT NULL DROP TABLE {stage_table};")

    n = _query(f"SELECT COUNT(*) FROM {schema}.{table}")[0][0]
    log.info(f"[GOLD-WH] {schema}.{table}: {n} rows total")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5 — Master Gold-WH build loop
results = []
row_counts = {}

for domain in get_all_domains():
    schema_cfg = STAR_SCHEMA_REGISTRY[domain]
    # Ensure target schema exists
    _exec(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{domain}') EXEC('CREATE SCHEMA {domain}');")
    built_dims = set()

    # Build dimensions first
    for dim_name, dim_cfg in schema_cfg["dimensions"].items():
        try:
            built = build_dimension_wh(dim_name, dim_cfg, domain)
            built_dims.add(dim_name)
            tbl = dim_cfg["gold_path"].split("/")[-1]
            n   = _query(f"SELECT COUNT(*) FROM {domain}.{tbl}")[0][0]
            row_counts[f"{domain}.{tbl}"] = n
            results.append({"layer": "dim", "name": dim_name, "status": "SUCCESS"})
        except Exception as e:
            log.error(f"[GOLD-WH] {dim_name}: {e}")
            results.append({"layer": "dim", "name": dim_name, "status": "FAILED", "error": str(e)})

    # Build facts. Register each built fact in built_dims as it completes — a later
    # fact's dimension_joins can name an earlier fact as a degenerate "dim" (e.g.
    # fact_procedures -> fact_encounters) to inherit its surrogate key. Registry
    # order MUST build the referenced fact before the fact that joins to it.
    for fact_name, fact_cfg in schema_cfg["facts"].items():
        try:
            build_fact_wh(fact_name, fact_cfg, domain, built_dims)
            built_dims.add(fact_name)
            tbl = fact_cfg["gold_path"].split("/")[-1]
            n   = _query(f"SELECT COUNT(*) FROM {domain}.{tbl}")[0][0]
            row_counts[f"{domain}.{tbl}"] = n
            results.append({"layer": "fact", "name": fact_name, "status": "SUCCESS"})
        except Exception as e:
            log.error(f"[GOLD-WH] {fact_name}: {e}")
            results.append({"layer": "fact", "name": fact_name, "status": "FAILED", "error": str(e)})

failed = [r for r in results if r["status"] == "FAILED"]
print("\nGOLD-WH BUILD SUMMARY")
for r in results:
    icon   = "✓" if r["status"] == "SUCCESS" else "✗"
    detail = f"\n       ERROR: {r['error']}" if r.get("error") else ""
    print(f"  {icon}  [{r['layer']}] {r['name']} — {r['status']}{detail}")

# Write RI warnings to GoldLH/_triage/gold_warnings.json so the Triage tab picks
# them up. Uses a direct abfss path via WORKSPACE_GUID + GOLD_ITEM_ID so GoldLH
# does not need to be attached to this notebook (warehouse-only deployments have
# GoldLH detached). Skipped with a warning if GOLD_ITEM_ID is not yet configured.
if GOLD_ITEM_ID:
    _warnings_path = (
        f"abfss://{WORKSPACE_GUID}@{ONELAKE_HOST}"
        f"/{GOLD_ITEM_ID}/Files/_triage/gold_warnings.json"
    )
    try:
        mssparkutils.fs.put(_warnings_path, json.dumps(_gold_wh_ri_warnings, indent=2), overwrite=True)
        log.info(
            f"[GOLD-WH] {len(_gold_wh_ri_warnings)} RI warning(s) written to _triage/gold_warnings.json"
            if _gold_wh_ri_warnings
            else "[GOLD-WH] no RI warnings — wrote empty _triage/gold_warnings.json"
        )
    except Exception as _warn_exc:
        log.warning(f"[GOLD-WH] Could not write gold_warnings.json to GoldLH: {_warn_exc}")
else:
    log.warning("[GOLD-WH] GOLD_ITEM_ID not set — skipping gold_warnings.json write; Triage tab will show no Gold RI warnings")

mssparkutils.notebook.exit(json.dumps({
    "status":     "FAILED" if failed else "SUCCESS",
    "failed":     [r["name"] for r in failed],
    "row_counts": row_counts,    # consumed by scripts/test_gold_parity.py
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
