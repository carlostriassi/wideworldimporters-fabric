# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Apply Warehouse Security
# Wrapper notebook called by `master_pipeline` → **Apply Warehouse Security Gate** activity.
#
# Resolves BronzeLH via `mssparkutils.lakehouse.get("BronzeLH")` (no attached lakehouse
# required) then copies `config/`, `scripts/`, and `warehouse/` to the driver's `/tmp/wh_security/`
# and runs `apply_warehouse_security.py`, which executes `warehouse/security_ddl.sql`
# against the GoldWH Fabric Warehouse SQL endpoint.
#
# **Pre-conditions (must all be true before this notebook runs):**
# 1. `scripts/bootstrap_warehouse.py` has been run → GoldWH item exists in the workspace
# 2. OneLake shortcuts from GoldLH tables → GoldWH have been created manually in the Fabric UI
# 3. `WAREHOUSE_ENABLED = True` is set in `config/workspace_config.py`
# 4. `WAREHOUSE_ITEM_ID` (or `WAREHOUSE_SQL_ENDPOINT`) is set in `config/workspace_config.py`
#
# The script itself checks `WAREHOUSE_ENABLED` and exits cleanly if False — safe to call
# even when the warehouse is not yet provisioned, but the pipeline's `enable_warehouse`
# parameter gate means this notebook only runs when the operator explicitly sets it to true.
#
# **Idempotency:** every DDL statement in `security_ddl.sql` is guarded by
# `IF NOT EXISTS` / `CREATE OR ALTER`. Re-running after a partial failure is safe.

# CELL ********************

import sys, os, json, subprocess

# ── 1. Resolve BronzeLH via workspace item registry — no attached lakehouse needed ──

_lh   = mssparkutils.lakehouse.get("BronzeLH")
_base = f"abfss://{_lh.workspaceId}@onelake.dfs.fabric.microsoft.com/{_lh.id}/Files"
_tmp  = "/tmp/wh_security"


def _copy_text_files(src_abfss: str, dst_local: str, max_bytes: int = 4_000_000) -> int:
    """Recursively copy text files (.py, .sql, .json) from abfss to the driver filesystem.
    Uses mssparkutils.fs — no FUSE mount required."""
    os.makedirs(dst_local, exist_ok=True)
    copied = 0
    for entry in mssparkutils.fs.ls(src_abfss):
        local_path = os.path.join(dst_local, entry.name)
        if entry.isDir:
            copied += _copy_text_files(entry.path, local_path, max_bytes)
        elif any(entry.name.endswith(s) for s in (".py", ".sql", ".json")):
            content = mssparkutils.fs.head(entry.path, max_bytes)
            with open(local_path, "w", encoding="utf-8") as fh:
                fh.write(content)
            copied += 1
    return copied


print("Copying config/ ...")
n = _copy_text_files(f"{_base}/config", f"{_tmp}/config")
print(f"  {n} file(s)")

print("Copying scripts/ ...")
n = _copy_text_files(f"{_base}/scripts", f"{_tmp}/scripts")
print(f"  {n} file(s)")

print("Copying warehouse/ ...")
n = _copy_text_files(f"{_base}/warehouse", f"{_tmp}/warehouse")
print(f"  {n} file(s)")

print("Copy complete.\n")

# CELL ********************

# ── 2. Run apply_warehouse_security.py from the copied location ──
# PROJECT_ROOT inside the script resolves to /tmp/wh_security via Path(__file__).parent.parent,
# so config/workspace_config.py and warehouse/security_ddl.sql both resolve correctly.

result = subprocess.run(
    [sys.executable, f"{_tmp}/scripts/apply_warehouse_security.py"],
    capture_output=True,
    text=True,
    env={**os.environ, "PYTHONUTF8": "1"},
)

print(result.stdout)

if result.returncode != 0:
    print("STDERR:", result.stderr)
    raise RuntimeError(
        f"apply_warehouse_security.py exited with code {result.returncode}. "
        "See output above for failed DDL batches."
    )

print("apply_warehouse_security.py completed successfully.")

# CELL ********************

# ── 3. Exit signal ──

mssparkutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "notebook": "apply_warehouse_security",
}))
