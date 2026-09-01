# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Bronze Dispatcher
# Runs one notebook instance per active connector type in parallel.
# Each notebook receives its `connector_type` as a parameter and
# processes only the sources registered for that type.
# **To activate a new connector type:**
# 1. Add its entries to `config/source_registry.py`
# 2. Uncomment it in `ACTIVE_CONNECTOR_TYPES` in `config/workspace_config.py`
# 3. Re-upload workspace_config.py to BronzeLH/Files/config/
# ### Dispatch flow
# ```
# ACTIVE_CONNECTOR_TYPES ∩ SOURCE_REGISTRY  →  CONNECTOR_NOTEBOOK_MAP
#    {blob, sqlserver_sqlauth}  →  [(blob, bronze_blob),
#                                   (sqlserver_sqlauth, bronze_sql_server)]
#       └─ ThreadPoolExecutor — each run gets connector_type as parameter
# ```

# CELL ********************

# CELL 1 — Imports
import sys, os, json, logging

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

from workspace_config import CONNECTOR_NOTEBOOK_MAP, ACTIVE_CONNECTOR_TYPES, PIPELINE_TIER
from source_registry import SOURCE_REGISTRY

log = logging.getLogger('bronze_dispatcher')
if not log.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(levelname)s — %(message)s'))
    log.addHandler(_h)
    log.setLevel(logging.INFO)
    log.propagate = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2 — Resolve which notebooks to run
# Intersection of ACTIVE_CONNECTOR_TYPES (workspace_config.py) and
# what is actually registered in SOURCE_REGISTRY. Connector types that
# exist in the registry but are NOT in ACTIVE_CONNECTOR_TYPES are skipped.
registry_types = {cfg['connector_type'] for cfg in SOURCE_REGISTRY.values()}
active_types   = registry_types & set(ACTIVE_CONNECTOR_TYPES)

if not active_types:
    log.warning('[DISPATCHER] No active connector types — nothing to run.')
    mssparkutils.notebook.exit(json.dumps({
        'status': 'SUCCESS', 'notebooks': 0, 'failed': [], 'detail': []
    }))

unknown = set(ACTIVE_CONNECTOR_TYPES) - set(CONNECTOR_NOTEBOOK_MAP)
if unknown:
    raise ValueError(
        f'connector_type(s) {unknown} in ACTIVE_CONNECTOR_TYPES have no mapped notebook. '
        f'Add them to CONNECTOR_NOTEBOOK_MAP in config/workspace_config.py.'
    )

# One (connector_type, notebook_name) pair per active type — never deduplicated.
# Two types mapping to the same notebook (e.g. sqlserver_sqlauth + sqlserver_entra
# both → bronze_sql_server) produce two independent parallel runs, each receiving
# only its own connector_type as a parameter.
notebooks_to_run = sorted((t, CONNECTOR_NOTEBOOK_MAP[t]) for t in active_types)

log.info(f'[DISPATCHER] Tier          : {PIPELINE_TIER}')
log.info(f'[DISPATCHER] Registry types  : {sorted(registry_types)}')
log.info(f'[DISPATCHER] Active types    : {sorted(active_types)}')
log.info(f'[DISPATCHER] Notebooks queued: {notebooks_to_run}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sorted(registry_types))
display(sorted(active_types))
display([(t, nb) for t, nb in notebooks_to_run])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3 — Run one notebook instance per connector type in parallel.
# Each run receives connector_type as a parameter so the notebook fetches
# only its own sources from SOURCE_REGISTRY. Two types that share a notebook
# (e.g. sqlserver_sqlauth + sqlserver_entra → bronze_sql_server) run as
# independent parallel executions with no shared state.
# Returns: {connector_type: {"notebook": nb_name, "exitValue": "<json>", "exception": None|str}}
from concurrent.futures import ThreadPoolExecutor

def _run(connector_type, nb_name):
    try:
        # bronze_sql_server reads args as a JSON string for cross-runtime compatibility.
        # All other notebooks receive connector_type as a direct string parameter.
        if nb_name == 'bronze_sql_server':
            import json as _j
            params = {"args": _j.dumps({"connector_type": connector_type})}
        else:
            params = {"connector_type": connector_type}
        exit_val = mssparkutils.notebook.run(nb_name, 86400, params)  # 24 h ceiling
        return connector_type, {"notebook": nb_name, "exitValue": exit_val, "exception": None}
    except Exception as exc:
        return connector_type, {"notebook": nb_name, "exitValue": "{}", "exception": str(exc)}

with ThreadPoolExecutor(max_workers=len(notebooks_to_run)) as pool:
    futures = [pool.submit(_run, t, nb) for t, nb in notebooks_to_run]

results = dict(f.result() for f in futures)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4 — Parse per-connector-type exit values into a unified summary
summary = []
for connector_type, result in results.items():
    nb_name   = result.get('notebook', '')
    exit_raw  = result.get('exitValue', '{}')
    exception = result.get('exception')
    try:
        exit_data = json.loads(exit_raw) if exit_raw else {}
    except Exception:
        exit_data = {'status': 'FAILED', 'error': exit_raw}

    if exception:
        exit_data['status'] = 'FAILED'
        exit_data['error']  = str(exception)

    status = exit_data.get('status', 'FAILED')
    summary.append({
        'connector_type': connector_type,
        'notebook':       nb_name,
        'status':         status,
        'tables':         exit_data.get('tables', 0),
        'failed':         exit_data.get('failed', []),
        'error':          exit_data.get('error', ''),
    })
    log.info(f'[DISPATCHER] {connector_type} ({nb_name}): {status}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5 — Exit with aggregated status (read by master_pipeline If-Condition)
failed = [s for s in summary if s['status'] != 'SUCCESS']

mssparkutils.notebook.exit(json.dumps({
    'status':    'FAILED' if failed else 'SUCCESS',
    'notebooks': len(summary),
    'failed':    [s['connector_type'] for s in failed],
    'detail':    summary,
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
