# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Teams Alert
# Wrapper notebook called by `master_pipeline_lh` / `master_pipeline_lh_wh` →
# **Teams Alert Bronze Failure** activity (and any future Silver/Gold failure
# gates that adopt the same pattern).
#
# Resolves BronzeLH via `mssparkutils.lakehouse.get("BronzeLH")`, copies `config/`
# to the driver's `/tmp/nb_config/`, and POSTs a Teams incoming-webhook message
# if `TEAMS_ALERTS_ENABLED = True` in `config/workspace_config.py`.
#
# **This notebook is always wired as Active in the pipeline — it self-gates.**
# Safe to call even when Teams alerting is not configured for a client:
# - `TEAMS_ALERTS_ENABLED = False` (the default) → exits immediately, no Key
#   Vault call at all.
# - `TEAMS_ALERTS_ENABLED = True` but the `teams-webhook-url` KV secret is
#   missing/empty → exits cleanly, does not fail the pipeline.
# - The webhook POST itself is wrapped in try/except — a Teams-side outage or
#   bad webhook URL never fails the pipeline run.

# CELL ********************

# ── Parameters (Fabric pipeline "parameters" binding on the TridentNotebook
# activity overrides these at run time; if the binding doesn't fire for any
# reason, these defaults still produce a valid, if less detailed, alert) ──

layer_name = "Bronze"
failed_json = "[]"
run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "tags": ["parameters"]
# META }

# CELL ********************

import json

# ── 1. Copy config/ from BronzeLH so workspace_config.py is importable ──
# Mirrors the config-copy pattern used by every other notebook (see CLAUDE.md
# "Config copy pattern") — avoids relying on the /lakehouse/ FUSE mount.

_lh = mssparkutils.lakehouse.get("BronzeLH")
_base = f"abfss://{_lh.workspaceId}@onelake.dfs.fabric.microsoft.com/{_lh.id}/Files"
_cfg_local = "/tmp/nb_config"

import os
os.makedirs(_cfg_local, exist_ok=True)
for entry in mssparkutils.fs.ls(f"{_base}/config"):
    if entry.name.endswith(".py"):
        content = mssparkutils.fs.head(entry.path, 4_000_000)
        with open(os.path.join(_cfg_local, entry.name), "w", encoding="utf-8") as fh:
            fh.write(content)

import sys, importlib
sys.modules.pop("workspace_config", None)
sys.path.insert(0, _cfg_local)
importlib.invalidate_caches()
import workspace_config

# CELL ********************

# ── 2. Send the Teams alert, gated by TEAMS_ALERTS_ENABLED ──

alert_sent = False
skip_reason = None

if not getattr(workspace_config, "TEAMS_ALERTS_ENABLED", False):
    skip_reason = "TEAMS_ALERTS_ENABLED is False"
else:
    try:
        webhook_url = workspace_config.get_secret("teams_webhook_url")
    except Exception as exc:
        webhook_url = None
        skip_reason = f"could not resolve teams_webhook_url secret: {exc}"

    if not webhook_url:
        skip_reason = skip_reason or "teams_webhook_url secret is empty"
    else:
        try:
            failed_tables = json.loads(failed_json) if failed_json else []
        except (TypeError, ValueError):
            failed_tables = [failed_json] if failed_json else []

        failed_text = ", ".join(failed_tables) if failed_tables else "(see pipeline run for detail)"
        payload = {
            "text": (
                f"\U0001F534 **master_pipeline** — {layer_name} layer FAILED.\n"
                f"Failed: {failed_text}\n"
                f"Run ID: {run_id}"
            )
        }

        import requests
        try:
            resp = requests.post(webhook_url, json=payload, timeout=10)
            alert_sent = resp.ok
            if not resp.ok:
                skip_reason = f"webhook returned HTTP {resp.status_code}"
        except Exception as exc:
            skip_reason = f"webhook POST failed: {exc}"

print(f"alert_sent={alert_sent} skip_reason={skip_reason}")

# CELL ********************

# ── 3. Exit signal — always SUCCESS; an unsent alert never fails the pipeline ──

mssparkutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "notebook": "teams_alert",
    "alert_sent": alert_sent,
    "skip_reason": skip_reason,
}))
