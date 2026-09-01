# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
sys.modules.pop("workspace_config", None)
sys.path.insert(0, _cfg_local)
importlib.invalidate_caches()

from workspace_config import WATERMARK_PATH

spark.read.format("delta").load(WATERMARK_PATH) \
    .orderBy("last_run_at", ascending=False) \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
sys.path.insert(0, _cfg_local)

from workspace_config import SILVER_METRICS

spark.read.format("delta").load(SILVER_METRICS) \
    .filter("table_name = 'insurance_claims'") \
    .orderBy("run_at", ascending=False) \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


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
sys.path.insert(0, _cfg_local)

from workspace_config import SILVER_QUARANTINE

spark.read.format("delta") \
       .load(f"{SILVER_QUARANTINE}/insurance_claims") \
       .groupBy("_dq_stage") \
       .count() \
       .orderBy("count", ascending=False) \
       .show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
