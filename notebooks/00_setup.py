# Databricks notebook source
# MAGIC %md
# MAGIC # HIFL 00 - Setup
# MAGIC
# MAGIC Creates the HIFL schema and (optionally) loads the **synthetic** demo tables from `sample_data/`
# MAGIC that the example controls in `conf/control_bindings.json` check.
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `catalog` | Unity Catalog name. Leave blank to use the workspace default catalog / `hive_metastore`. |
# MAGIC | `schema` | Schema that will hold every HIFL table. |
# MAGIC | `load_demo_data` | `true` loads the demo tables (`demo_*`). Use `false` with your own data. |

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog (blank = default)")
dbutils.widgets.text("schema", "hifl", "Schema")
dbutils.widgets.dropdown("load_demo_data", "true", ["true", "false"], "Load demo tables")
dbutils.widgets.dropdown("table_format", "delta", ["delta", "parquet"], "Table format")

# COMMAND ----------

import os
import sys

# Notebooks run with the notebook folder as the working directory (Git folders and bundles).
REPO_ROOT = os.path.dirname(os.path.abspath(os.getcwd()))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from hifl.config import HIFLConfig
from hifl.demo import load_demo_tables
from hifl.io import ensure_schema

cfg = HIFLConfig(
    catalog=dbutils.widgets.get("catalog").strip() or None,
    schema=dbutils.widgets.get("schema").strip() or "hifl",
    table_format=dbutils.widgets.get("table_format"),
)
print(f"Repository root : {REPO_ROOT}")
print(f"HIFL schema     : {cfg.schema_fqn}")

# COMMAND ----------

ensure_schema(spark, cfg)

if dbutils.widgets.get("load_demo_data") == "true":
    tables = load_demo_tables(spark, cfg, REPO_ROOT)
    print("Loaded demo tables:")
    for t in tables:
        print("  ", t)
else:
    print("Demo data skipped.")
