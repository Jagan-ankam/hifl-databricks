# Databricks notebook source
# MAGIC %md
# MAGIC # HIFL 01 - Run the full feedback loop
# MAGIC
# MAGIC Runs all six phases of the Hypercare Incident Feedback Loop from a JSON run configuration:
# MAGIC
# MAGIC 1. **Capture** incidents from every configured channel
# MAGIC 2. **Classify** them into the six-category taxonomy
# MAGIC 3. **Root-cause intelligence** (five whys, fishbone dimension, recurrence)
# MAGIC 4. **Knowledge extraction** (assets per category)
# MAGIC 5. **Preventive control generation** + closure rule
# MAGIC 6. **Release validation** with Incident-Based Regression Testing (IBRT)
# MAGIC
# MAGIC Run `00_setup` first. Edit `conf/hifl_config.json` (sources), `conf/control_bindings.json` (controls) and
# MAGIC `conf/classification_rules.json` (taxonomy keywords) to point HIFL at your own data.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog (blank = default)")
dbutils.widgets.text("schema", "hifl", "Schema")
dbutils.widgets.text("config_path", "conf/hifl_config.json", "Run config (relative to repo root)")
dbutils.widgets.text("release_id", "R3-demo", "Release being validated")
dbutils.widgets.dropdown("fail_on_gate", "false", ["true", "false"], "Fail the run if the gate fails")
dbutils.widgets.dropdown("table_format", "delta", ["delta", "parquet"], "Table format")

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.path.abspath(os.getcwd()))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from hifl.pipeline import run_from_config

config_path = dbutils.widgets.get("config_path").strip()
if not os.path.isabs(config_path):
    config_path = os.path.join(REPO_ROOT, config_path)

run = run_from_config(
    spark,
    config_path,
    base_dir=REPO_ROOT,
    overrides={
        "catalog": dbutils.widgets.get("catalog").strip() or "none",
        "schema": dbutils.widgets.get("schema").strip() or None,
        "table_format": dbutils.widgets.get("table_format"),
    },
    release_id=dbutils.widgets.get("release_id").strip() or None,
)

# COMMAND ----------

# MAGIC %md ### Headline metrics

# COMMAND ----------

display(run.metrics)

# COMMAND ----------

# MAGIC %md ### Incidents by category and phase

# COMMAND ----------

from hifl.metrics import category_distribution, release_distribution

display(release_distribution(run.incidents))
display(category_distribution(run.incidents))

# COMMAND ----------

# MAGIC %md ### Release gate (IBRT)

# COMMAND ----------

if run.results is not None:
    display(run.results.orderBy("result", "severity"))
if run.gate is not None:
    print(run.gate.summary())
    for b in run.gate.blocking:
        print(f"  BLOCKING  {b['severity']:<8} {b['check_type']:<26} {b['message']}")
    if dbutils.widgets.get("fail_on_gate") == "true" and not run.gate.passed:
        raise RuntimeError(run.gate.summary())
