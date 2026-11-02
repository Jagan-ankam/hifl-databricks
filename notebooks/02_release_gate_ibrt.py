# Databricks notebook source
# MAGIC %md
# MAGIC # HIFL 02 - Release gate (Incident-Based Regression Testing)
# MAGIC
# MAGIC Runs the existing control library (`hifl_controls`) against the current data for a new release,
# MAGIC appends the results to `hifl_regression_results`, and evaluates the release gate.
# MAGIC Use this notebook as a pre-go-live gate in a deployment pipeline: with `fail_on_gate = true` the
# MAGIC run fails when any control with a blocking severity (default `CRITICAL`, `HIGH`) fails or errors.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog (blank = default)")
dbutils.widgets.text("schema", "hifl", "Schema")
dbutils.widgets.text("release_id", "R4", "Release being validated")
dbutils.widgets.text("blocking_severities", "CRITICAL,HIGH", "Blocking severities")
dbutils.widgets.dropdown("fail_on_gate", "true", ["true", "false"], "Fail the run if the gate fails")
dbutils.widgets.dropdown("table_format", "delta", ["delta", "parquet"], "Table format")

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.path.abspath(os.getcwd()))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from hifl.config import HIFLConfig
from hifl.io import read_table, write_table
from hifl.validation import release_gate, run_regression_suite

cfg = HIFLConfig(
    catalog=dbutils.widgets.get("catalog").strip() or None,
    schema=dbutils.widgets.get("schema").strip() or "hifl",
    table_format=dbutils.widgets.get("table_format"),
    gate_fail_severities=[s.strip().upper() for s in dbutils.widgets.get("blocking_severities").split(",") if s.strip()],
)
release_id = dbutils.widgets.get("release_id").strip() or "unspecified"

controls = read_table(spark, cfg, "controls")
results = run_regression_suite(spark, controls, release_id)
write_table(results, cfg, "regression_results", mode="append")
gate = release_gate(results, cfg, release_id)

display(results.orderBy("result", "severity"))
print(gate.summary())

# COMMAND ----------

if dbutils.widgets.get("fail_on_gate") == "true" and not gate.passed:
    raise RuntimeError(gate.summary())
