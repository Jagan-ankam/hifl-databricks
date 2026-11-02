# Databricks notebook source
# MAGIC %md
# MAGIC # HIFL 03 - Dashboard
# MAGIC
# MAGIC Read-only views over the HIFL tables. Each `display()` output can be turned into a chart with the
# MAGIC **+** button and pinned to a Databricks dashboard, or the same SQL can be used in Databricks SQL.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog (blank = default)")
dbutils.widgets.text("schema", "hifl", "Schema")

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.path.abspath(os.getcwd()))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from pyspark.sql import functions as F

from hifl.config import HIFLConfig

cfg = HIFLConfig(
    catalog=dbutils.widgets.get("catalog").strip() or None,
    schema=dbutils.widgets.get("schema").strip() or "hifl",
)
t = cfg.table

# COMMAND ----------

# MAGIC %md ### Headline metrics

# COMMAND ----------

display(spark.table(t("metrics")))

# COMMAND ----------

# MAGIC %md ### Where quality events are discovered (paper, Table II / Figure 2)

# COMMAND ----------

display(spark.table(t("release_distribution")))

# COMMAND ----------

# MAGIC %md ### Defect categories

# COMMAND ----------

display(spark.table(t("category_distribution")))

# COMMAND ----------

# MAGIC %md ### Closure rule: incidents closed without a preventive asset

# COMMAND ----------

closure = spark.table(t("closure_compliance"))
display(closure.groupBy("closure_status").count().orderBy("closure_status"))
display(closure.filter(~F.col("closure_rule_ok")).orderBy("release_cycle", "source_record_id"))

# COMMAND ----------

# MAGIC %md ### Recurring root causes

# COMMAND ----------

display(
    spark.table(t("incidents"))
    .filter(F.col("is_recurring") & ~F.col("is_duplicate"))
    .groupBy("category", "primary_root_cause")
    .agg(F.count(F.lit(1)).alias("incidents"), F.max("releases_affected").alias("releases_affected"))
    .orderBy(F.col("incidents").desc())
)

# COMMAND ----------

# MAGIC %md ### Assets still waiting for a control binding

# COMMAND ----------

display(
    spark.table(t("knowledge_assets"))
    .filter(F.col("asset_status") == "NEEDS_BINDING")
    .groupBy("control_template", "business_object")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

# MAGIC %md ### Regression (IBRT) history

# COMMAND ----------

display(
    spark.table(t("regression_results"))
    .groupBy("release_id", "run_ts")
    .agg(
        F.count(F.lit(1)).alias("controls"),
        F.sum(F.when(F.col("result") == "PASS", 1).otherwise(0)).alias("passed"),
        F.sum(F.when(F.col("result") == "FAIL", 1).otherwise(0)).alias("failed"),
        F.sum(F.when(F.col("result") == "ERROR", 1).otherwise(0)).alias("errored"),
    )
    .orderBy(F.col("run_ts").desc())
)
