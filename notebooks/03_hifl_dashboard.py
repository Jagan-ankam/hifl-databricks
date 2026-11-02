# Databricks notebook source
# MAGIC %md
# MAGIC # HIFL 03 - Dashboard
# MAGIC
# MAGIC Read-only views over the HIFL tables. Each `display()` output can be turned into a chart with the
# MAGIC **+** button and pinned to a Databricks dashboard, or the same SQL can be used in Databricks SQL.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog (blank = default)")
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

release = spark.table(t("release_distribution")).orderBy("release_cycle")
display(release)

# COMMAND ----------

# MAGIC %md ### Chart: quality events by release cycle
# MAGIC Blue bars are hypercare cycles, orange is UAT. You can also click **+ > Visualization > Bar** on the table above.

# COMMAND ----------

try:
    import matplotlib.pyplot as plt
except ImportError:  # matplotlib ships with Databricks runtimes; this is only a safeguard
    plt = None
    print("matplotlib is not available - use + > Visualization > Bar on the table above.")

if plt is not None:
    rows = release.collect()
    labels = [r["release_cycle"].replace(" ", "\n", 1) for r in rows]
    counts = [int(r["events"]) for r in rows]
    shares = [float(r["share_pct"]) for r in rows]
    colors = ["#2a78d6" if r["phase"] == "HYPERCARE" else "#eb6834" for r in rows]

    fig, ax = plt.subplots(figsize=(7, 4.2))
    bars = ax.bar(labels, counts, color=colors, width=0.6, zorder=3)
    for bar, n, pct in zip(bars, counts, shares, strict=True):
        ax.text(bar.get_x() + bar.get_width() / 2, n + 0.5, f"{n} ({pct}%)", ha="center", va="bottom", fontweight="bold")
    ax.set_ylabel("Number of quality events")
    ax.set_title("Quality events by release cycle")
    ax.set_ylim(0, max(counts) * 1.2 if counts else 1)
    ax.grid(axis="y", color="#e4e3dd", zorder=0)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    fig.tight_layout()
    plt.show()

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
