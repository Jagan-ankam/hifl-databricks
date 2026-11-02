"""Descriptive HIFL metrics (paper, Sections IV and VII).

All metrics are descriptive. They show where quality events are found and how
well the feedback loop is being followed; they are not tests of significance.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

METRIC_SCHEMA = StructType(
    [
        StructField("metric", StringType(), False),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("definition", StringType(), True),
    ]
)


def _active(incidents: DataFrame) -> DataFrame:
    return incidents.filter(~F.col("is_duplicate")) if "is_duplicate" in incidents.columns else incidents


def release_distribution(incidents: DataFrame) -> DataFrame:
    """Events per release cycle and discovery phase with their share of the total (Table II)."""
    base = _active(incidents)
    counts = base.groupBy("release_cycle", "phase").agg(F.count(F.lit(1)).alias("events"))
    total = F.sum("events").over(Window.partitionBy())
    return counts.withColumn("share_pct", F.round(F.col("events") * 100.0 / total, 1)).orderBy("release_cycle")


def category_distribution(incidents: DataFrame) -> DataFrame:
    """Events per defect category and phase."""
    base = _active(incidents)
    counts = base.groupBy("category", "phase").agg(F.count(F.lit(1)).alias("events"))
    total = F.sum("events").over(Window.partitionBy())
    return counts.withColumn("share_pct", F.round(F.col("events") * 100.0 / total, 1)).orderBy(
        F.col("events").desc(), "category"
    )


def _pct(num: float, den: float) -> Optional[float]:
    return round(100.0 * num / den, 2) if den else None


def compute_metrics(
    incidents: DataFrame,
    assets: DataFrame,
    controls: DataFrame,
    closure: DataFrame,
    results: Optional[DataFrame] = None,
) -> DataFrame:
    """Return a long table of headline metrics: (metric, value, unit, definition)."""
    spark = incidents.sparkSession
    base = _active(incidents)
    agg = base.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col("phase") == "HYPERCARE", 1).otherwise(0)).alias("hypercare"),
        F.sum(F.when(F.col("phase") == "UAT", 1).otherwise(0)).alias("uat"),
        F.sum(F.when(F.col("category") != "UNCLASSIFIED", 1).otherwise(0)).alias("classified"),
        F.sum(F.col("is_recurring").cast("int")).alias("recurring"),
        F.avg(
            F.when(
                F.col("resolved_at").isNotNull() & F.col("opened_at").isNotNull(),
                (F.unix_timestamp(F.col("resolved_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600.0,
            )
        ).alias("mttr_h"),
    ).collect()[0]
    dup_count = incidents.filter(F.col("is_duplicate")).count() if "is_duplicate" in incidents.columns else 0

    closure_row = closure.agg(
        F.sum(F.col("is_closed").cast("int")).alias("closed"),
        F.sum(F.when(F.col("closure_status") == "COMPLIANT", 1).otherwise(0)).alias("compliant"),
    ).collect()[0]
    asset_row = assets.agg(
        F.count(F.lit(1)).alias("assets"),
        F.sum(F.when(F.col("asset_status") == "NEEDS_BINDING", 1).otherwise(0)).alias("unbound"),
        F.countDistinct("control_id").alias("controls_used"),
        F.sum(F.when(F.col("control_id").isNotNull(), 1).otherwise(0)).alias("bound_assets"),
    ).collect()[0]
    n_controls = controls.count()

    total = float(agg["total"] or 0)
    closed = float(closure_row["closed"] or 0)
    bound_assets = float(asset_row["bound_assets"] or 0)
    metrics: List[Tuple[str, Optional[float], str, str]] = [
        ("total_quality_events", total, "count", "Distinct quality events after removing cross-channel duplicates"),
        ("cross_channel_duplicates", float(dup_count), "count", "Events raised through more than one channel"),
        ("hypercare_events", float(agg["hypercare"] or 0), "count", "Events discovered during hypercare"),
        ("uat_events", float(agg["uat"] or 0), "count", "Events discovered during UAT"),
        (
            "hypercare_discovery_rate",
            _pct(float(agg["hypercare"] or 0), total),
            "percent",
            "Share of all events discovered during hypercare (descriptive, not a leakage rate)",
        ),
        ("classification_coverage", _pct(float(agg["classified"] or 0), total), "percent", "Events assigned to a taxonomy category"),
        (
            "mean_time_to_resolution",
            None if agg["mttr_h"] is None else round(float(agg["mttr_h"]), 2),
            "hours",
            "Average of resolved_at - opened_at",
        ),
        ("recurrence_rate", _pct(float(agg["recurring"] or 0), total), "percent", "Events whose root cause appears in more than one release"),
        ("knowledge_assets", float(asset_row["assets"] or 0), "count", "Knowledge assets extracted (Phase 4)"),
        ("assets_needing_binding", float(asset_row["unbound"] or 0), "count", "Executable assets still waiting for a control binding"),
        ("active_controls", float(n_controls), "count", "Distinct controls in the IBRT library"),
        (
            "knowledge_reuse_ratio",
            round(bound_assets / n_controls, 2) if n_controls else None,
            "assets per control",
            "Bound assets per distinct control; above 1 means controls are shared across incidents",
        ),
        ("closure_compliance", _pct(float(closure_row["compliant"] or 0), closed), "percent", "Closed events linked to at least one preventive asset"),
    ]
    if results is not None:
        r = results.agg(
            F.count(F.lit(1)).alias("n"), F.sum(F.when(F.col("result") == "PASS", 1).otherwise(0)).alias("p")
        ).collect()[0]
        metrics.append(
            ("regression_pass_rate", _pct(float(r["p"] or 0), float(r["n"] or 0)), "percent", "IBRT controls passing in this run")
        )
    rows = [(m, None if v is None else float(v), u, d) for m, v, u, d in metrics]
    return spark.createDataFrame(rows, schema=METRIC_SCHEMA)
