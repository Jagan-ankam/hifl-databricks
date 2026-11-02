"""Phase 5 - Preventive control generation and the HIFL closure rule.

Knowledge assets are turned into executable controls by *bindings*. A binding
tells HIFL which concrete check to run for a control template and business
object, for example::

    {"control_template": "mapping_reconciliation",
     "business_object": "REASON_CODE",
     "check_type": "mapping_coverage",
     "severity": "HIGH",
     "params": {"source_table": "main.legacy.orders", "source_column": "reason_code",
                "mapping_table": "main.s4.reason_code_map", "mapping_column": "legacy_code"}}

``business_object`` may be ``"*"`` to apply to every object; an exact match
wins over the wildcard. Identical controls produced by several incidents are
merged into one library entry, which is how knowledge is reused.

Central rule: an incident is not closed until it has at least one preventive
asset (an ACTIVE control or a DOCUMENTED governance/runbook asset).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from hifl.config import HIFLConfig
from hifl.rules_engine import CHECKS
from hifl.schemas import CONTROL_SCHEMA
from hifl.taxonomy import NON_EXECUTABLE_TEMPLATES

_BINDING_SCHEMA = StructType(
    [
        StructField("b_template", StringType(), False),
        StructField("b_object", StringType(), False),
        StructField("b_check_type", StringType(), False),
        StructField("b_params_json", StringType(), False),
        StructField("b_severity", StringType(), True),
        StructField("b_description", StringType(), True),
    ]
)


def validate_bindings(bindings: Sequence[Dict[str, Any]]) -> List[Tuple[str, str, str, str, str, str]]:
    """Check binding definitions and return them as rows. Raises ``ValueError``."""
    rows = []
    for i, b in enumerate(bindings or []):
        for key in ("control_template", "check_type", "params"):
            if key not in b:
                raise ValueError(f"Binding #{i} is missing '{key}'")
        if b["check_type"] not in CHECKS:
            raise ValueError(f"Binding #{i}: unknown check_type '{b['check_type']}'")
        rows.append(
            (
                str(b["control_template"]),
                str(b.get("business_object", "*")).upper(),
                str(b["check_type"]),
                json.dumps(b["params"], sort_keys=True),
                str(b.get("severity", "MEDIUM")).upper(),
                str(b.get("description", "")),
            )
        )
    return rows


def generate_controls(assets: DataFrame, bindings: Optional[Sequence[Dict[str, Any]]]) -> Tuple[DataFrame, DataFrame]:
    """Bind knowledge assets to executable checks.

    Returns ``(assets_with_status, controls)``:

    * ``assets_with_status`` adds ``control_id`` and sets ``asset_status`` to
      ``ACTIVE`` (bound), ``DOCUMENTED`` (governance/runbook asset) or
      ``NEEDS_BINDING`` (an executable template with no binding yet).
    * ``controls`` is the deduplicated control library (one row per control).
    """
    spark = assets.sparkSession
    rows = validate_bindings(bindings or [])
    bdf = spark.createDataFrame(rows, schema=_BINDING_SCHEMA)

    joined = assets.join(
        bdf,
        (F.col("control_template") == F.col("b_template"))
        & ((F.col("b_object") == F.lit("*")) | (F.col("b_object") == F.upper(F.coalesce(F.col("business_object"), F.lit(""))))),
        "left",
    )
    pick = Window.partitionBy("asset_id").orderBy(
        F.when(F.col("b_object") == F.lit("*"), 1).otherwise(0), F.col("b_check_type").asc_nulls_last()
    )
    best = joined.withColumn("_rn", F.row_number().over(pick)).filter(F.col("_rn") == 1).drop("_rn")

    non_exec = F.array(*[F.lit(t) for t in sorted(NON_EXECUTABLE_TEMPLATES)])
    control_id = F.when(
        F.col("b_check_type").isNotNull(),
        F.sha2(F.concat_ws("||", F.col("control_template"), F.col("b_check_type"), F.col("b_params_json")), 256),
    )
    assets_out = (
        best.withColumn("control_id", control_id)
        .withColumn(
            "asset_status",
            F.when(F.col("control_id").isNotNull(), F.lit("ACTIVE"))
            .when(F.array_contains(non_exec, F.col("control_template")), F.lit("DOCUMENTED"))
            .otherwise(F.lit("NEEDS_BINDING")),
        )
    )

    bound = assets_out.filter(F.col("control_id").isNotNull())
    first = Window.partitionBy("control_id").orderBy(F.col("origin_release").asc_nulls_last(), F.col("asset_id"))
    controls = (
        bound.withColumn("_rn", F.row_number().over(first))
        .withColumn("source_incident_count", F.count(F.lit(1)).over(Window.partitionBy("control_id")))
        .filter(F.col("_rn") == 1)
        .select(
            "control_id",
            "asset_id",
            "incident_key",
            "category",
            "control_template",
            F.col("b_check_type").alias("check_type"),
            F.col("b_params_json").alias("params_json"),
            F.col("b_severity").alias("severity"),
            F.when(F.length(F.col("b_description")) > 0, F.col("b_description"))
            .otherwise(F.col("description"))
            .alias("description"),
            F.lit("ACTIVE").alias("status"),
            "origin_release",
            "source_incident_count",
        )
    )
    assets_out = assets_out.drop(*[c for c in assets_out.columns if c.startswith("b_")])
    return assets_out, controls


def empty_controls(spark) -> DataFrame:
    return spark.createDataFrame([], CONTROL_SCHEMA)


def closure_compliance(incidents: DataFrame, assets: DataFrame, cfg: HIFLConfig) -> DataFrame:
    """Evaluate the HIFL closure rule for every (non-duplicate) incident.

    ``closure_status`` is one of:

    * ``COMPLIANT``             closed and linked to at least one preventive asset
    * ``CLOSED_WITHOUT_ASSET``  closed, but no preventive asset (rule violated)
    * ``OPEN_ASSET_READY``      still open, preventive asset already in place
    * ``OPEN_PENDING_ASSET``    still open, preventive asset still needed
    """
    base = incidents.filter(~F.col("is_duplicate")) if "is_duplicate" in incidents.columns else incidents
    per_incident = assets.groupBy("incident_key").agg(
        F.sum(F.when(F.col("asset_status").isin("ACTIVE", "DOCUMENTED"), 1).otherwise(0)).alias("preventive_assets"),
        F.sum(F.when(F.col("asset_status") == "NEEDS_BINDING", 1).otherwise(0)).alias("assets_needing_binding"),
    )
    closed = F.upper(F.coalesce(F.col("status"), F.lit(""))).isin(*[s.upper() for s in cfg.closed_statuses])
    has_asset = F.coalesce(F.col("preventive_assets"), F.lit(0)) > 0
    return (
        base.select("incident_key", "source_system", "source_record_id", "title", "release_cycle", "phase", "status", "category")
        .join(per_incident, "incident_key", "left")
        .fillna({"preventive_assets": 0, "assets_needing_binding": 0})
        .withColumn("is_closed", closed)
        .withColumn(
            "closure_status",
            F.when(closed & has_asset, F.lit("COMPLIANT"))
            .when(closed, F.lit("CLOSED_WITHOUT_ASSET"))
            .when(has_asset, F.lit("OPEN_ASSET_READY"))
            .otherwise(F.lit("OPEN_PENDING_ASSET")),
        )
        .withColumn("closure_rule_ok", F.col("closure_status") != F.lit("CLOSED_WITHOUT_ASSET"))
    )
