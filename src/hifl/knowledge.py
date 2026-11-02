"""Phase 4 - Knowledge extraction.

Translates each classified incident into one or more knowledge assets
(validation rules, governance controls, monitoring rules, test cases, runbook
updates) using the category-to-asset mapping of the paper's Table I.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from hifl.taxonomy import CATEGORY_ASSET_MAP, UNCLASSIFIED

_ASSET_STRUCT = ArrayType(
    StructType([StructField("asset_type", StringType()), StructField("control_template", StringType())])
)


def extract_knowledge_assets(
    incidents: DataFrame,
    asset_map: Optional[Dict[str, List[Tuple[str, str]]]] = None,
) -> DataFrame:
    """Return one row per (incident, knowledge asset).

    Cross-channel duplicates (``is_duplicate = true``) are skipped so that the
    same problem does not generate the same asset twice.
    """
    mapping = {k.upper(): [tuple(x) for x in v] for k, v in (asset_map or CATEGORY_ASSET_MAP).items()}
    pairs = []
    for cat, assets in mapping.items():
        pairs.append(F.lit(cat))
        pairs.append(
            F.array(*[F.struct(F.lit(a).alias("asset_type"), F.lit(t).alias("control_template")) for a, t in assets])
        )
    asset_lookup = F.create_map(*pairs)
    fallback = mapping.get(UNCLASSIFIED, [("RUNBOOK_UPDATE", "triage_runbook")])
    fallback_arr = F.array(
        *[F.struct(F.lit(a).alias("asset_type"), F.lit(t).alias("control_template")) for a, t in fallback]
    )

    base = incidents.filter(~F.col("is_duplicate")) if "is_duplicate" in incidents.columns else incidents
    exploded = base.withColumn(
        "_asset", F.explode(F.coalesce(F.element_at(asset_lookup, F.col("category")), fallback_arr).cast(_ASSET_STRUCT))
    )
    return exploded.select(
        F.sha2(
            F.concat_ws("||", F.col("incident_key"), F.col("_asset.asset_type"), F.col("_asset.control_template")), 256
        ).alias("asset_id"),
        "incident_key",
        "source_system",
        "source_record_id",
        "category",
        "dq_dimension",
        F.col("_asset.asset_type").alias("asset_type"),
        F.col("_asset.control_template").alias("control_template"),
        "business_object",
        F.col("release_cycle").alias("origin_release"),
        "primary_root_cause",
        F.concat(
            F.col("_asset.control_template"),
            F.lit(" derived from incident "),
            F.col("source_record_id"),
            F.lit(": "),
            F.coalesce(F.col("title"), F.lit("(no title)")),
        ).alias("description"),
        F.lit("PROPOSED").alias("asset_status"),
        F.current_timestamp().alias("created_at"),
    )
