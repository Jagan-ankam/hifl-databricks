"""Phase 2 - Incident classification.

A transparent, rule-based classifier assigns every incident to one of the six
HIFL defect categories. Rules are plain keyword lists, so data quality leads can
review and tune them. A manual category supplied by the support team always
wins over the rule-based prediction.

The implementation uses only native Spark functions (no Python UDFs), which
keeps it fast and compatible with serverless / shared-access compute.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from hifl.taxonomy import (
    CATEGORIES,
    CATEGORY_DQ_DIMENSION,
    DEFAULT_CLASSIFICATION_RULES,
    UNCLASSIFIED,
)

TEXT_COLUMNS = ["title", "description", "root_cause_text", "resolution_text", "business_object"]


def keyword_pattern(keyword: str) -> str:
    """Whole-word / whole-phrase, case-insensitive Java regex for a keyword."""
    escaped = re.escape(keyword.strip().lower())
    return rf"(^|[^a-z0-9]){escaped}($|[^a-z0-9])"


def _ordered_categories(rules: Dict[str, List[str]]) -> List[str]:
    extra = [c for c in rules if c not in CATEGORIES]
    return [c for c in CATEGORIES if c in rules] + sorted(extra)


def _mapping_expr(mapping: Dict[str, str], key: Column, default: str) -> Column:
    pairs: List[Column] = []
    for k, v in mapping.items():
        pairs.extend([F.lit(k), F.lit(v)])
    if not pairs:
        return F.lit(default)
    return F.coalesce(F.element_at(F.create_map(*pairs), key), F.lit(default))


def classify_incidents(
    df: DataFrame,
    rules: Optional[Dict[str, List[str]]] = None,
    dq_dimensions: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Add classification columns to an incident DataFrame.

    Added columns
    -------------
    predicted_category      rule-based category or ``UNCLASSIFIED``
    category_scores         map of keyword hits per category
    classification_confidence  share of keyword hits won by the predicted category
    category                final category (manual override, else prediction)
    classification_method   ``MANUAL``, ``RULE`` or ``NONE``
    dq_dimension            data quality dimension implied by the category
    """
    rules = {k.upper(): v for k, v in (rules or DEFAULT_CLASSIFICATION_RULES).items()}
    dq_dimensions = {k.upper(): v for k, v in (dq_dimensions or CATEGORY_DQ_DIMENSION).items()}
    categories = _ordered_categories(rules)
    if not categories:
        raise ValueError("Classification rules are empty")

    present = [c for c in TEXT_COLUMNS if c in df.columns]
    text = F.lower(F.concat_ws(" ", *[F.coalesce(F.col(c), F.lit("")) for c in present]))

    score_cols: Dict[str, Column] = {}
    for cat in categories:
        hits = [F.when(text.rlike(keyword_pattern(kw)), F.lit(1)).otherwise(F.lit(0)) for kw in rules[cat] if kw.strip()]
        score_cols[cat] = sum(hits[1:], hits[0]) if hits else F.lit(0)

    scored = df
    for cat, col in score_cols.items():
        scored = scored.withColumn(f"_score_{cat}", col.cast("int"))

    n = len(categories)
    candidates = [
        F.struct(
            F.col(f"_score_{cat}").alias("score"),
            F.lit(n - i).alias("priority"),
            F.lit(cat).alias("category"),
        )
        for i, cat in enumerate(categories)
    ]
    best = F.greatest(*candidates) if n > 1 else candidates[0]
    total = sum((F.col(f"_score_{c}") for c in categories[1:]), F.col(f"_score_{categories[0]}"))

    scored = (
        scored.withColumn("_best", best)
        .withColumn("_total", total)
        .withColumn(
            "predicted_category",
            F.when(F.col("_best.score") > 0, F.col("_best.category")).otherwise(F.lit(UNCLASSIFIED)),
        )
        .withColumn(
            "classification_confidence",
            F.when(F.col("_total") > 0, F.round(F.col("_best.score") / F.col("_total"), 3)).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "category_scores",
            F.create_map(*[x for c in categories for x in (F.lit(c), F.col(f"_score_{c}"))]),
        )
    )

    valid = F.array(*[F.lit(c) for c in categories])
    if "manual_category" in scored.columns:
        manual = F.upper(F.trim(F.col("manual_category")))
        manual_ok = manual.isNotNull() & F.array_contains(valid, manual)
    else:
        manual = F.lit(None).cast("string")
        manual_ok = F.lit(False)

    result = (
        scored.withColumn("category", F.when(manual_ok, manual).otherwise(F.col("predicted_category")))
        .withColumn(
            "classification_method",
            F.when(manual_ok, F.lit("MANUAL"))
            .when(F.col("predicted_category") != F.lit(UNCLASSIFIED), F.lit("RULE"))
            .otherwise(F.lit("NONE")),
        )
    )
    result = result.withColumn("dq_dimension", _mapping_expr(dq_dimensions, F.col("category"), "UNKNOWN"))
    return result.drop("_best", "_total", *[f"_score_{c}" for c in categories])
