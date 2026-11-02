"""Phase 3 - Root-cause intelligence.

Applies the same lightweight structure to every incident so that root causes can
be aggregated across incidents and releases:

* ``five_whys`` - the root-cause narrative split into its "why" steps
  (separate steps with ``->``, ``;`` or ``|`` in the source text);
* ``primary_root_cause`` - the deepest (last) why;
* ``fishbone_dimension`` - Ishikawa dimension inferred from keywords;
* ``root_cause_key`` - stable key used to detect recurrence across releases.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from hifl.classify import keyword_pattern
from hifl.taxonomy import FISHBONE_RULES


def _fishbone(text: Column, rules: Dict[str, List[str]]) -> Column:
    expr: Optional[Column] = None
    for dimension, keywords in rules.items():
        cond = None
        for kw in keywords:
            c = text.rlike(keyword_pattern(kw))
            cond = c if cond is None else (cond | c)
        if cond is None:
            continue
        expr = F.when(cond, F.lit(dimension)) if expr is None else expr.when(cond, F.lit(dimension))
    return F.lit("UNDETERMINED") if expr is None else expr.otherwise(F.lit("UNDETERMINED"))


def analyse_root_causes(df: DataFrame, fishbone_rules: Optional[Dict[str, List[str]]] = None) -> DataFrame:
    """Add structured root-cause columns and cross-release recurrence flags."""
    rules = fishbone_rules or FISHBONE_RULES
    whys = F.filter(
        F.transform(
            F.split(F.coalesce(F.col("root_cause_text"), F.lit("")), r"\s*(->|;|\|)\s*"),
            lambda x: F.trim(x),
        ),
        lambda x: F.length(x) > 0,
    )
    primary = F.when(F.size(F.col("five_whys")) > 0, F.element_at(F.col("five_whys"), -1))
    rca_text = F.lower(
        F.concat_ws(" ", F.coalesce(F.col("root_cause_text"), F.lit("")), F.coalesce(F.col("resolution_text"), F.lit("")))
    )
    norm_primary = F.trim(F.regexp_replace(F.lower(F.col("primary_root_cause")), "[^a-z0-9]+", " "))

    out = (
        df.withColumn("five_whys", whys)
        .withColumn("why_depth", F.size(F.col("five_whys")))
        .withColumn("primary_root_cause", primary)
        .withColumn("fishbone_dimension", _fishbone(rca_text, rules))
        .withColumn(
            "root_cause_key",
            F.when(
                F.col("primary_root_cause").isNotNull(),
                F.sha2(F.concat_ws("||", F.col("category"), norm_primary), 256),
            ),
        )
    )
    by_cause = Window.partitionBy("root_cause_key")
    releases = F.size(F.collect_set(F.coalesce(F.col("release_cycle"), F.lit("?"))).over(by_cause))
    return out.withColumn(
        "releases_affected", F.when(F.col("root_cause_key").isNotNull(), releases).otherwise(F.lit(0))
    ).withColumn("is_recurring", F.col("releases_affected") > 1)


def root_cause_summary(df: DataFrame) -> DataFrame:
    """Aggregate root causes by category and fishbone dimension."""
    base = df.filter(~F.col("is_duplicate")) if "is_duplicate" in df.columns else df
    return (
        base.groupBy("category", "fishbone_dimension")
        .agg(
            F.count(F.lit(1)).alias("incidents"),
            F.countDistinct("root_cause_key").alias("distinct_root_causes"),
            F.sum(F.col("is_recurring").cast("int")).alias("recurring_incidents"),
        )
        .orderBy(F.col("incidents").desc(), "category")
    )
