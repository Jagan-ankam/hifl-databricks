"""Phase 1 - Incident capture.

Collects incidents from every operational channel (ServiceNow, Jira, hypercare
queues, business escalations, support logs), maps each source onto one canonical
schema, removes exact duplicates, and flags cross-channel duplicates so that no
single ticketing system is treated as complete on its own.
"""

from __future__ import annotations

from functools import reduce
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

from hifl.config import HIFLConfig
from hifl.io import null_if_blank, read_source
from hifl.schemas import INCIDENT_COLUMNS

_SLASH_DATETIME = r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}$"
_SLASH_DATE = r"^\d{1,2}/\d{1,2}/\d{4}$"


def parse_timestamp(col: Column, day_first: bool = False) -> Column:
    """Parse common timestamp layouts without failing under ANSI mode.

    ISO-like values (``2025-03-01``, ``2025-03-01 10:15:00``,
    ``2025-03-01T10:15:00.123Z``) are matched by shape and parsed with
    ``try_to_timestamp``. Slash dates are
    matched with an exact regular expression first, so the parser is only
    applied to values that have the expected shape.
    """
    s = F.trim(col.cast(StringType()))
    dt_fmt = "d/M/yyyy H:mm" if day_first else "M/d/yyyy H:mm"
    d_fmt = "d/M/yyyy" if day_first else "M/d/yyyy"
    iso_value = F.when(
        s.rlike(r"^\d{4}-\d{2}-\d{2}([ T]\d{1,2}:\d{2}(:\d{2}(\.\d{1,9})?)?)?(Z|[+-]\d{2}:?\d{2})?$"),
        F.try_to_timestamp(F.regexp_replace(s, "T", " ")),
    )
    return F.coalesce(
        iso_value,
        F.when(s.rlike(_SLASH_DATETIME), F.try_to_timestamp(s, F.lit(dt_fmt))),
        F.when(s.rlike(_SLASH_DATE), F.try_to_timestamp(s, F.lit(d_fmt))),
    ).cast(TimestampType())


def normalize_source(df: DataFrame, spec: Dict[str, Any]) -> DataFrame:
    """Map one raw source onto the canonical incident schema.

    ``spec["column_map"]`` maps canonical names to source column names, for example
    ``{"source_record_id": "number", "title": "short_description"}``. Canonical
    columns that are neither mapped nor present become nulls. ``spec["defaults"]``
    supplies constant values (e.g. ``{"release_cycle": "R1 Hypercare"}``).
    """
    column_map: Dict[str, str] = spec.get("column_map", {})
    defaults: Dict[str, str] = spec.get("defaults", {})
    day_first = bool(spec.get("day_first", False))
    available = set(df.columns)

    selected: List[Column] = []
    for name, dtype in INCIDENT_COLUMNS.items():
        source_col = column_map.get(name, name)
        if source_col in available:
            raw = F.col(f"`{source_col}`")
        else:
            raw = F.lit(None).cast(StringType())
        if name in defaults:
            raw = F.coalesce(null_if_blank(raw.cast(StringType())), F.lit(str(defaults[name])))
        if isinstance(dtype, TimestampType):
            value = parse_timestamp(raw, day_first=day_first)
        else:
            value = null_if_blank(raw.cast(StringType()))
        selected.append(value.alias(name))

    source_name = spec.get("source_system", spec.get("name", "UNKNOWN"))
    return df.select(F.lit(str(source_name)).alias("source_system"), *selected)


def _derive_phase(cfg: HIFLConfig) -> Column:
    basis = F.coalesce(F.col("phase"), F.col("release_cycle"), F.lit(""))
    return (
        F.when(basis.rlike(cfg.hypercare_pattern), F.lit("HYPERCARE"))
        .when(basis.rlike(cfg.uat_pattern), F.lit("UAT"))
        .when(F.col("phase").isNotNull(), F.upper(F.col("phase")))
        .otherwise(F.lit("UNKNOWN"))
    )


def standardize_incidents(df: DataFrame, cfg: HIFLConfig) -> DataFrame:
    """Add keys, derive the discovery phase, and deduplicate.

    * ``incident_key``: stable SHA-256 key per (source system, source record id).
    * exact duplicates of the same key keep the most recent version.
    * ``is_duplicate`` / ``duplicate_of``: the same problem raised through more
      than one channel (same normalized title within the same release cycle).
    """
    fallback_id = F.sha2(
        F.concat_ws("||", F.coalesce(F.col("title"), F.lit("")), F.coalesce(F.col("opened_at").cast("string"), F.lit(""))),
        256,
    )
    keyed = (
        df.withColumn("source_record_id", F.coalesce(F.col("source_record_id"), fallback_id))
        .withColumn(
            "incident_key",
            F.sha2(F.concat_ws("||", F.lower(F.col("source_system")), F.col("source_record_id")), 256),
        )
        .withColumn("phase", _derive_phase(cfg))
        .withColumn("status", F.upper(F.col("status")))
        .withColumn("priority", F.upper(F.col("priority")))
        .withColumn("business_object", F.upper(F.col("business_object")))
    )

    latest = Window.partitionBy("incident_key").orderBy(
        F.coalesce(F.col("resolved_at"), F.col("opened_at")).desc_nulls_last(), F.col("source_system")
    )
    deduped = keyed.withColumn("_rn", F.row_number().over(latest)).filter(F.col("_rn") == 1).drop("_rn")

    norm_title = F.regexp_replace(F.lower(F.coalesce(F.col("title"), F.lit(""))), "[^a-z0-9]", "")
    dup_key = F.when(
        F.length(norm_title) > 0,
        F.sha2(F.concat_ws("||", norm_title, F.coalesce(F.col("release_cycle"), F.lit(""))), 256),
    )
    with_dup = deduped.withColumn("_dup_key", dup_key)
    first_seen = Window.partitionBy("_dup_key").orderBy(
        F.col("opened_at").asc_nulls_last(), F.col("source_system"), F.col("incident_key")
    )
    return (
        with_dup.withColumn("_dup_rn", F.row_number().over(first_seen))
        .withColumn("_dup_first", F.first("incident_key").over(first_seen))
        .withColumn("is_duplicate", F.col("_dup_key").isNotNull() & (F.col("_dup_rn") > 1))
        .withColumn("duplicate_of", F.when(F.col("is_duplicate"), F.col("_dup_first")))
        .withColumn("captured_at", F.current_timestamp())
        .drop("_dup_key", "_dup_rn", "_dup_first")
    )


def capture_incidents(
    spark: SparkSession,
    sources: Iterable[Dict[str, Any]],
    cfg: HIFLConfig,
    base_dir: Optional[str] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Run Phase 1 over all configured sources.

    Returns ``(raw_union, incidents)`` where ``raw_union`` is the canonical union
    before deduplication and ``incidents`` is the standardized, deduplicated set.
    """
    frames = [normalize_source(read_source(spark, spec, base_dir), spec) for spec in sources]
    if not frames:
        raise ValueError("At least one incident source must be configured")
    raw_union = reduce(lambda a, b: a.unionByName(b), frames)
    return raw_union, standardize_incidents(raw_union, cfg)
