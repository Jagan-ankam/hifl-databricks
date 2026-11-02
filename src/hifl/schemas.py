"""Canonical schemas used across the HIFL pipeline."""

from __future__ import annotations

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Canonical incident columns produced by Phase 1 (capture).
INCIDENT_COLUMNS = {
    "source_record_id": StringType(),
    "title": StringType(),
    "description": StringType(),
    "release_cycle": StringType(),
    "phase": StringType(),
    "priority": StringType(),
    "status": StringType(),
    "opened_at": TimestampType(),
    "resolved_at": TimestampType(),
    "business_object": StringType(),
    "assignment_group": StringType(),
    "root_cause_text": StringType(),
    "resolution_text": StringType(),
    "manual_category": StringType(),
}

TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd",
    "dd/MM/yyyy HH:mm",
    "dd/MM/yyyy",
    "MM/dd/yyyy HH:mm",
    "MM/dd/yyyy",
]

CONTROL_SCHEMA = StructType(
    [
        StructField("control_id", StringType(), False),
        StructField("asset_id", StringType(), True),
        StructField("incident_key", StringType(), True),
        StructField("category", StringType(), True),
        StructField("control_template", StringType(), True),
        StructField("check_type", StringType(), True),
        StructField("params_json", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("description", StringType(), True),
        StructField("status", StringType(), True),
        StructField("origin_release", StringType(), True),
        StructField("source_incident_count", LongType(), True),
    ]
)

RESULT_SCHEMA = StructType(
    [
        StructField("control_id", StringType(), False),
        StructField("check_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("result", StringType(), True),
        StructField("failed_count", LongType(), True),
        StructField("checked_count", LongType(), True),
        StructField("message", StringType(), True),
        StructField("duration_ms", IntegerType(), True),
    ]
)
