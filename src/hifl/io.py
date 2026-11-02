"""Read/write helpers that work on Databricks (Unity Catalog or hive_metastore),
on serverless / Spark Connect, and on local Spark.

The helpers deliberately avoid ``sparkContext`` and RDD APIs so that they run on
shared-access and serverless compute.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from hifl.config import HIFLConfig


def ensure_schema(spark: SparkSession, cfg: HIFLConfig) -> None:
    """Create the HIFL schema if it does not exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_fqn}")


def write_table(df: DataFrame, cfg: HIFLConfig, name: str, mode: str = "overwrite") -> str:
    """Write ``df`` as a managed HIFL table and return the table name."""
    target = cfg.table(name)
    writer = df.write.format(cfg.table_format).mode(mode)
    if cfg.table_format == "delta":
        writer = writer.option("overwriteSchema", "true") if mode == "overwrite" else writer.option(
            "mergeSchema", "true"
        )
    writer.saveAsTable(target)
    return target


def read_table(spark: SparkSession, cfg: HIFLConfig, name: str) -> DataFrame:
    return spark.table(cfg.table(name))


def table_exists(spark: SparkSession, fqn: str) -> bool:
    """Spark-Connect-safe existence check for a (possibly quoted) table name."""
    try:
        spark.table(fqn).limit(0).collect()
        return True
    except Exception:  # noqa: BLE001 - any analysis error means "not there"
        return False


def read_csv_local(spark: SparkSession, path: str) -> DataFrame:
    """Read a small CSV that lives next to the code (e.g. in a Databricks Git folder).

    The file is read on the driver with Python's ``csv`` module and turned into a
    DataFrame of strings. This works on every Databricks access mode, including
    serverless, where ``spark.read`` cannot always see workspace files. Empty
    cells become nulls. Use :func:`read_source` for large or cloud-stored files.
    """
    import csv

    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        try:
            columns = [c.strip() for c in next(reader)]
        except StopIteration as exc:
            raise ValueError(f"CSV file is empty: {path}") from exc
        width = len(columns)
        rows = []
        for rec in reader:
            if not rec or all(not v.strip() for v in rec):
                continue
            rec = (rec + [""] * width)[:width]
            rows.append(tuple(None if v.strip() == "" else v for v in rec))
    schema = ", ".join("`" + c.replace("`", "``") + "` STRING" for c in columns)
    return spark.createDataFrame(rows, schema=schema)


def read_source(spark: SparkSession, spec: Dict[str, Any], base_dir: Optional[str] = None) -> DataFrame:
    """Read one incident source described by a source spec.

    Supported spec keys::

        {"name": "servicenow",
         "table": "main.itsm.incidents"}                         # an existing table
        {"name": "jira", "format": "csv", "path": "/Volumes/...",  # files via Spark
         "options": {"header": "true"}}
        {"name": "sample", "local_csv": "sample_data/x.csv"}       # small repo file

    Relative ``local_csv`` paths are resolved against ``base_dir``.
    """
    if "table" in spec:
        return spark.table(spec["table"])
    if "local_csv" in spec:
        path = spec["local_csv"]
        if base_dir and not os.path.isabs(path):
            path = os.path.join(base_dir, path)
        return read_csv_local(spark, path)
    if "path" in spec:
        fmt = spec.get("format", "csv")
        options = {"header": "true"} if fmt == "csv" else {}
        options.update(spec.get("options", {}))
        return spark.read.format(fmt).options(**options).load(spec["path"])
    raise ValueError(f"Source spec {spec.get('name', '?')!r} needs one of: table, path, local_csv")


def null_if_blank(col):
    """Trim a string column and turn empty strings into null."""
    trimmed = F.trim(col)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)
