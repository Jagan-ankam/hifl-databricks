"""Declarative data quality checks executed with PySpark.

Each check takes a ``params`` dict (stored as JSON in the controls table) and
returns ``(failed_count, checked_count, message)``. A check passes when
``failed_count == 0``, except where a tolerance applies.

Datasets are referenced by table name (``"table": "main.sales.orders"``) or by
a SQL query (``"table_sql": "SELECT ... FROM ..."``). The same convention holds
for every dataset parameter, e.g. ``ref_table`` / ``ref_table_sql``.

Supported ``check_type`` values
------------------------------
not_null                 table, columns
unique                   table, columns
accepted_values          table, column, values, [allow_null]
referential_integrity    table, column, ref_table, ref_column
mapping_coverage         source_table, source_column, mapping_table, mapping_column
row_count_reconciliation source_table, target_table, [tolerance_pct]
control_total            source_table, target_table, column | source_column+target_column, [tolerance_abs]
expression               table, condition   (SQL boolean that every row must satisfy)
sql_assertion            sql                (query returning the violating rows)
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CheckResult = Tuple[int, int, str]


def _dataset(spark: SparkSession, params: Dict[str, Any], key: str) -> DataFrame:
    if params.get(key):
        return spark.table(params[key])
    if params.get(f"{key}_sql"):
        return spark.sql(params[f"{key}_sql"])
    raise ValueError(f"Missing parameter '{key}' (or '{key}_sql')")


def _require(params: Dict[str, Any], *keys: str) -> None:
    missing = [k for k in keys if params.get(k) in (None, "", [])]
    if missing:
        raise ValueError(f"Missing parameter(s): {', '.join(missing)}")


def _as_list(value: Any) -> List[str]:
    return [value] if isinstance(value, str) else list(value)


def _c(name: str):
    return F.col(f"`{name}`")


def check_not_null(spark: SparkSession, p: Dict[str, Any]) -> CheckResult:
    _require(p, "columns")
    df = _dataset(spark, p, "table")
    cols = _as_list(p["columns"])
    cond = None
    for c in cols:
        cond = _c(c).isNull() if cond is None else (cond | _c(c).isNull())
    row = df.agg(F.count(F.lit(1)).alias("n"), F.sum(F.when(cond, 1).otherwise(0)).alias("bad")).collect()[0]
    bad = int(row["bad"] or 0)
    return bad, int(row["n"]), f"{bad} row(s) with null in {cols}"


def check_unique(spark: SparkSession, p: Dict[str, Any]) -> CheckResult:
    _require(p, "columns")
    df = _dataset(spark, p, "table")
    cols = _as_list(p["columns"])
    total = df.count()
    dupes = (
        df.groupBy(*[_c(c) for c in cols])
        .agg(F.count(F.lit(1)).alias("n"))
        .filter(F.col("n") > 1)
        .agg(F.sum(F.col("n") - 1).alias("extra"))
        .collect()[0]["extra"]
    )
    bad = int(dupes or 0)
    return bad, total, f"{bad} duplicate row(s) on key {cols}"


def check_accepted_values(spark: SparkSession, p: Dict[str, Any]) -> CheckResult:
    _require(p, "column", "values")
    df = _dataset(spark, p, "table")
    col = _c(p["column"])
    allowed = [str(v) for v in p["values"]]
    allow_null = bool(p.get("allow_null", False))
    invalid = ~col.cast("string").isin(allowed)
    cond = (col.isNotNull() & invalid) if allow_null else (col.isNull() | invalid)
    row = df.agg(F.count(F.lit(1)).alias("n"), F.sum(F.when(cond, 1).otherwise(0)).alias("bad")).collect()[0]
    bad = int(row["bad"] or 0)
    return bad, int(row["n"]), f"{bad} row(s) in {p['column']} outside accepted values"


def check_referential_integrity(spark: SparkSession, p: Dict[str, Any]) -> CheckResult:
    _require(p, "column", "ref_column")
    child = _dataset(spark, p, "table").select(_c(p["column"]).cast("string").alias("_k")).filter(F.col("_k").isNotNull())
    parent = _dataset(spark, p, "ref_table").select(_c(p["ref_column"]).cast("string").alias("_k")).distinct()
    total = child.count()
    orphans = child.join(parent, on="_k", how="left_anti").count()
    return orphans, total, f"{orphans} row(s) in {p['column']} without a match in {p['ref_column']}"


def check_mapping_coverage(spark: SparkSession, p: Dict[str, Any]) -> CheckResult:
    _require(p, "source_column", "mapping_column")
    src = (
        _dataset(spark, p, "source_table")
        .select(_c(p["source_column"]).cast("string").alias("_v"))
        .filter(F.col("_v").isNotNull())
        .distinct()
    )
    mapped = _dataset(spark, p, "mapping_table").select(_c(p["mapping_column"]).cast("string").alias("_v")).distinct()
    total = src.count()
    unmapped_df = src.join(mapped, on="_v", how="left_anti")
    unmapped = unmapped_df.count()
    sample = [r["_v"] for r in unmapped_df.orderBy("_v").limit(5).collect()]
    msg = f"{unmapped} of {total} distinct source value(s) have no mapping"
    if sample:
        msg += f" (e.g. {', '.join(sample)})"
    return unmapped, total, msg


def check_row_count_reconciliation(spark: SparkSession, p: Dict[str, Any]) -> CheckResult:
    src = _dataset(spark, p, "source_table")
    tgt = _dataset(spark, p, "target_table")
    if p.get("source_filter"):
        src = src.filter(F.expr(p["source_filter"]))
    if p.get("target_filter"):
        tgt = tgt.filter(F.expr(p["target_filter"]))
    n_src, n_tgt = src.count(), tgt.count()
    diff = abs(n_src - n_tgt)
    tolerance = float(p.get("tolerance_pct", 0.0)) / 100.0 * n_src
    failed = 0 if diff <= tolerance else diff
    return failed, n_src, f"source={n_src}, target={n_tgt}, difference={diff}, tolerance={tolerance:.1f}"


def check_control_total(spark: SparkSession, p: Dict[str, Any]) -> CheckResult:
    s_col = p.get("source_column") or p.get("column")
    t_col = p.get("target_column") or p.get("column")
    if not s_col or not t_col:
        raise ValueError("Missing parameter 'column' (or 'source_column' and 'target_column')")
    src = _dataset(spark, p, "source_table")
    tgt = _dataset(spark, p, "target_table")
    s_row = src.agg(F.sum(_c(s_col).cast("double")).alias("t"), F.count(F.lit(1)).alias("n")).collect()[0]
    t_total = tgt.agg(F.sum(_c(t_col).cast("double")).alias("t")).collect()[0]["t"]
    s_total = float(s_row["t"] or 0.0)
    t_total = float(t_total or 0.0)
    diff = abs(s_total - t_total)
    tolerance = float(p.get("tolerance_abs", 0.0))
    failed = 0 if diff <= tolerance else 1
    return failed, int(s_row["n"]), f"source total={s_total:.2f}, target total={t_total:.2f}, difference={diff:.2f}"


def check_expression(spark: SparkSession, p: Dict[str, Any]) -> CheckResult:
    _require(p, "condition")
    df = _dataset(spark, p, "table")
    ok = F.coalesce(F.expr(p["condition"]).cast("boolean"), F.lit(False))
    row = df.agg(F.count(F.lit(1)).alias("n"), F.sum(F.when(~ok, 1).otherwise(0)).alias("bad")).collect()[0]
    bad = int(row["bad"] or 0)
    return bad, int(row["n"]), f"{bad} row(s) violate: {p['condition']}"


def check_sql_assertion(spark: SparkSession, p: Dict[str, Any]) -> CheckResult:
    _require(p, "sql")
    bad = spark.sql(p["sql"]).count()
    return bad, bad, f"{bad} violating row(s) returned by assertion query"


CHECKS: Dict[str, Callable[[SparkSession, Dict[str, Any]], CheckResult]] = {
    "not_null": check_not_null,
    "unique": check_unique,
    "accepted_values": check_accepted_values,
    "referential_integrity": check_referential_integrity,
    "mapping_coverage": check_mapping_coverage,
    "row_count_reconciliation": check_row_count_reconciliation,
    "control_total": check_control_total,
    "expression": check_expression,
    "sql_assertion": check_sql_assertion,
}


def run_check(spark: SparkSession, check_type: str, params: Dict[str, Any]) -> CheckResult:
    """Run one check. Raises ``ValueError`` for unknown check types or bad params."""
    try:
        fn = CHECKS[check_type]
    except KeyError as exc:
        raise ValueError(f"Unknown check_type '{check_type}'. Supported: {sorted(CHECKS)}") from exc
    return fn(spark, params or {})
