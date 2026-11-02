import pytest

from hifl.rules_engine import run_check


@pytest.fixture(scope="module")
def tables(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS re_test")
    spark.createDataFrame([("a", "1", 10.0), ("b", None, 5.0), ("b", "3", 2.5)], "id string, code string, amt double") \
        .write.mode("overwrite").format("parquet").saveAsTable("re_test.t")
    spark.createDataFrame([("a",), ("c",)], "id string").write.mode("overwrite").format("parquet").saveAsTable("re_test.parent")
    spark.createDataFrame([("1",), ("3",)], "code string").write.mode("overwrite").format("parquet").saveAsTable("re_test.map")
    spark.createDataFrame([("x", 10.0), ("y", 7.5)], "id string, amt double").write.mode("overwrite").format("parquet").saveAsTable("re_test.t2")
    return "re_test"


def test_not_null(spark, tables):
    assert run_check(spark, "not_null", {"table": "re_test.t", "columns": ["id"]})[0] == 0
    failed, checked, _ = run_check(spark, "not_null", {"table": "re_test.t", "columns": ["id", "code"]})
    assert (failed, checked) == (1, 3)


def test_unique(spark, tables):
    assert run_check(spark, "unique", {"table": "re_test.t", "columns": "id"})[0] == 1
    assert run_check(spark, "unique", {"table": "re_test.t", "columns": ["id", "code"]})[0] == 0


def test_accepted_values(spark, tables):
    assert run_check(spark, "accepted_values", {"table": "re_test.t", "column": "code", "values": ["1", "3"]})[0] == 1
    assert run_check(spark, "accepted_values", {"table": "re_test.t", "column": "code", "values": ["1", "3"], "allow_null": True})[0] == 0


def test_referential_integrity(spark, tables):
    failed, checked, msg = run_check(
        spark, "referential_integrity", {"table": "re_test.t", "column": "id", "ref_table": "re_test.parent", "ref_column": "id"}
    )
    assert (failed, checked) == (2, 3)


def test_mapping_coverage_and_sql_source(spark, tables):
    failed, checked, msg = run_check(
        spark,
        "mapping_coverage",
        {"source_table_sql": "SELECT code FROM re_test.t", "source_column": "code", "mapping_table": "re_test.map", "mapping_column": "code"},
    )
    assert (failed, checked) == (0, 2)


def test_row_count_and_control_total(spark, tables):
    failed, _, _ = run_check(spark, "row_count_reconciliation", {"source_table": "re_test.t", "target_table": "re_test.t2"})
    assert failed == 1
    failed, _, _ = run_check(
        spark, "row_count_reconciliation", {"source_table": "re_test.t", "target_table": "re_test.t2", "tolerance_pct": 50}
    )
    assert failed == 0
    assert run_check(spark, "control_total", {"source_table": "re_test.t", "target_table": "re_test.t2", "column": "amt"})[0] == 0


def test_expression_and_sql_assertion(spark, tables):
    assert run_check(spark, "expression", {"table": "re_test.t", "condition": "amt > 3"})[0] == 1
    # null condition result counts as a failure
    assert run_check(spark, "expression", {"table": "re_test.t", "condition": "code IS NOT NULL AND code <> '9'"})[0] == 1
    assert run_check(spark, "sql_assertion", {"sql": "SELECT * FROM re_test.t WHERE amt < 0"})[0] == 0


def test_bad_inputs_raise(spark, tables):
    with pytest.raises(ValueError):
        run_check(spark, "nope", {})
    with pytest.raises(ValueError):
        run_check(spark, "not_null", {"table": "re_test.t"})
