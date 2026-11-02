import json
import os

import pytest

from hifl.config import HIFLConfig
from hifl.controls import validate_bindings
from hifl.demo import load_demo_tables
from hifl.pipeline import main, run_from_config


@pytest.fixture(scope="module")
def local_config(tmp_path_factory, repo_root):
    """The shipped conf/hifl_config.json, switched to local Spark settings."""
    with open(os.path.join(repo_root, "conf", "hifl_config.json"), encoding="utf-8") as fh:
        conf = json.load(fh)
    conf["hifl"].update({"catalog": None, "schema": "hifl_e2e", "table_format": "parquet"})
    conf_dir = tmp_path_factory.mktemp("conf")
    path = conf_dir / "hifl_config.json"
    path.write_text(json.dumps(conf))
    return str(path)


@pytest.fixture(scope="module")
def run(spark, local_config, repo_root):
    cfg = HIFLConfig(catalog=None, schema="hifl_e2e", table_format="parquet")
    load_demo_tables(spark, cfg, repo_root)
    return run_from_config(spark, local_config, base_dir=repo_root)


def _metric(run, name):
    return {r["metric"]: r["value"] for r in run.metrics.collect()}[name]


def test_release_distribution_matches_paper(spark, run):
    dist = {r["release_cycle"]: (r["events"], r["share_pct"]) for r in spark.table("hifl_e2e.hifl_release_distribution").collect()}
    assert dist == {"R1 Hypercare": (35, 54.7), "R2C1 Hypercare": (11, 17.2), "R2C2/R5 UAT": (18, 28.1)}
    assert _metric(run, "total_quality_events") == 64
    assert _metric(run, "cross_channel_duplicates") == 3
    assert _metric(run, "hypercare_discovery_rate") == 71.88


def test_all_tables_written(spark, run):
    names = {r["tableName"] for r in spark.sql("SHOW TABLES IN hifl_e2e").collect()}
    expected = {
        "hifl_incidents_raw", "hifl_incidents", "hifl_knowledge_assets", "hifl_controls", "hifl_closure_compliance",
        "hifl_regression_results", "hifl_release_distribution", "hifl_category_distribution",
        "hifl_root_cause_summary", "hifl_metrics",
    }
    assert expected <= names


def test_classification_and_assets(run):
    assert _metric(run, "classification_coverage") == 100.0
    statuses = {r["asset_status"] for r in run.assets.collect()}
    assert statuses <= {"ACTIVE", "DOCUMENTED", "NEEDS_BINDING"}
    assert _metric(run, "active_controls") == 7
    assert _metric(run, "knowledge_reuse_ratio") > 1


def test_regression_suite_and_gate(run):
    results = {r["check_type"] + ":" + r["severity"]: r["result"] for r in run.results.collect()}
    assert results["mapping_coverage:HIGH"] == "FAIL"        # RC06 unmapped (by design)
    assert results["referential_integrity:HIGH"] == "FAIL"   # orphan supplier (by design)
    assert results["not_null:MEDIUM"] == "FAIL"              # non-blocking
    assert results["row_count_reconciliation:HIGH"] == "PASS"
    assert results["expression:CRITICAL"] == "PASS"
    assert results["control_total:HIGH"] == "PASS"
    assert results["unique:HIGH"] == "PASS"
    assert run.gate.passed is False
    assert len(run.gate.blocking) == 2
    assert "FAILED" in run.gate.summary()


def test_closure_rule(run):
    rows = run.closure.collect()
    assert len(rows) == 64
    assert {r["closure_status"] for r in rows} <= {"COMPLIANT", "CLOSED_WITHOUT_ASSET", "OPEN_ASSET_READY", "OPEN_PENDING_ASSET"}
    for r in rows:
        assert r["closure_rule_ok"] == (r["closure_status"] != "CLOSED_WITHOUT_ASSET")


def test_rerun_appends_results(spark, local_config, repo_root, run):
    before = spark.table("hifl_e2e.hifl_regression_results").count()
    rerun = run_from_config(spark, local_config, base_dir=repo_root, release_id="R4")
    after = spark.table("hifl_e2e.hifl_regression_results").count()
    assert after == before + rerun.results.count()


def test_cli_exit_codes(spark, local_config, repo_root, run):
    args = ["--config", local_config, "--base-dir", repo_root, "--release-id", "cli"]
    assert main(args) == 0
    assert main(args + ["--fail-on-gate"]) == 1


def test_invalid_binding_rejected():
    with pytest.raises(ValueError):
        validate_bindings([{"control_template": "x", "check_type": "bogus", "params": {}}])
    with pytest.raises(ValueError):
        validate_bindings([{"control_template": "x", "params": {}}])
