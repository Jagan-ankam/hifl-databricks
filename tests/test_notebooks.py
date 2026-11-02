"""Smoke-test the Databricks notebooks locally with stand-ins for dbutils/display."""

import os

import pytest

NOTEBOOKS = ["00_setup.py", "01_run_hifl_pipeline.py", "02_release_gate_ibrt.py", "03_hifl_dashboard.py"]


class _Widgets:
    def __init__(self, overrides):
        self._values = {}
        self._overrides = overrides

    def text(self, name, default, label=None):
        self._values[name] = self._overrides.get(name, default)

    def dropdown(self, name, default, choices, label=None):
        value = self._overrides.get(name, default)
        assert value in choices, f"{name}={value} not in {choices}"
        self._values[name] = value

    def get(self, name):
        return self._values[name]


class _DBUtils:
    def __init__(self, overrides):
        self.widgets = _Widgets(overrides)


def _run_notebook(path, spark, overrides):
    displayed = []

    def display(df):
        displayed.append(df.limit(20).collect())

    with open(path, encoding="utf-8") as fh:
        source = fh.read()
    assert source.startswith("# Databricks notebook source")
    namespace = {"spark": spark, "dbutils": _DBUtils(overrides), "display": display, "__name__": "__main__"}
    cwd = os.getcwd()
    os.chdir(os.path.dirname(path))
    try:
        exec(compile(source, path, "exec"), namespace)  # noqa: S102 - trusted repo file
    finally:
        os.chdir(cwd)
    return displayed


def test_notebooks_run_in_order(spark, repo_root):
    overrides = {
        "catalog": "",
        "schema": "hifl_nb",
        "table_format": "parquet",
        "fail_on_gate": "false",
        "release_id": "NB-1",
    }
    outputs = {}
    for name in NOTEBOOKS:
        outputs[name] = _run_notebook(os.path.join(repo_root, "notebooks", name), spark, overrides)
    assert outputs["01_run_hifl_pipeline.py"], "pipeline notebook displayed nothing"
    assert len(outputs["03_hifl_dashboard.py"]) >= 8
    history = spark.table("hifl_nb.hifl_regression_results").select("release_id").distinct().collect()
    assert {r["release_id"] for r in history} == {"NB-1"}


def test_gate_notebook_fails_when_requested(spark, repo_root):
    overrides = {"catalog": "", "schema": "hifl_nb", "table_format": "parquet", "fail_on_gate": "true", "release_id": "NB-2"}
    with pytest.raises(RuntimeError, match="Release gate FAILED"):
        _run_notebook(os.path.join(repo_root, "notebooks", "02_release_gate_ibrt.py"), spark, overrides)
