"""Phase 6 - Release validation with Incident-Based Regression Testing (IBRT).

Every active control in the library was created from a real incident. Running
the library against a new release checks it against the record of failures that
have already been observed. The release gate fails when any control with a
blocking severity fails or errors.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from hifl.config import HIFLConfig
from hifl.rules_engine import run_check
from hifl.schemas import RESULT_SCHEMA


@dataclass
class GateResult:
    release_id: str
    passed: bool
    total: int
    passed_controls: int
    failed_controls: int
    errored_controls: int
    blocking: List[Dict[str, Any]] = field(default_factory=list)

    def summary(self) -> str:
        verdict = "PASSED" if self.passed else "FAILED"
        return (
            f"Release gate {verdict} for '{self.release_id}': {self.passed_controls}/{self.total} controls passed, "
            f"{self.failed_controls} failed, {self.errored_controls} errored, {len(self.blocking)} blocking."
        )


def run_regression_suite(
    spark: SparkSession,
    controls: DataFrame,
    release_id: str,
    max_message_len: int = 1000,
) -> DataFrame:
    """Execute every ACTIVE control and return one result row per control.

    A control that raises an exception is recorded as ``ERROR`` (with the error
    message) and does not stop the rest of the suite.
    """
    active = (
        controls.filter((F.col("status") == "ACTIVE") & F.col("check_type").isNotNull())
        .select("control_id", "check_type", "params_json", "severity")
        .orderBy("control_id")
        .collect()
    )
    rows = []
    for c in active:
        started = time.time()
        try:
            params = json.loads(c["params_json"] or "{}")
            failed, checked, message = run_check(spark, c["check_type"], params)
            result = "PASS" if failed == 0 else "FAIL"
        except Exception as exc:  # noqa: BLE001 - recorded, never raised
            failed, checked, result = None, None, "ERROR"
            message = f"{type(exc).__name__}: {exc}"
        rows.append(
            (
                c["control_id"],
                c["check_type"],
                (c["severity"] or "MEDIUM").upper(),
                result,
                None if failed is None else int(failed),
                None if checked is None else int(checked),
                str(message)[:max_message_len],
                int((time.time() - started) * 1000),
            )
        )
    df = spark.createDataFrame(rows, schema=RESULT_SCHEMA)
    return df.withColumn("release_id", F.lit(release_id)).withColumn("run_ts", F.current_timestamp())


def release_gate(results: DataFrame, cfg: HIFLConfig, release_id: Optional[str] = None) -> GateResult:
    """Summarize regression results into a pass/fail release decision."""
    blocking_sev = [s.upper() for s in cfg.gate_fail_severities]
    rows = results.select("control_id", "check_type", "severity", "result", "failed_count", "message").collect()
    rid = release_id
    if rid is None and "release_id" in results.columns:
        first = results.select("release_id").limit(1).collect()
        rid = first[0]["release_id"] if first else "unknown"
    blocking = [
        r.asDict()
        for r in rows
        if r["result"] in ("FAIL", "ERROR") and (r["severity"] or "MEDIUM").upper() in blocking_sev
    ]
    return GateResult(
        release_id=rid or "unknown",
        passed=len(blocking) == 0,
        total=len(rows),
        passed_controls=sum(1 for r in rows if r["result"] == "PASS"),
        failed_controls=sum(1 for r in rows if r["result"] == "FAIL"),
        errored_controls=sum(1 for r in rows if r["result"] == "ERROR"),
        blocking=blocking,
    )
