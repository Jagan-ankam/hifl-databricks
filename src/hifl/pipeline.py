"""End-to-end HIFL orchestration.

``run_hifl`` executes the six phases and (optionally) writes every output as a
table in the configured schema:

==========================  ======================================================
table (prefix ``hifl_``)    content
==========================  ======================================================
incidents_raw               canonical union of all sources (Phase 1, before dedup)
incidents                   classified incidents with root-cause fields (Phases 1-3)
knowledge_assets            one row per incident x asset, with status (Phases 4-5)
controls                    deduplicated IBRT control library (Phase 5)
closure_compliance          HIFL closure rule per incident (Phase 5)
regression_results          control results per release run, appended (Phase 6)
release_distribution        events per release cycle and phase
category_distribution       events per category and phase
root_cause_summary          root causes by category and fishbone dimension
metrics                     headline metrics for the latest run
==========================  ======================================================

Run from the command line (e.g. a Databricks Python script / wheel task)::

    python -m hifl.pipeline --config conf/hifl_config.json --release-id R3
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

from hifl.capture import capture_incidents
from hifl.classify import classify_incidents
from hifl.config import HIFLConfig, load_json
from hifl.controls import closure_compliance, generate_controls
from hifl.io import ensure_schema, read_table, write_table
from hifl.knowledge import extract_knowledge_assets
from hifl.metrics import category_distribution, compute_metrics, release_distribution
from hifl.root_cause import analyse_root_causes, root_cause_summary
from hifl.validation import GateResult, release_gate, run_regression_suite


@dataclass
class HIFLRun:
    incidents: DataFrame
    assets: DataFrame
    controls: DataFrame
    closure: DataFrame
    metrics: DataFrame
    results: Optional[DataFrame] = None
    gate: Optional[GateResult] = None


def render_placeholders(obj: Any, cfg: HIFLConfig) -> Any:
    """Replace ``${catalog}``, ``${schema}`` and ``${schema_fqn}`` in configuration values."""
    text = json.dumps(obj)
    text = text.replace("${schema_fqn}", cfg.schema_fqn.replace('"', '\\"'))
    text = text.replace("${catalog}", cfg.catalog or "")
    text = text.replace("${schema}", cfg.schema)
    return json.loads(text)


def run_hifl(
    spark: SparkSession,
    cfg: HIFLConfig,
    sources: Sequence[Dict[str, Any]],
    *,
    base_dir: Optional[str] = None,
    rules: Optional[Dict[str, List[str]]] = None,
    bindings: Optional[Sequence[Dict[str, Any]]] = None,
    release_id: Optional[str] = None,
    run_validation: bool = True,
    write: bool = True,
) -> HIFLRun:
    """Run all six HIFL phases.

    Parameters
    ----------
    sources:      incident source specs (see :func:`hifl.io.read_source`)
    base_dir:     directory that relative ``local_csv`` paths are resolved against
    rules:        classification keyword rules (defaults to the built-in rules)
    bindings:     control bindings (see :mod:`hifl.controls`)
    release_id:   label of the release being validated (Phase 6)
    run_validation: run the IBRT suite and the release gate
    write:        persist every output table (set ``False`` for dry runs/tests)
    """
    sources = render_placeholders(list(sources), cfg)
    bindings = render_placeholders(list(bindings or []), cfg)
    if write:
        ensure_schema(spark, cfg)

    # Phases 1-3
    raw, captured = capture_incidents(spark, sources, cfg, base_dir=base_dir)
    incidents = analyse_root_causes(classify_incidents(captured, rules=rules))
    if write:
        write_table(raw, cfg, "incidents_raw")
        write_table(incidents, cfg, "incidents")
        incidents = read_table(spark, cfg, "incidents")

    # Phases 4-5
    assets = extract_knowledge_assets(incidents)
    assets, controls = generate_controls(assets, bindings)
    if write:
        write_table(assets, cfg, "knowledge_assets")
        write_table(controls, cfg, "controls")
        assets = read_table(spark, cfg, "knowledge_assets")
        controls = read_table(spark, cfg, "controls")
    closure = closure_compliance(incidents, assets, cfg)

    # Phase 6
    results = None
    gate = None
    if run_validation:
        rid = release_id or "unspecified"
        results = run_regression_suite(spark, controls, rid)
        # ``results`` is built from rows already collected on the driver, so re-using it
        # never re-runs the checks.
        if write:
            write_table(results, cfg, "regression_results", mode="append")
        gate = release_gate(results, cfg, rid)

    metrics = compute_metrics(incidents, assets, controls, closure, results)
    if write:
        write_table(closure, cfg, "closure_compliance")
        write_table(release_distribution(incidents), cfg, "release_distribution")
        write_table(category_distribution(incidents), cfg, "category_distribution")
        write_table(root_cause_summary(incidents), cfg, "root_cause_summary")
        write_table(metrics, cfg, "metrics")

    return HIFLRun(incidents, assets, controls, closure, metrics, results, gate)


def run_from_config(
    spark: SparkSession,
    config_path: str,
    base_dir: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
    release_id: Optional[str] = None,
    write: bool = True,
) -> HIFLRun:
    """Load a JSON run configuration (see ``conf/hifl_config.json``) and run HIFL.

    Relative file paths in the configuration are resolved against ``base_dir``,
    which defaults to the parent of the folder containing the config file (the
    repository root for ``conf/hifl_config.json``).
    """
    config_path = os.path.abspath(config_path)
    base_dir = base_dir or os.path.dirname(os.path.dirname(config_path))
    run_cfg = load_json(config_path)
    hifl_section = dict(run_cfg.get("hifl", {}))
    hifl_section.update({k: v for k, v in (overrides or {}).items() if v is not None})
    cfg = HIFLConfig.from_dict(hifl_section)

    def _resolve(p: Optional[str]) -> Optional[str]:
        if not p:
            return None
        return p if os.path.isabs(p) else os.path.join(base_dir, p)

    rules = load_json(_resolve(run_cfg.get("classification_rules_path")))
    bindings = load_json(_resolve(run_cfg.get("control_bindings_path")), default=[])
    return run_hifl(
        spark,
        cfg,
        run_cfg.get("sources", []),
        base_dir=base_dir,
        rules=rules,
        bindings=bindings,
        release_id=release_id or run_cfg.get("release_id"),
        run_validation=bool(run_cfg.get("run_validation", True)),
        write=write,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Hypercare Incident Feedback Loop (HIFL) pipeline")
    parser.add_argument("--config", required=True, help="Path to the JSON run configuration")
    parser.add_argument("--catalog", default=None, help="Override the Unity Catalog name")
    parser.add_argument("--schema", default=None, help="Override the target schema")
    parser.add_argument("--release-id", default=None, help="Release label for the regression run")
    parser.add_argument(
        "--base-dir", default=None, help="Folder that relative paths in the config resolve against (default: repo root)"
    )
    parser.add_argument("--fail-on-gate", action="store_true", help="Exit with code 1 if the release gate fails")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    run = run_from_config(
        spark,
        args.config,
        base_dir=args.base_dir,
        overrides={"catalog": args.catalog, "schema": args.schema},
        release_id=args.release_id,
    )
    if run.gate is not None:
        print(run.gate.summary())
        if args.fail_on_gate and not run.gate.passed:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
