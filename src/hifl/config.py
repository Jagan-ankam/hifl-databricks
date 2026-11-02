"""Runtime configuration for HIFL."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class HIFLConfig:
    """Where HIFL writes its tables and how it interprets release cycles.

    Attributes
    ----------
    catalog:
        Unity Catalog name (e.g. ``"main"``). Use ``None`` for the legacy
        ``hive_metastore`` or for local Spark.
    schema:
        Schema (database) that holds all HIFL tables.
    table_format:
        ``"delta"`` on Databricks. ``"parquet"`` is convenient for local tests.
    table_prefix:
        Prefix added to every HIFL table name.
    hypercare_pattern / uat_pattern:
        Case-insensitive regular expressions used to derive the discovery phase
        from a release-cycle label when the source does not provide it.
    closed_statuses:
        Incident statuses treated as resolved for the HIFL closure rule.
    gate_fail_severities:
        Control severities whose failure blocks a release (Phase 6 gate).
    """

    catalog: Optional[str] = None
    schema: str = "hifl"
    table_format: str = "delta"
    table_prefix: str = "hifl_"
    hypercare_pattern: str = r"(?i)hypercare|post[- ]?go[- ]?live|hc\b"
    uat_pattern: str = r"(?i)\buat\b|user acceptance"
    closed_statuses: List[str] = field(default_factory=lambda: ["RESOLVED", "CLOSED", "DONE"])
    gate_fail_severities: List[str] = field(default_factory=lambda: ["CRITICAL", "HIGH"])

    # ------------------------------------------------------------------ names
    @staticmethod
    def _quote(identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    @property
    def schema_fqn(self) -> str:
        if self.catalog:
            return f"{self._quote(self.catalog)}.{self._quote(self.schema)}"
        return self._quote(self.schema)

    def table(self, name: str) -> str:
        """Fully qualified, quoted name of a HIFL table, e.g. ``table("incidents")``."""
        return f"{self.schema_fqn}.{self._quote(self.table_prefix + name)}"

    # ------------------------------------------------------------- loaders
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HIFLConfig":
        known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        if known.get("catalog") in ("", "none", "None"):
            known["catalog"] = None
        return cls(**known)

    @classmethod
    def from_json(cls, path: str) -> "HIFLConfig":
        with open(path, "r", encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh))

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def load_json(path: Optional[str], default: Any = None) -> Any:
    """Read a JSON file, returning ``default`` when ``path`` is empty."""
    if not path:
        return default
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)
