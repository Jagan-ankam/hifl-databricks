"""HIFL incident taxonomy (paper, Table I) and default mappings.

Everything here is a *default*. Each mapping can be overridden through
configuration files (see ``conf/``) without changing code.
"""

from __future__ import annotations

BUSINESS_LOGIC = "BUSINESS_LOGIC"
MAPPING = "MAPPING"
MASTER_DATA = "MASTER_DATA"
COMPLETENESS = "COMPLETENESS"
INTEGRATION = "INTEGRATION"
SOURCE_SYSTEM = "SOURCE_SYSTEM"
UNCLASSIFIED = "UNCLASSIFIED"

# Order matters: it is the tie-break priority when two categories score equally.
CATEGORIES = [MAPPING, MASTER_DATA, COMPLETENESS, BUSINESS_LOGIC, INTEGRATION, SOURCE_SYSTEM]

CATEGORY_DESCRIPTIONS = {
    BUSINESS_LOGIC: "A transformation or business rule is implemented incorrectly or incompletely",
    MAPPING: "A source-to-target field or value mapping is missing or wrong",
    MASTER_DATA: "Master records are missing, duplicated, or not governed",
    COMPLETENESS: "Records or attributes are lost during extraction, transformation, or load",
    INTEGRATION: "An interface fails or exchanges data in a mismatched format or sequence",
    SOURCE_SYSTEM: "A defect is inherited from legacy source data",
}

# Default data quality dimension per category (paper, Table III and Section IV).
CATEGORY_DQ_DIMENSION = {
    BUSINESS_LOGIC: "ACCURACY",
    MAPPING: "CONSISTENCY",
    MASTER_DATA: "REFERENTIAL_INTEGRITY",
    COMPLETENESS: "COMPLETENESS",
    INTEGRATION: "CONSISTENCY",
    SOURCE_SYSTEM: "VALIDITY",
    UNCLASSIFIED: "UNKNOWN",
}

# Default keyword rules for the rule-based classifier (Phase 2).
# Keywords are matched case-insensitively as whole words / phrases.
DEFAULT_CLASSIFICATION_RULES = {
    MAPPING: [
        "mapping", "mapped", "unmapped", "map table", "cross reference", "xref",
        "conversion rule", "value translation", "reason code", "code mismatch", "lookup",
    ],
    MASTER_DATA: [
        "master data", "supplier", "vendor", "customer master", "material master",
        "business partner", "duplicate record", "plant", "supplier location", "golden record",
    ],
    COMPLETENESS: [
        "missing records", "missing rows", "record count", "row count", "not loaded",
        "incomplete", "dropped", "truncated", "partial load", "missing file", "files missing",
    ],
    BUSINESS_LOGIC: [
        "business rule", "calculation", "logic", "flag", "derivation", "incorrect value",
        "wrong amount", "pricing", "tax", "rounding", "condition",
    ],
    INTEGRATION: [
        "interface", "idoc", "api", "timeout", "middleware", "queue", "batch job failed",
        "connection", "sync", "replication", "message", "integration",
    ],
    SOURCE_SYSTEM: [
        "legacy", "source system", "source data", "data entry", "upstream",
        "bad source", "invalid source", "garbage", "historical data",
    ],
}

# Paper, Table I right-hand column: category -> knowledge assets / control templates.
# Each entry: (asset_type, control_template)
CATEGORY_ASSET_MAP = {
    BUSINESS_LOGIC: [("TEST_CASE", "rule_regression_test")],
    MAPPING: [("VALIDATION_RULE", "mapping_reconciliation")],
    MASTER_DATA: [
        ("VALIDATION_RULE", "master_data_validation"),
        ("GOVERNANCE_CONTROL", "master_data_governance"),
    ],
    COMPLETENESS: [("MONITORING_RULE", "record_count_reconciliation")],
    INTEGRATION: [("MONITORING_RULE", "interface_monitoring")],
    SOURCE_SYSTEM: [
        ("VALIDATION_RULE", "source_profiling"),
        ("RUNBOOK_UPDATE", "cleansing_runbook"),
    ],
    UNCLASSIFIED: [("RUNBOOK_UPDATE", "triage_runbook")],
}

# Control templates that need no executable check (documentation-type assets).
NON_EXECUTABLE_TEMPLATES = {"master_data_governance", "cleansing_runbook", "triage_runbook"}

# Ishikawa (fishbone) dimensions used in Phase 3, with keyword hints.
FISHBONE_RULES = {
    "PROCESS": ["process", "procedure", "approval", "workflow", "sign-off", "handover"],
    "PEOPLE": ["training", "user error", "manual", "knowledge", "resource"],
    "TECHNOLOGY": ["job", "interface", "system", "performance", "timeout", "code", "config"],
    "DATA": ["data", "record", "field", "value", "mapping", "master"],
    "GOVERNANCE": ["governance", "ownership", "stewardship", "policy", "standard"],
}
