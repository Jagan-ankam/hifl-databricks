from hifl.classify import classify_incidents
from hifl.knowledge import extract_knowledge_assets
from hifl.root_cause import analyse_root_causes, root_cause_summary

COLS = (
    "incident_key string, source_system string, source_record_id string, title string, description string, "
    "root_cause_text string, resolution_text string, business_object string, release_cycle string, "
    "manual_category string, is_duplicate boolean"
)


def _df(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_rule_classification_and_override(spark):
    df = _df(
        spark,
        [
            ("k1", "s", "1", "Reason code mapping incomplete", None, None, None, "REASON_CODE", "R1", None, False),
            ("k2", "s", "2", "Supplier blocked for plant", None, None, None, "SUPPLIER", "R1", None, False),
            ("k3", "s", "3", "Something odd happened", None, None, None, None, "R1", None, False),
            ("k4", "s", "4", "Pricing calculation wrong", None, None, None, None, "R1", "integration", False),
            ("k5", "s", "5", "Record count low", None, None, None, None, "R1", "NOT_A_CATEGORY", False),
        ],
    )
    out = {r["incident_key"]: r for r in classify_incidents(df).collect()}
    assert out["k1"]["category"] == "MAPPING" and out["k1"]["classification_method"] == "RULE"
    assert out["k1"]["dq_dimension"] == "CONSISTENCY"
    assert out["k2"]["category"] == "MASTER_DATA"
    assert out["k3"]["category"] == "UNCLASSIFIED" and out["k3"]["classification_method"] == "NONE"
    assert out["k3"]["classification_confidence"] == 0.0
    assert out["k4"]["predicted_category"] == "BUSINESS_LOGIC"
    assert out["k4"]["category"] == "INTEGRATION" and out["k4"]["classification_method"] == "MANUAL"
    assert out["k5"]["category"] == "COMPLETENESS"  # invalid manual value ignored


def test_whole_word_matching(spark):
    df = _df(spark, [("k1", "s", "1", "Remapping of apis", None, None, None, None, "R1", None, False)])
    row = classify_incidents(df, rules={"MAPPING": ["mapping"], "INTEGRATION": ["api"]}).collect()[0]
    assert row["category"] == "UNCLASSIFIED"  # 'remapping' and 'apis' are not whole-word hits


def test_custom_rules_tie_break_follows_priority(spark):
    df = _df(spark, [("k1", "s", "1", "alpha beta", None, None, None, None, "R1", None, False)])
    row = classify_incidents(df, rules={"MASTER_DATA": ["beta"], "MAPPING": ["alpha"]}).collect()[0]
    assert row["category"] == "MAPPING"  # MAPPING precedes MASTER_DATA in the taxonomy order
    assert row["classification_confidence"] == 0.5


def test_root_cause_and_recurrence(spark):
    df = _df(
        spark,
        [
            ("k1", "s", "1", "Reason code mapping", None, "Codes differ -> list incomplete -> mapping gap", "fixed", None, "R1", None, False),
            ("k2", "s", "2", "Reason code mapping", None, "Codes differ; list incomplete; Mapping   gap", None, None, "R2", None, False),
            ("k3", "s", "3", "Reason code mapping", None, None, None, None, "R2", None, False),
        ],
    )
    out = {r["incident_key"]: r for r in analyse_root_causes(classify_incidents(df)).collect()}
    assert out["k1"]["five_whys"] == ["Codes differ", "list incomplete", "mapping gap"]
    assert out["k1"]["primary_root_cause"] == "mapping gap"
    assert out["k1"]["root_cause_key"] == out["k2"]["root_cause_key"]
    assert out["k1"]["is_recurring"] is True and out["k1"]["releases_affected"] == 2
    assert out["k3"]["root_cause_key"] is None and out["k3"]["is_recurring"] is False
    assert out["k1"]["fishbone_dimension"] == "DATA"
    summary = root_cause_summary(analyse_root_causes(classify_incidents(df))).collect()
    assert sum(r["incidents"] for r in summary) == 3


def test_knowledge_assets_follow_table_one(spark):
    df = _df(
        spark,
        [
            ("k1", "s", "1", "Supplier blocked", None, None, None, "SUPPLIER", "R1", None, False),
            ("k2", "s", "2", "Supplier blocked", None, None, None, "SUPPLIER", "R1", None, True),  # duplicate skipped
            ("k3", "s", "3", "Unknown thing", None, None, None, None, "R1", None, False),
        ],
    )
    assets = extract_knowledge_assets(analyse_root_causes(classify_incidents(df))).collect()
    by_incident = {}
    for a in assets:
        by_incident.setdefault(a["incident_key"], set()).add(a["control_template"])
    assert by_incident["k1"] == {"master_data_validation", "master_data_governance"}
    assert "k2" not in by_incident
    assert by_incident["k3"] == {"triage_runbook"}
    assert len({a["asset_id"] for a in assets}) == len(assets)
