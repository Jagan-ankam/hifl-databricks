from datetime import datetime

from pyspark.sql import functions as F

from hifl.capture import normalize_source, parse_timestamp, standardize_incidents


def test_parse_timestamp_formats(spark):
    df = spark.createDataFrame(
        [("2025-03-01 10:15:00",), ("2025-03-01T10:15:00",), ("2025-03-01",), ("3/1/2025 10:15",), ("3/1/2025",), ("not a date",), (None,)],
        "v string",
    )
    out = [r["ts"] for r in df.select(parse_timestamp(F.col("v")).alias("ts")).collect()]
    assert out[0] == datetime(2025, 3, 1, 10, 15)
    assert out[1] == datetime(2025, 3, 1, 10, 15)
    assert out[2] == datetime(2025, 3, 1)
    assert out[3] == datetime(2025, 3, 1, 10, 15)
    assert out[4] == datetime(2025, 3, 1)
    assert out[5] is None and out[6] is None


def test_parse_timestamp_day_first(spark):
    df = spark.createDataFrame([("03/01/2025",)], "v string")
    ts = df.select(parse_timestamp(F.col("v"), day_first=True).alias("t")).collect()[0]["t"]
    assert ts == datetime(2025, 1, 3)


def test_normalize_source_maps_columns_and_defaults(spark):
    raw = spark.createDataFrame([("INC1", "  Title ", "R1 Hypercare", "")], "number string, short string, rel string, notes string")
    spec = {
        "name": "ServiceNow",
        "column_map": {"source_record_id": "number", "title": "short", "release_cycle": "rel", "resolution_text": "notes"},
        "defaults": {"assignment_group": "Team A"},
    }
    row = normalize_source(raw, spec).collect()[0]
    assert row["source_system"] == "ServiceNow"
    assert row["title"] == "Title"
    assert row["resolution_text"] is None  # blank -> null
    assert row["assignment_group"] == "Team A"
    assert row["opened_at"] is None


def test_standardize_derives_phase_and_flags_duplicates(spark, cfg):
    raw = spark.createDataFrame(
        [
            ("ServiceNow", "INC1", "Supplier blocked!", "R1 Hypercare", None, "2025-01-01 10:00:00"),
            ("Jira", "HC-1", "supplier blocked", "R1 Hypercare", None, "2025-01-01 11:00:00"),
            ("Jira", "HC-2", "Other issue", "R2 UAT", None, "2025-02-01 11:00:00"),
            ("Jira", "HC-2", "Other issue", "R2 UAT", None, "2025-02-01 11:00:00"),  # exact duplicate
            ("Jira", "HC-3", "Third", "Cutover", "Hypercare", None),
        ],
        "source_system string, source_record_id string, title string, release_cycle string, phase string, opened string",
    )
    raw = raw.withColumnRenamed("opened", "opened_at")
    normalized = normalize_source(raw.filter("source_system = 'ServiceNow'"), {"name": "ServiceNow"}).unionByName(
        normalize_source(raw.filter("source_system = 'Jira'"), {"name": "Jira"})
    )
    out = standardize_incidents(normalized, cfg)
    rows = {r["source_record_id"]: r for r in out.collect()}
    assert len(rows) == 4  # exact duplicate removed
    assert rows["INC1"]["phase"] == "HYPERCARE"
    assert rows["HC-2"]["phase"] == "UAT"
    assert rows["HC-3"]["phase"] == "HYPERCARE"  # explicit phase wins over release label
    assert rows["INC1"]["is_duplicate"] is False
    assert rows["HC-1"]["is_duplicate"] is True
    assert rows["HC-1"]["duplicate_of"] == rows["INC1"]["incident_key"]
