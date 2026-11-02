import os
import sys
import time

import pytest

# Collected timestamps are converted with the local zone; pin it so tests are deterministic.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

# Make Spark workers use the same interpreter as the test driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from hifl.config import HIFLConfig  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark session.

    Set ``HIFL_TEST_CONNECT=1`` to run the suite through Spark Connect (PySpark 4,
    ``pip install "pyspark[connect]"``), the protocol used by Databricks serverless
    and shared-access compute.
    """
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = SparkSession.builder
    builder = builder.remote("local[2]") if os.environ.get("HIFL_TEST_CONNECT") == "1" else builder.master("local[2]")
    session = (
        builder.appName("hifl-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.ansi.enabled", "true")  # same as Databricks serverless / Spark 4 default
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def cfg():
    return HIFLConfig(catalog=None, schema="hifl_test", table_format="parquet")


@pytest.fixture(scope="session")
def repo_root():
    return ROOT
