"""Load the synthetic demo tables shipped in ``sample_data/``.

The demo tables stand in for the legacy and S/4HANA datasets that the generated
controls check. They are written to the HIFL schema with the prefix ``demo_``.
"""

from __future__ import annotations

import glob
import os
from typing import List

from pyspark.sql import SparkSession

from hifl.config import HIFLConfig
from hifl.io import ensure_schema, read_csv_local


def load_demo_tables(spark: SparkSession, cfg: HIFLConfig, base_dir: str) -> List[str]:
    """Write every ``sample_data/demo_*.csv`` file as a table and return their names."""
    ensure_schema(spark, cfg)
    written = []
    for path in sorted(glob.glob(os.path.join(base_dir, "sample_data", "demo_*.csv"))):
        name = os.path.splitext(os.path.basename(path))[0]
        target = f"{cfg.schema_fqn}.`{name}`"
        df = read_csv_local(spark, path)
        writer = df.write.format(cfg.table_format).mode("overwrite")
        if cfg.table_format == "delta":
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(target)
        written.append(target)
    return written
