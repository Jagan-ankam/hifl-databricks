"""HIFL: Hypercare Incident Feedback Loop for Databricks / PySpark.

Reference implementation of the framework described in
"The Hypercare Incident Feedback Loop: A Case Study Framework for Turning
Post-Go-Live Incidents into Data Quality Controls in SAP S/4HANA Programs".

Phases
------
1. capture      -> :mod:`hifl.capture`
2. classify     -> :mod:`hifl.classify`
3. root cause   -> :mod:`hifl.root_cause`
4. knowledge    -> :mod:`hifl.knowledge`
5. controls     -> :mod:`hifl.controls`
6. validation   -> :mod:`hifl.validation` (Incident-Based Regression Testing)
"""

from hifl.config import HIFLConfig
from hifl.taxonomy import CATEGORIES

__all__ = ["HIFLConfig", "CATEGORIES", "__version__"]
__version__ = "0.1.0"
