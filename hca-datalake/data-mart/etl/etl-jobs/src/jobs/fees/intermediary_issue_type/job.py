from typing import Dict, List, Any
import pyspark
import pyspark.sql.functions as F
from common.etl_job import ETLJob


class Job(ETLJob):
    target_table = "intermediary_issue_type"
    business_key = ["intrm_iss_type"]
    primary_key = {"intrm_iss_type_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "raw": {
            "type": "file",
            "source": "intermediary_issue_type"
        }
    }
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("raw.intrm_iss_type"), "target": "intrm_iss_type"}
    ]