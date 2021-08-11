from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "perf_run_type"
    business_key = ["run_type_cd"]
    primary_key = {"perf_run_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "perf_run_type": {
            "type": "file",
            "source": "perf_run_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("perf_run_type.run_type_cd"), "target": "run_type_cd"},
        {"source": F.col("perf_run_type.run_type_desc"), "target": "run_type_desc"}
    ]

