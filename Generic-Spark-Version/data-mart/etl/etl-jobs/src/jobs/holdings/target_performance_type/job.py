from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "target_performance_type"
    business_key = ["perf_sort_ord"]
    primary_key = {"trgt_perf_type_key": "int"}
    sources:Dict[str, Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "target_performance_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("raw.trgt_perf_type"), "target": "trgt_perf_type"},
        {"source": F.col("raw.trgt_perf_type_desc"), "target": "trgt_perf_type_desc"},
        {"source": F.col("raw.perf_sort_ord"), "target": "perf_sort_ord"},
        {"source": F.col("raw.perf_type_flg"), "target": "perf_type_flg"},
        {"source": F.col("raw.obj_type"), "target": "obj_type"}
    ]
