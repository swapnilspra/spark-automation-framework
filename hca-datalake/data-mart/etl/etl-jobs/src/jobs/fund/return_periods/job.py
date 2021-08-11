from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "return_periods"
    business_key = ["per_nm"]
    primary_key = {"per_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "return_periods": {
            "type": "file",
            "source": "return_periods"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("return_periods.per_nm"), "target": "per_nm"},
        {"source": F.col("return_periods.per_desc"), "target": "per_desc"},
        {"source": F.col("return_periods.per_ut"), "target": "per_ut"},
        {"source": F.col("return_periods.per_msr"), "target": "per_msr"},
        {"source": F.col("return_periods.annl_fctr"), "target": "annl_fctr"},
        {"source": F.col("return_periods.daily_msr_flg"), "target": "daily_msr_flg"},
        {"source": F.col("return_periods.mo_end_msr_flg"), "target": "mo_end_msr_flg"},
        {"source": F.col("return_periods.per_sort_ord"), "target": "per_sort_ord"},
        {"source": F.col("return_periods.full_desc"), "target": "full_desc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"}

    ]

