from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "awd_work_type"
    business_key = ["wk_type_nm"]
    primary_key = {"wk_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "wt": {
            "type": "file",
            "source": "awd_work_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("wt.wrktype"), "target": "wk_type_nm" },
        { "source": F.col("wt.wrktype"), "target": "wk_type_desc" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]