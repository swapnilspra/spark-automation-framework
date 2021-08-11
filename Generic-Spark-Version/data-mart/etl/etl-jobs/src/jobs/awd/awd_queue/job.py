from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "awd_queue"
    business_key = ["q_cd"]
    primary_key = {"q_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "role": {
            "type": "file",
            "source": "awd_queue"
        }
    }

    target_mappings:List[Dict[str,Any]] = [

        { "source": F.col("role.queuecd"), "target": "q_cd" },
        { "source": F.col("role.queuecd"), "target": "q_desc" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]