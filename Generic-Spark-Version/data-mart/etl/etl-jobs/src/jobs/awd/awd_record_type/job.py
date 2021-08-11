from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "awd_record_type"
    business_key = ["rec_cd"]
    primary_key = {"rec_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "rt": {
            "type": "file",
            "source": "awd_record_type"
        }
    }

    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("rt.rec_cd"), "target": "rec_cd" },
        { "source": F.col("rt.rec_desc"), "target": "rec_desc" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]