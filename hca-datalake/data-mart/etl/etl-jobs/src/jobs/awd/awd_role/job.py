from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "awd_role"
    business_key = ["role_nm"]
    primary_key = {"role_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "role": {
            "type": "file",
            "source": "awd_role"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("role.rolename"), "target": "role_nm" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]