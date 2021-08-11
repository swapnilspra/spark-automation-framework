from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "social_security_status"
    business_key = ["ssn_stat_cd"]
    primary_key = {"ssn_stat_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "social_security_status": {
            "type": "file",
            "source": "social_security_status"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("social_security_status.ssn_stat_cd"), "target": "ssn_stat_cd"},
        {"source": F.col("social_security_status.ssn_stat_desc"), "target": "ssn_stat_desc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"}
    ]

