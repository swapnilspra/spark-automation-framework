from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "reason_code"
    business_key = ["rsn_cd"]
    primary_key = {"rsn_cd": "string"}
    sources:Dict[str,Dict[str,Any]] = {
        "reason_code": {
            "type": "file",
            "source": "reason_code"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("reason_code.entity_rsn_cd"), "target": "entity_rsn_cd"},
        {"source": F.col("reason_code.entity_desc"), "target": "entity_desc"},
        {"source": F.col("reason_code.rsn_cd"), "target": "rsn_cd"},
        {"source": F.col("reason_code.rsn_shrt_desc"), "target": "rsn_shrt_desc"},
        {"source": F.col("reason_code.rsn_long_desc"), "target": "rsn_long_desc"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

