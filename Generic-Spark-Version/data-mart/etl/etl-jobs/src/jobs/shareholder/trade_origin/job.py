from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob

class Job(ETLJob):
    target_table = "trade_origin"
    business_key = ["trde_orig_id"]
    primary_key = {"trde_orig_id": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "to": {
            "type": "file",
            "source": "trade_origin"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("to.trde_orig_id"), "target": "trde_orig_id"},
        {"source": F.col("to.trde_orig_desc"), "target": "trde_orig_desc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.lit(1), "target": "etl_load_cyc_key"},
        {"source": F.current_timestamp(), "target": "row_strt_dttm"}
    ]
