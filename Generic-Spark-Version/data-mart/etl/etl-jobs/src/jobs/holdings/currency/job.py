from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "currency"
    business_key = ["currcy_cd","currcy_nm"]
    primary_key = {"currcy_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "currency"
        }
    }
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("raw.currcy_cd"), "target": "currcy_cd"},
        {"source": F.col("raw.currcy_nm"), "target": "currcy_nm"},
        {"source": F.col("raw.currcy_desc"), "target": "currcy_desc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.current_timestamp(), "target": "row_strt_dttm"},
        {"source": F.lit(None), "target": "row_stop_dttm"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.lit(None), "target": "etl_load_cyc_key"}
    ]