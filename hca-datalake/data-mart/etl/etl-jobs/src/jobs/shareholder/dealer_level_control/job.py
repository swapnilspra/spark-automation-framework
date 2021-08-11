from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "dealer_level_control"
    business_key = ["dlr_lvl_cntl_cd"]
    primary_key = {"dlr_lvl_cntl_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "dealer_level_control"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("raw.dlr_lvl_cntl_cd"), "target": "dlr_lvl_cntl_cd"},
        {"source": F.col("raw.dlr_lvl_cntl_desc"), "target": "dlr_lvl_cntl_desc"},
        { "source": F.lit(4), "target": "src_sys_id" },
        { "source": F.lit(1), "target": "etl_load_cyc_key"},
        { "source": F.lit("Y"), "target": "curr_row_flg"},
    ]