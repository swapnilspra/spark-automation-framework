from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "trade_entry_method"
    business_key = ["trde_entry_mthd_cd"]
    primary_key = {"trde_entry_mthd_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "trade_entry_method"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("raw.trde_entry_mthd_cd"), "target": "trde_entry_mthd_cd"},
        {"source": F.col("raw.trde_entry_mthd_desc"), "target": "trde_entry_mthd_desc"},
        { "source": F.lit(4), "target": "src_sys_id" },
        { "source": F.lit(1), "target": "etl_load_cyc_key"},
        { "source": F.lit("Y"), "target": "curr_row_flg"},
    ]