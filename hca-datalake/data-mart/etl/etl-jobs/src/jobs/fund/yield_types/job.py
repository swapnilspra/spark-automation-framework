from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "yield_types"
    business_key = ["yld_cd"]
    primary_key = {"yld_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "yield_types": {
            "type": "file",
            "source": "yield_types"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("yield_types.yld_cd"), "target": "yld_cd"},
        {"source": F.col("yield_types.yld_desc"), "target": "yld_desc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"}

    ]

