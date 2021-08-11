from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime

class Job(ETLJob):
    target_table = "cct_spend_category"
    business_key = ["spend_cat"]
    primary_key = {"spend_cat_key": "int"}    
    sources:Dict[str,Dict[str,Any]] = {
        "cct": {
            "type": "file",
            "source": "cct_spend_category"
        }
    }

    target_mappings:List[Dict[str,Any]] = [
        { "source": F.upper(F.col("Spend Category as Worktag")), "target": "spend_cat" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]

