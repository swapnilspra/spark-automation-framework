from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime

class Job(ETLJob):
    target_table = "cct_city_tax_list"
    business_key = ["city_tax_desc"]
    primary_key = {"city_tax_list_key": "int"}    
    sources:Dict[str,Dict[str,Any]] = {
        "cct": {
            "type": "file",
            "source": "cct_city_tax_list"
        }
    }

    target_mappings:List[Dict[str,Any]] = [
        { "source": F.upper(F.col("City Tax Description")), "target": "city_tax_desc" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]

