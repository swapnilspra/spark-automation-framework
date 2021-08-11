from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime

class Job(ETLJob):
    target_table = "cct_tax_detail"
    business_key = ["tax_sum_key","city_tax_list_key"]
    primary_key = {"tax_det_key": "int"}    
    sources:Dict[str,Dict[str,Any]] = {
        "tax_det": {
            "type": "file",
            "source": "cct_tax_detail"
        },
        "tax_list" : {
            "type": "table",
            "source": "cct_city_tax_list"
        },
        "tax_sum" : {
            "type": "table",
            "source": "cct_tax_summary"
        }        
    }
    # JOIN: Calender, User, Queue
    joins:List[Dict[str,Any]] = [
        {
            "source": "tax_det"
        },
        {
            "source": "tax_list",
            "conditions": [
                F.col("tax_list.city_tax_desc") == F.upper(F.col("tax_det.Tax Type"))
            ]
        },
        {
            "source": "tax_sum",
            "conditions": [
                F.col("tax_sum.key_ref") == F.col("tax_det.Key")
            ]
        }
    ]


    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("tax_sum.tax_sum_key"), "target": "tax_sum_key" },
        { "source": F.col("tax_list.city_tax_list_key"), "target": "city_tax_list_key" },
        { "source": F.col("tax_det.Tax Amount"), "target": "city_tax_amt" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]


