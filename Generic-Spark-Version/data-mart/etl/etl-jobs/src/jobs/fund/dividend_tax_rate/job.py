from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "dividend_tax_rate"
    business_key = ["tax_yr"]
    primary_key = {"divd_tax_rt_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "dividend_tax_rate": {
            "type": "file",
            "source": "dividend_tax_rate"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("dividend_tax_rate.tax_yr"), "target": "tax_yr"},
        {"source": F.col("dividend_tax_rate.ordn_incm_rt"), "target": "ordn_incm_rt"},
        {"source": F.col("dividend_tax_rate.ltcg_rt"), "target": "ltcg_rt"},
        {"source": F.col("dividend_tax_rate.stcg_rt"), "target": "stcg_rt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"}

    ]

