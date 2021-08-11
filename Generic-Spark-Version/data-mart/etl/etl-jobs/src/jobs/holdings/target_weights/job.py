from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "target_weights"
    business_key = ["trgt_fund_key", "fund_key"]
    primary_key = {"trgt_wgt_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "target_weights"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "target_fund": {
            "type": "table",
            "source": "fund"
        }

    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "raw"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("raw.fund_nbr") == F.col("fund.fund_nbr")
            ]
        },
        {
            "source": "target_fund",
            "conditions": [
                F.col("raw.fund_nbr") == F.col("target_fund.fund_nbr")
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("target_fund.fund_key"), "target": "trgt_fund_key"},
        {"source": F.col("raw.efftv_strt_dt"), "target": "efftv_strt_dt"},
        {"source": F.col("raw.efftv_end_dt"), "target": "efftv_end_dt"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("raw.trgt_fund_wgt"), "target": "trgt_fund_wgt"},
        {"source": F.col("raw.wgt_type"), "target": "wgt_type"}
    ]