from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "target_daily_weights"
    business_key = ["trgt_fund_key","day_key","fund_key"]
    primary_key = {"trgt_daily_wgt_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "ssb_hlds"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "target_fund": {
            "type": "table",
            "source": "fund"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "fund_valuation": {
            "type": "table",
            "source": "fund_valuation"
        }

    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "raw"
        },
        {
            "source": "cal",
            "conditions": [
                F.col("raw.calen_dt") == F.col("cal.cal_day")
            ]
        },
        {
            "source": "target_fund",
            "conditions": [
                F.col("raw.fund_id") == F.col("target_fund.st_str_fund_nbr")
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("raw.ticker_symb") == F.col("fund.quot_sym")
            ]
        },
        {
            "source": "fund_valuation",
            "conditions": [
                F.col("raw.calen_dt") == F.col("fund_valuation.vltn_dt"),
                F.col("raw.fund_id") == F.col("fund_valuation.fund_key")
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("target_fund.fund_key"), "target": "trgt_fund_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("raw.shrpar_qty"), "target": "shr_qty"},
        {"source": F.col("raw.mktval_btl"), "target": "mkt_val"},
        {"source": F.lit(0.1), "target": "daily_holdg_wgt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

