from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from itertools import chain

class Job(ETLJob):
    target_table = "bluesky_transactions"
    business_key = ["day_key", "fund_key", "reg_key"]
    primary_key = {"blue_sky_txn_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "bluesky_transactions": {
            "type": "file",
            "source": "DTO.HCA.BSK.TIP88"
        },
        "region": {
            "type": "table",
            "source": "region"
        },
        "country": {
            "type": "dimension",
            "source": "dst_st_country"
        },

        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        }

    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "bluesky_transactions"
        },
        {
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("calendar.cal_day")) == F.to_date(F.col("bluesky_transactions.super-sheet-date"),'yyyyMMdd')
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.col("bluesky_transactions.fund-code")
            ]
        },
        {
            "source": "country",
            "conditions": [
                F.col("country.DST_ST_CD") == F.col("bluesky_transactions.state-code")
            ]
        },
        {
            "source": "region",
            "conditions": [
                F.col("region.ST_CD") == F.col("country.ST_CD")
            ],
            "type": "left"
        }
    ]

    target_mappings:List[Dict[str,Any]] = [

        {"source": F.col("calendar.day_key"), "target": "day_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.coalesce(F.col("region.reg_key"), F.lit('300')), "target": "reg_key"},
        {"source": F.col("bluesky_transactions.share-data"), "target": "shr_qty"},
        {"source": F.col("bluesky_transactions.amount-data"), "target": "txn_amt"},
        {"source": F.col("bluesky_transactions.mtd-repeat-amt"), "target": "mtd_rept_amt"},
        {"source": F.col("bluesky_transactions.mtd-reopen-amt"), "target": "mtd_re_open_amt"},
        {"source": F.lit(None), "target": "curr_row_flg"}

    ]