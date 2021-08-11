from pyspark.sql.types import ArrayType, StringType, MapType, StructField, StructType, FloatType
from typing import Dict, List, Any
import math
import logging
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime
class Job(ETLJob):
    target_table = "fund_dividend"
    business_key = ["fund_key","rec_day_key", "reinvest_day_key", "paybl_day_key"]
    primary_key = {"fund_divd_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "fund_dividend": {
            "type": "file",
            "source": "fund_dividend"
        },
        "calendar_rec": {
            "type": "table",
            "source": "calendar"
        },
        "calendar_reinvest": {
            "type": "table",
            "source": "calendar"
        },
        "calendar_paybl": {
            "type": "table",
            "source": "calendar"
        }

    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "fund_dividend"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.col("fund_dividend.fund_nbr")
            ]
        },
        {
            "source": "calendar_rec",
            "conditions": [
                F.to_date(F.col("calendar_rec.cal_day")) == F.to_date(F.col("fund_dividend.record_date"), "MM/dd/yyyy")
            ],
            "type":'leftouter'
        },
        {
            "source": "calendar_reinvest",
            "conditions": [
                F.to_date(F.col("calendar_reinvest.cal_day")) == F.to_date(F.col("fund_dividend.reinvest_date"), "MM/dd/yyyy")
            ],
            "type":'leftouter'
        },
        {
            "source": "calendar_paybl",
            "conditions": [
                F.to_date(F.col("calendar_paybl.cal_day")) == F.to_date(F.col("fund_dividend.payable_date"), "MM/dd/yyyy")
            ],
            "type":'leftouter'
        }
    ]
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("calendar_rec.day_key"), "target": "rec_day_key"},
        {"source": F.col("calendar_reinvest.day_key"), "target": "reinvest_day_key"},
        {"source": F.col("calendar_paybl.day_key"), "target": "paybl_day_key"},
        {"source": F.col("fund_dividend.net_invest_income"), "target": "net_invmt_incm"},
        {"source": F.col("fund_dividend.shrs_of_record"), "target": "shrs_of_rec"},
        {"source": F.col("fund_dividend.reinvest_price"), "target": "re_invst_pr"},
        {"source": F.col("fund_dividend.ord_income"), "target": "ordn_incm"},
        {"source": F.col("fund_dividend.total_gains_short"), "target": "tot_shrt_term_gains"},
        {"source": F.col("fund_dividend.total_gains_mid"), "target": "tot_mid_term_gains"},
        {"source": F.col("fund_dividend.total_gains_long"), "target": "tot_long_term_gains"},
        {"source": F.col("fund_dividend.cap_gains_short"), "target": "shrt_term_cap_gains"},
        {"source": F.col("fund_dividend.cap_gains_mid"), "target": "mid_term_cap_gains"},
        {"source": F.col("fund_dividend.cap_gains_long"), "target": "long_term_cap_gains"},
        {"source": F.col("fund_dividend.non_taxable_dis"), "target": "non_txbl_discnt"},
        {"source": F.col("fund_dividend.foreign_tax_credit"), "target": "forgn_tax_cr"},
        {"source": F.col("fund_dividend.reinvest_index"), "target": "re_invst_idx"},
        {"source": F.col("fund_dividend.estimated_dividend"), "target": "est_divd_flg"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source":  F.col("fund_dividend.cap_gains_short")+F.col("fund_dividend.cap_gains_mid") + F.col("fund_dividend.cap_gains_long") + F.col("fund_dividend.ord_income") + F.col("fund_dividend.non_taxable_dis"),"target": "tot_divd"}
    ]

