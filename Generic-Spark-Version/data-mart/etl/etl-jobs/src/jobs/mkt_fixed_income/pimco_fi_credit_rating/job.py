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
    target_table = "fi_credit_rating"
    business_key = ["fund_compst_key","day_key","fi_cr_rtng_list_key"]
    primary_key = {"fi_cr_rtngs_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "pimco_quality"
        },
        "parent": {
            "type": "table",
            "source": "fi_credit_rating_list"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "acctref": {
            "type": "dimension",
            "source": "pimco_account_reference"
        },
        "compst": {
            "type": "table",
            "source": "fund_composite"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "infile"
        },
        {
            "source": "acctref",
            "conditions": [
                F.upper(F.col("infile.`Acct No`")) == F.col("acctref.PMC_ACCT_NBR")
            ]
        },
        {
            "source": "compst",
            "conditions": [
                F.upper(F.col("acctref.`FUND_COMPST_NM`")) == F.upper(F.col("compst.compst_nm"))
            ]
        },
        {
            "source": "cal",
            "conditions": [
                F.date_trunc('day',(F.to_timestamp(F.col("infile.`Asof Date`"), "MM/dd/yyyy"))) == F.to_date(F.col("cal.cal_day"))
            ]
        },
        {
            "source": "parent",
            "conditions": [
                F.upper(F.col("infile.`Bucket`")) == F.upper(F.col("parent.cr_rtng_nm"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("compst.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("parent.fi_cr_rtng_list_key"), "target": "fi_cr_rtng_list_key"},
        {"source": F.col("infile.ACCT_DWE"), "target": "fund_dwe_inclv_cash"},
        {"source": F.col("infile.BM_DWE"), "target": "prim_bmk_dwe"},
        {"source": F.col("infile.ACCT_EXP_AMT"), "target": "fund_expsr_amt"},
        {"source": F.col("infile.ACCT_EXP_PCT")/100, "target": "fund_expsr_rt"},
        {"source": F.col("infile.ACCT_DUR_PCT")/100, "target": "fund_dur_rt"},
        {"source": F.col("infile.BM_EXP_PCT")/100, "target": "bmk_expsr_rt"},
        {"source": F.col("infile.BM_DUR_PCT")/100, "target": "bmk_dur_rt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]
