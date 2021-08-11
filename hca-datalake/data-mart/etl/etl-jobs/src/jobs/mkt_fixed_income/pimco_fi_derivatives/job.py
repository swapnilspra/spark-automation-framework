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
    target_table = "fi_derivatives"
    business_key = ["fund_compst_key","day_key","fi_dervtv_list_key"]
    primary_key = {"fi_dervtv_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "pimco_deriv"
        },
        "parent": {
            "type": "table",
            "source": "fi_derivative_list"
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
                F.upper(F.col("infile.`Bucket`")) == F.upper(F.col("parent.dervtv_nm"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("compst.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("parent.fi_dervtv_list_key"), "target": "fi_dervtv_list_key"},
        {"source": F.col("infile.Notional"), "target": "ntional_amt"},
        {"source": F.col("infile.MV AMT"), "target": "mkt_val"},
        {"source": F.col("infile.` MV%`")/100, "target": "mkt_val_rt"},
        {"source": F.col("infile.`DUR%`")/100, "target": "dur_rt"},
        {"source": F.col("infile.DWE"), "target": "dwe"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]
