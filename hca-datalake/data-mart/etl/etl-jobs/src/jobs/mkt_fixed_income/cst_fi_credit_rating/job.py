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
            "source": "cst_quality"
        },
        "parent": {
            "type": "table",
            "source": "fi_credit_rating_list"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "fundcomp": {
            "type": "table",
            "source": "fund_composite"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "infile"
        },
        {
            "source": "fundcomp",
            "conditions": [
                F.upper(F.lit("High-Yield Opportunities Fund")) == F.upper(F.col("fundcomp.compst_nm"))
            ],
            "type": "cross"
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.col("cal.cal_day")) == F.date_trunc('day',(F.to_timestamp(F.col("infile.Date"), "MM/dd/yyyy")))
            ]
        },
        {
            "source": "parent",
            "conditions": [
                F.upper(F.col("infile.`Credit Rating`")) == F.upper(F.col("parent.cr_rtng_nm"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fundcomp.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("parent.fi_cr_rtng_list_key"), "target": "fi_cr_rtng_list_key"},
        {"source": F.col("infile.`Fund Value Ex Cash`"), "target": "fund_rtng_exclsv_cash"},
        {"source": F.col("infile.`Fund Value Incl Cash`"), "target": "fund_rtng_inclv_cash"},
        {"source": F.col("infile.`Benchmark 1`"), "target": "prim_bmk_rtng"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

