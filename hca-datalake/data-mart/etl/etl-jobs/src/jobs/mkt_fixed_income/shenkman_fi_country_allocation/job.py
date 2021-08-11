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
    target_table = "fi_country_allocation"
    business_key = ["fund_compst_key","day_key","fi_crty_key"]
    primary_key = {"fi_crty_allocn_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "shenkman_country",
        },
        "parent": {
            "type": "table",
            "source": "fi_country"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "fundcomp": {
            "type": "table",
            "source": "fund_composite"
        },
        "shenkman_names": {
            "type": "dimension",
            "source": "shenkman_fund_names"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "infile"
        },
        {
            "source": "shenkman_names",
            "conditions": [
                F.upper(F.col("infile.fund")) == F.col("shenkman_names.filename")
            ]
        },
        {
            "source": "fundcomp",
            "conditions": [
                F.upper(F.col("shenkman_names.dbname")) == F.upper(F.col("fundcomp.compst_nm"))
            ]
        },
        {
            "source": "cal",
            "conditions": [
                F.date_trunc('day',(F.to_timestamp(F.col("infile.`Date`"), "MM/dd/yy"))) == F.to_date(F.col("cal.cal_day"))
            ]
        },
        {
            "source": "parent",
            "conditions": [
                F.upper(F.col("infile.`Country`")) == F.upper(F.col("parent.crty"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fundcomp.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("parent.fi_crty_key"), "target": "fi_crty_key"},

        {"source": F.col("infile.Fund Value"), "target": "fund_allocn"},
        {"source": F.col("infile.Benchmark 1"), "target": "prim_bmk_allocn"},
        {"source": F.col("infile.Benchmark 2"), "target": "secy_bmk_allocn"},
        
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

