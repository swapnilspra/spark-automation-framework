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
            "source": "cst_country"
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
                F.date_trunc('day',(F.to_timestamp(F.col("infile.Date"), "MM/dd/yyyy"))) == F.to_date(F.col("cal.cal_day"))
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
        {"source": F.col("infile.`Allocation Percentage`"), "target": "fund_allocn"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

