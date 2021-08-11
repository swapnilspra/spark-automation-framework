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
    target_table = "fi_top_issuers"
    business_key = ["fund_compst_key","day_key","fi_issr_key"]
    primary_key = {"fi_top_issr_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "shenkman_issuer"
        },
        "parent": {
            "type": "table",
            "source": "fi_issuer"
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
                F.upper(F.col("infile.Issuer")) == F.upper(F.col("parent.issr_nm"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fundcomp.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("parent.fi_issr_key"), "target": "fi_issr_key"},
        {"source": F.col("infile.`Pct Issuer`"), "target": "issr_pct"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

