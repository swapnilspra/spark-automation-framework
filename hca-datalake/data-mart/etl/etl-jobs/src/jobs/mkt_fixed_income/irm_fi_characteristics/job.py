from pyspark.sql.types import ArrayType, StringType, MapType, StructField, StructType, FloatType
from typing import Dict, List, Any
import math
import logging
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime
import pyspark.sql.types as T

class Job(ETLJob):
    target_table = "fi_characteristics"
    business_key = ["fund_compst_key","day_key","fi_charctc_list_key"]
    primary_key = {"fi_charctc_fact_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "irm_characteristics"
        },
        "parent": {
            "type": "table",
            "source": "fi_characteristic_list"
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
                F.upper(F.col("infile.Fund")) == F.upper(F.col("fundcomp.compst_nm"))
            ]
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.col("cal.cal_day")) == F.date_trunc('day',(F.to_timestamp(F.col("infile.Date"), "MM/dd/yyyy hh:mm:ss a")))
            ]
        },
        {
            "source": "parent",
            "conditions": [
                F.upper(F.col("infile.Characteristics")) == F.upper(F.col("parent.charctc_nm"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fundcomp.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("parent.fi_charctc_list_key"), "target": "fi_charctc_list_key"},
        {"source": F.col("infile.`Fund Value`"), "target": "fund_charctc_val"},
        {"source": F.col("infile.`Benchmark Value`"), "target": "prim_bmk_charctc"},
        {"source": F.lit(None).cast(T.DecimalType()), "target": "secy_bmk_charctc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

