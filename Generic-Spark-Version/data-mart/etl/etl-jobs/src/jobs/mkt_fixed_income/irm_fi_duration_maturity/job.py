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
    target_table = "fi_duration_maturity"
    business_key = ["fund_compst_key","day_key","fi_tm_per_key"]
    primary_key = {"fi_dur_mtry_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "irm_duration"
        },
        "parent": {
            "type": "table",
            "source": "fi_time_period"
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
                F.upper(F.col("infile.`Duration / Maturity`")) == F.upper(F.col("parent.tm_per"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fundcomp.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("parent.fi_tm_per_key"), "target": "fi_tm_per_key"},
        {"source": F.col("infile.`Fund YCA - Maturity (Incl Cash)`"), "target": "fund_yca_mtry_inclv_cash"},
        {"source": F.col("infile.`Fund YCA - Maturity (Excl Cash)`"), "target": "fund_yca_mtry_exclsv_cash"},
        {"source": F.col("infile.`Benchmark Maturity`"), "target": "prim_bmk_mtry"},
        {"source": F.col("infile.`Fund YCA - Duration (Incl Cash)`"), "target": "fund_yca_dur_inclv_cash"},
        {"source": F.col("infile.`Fund YCA - Duration (Excl Cash)`"), "target": "fund_yca_dur_exclsv_cash"},
        {"source": F.col("infile.`Benchmark Duration`"), "target": "prim_bmk_dur"},
        #{"source": F.col("infile.`Fund DWE (Incl Cash)`"), "target": "fund_dwe_inclv_cash"},
        #{"source": F.col("infile.`Fund DWE (Excl Cash)`"), "target": "fund_dwe_exclsv_cash"},
        #{"source": F.col("infile.`Benchmark DWE`"), "target": "prim_bmk_dwe"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

