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
            "source": "pimco_survey"
        },
        "parent": {
            "type": "table",
            "source": "fi_country"
        },
        "acctref": {
            "type": "dimension",
            "source": "pimco_account_reference"
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
            "source": "acctref",
            "conditions": [
                F.upper(F.col("infile.`Acct No`")) == F.col("acctref.PMC_ACCT_NBR")
            ]
        },
        {
            "source": "fundcomp",
            "conditions": [
                F.upper(F.col("acctref.`FUND_COMPST_NM`")) == F.upper(F.col("fundcomp.compst_nm"))
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
                F.upper(F.col("infile.`Country`")) == F.upper(F.col("parent.crty"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fundcomp.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("parent.fi_crty_key"), "target": "fi_crty_key"},
        {"source": F.col("portfolio_country"), "target": "fund_allocn"},
        {"source": F.col("portfolio_currency"), "target": "prim_bmk_allocn"},        
        {"source": F.col("benchmark_country"), "target": "fund_currcy_rt"},
        {"source": F.col("benchmark_currency"), "target": "bmk_currcy_rt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:        
        df_inputs = super().extract(catalog)
        df_temp = df_inputs["infile"]
        # remove percentage sign and convert string to number
        todecimal=F.udf(lambda x: float(x.replace('%',''))/100)
        df_inputs["infile"] = df_temp.filter(df_temp["`Country`"].isNotNull()).withColumn("portfolio_country",todecimal("`Portfolio Country %`").cast(FloatType())).withColumn("portfolio_currency",todecimal("`Portfolio Currency %`").cast(FloatType()))
        df_inputs["infile"] = df_inputs["infile"].withColumn("benchmark_country",todecimal("`Benchmark Country %`").cast(FloatType())).withColumn("benchmark_currency",todecimal("`Benchmark Currency %`").cast(FloatType()))
        return df_inputs


