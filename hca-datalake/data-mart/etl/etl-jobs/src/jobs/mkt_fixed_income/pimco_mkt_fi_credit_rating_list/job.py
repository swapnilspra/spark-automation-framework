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
    target_table = "fi_credit_rating_list"
    business_key = ["cr_rtng_nm"]
    business_key_props:Dict[str,Dict[str,Any]] = {"cr_rtng_nm":{"case_sensitive": False}}
    primary_key = {"fi_cr_rtng_list_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "pimco_mkt_quality"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("infile.`Quality Bucket`"), "target": "cr_rtng_nm"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
    ]

    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:        
        df_inputs = super().extract(catalog)
        df_quality = df_inputs["infile"]
        # filter out the rows if input file column Quality Bucket is null
        df_inputs["infile"] = df_quality.filter(df_quality["`Quality Bucket`"].isNotNull())
        return df_inputs

