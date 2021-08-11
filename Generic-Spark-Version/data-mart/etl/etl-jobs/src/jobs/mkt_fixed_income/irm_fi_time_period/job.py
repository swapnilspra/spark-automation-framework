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
    target_table = "fi_time_period"
    business_key = ["tm_per"]
    business_key_props:Dict[str,Dict[str,Any]] = {"tm_per":{"case_sensitive": False}}
    primary_key = {"fi_tm_per_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "irm_duration"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("infile.`Duration / Maturity`"), "target": "tm_per"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

