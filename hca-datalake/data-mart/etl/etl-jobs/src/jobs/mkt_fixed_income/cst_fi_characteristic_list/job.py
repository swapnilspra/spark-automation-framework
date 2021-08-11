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
    target_table = "fi_characteristic_list"
    business_key = ["charctc_nm"]
    business_key_props:Dict[str,Dict[str,Any]] = {"charctc_nm":{"case_sensitive": False}}
    primary_key = {"fi_charctc_list_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "cst_characteristics"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("infile.Characteristics"), "target": "charctc_nm"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
    ]

