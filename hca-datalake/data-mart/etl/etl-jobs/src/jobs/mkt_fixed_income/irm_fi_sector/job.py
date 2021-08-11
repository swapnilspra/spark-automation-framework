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
    target_table = "fi_sector"
    business_key = ["sctr_type","sctr_nm","sub_sctr_nm"]
    business_key_props:Dict[str,Dict[str,Any]] = {"sctr_nm":{"case_sensitive": False},"sub_sctr_nm":{"case_sensitive": False}}
    primary_key = {"fi_sctr_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "irm_sector"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.lit("IRM"), "target": "sctr_type"},
        {"source": F.col("infile.`Sector`"), "target": "sctr_nm"},
        {"source": F.col("infile.`Sub Sector`"), "target": "sub_sctr_nm"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

