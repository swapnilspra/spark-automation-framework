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
    target_table = "fi_country"
    business_key = ["crty"]
    business_key_props:Dict[str,Dict[str,Any]] = {"crty":{"case_sensitive": False}}
    primary_key = {"fi_crty_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "pimco_country"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("infile.`Country Description`"), "target": "crty"},
        {"source": F.col("infile.`PM EM`"), "target": "crty_type"},
        {"source": F.col("infile.`Rpt Region Desc`"), "target": "reg"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

