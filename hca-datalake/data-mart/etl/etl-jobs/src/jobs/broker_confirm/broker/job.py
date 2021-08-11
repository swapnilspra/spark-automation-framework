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
    target_table = "broker"
    business_key = ["posn_type_key", "brkr_nm","st_str_nbr"]
    primary_key = {"brkr_key": "int"}
    sources:Dict[str, Dict[str, Any]] = {
        "broker": {
            "type": "file",
            "source": "broker"
        },
        "posn": {
            "type": "dimension",
            "source": "position_type"
        },
        "fund_composite": {
            "type": "table",
            "source": "fund_composite"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "broker"
        },
        {
            "source": "posn",
            "conditions": [
                F.col("broker.posn_cd") == F.col("posn.posn_cd")
    ]
        },
        {
            "source": "fund_composite",
            "conditions": [
                F.col("broker.st_str_nbr") == F.col("fund_composite.st_str_fund_nbr")
    ]
        }

    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("posn.posn_type_key"), "target": "posn_type_key"},
        {"source": F.col("broker.brkr_nm"), "target": "brkr_nm"},
        {"source": F.col("broker.st_str_nbr"), "target": "st_str_nbr"},
        {"source": F.col("fund_composite.compst_nm"), "target": "compst_nm"},
        {"source": F.col("broker.brkr_acct_nbr"), "target": "brkr_acct_nbr"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]
