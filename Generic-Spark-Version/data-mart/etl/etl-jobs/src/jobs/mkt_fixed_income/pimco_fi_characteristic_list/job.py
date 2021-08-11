import pyspark.sql.types as T
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
            "type": "dimension",
            "source": "mkt_characteristics"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("infile.target_characteristics"), "target": "charctc_nm"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
    ]

    def transform(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_input = super().transform(df_joined)
        df_output=df_input.dropDuplicates()
        return df_output