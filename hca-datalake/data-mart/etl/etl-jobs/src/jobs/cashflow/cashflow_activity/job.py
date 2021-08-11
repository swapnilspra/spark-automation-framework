from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob  # must be imported after spark has been set up
from jobs.cashflow.sdcm_cashflow_insert import job
import datetime


"""
    cashflow activity job is an aggregate table of sdcm cashflow job.
    the etl process works exactly the same.
    therefore, in order not to write the code twice and not to maintain the code twice 
    this job inherited the etl process from sdcm_cashflow_insert
    and overwrite the process in def transform.
    
"""

class Job(job.Job):
    target_table = "cashflow_activity"
    business_key = ["fund_key", "day_key"]
    primary_key = {"cashflow_act_hist_key": "int"}

    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("fund_key"), "target": "fund_key"},
        {"source": F.col("prev_bus_day_key"), "target": "day_key"},
        {"source": F.col("sub_shrs"), "target": "sub_shrs"},
        {"source": F.col("sub_amt"), "target": "sub_amt"},
        {"source": F.col("redmpn_shrs"), "target": "redmpn_shrs"},
        {"source": F.col("redmpn_amt"), "target": "redmpn_amt"},
        {"source": F.col("net_shrs"), "target": "net_shrs"},
        {"source": F.col("net_cashflow_amt"), "target": "net_cashflow_amt"},
        {"source": F.lit(None).cast("string"), "target": "curr_row_flg"}

    ]

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_transformed = super().transform(df_joined)
        df_transformed = df_transformed \
            .groupBy("fund_key", "day_key")\
            .agg(F.sum(F.col("sub_shrs")).cast(T.DecimalType(38, 3)).alias("sub_shrs"),
                 F.sum(F.col("sub_amt")).cast(T.DecimalType(38, 3)).alias("sub_amt"),
                 F.sum(F.col("redmpn_shrs")).cast(T.DecimalType(38, 3)).alias("redmpn_shrs"),
                 F.sum(F.col("redmpn_amt")).cast(T.DecimalType(38, 3)).alias("redmpn_amt"),
                 F.sum(F.col("net_shrs")).cast(T.DecimalType(38, 3)).alias("net_shrs"),
                 F.sum(F.col("net_cashflow_amt")).cast(T.DecimalType(38, 3)).alias("net_cashflow_amt"))

        return df_transformed
