from typing import Dict, List, Any
import math
import logging
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime
import pyspark.sql.types as T

class Job(ETLJob):
    target_table = "fin_mpt_statistics"
    business_key = ["day_key","fund_key","bmk_idx_key","per_key"]
    primary_key = {"mpt_stats_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "mstar_alphabeta"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "fir": {
            "type": "table",
            "source": "fund_index_rltn"
        },
        "prt": {
            "type": "table",
            "source": "perf_run_type"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "period": {
            "type": "table",
            "source": "return_periods"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "infile"
        },
        {
            "source": "fund",
            "conditions": [
                F.upper(F.col("ticker")) == F.col("fund.quot_sym")
            ]
        },
        {
            "source": "fir",
            "conditions": [
                F.col("fir.fund_compst_key") == F.col("fund.fund_compst_key"),
                F.col("fir.link_prio") == F.lit(1)
            ]
        },
        {
            "source": "prt",
            "conditions": [
                F.col("prt.perf_run_type_key") == F.col("fir.perf_run_type_key"),
                F.col("prt.run_type_cd") == F.lit("REPORTING")
            ]
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.col("cal.cal_day")) == F.date_trunc('day',(F.to_timestamp(F.col("end_date"), "MM/dd/yyyy")))
            ]
        },
        {
            "source": "period",
            "conditions": [
                F.col("pername") == F.upper(F.col("period.per_nm"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("fir.bmk_idx_key"), "target": "bmk_idx_key"},
        {"source": F.col("period.per_key"), "target": "per_key"},
        {"source": F.col("alpha"), "target": "alpha"},
        {"source": F.col("beta"), "target": "beta"},
        {"source": F.col("rsquared"), "target": "r_squared"},
        {"source": F.col("sharperatio"), "target": "sharpe_rate"},
        {"source": F.col("stddev"), "target": "std_deviation"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]
    
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:        
        df_inputs = super().extract(catalog)
        df_t1 = df_inputs["infile"]
        df_temp = df_t1.filter(df_t1["`EndDate`"].isNotNull()).drop("_c16")

        # we pass a struct represeting the entire row into a UDF
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug("load: total rows from MorningStar HarborFundsAlphaBeta file: %s" % df_temp.count())
        
        df_transformed = df_temp.withColumn(
            "stats_value",
            map_fields(F.struct([F.col(x) for x in df_temp.columns])) )
        
        # use explode to break out the dict into separate records      
        df_transformed = df_transformed.select("*",F.explode(F.col("stats_value")).alias("stats_dict"))\
            .select(
                F.col("EndDate").alias("end_date"),
                F.col("Ticker").alias("ticker"),
                "stats_dict.pername",
                "stats_dict.alpha",
                "stats_dict.beta",
                "stats_dict.rsquared",
                "stats_dict.sharperatio",
                "stats_dict.stddev"
            ).drop("stats_value")
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug("load: total rows after transform: %s" % df_transformed.count())
        df_transformed.show(20)
       
        df_inputs["infile"] = df_transformed

        return df_inputs

#
# UDF
#

@F.udf(returnType=T.ArrayType(T.StructType([
    T.StructField('pername', T.StringType()),
    T.StructField('alpha', T.DoubleType()),
    T.StructField('beta', T.DoubleType()),
    T.StructField('rsquared', T.DoubleType()),
    T.StructField('sharperatio', T.DoubleType()),
    T.StructField('stddev', T.DoubleType())
])))
def map_fields(row) -> List[Dict[str,Any]]:
    return_rows:List[Dict[str,Any]] = [
        {
            "pername":"YR1",
            "alpha": row['Alpha1Yr'],
            "beta": row['Beta1Yr'],
            "rsquared": row['Rsquared1Yr'],
            "sharperatio": row['SharpeRatio1Yr'],
            "stddev": row['StdDev1Yr']
        },        
        {
            "pername":"YR3",
            "alpha": row['Alpha3Yr'],
            "beta": row['Beta3Yr'],
            "rsquared": row['Rsquared3Yr'],
            "sharperatio": row['SharpeRatio3Yr'],
            "stddev": row['StdDev3Yr']
        }
    ]
    return return_rows