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
    target_table = "fi_characteristics"
    business_key = ["fund_compst_key","day_key","fi_charctc_list_key"]
    primary_key = {"fi_charctc_fact_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "infile": {
            "type": "file",
            "source": "pimco_mkt_statistics"
        },
        "acctref": {
            "type": "dimension",
            "source": "pimco_account_reference"
        },
        "parent": {
            "type": "table",
            "source": "fi_characteristic_list"
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
                F.upper(F.col("acct")) == F.col("acctref.PMC_ACCT_NBR")
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
                F.to_date(F.col("cal.cal_day")) == F.date_trunc('day',(F.to_timestamp(F.col("date"), "MM/dd/yyyy")))
            ]
        },
        {
            "source": "parent",
            "conditions": [
                F.upper(F.col("charnm")) == F.upper(F.col("parent.charctc_nm"))
            ]
        }
    ]    
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fundcomp.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("parent.fi_charctc_list_key"), "target": "fi_charctc_list_key"},
        {"source": F.col("fund_val"), "target": "fund_charctc_val"},
        {"source": F.col("bmk_val"), "target": "prim_bmk_charctc"},
        {"source": F.lit(None).cast(T.DecimalType()), "target": "secy_bmk_charctc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]


    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)

        df_temp = df_inputs["infile"]
        df_temp = df_temp.filter(df_temp["`Asof Date`"].isNotNull())

        # we pass a struct represeting the entire row into a UDF
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug("load: total rows from PIMCO_MKT_STATISTICS file: %s" % df_temp.count())
        df_transformed = df_temp.withColumn(
            "charc_value",
            map_fields(F.struct([F.col(x) if x!='Weighted Avg. Maturity - Benchmark' else "`Weighted Avg. Maturity - Benchmark`" for x in df_temp.columns])) )
        # use explode to break out the dict into separate records      
        df_transformed = df_transformed.select("*",F.explode(F.col("charc_value")).alias("charc_dict"))\
            .select(
                F.col("Acct No").alias("acct"),
                F.col("Asof Date").alias("date"),
                "charc_dict.charnm",
                "charc_dict.fund_val",
                "charc_dict.bmk_val"
            ).drop("charc_value")
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug("load: total rows after transform: %s" % df_transformed.count())
        df_transformed.show(10)
       
        df_inputs["infile"] = df_transformed
        return df_inputs

#
# UDF
#

@F.udf(returnType=T.ArrayType(T.StructType([
    T.StructField('charnm', T.StringType()),
    T.StructField('fund_val', T.DoubleType()),
    T.StructField('bmk_val', T.DoubleType())
])))
def map_fields(row) -> List[Dict[str,Any]]:
    tna_in:float = row['Total Net Assets - All Classes']
    fixedincome_in:float = row['Fixed Income Assets']
    cash_in:float = row['Cash & Other Assets Less Liabilities']
    issuesfund_in:float = row['Number of Issues - Fund']
    issuesbmk_in:float = row['Number of Issues - Benchmark']
    couponbmk_in:float = row['Average Market Coupon - Benchmark']
    maturitybmk_in:float = row["Weighted Avg. Maturity - Benchmark"]

    #charnm must match target_characteristics in mkt_characteristics.csv
    return_rows:List[Dict[str,Any]] = [
        {
            "charnm":"Total Net Assets - All Classes",
            "fund_val": float(tna_in),
            "bmk_val": None
        },
        {
            "charnm":"Fixed Income Assets",
            "fund_val": float(fixedincome_in),
            "bmk_val": None
        },
        {
            "charnm":"Cash & Other Assets Less Liabilities",
            "fund_val": float(cash_in),
            "bmk_val": None
        },
        {
            "charnm":"Number of Issues",
            "fund_val": float(issuesfund_in),
            "bmk_val": float(issuesbmk_in)
        },
        {
            "charnm":"Average Market Coupon (%)",
            "fund_val": None,
            "bmk_val": couponbmk_in
        },
        {
            "charnm":"Weighted Avg. Maturity (yrs)",
            "fund_val": None,
            "bmk_val": maturitybmk_in
        }
    ]
    return return_rows