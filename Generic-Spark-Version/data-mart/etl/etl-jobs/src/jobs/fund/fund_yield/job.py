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
    target_table = "fund_yield"
    business_key = ["fund_key","day_key","yld_type_key"]
    primary_key = {"fund_yld_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "sec_yld": {
            "type": "file",
            "source": "sec_yld"
        },
        "rates": {
            "type": "file",
            "source": "rates"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "yield_types": {
            "type": "table",
            "source": "yield_types"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "sec_yld"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.col("sec_yld.fund_id")
            ]
        },
        {
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("calendar.cal_day")) == F.col("sec_yld.date")
            ]
        },
        {
            "source": "yield_types",
            "conditions": [
                F.col("yield_types.yld_cd") == F.col("sec_yld.yld_cd")
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [

        { "source": F.col("fund.fund_key"), "target": "fund_key" },
        { "source": F.col("calendar.day_key"), "target": "day_key" },
        { "source": F.col("yield_types.yld_type_key"), "target": "yld_type_key" },
        { "source": F.col("sec_yld.fund_yld"), "target": "fund_yld" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" },
    ]

    #
    # override extract to explode each yield
    #
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)

        df_yield = df_inputs["sec_yld"]
        # add a column with a dict of yield types and values
        # we pass a struct represeting the entire row into a UDF
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug("load: total rows from sec yld file: %s" % df_yield.count())
        df_transformed = df_yield.withColumn(
            "yields_dict",
            map_yields(F.struct([F.col(x) for x in df_yield.columns])) )
        # use explode to break out the dict into separate records
        df_transformed = df_transformed.select("*",F.explode(F.col("yields_dict")).alias("yields"))\
            .select(
                F.col("alternate fund id").alias("fund_id"),
                F.to_date(F.col("date"),"MM/dd/yyyy HH:mm").alias("date"),
                "yields.fund_yld",
                "yields.yld_cd"
            ).drop("yields_dict")
        # logging
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug("load: total rows after transform: %s" % df_transformed.count())

        #
        # add money_market funds to df
        #
        df_mm_yield = df_inputs["rates"]
        # add a column with a dict of yield types and values
        # we pass a struct represeting the entire row into a UDF
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug("load: total rows from rates file: %s" % df_mm_yield.count())
        df_mm_transformed = df_mm_yield.withColumn(
            "yields_dict",
            map_mm_yields(F.struct([F.col(x) for x in df_mm_yield.columns])) )
        # use explode to break out the dict into separate records
        df_mm_transformed = df_mm_transformed.select("*",F.explode(F.col("yields_dict")).alias("yields"))\
            .select(
                F.col("ssb_fund").alias("fund_id"),
                F.to_date(F.col("upd_date"),"MM/dd/yyyy HH:mm").alias("date"),
                "yields.fund_yld",
                "yields.yld_cd"
            ).drop("yields_dict")

        # logging
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug("load: total rows after rates transform: %s" % df_mm_transformed.count())
        
        # concatenate both fund yields
        df_inputs["sec_yld"] = (df_transformed.union(df_mm_transformed)).alias("sec_yld")
        return df_inputs
#
# UDF
#
@F.udf(returnType=T.ArrayType(T.StructType([
    T.StructField('yld_cd', T.StringType()),
    T.StructField('fund_yld', T.DoubleType())
])))
def map_yields(row) -> List[Dict[str,Any]]:
    SEC_YLD_in:float = row['SEC_YLD Filler 9']
    SEC_YLD_WO_in:float = row['SEC_YLD W/O Filler 10']
    DAY_30_TIP_v:float = row['Distribution Adjustment Short']
    DAY_30_TIP_WO_v:float = row['Distribution Adjustment Long']
    # calculate days in year in previous year
    today:datetime = datetime.now()
    year_ago:datetime = today.replace(year=today.year-1)
    DAYS_IN_YEAR_v:float = float( (today-year_ago).days )

    return_rows:List[Dict[str,Any]] = [
        {
            "yld_cd":"30NOCU",
            "fund_yld":SEC_YLD_in
        },
        {
            "yld_cd":"U30NC",
            "fund_yld":SEC_YLD_WO_in
        },
        {
            "yld_cd":"30NOEF",
            "fund_yld":math.pow((SEC_YLD_in*30/DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1
        },
        {
            "yld_cd":"U30NE",
            "fund_yld":math.pow((SEC_YLD_WO_in*30/DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1
        },        
    ]
    if (DAY_30_TIP_v is not None):
        return_rows.extend([
            {
                "yld_cd":"30TIPNOCU",
                "fund_yld":DAY_30_TIP_v
            },
            {
                "yld_cd":"30TIPNOEF",
                "fund_yld":math.pow((DAY_30_TIP_v*30/DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1
            }
        ])

    if (DAY_30_TIP_WO_v is not None):
        return_rows.extend([
            {
                "yld_cd":"U30TIPNOCU",
                "fund_yld":DAY_30_TIP_WO_v
            },
            {
                "yld_cd":"U30TIPNE",
                "fund_yld":math.pow((DAY_30_TIP_WO_v*30/DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1
            }
        ])
    
    return return_rows

#
# UDF
#
@F.udf(returnType=T.ArrayType(T.StructType([
    T.StructField('yld_cd', T.StringType()),
    T.StructField('fund_yld', T.DoubleType())
])))
def map_mm_yields(row) -> List[Dict[str,Any]]:
    # calculate days in year in previous year
    today:datetime = datetime.now()
    year_ago:datetime = today.replace(year=today.year-1)
    DAYS_IN_YEAR_v:float = float( (today-year_ago).days )
    return_rows:Dict[str,float] = {}

    if row['WITH GL']=='N':
        return_rows = {
            "01NOCU" : row['1 DAY']/100,
            "U01NC" : row['1 DAY W/O']/100,
            "07NOCU" : row['7 DAY']/100,
            "U07NC" : row['7 DAY W/O']/100,
            "30NOCU" : row['30 DAY']/100,
            "U30NC" : row['30 DAY W/O']/100,
            "07NOGS" : row['7 DAY GROSS W/O GL']/100,
        }
        return_rows['01NOEF'] = math.pow((return_rows['01NOCU'] / DAYS_IN_YEAR_v+1),DAYS_IN_YEAR_v)-1
        return_rows['U01NE'] =  math.pow((return_rows['U01NC'] / DAYS_IN_YEAR_v+1),DAYS_IN_YEAR_v)-1
        return_rows['07NOEF'] = math.pow((return_rows['07NOCU']*7 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/7))-1
        return_rows['U07NE'] = math.pow((return_rows['U07NC']*7 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/7))-1
        return_rows['30NOEF'] = math.pow((return_rows['30NOCU']*30 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1
        return_rows['U30NE'] = math.pow((return_rows['U30NC']*30 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1
        return_rows['07NOGSEF'] = math.pow((return_rows['07NOGS']*7 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/7))-1
    elif row['WITH GL']=='Y':
        # WITH GL = Y
        return_rows = {
            "01GLCU" : row['1 DAY']/100,
            "U01GC" : row['1 DAY W/O']/100,
            "07GLCU" : row['7 DAY']/100,
            "U07GC" : row['7 DAY W/O']/100,
            "30GLCU" : row['30 DAY']/100,
            "U30GC" : row['30 DAY W/O']/100,
            "07GLGS" : row['7 DAY GROSS W/O GL']/100,
        }
        return_rows['01GLEF'] = math.pow((return_rows['01GLCU'] / DAYS_IN_YEAR_v+1),DAYS_IN_YEAR_v)-1
        return_rows['U01GE'] =  math.pow((return_rows['U01GC'] / DAYS_IN_YEAR_v+1),DAYS_IN_YEAR_v)-1
        return_rows['07GLEF'] = math.pow((return_rows['07GLCU']*7 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/7))-1
        return_rows['U07GE'] = math.pow((return_rows['U07GC']*7 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/7))-1
        return_rows['30GLEF'] = math.pow((return_rows['30GLCU']*30 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1
        return_rows['U30GE'] = math.pow((return_rows['U30GC']*30 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1
        return_rows['07GLGSEF'] = math.pow((return_rows['07GLGS']*7 / DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/7))-1

    # transform into a json-like dict
    return [
                {
                "yld_cd":key,
                "fund_yld":value
                }
            for key,value in return_rows.items()]

