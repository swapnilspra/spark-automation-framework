import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from decimal import Decimal
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.types import DoubleType
#from datetime import datetime
# In contrast to the other scripts, need more than just datetime.datetime so importing full datetime module
import datetime
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType
# For addition of derived "PROCESS_DATE" col to add to dataframe + empty CHAR string fix (str_empty_udf)
from pyspark.sql.functions import udf
#
from job_utils import get_s3_tmp, move_to_incoming, get_db_connection_from_glue_job, is_empty_string

str_empty_udf = udf(lambda z: is_empty_string(z), StringType())
#
# Not taking an argument: computing relative to current local time
# Getting serialized so outputting as string
#
def last_day_prev_mnth():
    now = datetime.datetime.now().date()
    last_day_prev_mnth = datetime.date(now.year, now.month, 1) - datetime.timedelta(1) 
#   formatting as mm/ dd/yyyy
    last_day_prev_mnth_format = last_day_prev_mnth.strftime('%m/%d/%Y')
    return str(last_day_prev_mnth_format)
#
# Make it a UDF
last_day_prev_mnth_udf = udf(lambda: last_day_prev_mnth(), StringType())

#
args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_bucket','s3_tmp_path','s3_dest_path','aws_region'])
job_name = args['JOB_NAME']
bucket_name = args['s3_bucket']
tmp_path = args['s3_tmp_path']
dest_path = args['s3_dest_path']
aws_region = args['aws_region']
#
sc = SparkContext.getOrCreate()
from pyspark.sql import SQLContext
#
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)
#
#
# initialize glue job with glue security config supplied in params
# to encrypt cloudwatch logs
#
job = Job(glueContext) 
job.init(job_name, args)
#
# parse db connection details from glue connection
#
jdbc_url, db_username, db_password, jdbc_driver_name = get_db_connection_from_glue_job(job_name, aws_region)
#
# Start job
#
# Create dataframes from connection
HBR_OPERSECU_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","DBO.HBR_OPERSECU").load()
HBR_OPERFUNC_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","DBO.HBR_OPERFUNC").load()
HBR_FACISECU_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","DBO.HBR_FACISECU").load()
#
# Convert dataframe to tables/views
HBR_OPERSECU_df.createOrReplaceTempView("HBR_OPERSECU")
HBR_OPERFUNC_df.createOrReplaceTempView("HBR_OPERFUNC")
HBR_FACISECU_df.createOrReplaceTempView("HBR_FACISECU")
#
# Query: dst_oper_status
dst_oper_status_select_df = sqlContext.sql("""
  SELECT HBR_OPERSECU.OPERATOR_ID AS OPERATOR_ID,
       HBR_OPERSECU.COPY_FROM_OPER_ID AS COPY_FROM_OPER_ID,
       HBR_OPERSECU.FIRST_LOGON_DTE AS FIRST_LOGON_DTE,
       HBR_OPERSECU.FIRST_LOGON_TIME AS FIRST_LOGON_TIME,
       HBR_OPERSECU.IWS_ACCESS_CDE AS IWS_ACCESS_CDE,
       HBR_OPERSECU.LAST_LOGON_DTE AS LAST_LOGON_DTE,
       HBR_OPERSECU.LAST_LOGON_TIME AS LAST_LOGON_TIME,
       HBR_OPERSECU.LASTMAINT AS OPERSECU_LASTMAINT,
       HBR_OPERSECU.LASTMAINT_ID AS OPERSECU_LASTMAINT_ID,
       HBR_OPERSECU.MGMT_CODE AS MGMT_CODE,
       HBR_OPERSECU.NBR_OF_LOGONS AS NBR_LOGONS,
       HBR_OPERSECU.OPER_EXPIRED_CDE AS OPER_EXPIRED_CDE,
       HBR_OPERSECU.OPER_NAME AS OPER_NAME,
       HBR_OPERSECU.OPR_EXPT_STR_DT AS OPR_EXPT_STR_DT,
       HBR_OPERSECU.OPR_LAS_PSW_RS_DT AS OPR_LAS_PSW_RS_DT,
       HBR_OPERSECU.PASSWORD_CHG_DTE AS PASSWORD_CHG_DTE,
       HBR_OPERSECU.PASSWORD_TRIES_CNT AS PASSWORD_TRIES_CNT,
       HBR_OPERSECU.PLAN_SPSR_ACCE_CDE AS PLAN_SPSR_ACCE_CDE,
       HBR_FACISECU.DESCRIPTION_TEXT AS DESCRIPTION_TEXT,
       HBR_FACISECU.FACILITY_FUNCTION_ID AS FACILITY_FUNCTION_ID,
       HBR_OPERFUNC.FACILITY_ID AS FACILITY_ID,
       HBR_OPERFUNC.AUTHOR_CDE AS AUTHOR_CDE,
       HBR_OPERFUNC.LASTMAINT AS OPERFUNC_LASTMAINT,
       HBR_FACISECU.FUNCTION_ADD_CODE AS FUNCTION_ADD_CODE,
       HBR_FACISECU.FUNCTION_BROWSE_CODE AS FUNCTION_BROWSE_CODE,
       HBR_FACISECU.FUNCTION_DELETE_CODE AS FUNCTION_DELETE_CODE,
       HBR_FACISECU.FUNCTION_UPDATE_CODE AS FUNCTION_UPDATE_CODE,
       HBR_FACISECU.MINOR_ADD_CODE AS MINOR_ADD_CODE,
       HBR_FACISECU.MINOR_BROWSE_CODE AS MINOR_BROWSE_CODE,
       HBR_FACISECU.MINOR_DELETE_CODE AS MINOR_DELETE_CODE,
       HBR_FACISECU.MINOR_UPDATE_CODE AS MINOR_UPDATE_CODE
FROM    ( HBR_OPERSECU
       INNER JOIN
          HBR_OPERFUNC
       ON HBR_OPERSECU.OPERATOR_ID = HBR_OPERFUNC.OPERATOR_ID)
   INNER JOIN
      HBR_FACISECU
   ON (HBR_OPERFUNC.FUNCTION_ID = HBR_FACISECU.FACILITY_FUNCTION_ID)
      AND (HBR_OPERFUNC.FACILITY_ID = HBR_FACISECU.FACILITY_ID)
""")
#
# Print schema of output
dst_oper_status_select_df.printSchema()
#print(dst_oper_status_df.count())
print("The number of rows returned by the query is " + str(dst_oper_status_select_df.count()))
print("The last day of the previous month formatted as mm/dd/yyyy is " + last_day_prev_mnth())
#
# schema
"""
The number of rows returned by the query is 19384

root
|-- OPERATOR_ID: string (nullable = true)
|-- COPY_FROM_OPER_ID: string (nullable = true)
|-- FIRST_LOGON_DTE: string (nullable = true)
|-- FIRST_LOGON_TIME: string (nullable = true)
|-- IWS_ACCESS_CDE: string (nullable = true)
|-- LAST_LOGON_DTE: string (nullable = true)
|-- LAST_LOGON_TIME: string (nullable = true)
|-- OPERSECU_LASTMAINT: date (nullable = true)
|-- OPERSECU_LASTMAINT_ID: string (nullable = true)
|-- MGMT_CODE: string (nullable = true)
|-- NBR_LOGONS: integer (nullable = true)
|-- OPER_EXPIRED_CDE: string (nullable = true)
|-- OPER_NAME: string (nullable = true)
|-- OPR_EXPT_STR_DT: date (nullable = true)
|-- OPR_LAS_PSW_RS_DT: date (nullable = true)
|-- PASSWORD_CHG_DTE: string (nullable = true)
|-- PASSWORD_TRIES_CNT: integer (nullable = true)
|-- PLAN_SPSR_ACCE_CDE: string (nullable = true)
|-- DESCRIPTION_TEXT: string (nullable = true)
|-- FACILITY_FUNCTION_ID: integer (nullable = true)
|-- FACILITY_ID: string (nullable = true)
|-- AUTHOR_CDE: string (nullable = true)
|-- OPERFUNC_LASTMAINT: date (nullable = true)
|-- FUNCTION_ADD_CODE: string (nullable = true)
|-- FUNCTION_BROWSE_CODE: string (nullable = true)
|-- FUNCTION_DELETE_CODE: string (nullable = true)
|-- FUNCTION_UPDATE_CODE: string (nullable = true)
|-- MINOR_ADD_CODE: string (nullable = true)
|-- MINOR_BROWSE_CODE: string (nullable = true)
|-- MINOR_DELETE_CODE: string (nullable = true)
|-- MINOR_UPDATE_CODE: string (nullable = true)
"""
#
#
# Add column for last day of previous month
dst_oper_status_output_df = dst_oper_status_select_df.withColumn("PROCESS_DATE",last_day_prev_mnth_udf())
# Scrub and format results
dst_oper_status_select_df_scrubbed = dst_oper_status_output_df.select(
    str_empty_udf('OPERATOR_ID').alias('OPERATOR_ID'),
    str_empty_udf('COPY_FROM_OPER_ID').alias('COPY_FROM_OPER_ID'),
    str_empty_udf('FIRST_LOGON_DTE').alias('FIRST_LOGON_DTE'),
    str_empty_udf('FIRST_LOGON_TIME').alias('FIRST_LOGON_TIME'),
    str_empty_udf('IWS_ACCESS_CDE').alias('IWS_ACCESS_CDE'),
    str_empty_udf('LAST_LOGON_DTE').alias('LAST_LOGON_DTE'),
    str_empty_udf('LAST_LOGON_TIME').alias('LAST_LOGON_TIME'),
    dst_oper_status_output_df.OPERSECU_LASTMAINT.cast(DateType()).alias('OPERSECU_LASTMAINT'),
    str_empty_udf('OPERSECU_LASTMAINT_ID').alias('OPERSECU_LASTMAINT_ID'),
    str_empty_udf('MGMT_CODE').alias('MGMT_CODE'),
    dst_oper_status_output_df.NBR_LOGONS.cast(IntegerType()).alias('NBR_LOGONS'),
    str_empty_udf('OPER_EXPIRED_CDE').alias('OPER_EXPIRED_CDE'),
    str_empty_udf('OPER_NAME').alias('OPER_NAME'),
    dst_oper_status_output_df.OPR_EXPT_STR_DT.cast(DateType()).alias('OPR_EXPT_STR_DT'),
    dst_oper_status_output_df.OPR_LAS_PSW_RS_DT.cast(DateType()).alias('OPR_LAS_PSW_RS_DT'),
    str_empty_udf('PASSWORD_CHG_DTE').alias('PASSWORD_CHG_DTE'),
    dst_oper_status_output_df.PASSWORD_TRIES_CNT.cast(IntegerType()).alias('PASSWORD_TRIES_CNT'),
    str_empty_udf('PLAN_SPSR_ACCE_CDE').alias('PLAN_SPSR_ACCE_CDE'),
    str_empty_udf('DESCRIPTION_TEXT').alias('DESCRIPTION_TEXT'),
    dst_oper_status_output_df.FACILITY_FUNCTION_ID.cast(IntegerType()).alias('FACILITY_FUNCTION_ID'),
    str_empty_udf('FACILITY_ID').alias('FACILITY_ID'),
    str_empty_udf('AUTHOR_CDE').alias('AUTHOR_CDE'),
    dst_oper_status_output_df.OPERFUNC_LASTMAINT.cast(DateType()).alias('OPERFUNC_LASTMAINT'),
    str_empty_udf('FUNCTION_ADD_CODE').alias('FUNCTION_ADD_CODE'),
    str_empty_udf('FUNCTION_BROWSE_CODE').alias('FUNCTION_BROWSE_CODE'),
    str_empty_udf('FUNCTION_DELETE_CODE').alias('FUNCTION_DELETE_CODE'),
    str_empty_udf('FUNCTION_UPDATE_CODE').alias('FUNCTION_UPDATE_CODE'),
    str_empty_udf('MINOR_ADD_CODE').alias('MINOR_ADD_CODE'),
    str_empty_udf('MINOR_BROWSE_CODE').alias('MINOR_BROWSE_CODE'),
    str_empty_udf('MINOR_DELETE_CODE').alias('MINOR_DELETE_CODE'),
    str_empty_udf('MINOR_UPDATE_CODE').alias('MINOR_UPDATE_CODE'),
    str_empty_udf('PROCESS_DATE').alias('PROCESS_DATE')
)
#
## Output sample rows -- note: non-PII
## print("The data looks like:")
## dst_oper_status_select_df_scrubbed.show()
# Coalesce results
dst_oper_status_select_write = dst_oper_status_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_oper_status_select_write is:") 
dst_oper_status_select_write.printSchema()
#
"""
root
 |-- OPERATOR_ID: string (nullable = true)
 |-- COPY_FROM_OPER_ID: string (nullable = true)
 |-- FIRST_LOGON_DTE: string (nullable = true)
 |-- FIRST_LOGON_TIME: string (nullable = true)
 |-- IWS_ACCESS_CDE: string (nullable = true)
 |-- LAST_LOGON_DTE: string (nullable = true)
 |-- LAST_LOGON_TIME: string (nullable = true)
 |-- OPERSECU_LASTMAINT: date (nullable = true)
 |-- OPERSECU_LASTMAINT_ID: string (nullable = true)
 |-- MGMT_CODE: string (nullable = true)
 |-- NBR_LOGONS: integer (nullable = true)
 |-- OPER_EXPIRED_CDE: string (nullable = true)
 |-- OPER_NAME: string (nullable = true)
 |-- OPR_EXPT_STR_DT: date (nullable = true)
 |-- OPR_LAS_PSW_RS_DT: date (nullable = true)
 |-- PASSWORD_CHG_DTE: string (nullable = true)
 |-- PASSWORD_TRIES_CNT: integer (nullable = true)
 |-- PLAN_SPSR_ACCE_CDE: string (nullable = true)
 |-- DESCRIPTION_TEXT: string (nullable = true)
 |-- FACILITY_FUNCTION_ID: integer (nullable = true)
 |-- FACILITY_ID: string (nullable = true)
 |-- AUTHOR_CDE: string (nullable = true)
 |-- OPERFUNC_LASTMAINT: date (nullable = true)
 |-- FUNCTION_ADD_CODE: string (nullable = true)
 |-- FUNCTION_BROWSE_CODE: string (nullable = true)
 |-- FUNCTION_DELETE_CODE: string (nullable = true)
 |-- FUNCTION_UPDATE_CODE: string (nullable = true)
 |-- MINOR_ADD_CODE: string (nullable = true)
 |-- MINOR_BROWSE_CODE: string (nullable = true)
 |-- MINOR_DELETE_CODE: string (nullable = true)
 |-- MINOR_UPDATE_CODE: string (nullable = true)
 |-- PROCESS_DATE: string (nullable = true)
"""
#
# Potential final output filename(s): oper_status
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_oper_status_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
