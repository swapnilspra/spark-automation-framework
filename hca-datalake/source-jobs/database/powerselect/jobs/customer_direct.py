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
from pyspark.sql.functions import udf
from decimal import Decimal
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from datetime import datetime
from job_utils import get_s3_tmp, move_to_incoming, get_db_connection_from_glue_job, is_empty_string

str_empty_udf = udf(lambda z: is_empty_string(z), StringType())

args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_bucket','s3_tmp_path','s3_dest_path','aws_region'])
job_name = args['JOB_NAME']
bucket_name = args['s3_bucket']
tmp_path = args['s3_tmp_path']
dest_path = args['s3_dest_path']
aws_region = args['aws_region']

sc = SparkContext.getOrCreate()
from pyspark.sql import SQLContext

glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

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
# Create dataframes from connection
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_CUSTDIRECTINTERFAC").load()
#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
#t1_df.printSchema()
#
# Query: dst_shareholder_consent_data
dst_custdirectinterfac_select_df = sqlContext.sql("""select * from t1""")
#
# Print schema of output
dst_custdirectinterfac_select_df.printSchema()
#print(dst_custdirectinterfac_select_df.count())
print("The number of rows returned by the query is " + str(dst_custdirectinterfac_select_df.count()))
#
# schema
"""
root
|-- FUND_SPONSOR_ID: integer (nullable = true)
|-- CDI_USER_ID: long (nullable = true)
|-- LOAD_DATE: date (nullable = true)
|-- LOAD_TIME: timestamp (nullable = true)
|-- ACCESS_METHOD: string (nullable = true)
|-- ACCOUNT: string (nullable = true)
|-- CDI_CMR_USER_ID: string (nullable = true)
|-- ESTABLISHED_DATE: date (nullable = true)
|-- ESTABLISHED_TIME: timestamp (nullable = true)
|-- FUND: integer (nullable = true)
|-- INACTIVE_REASON_CD: string (nullable = true)
|-- INVALID_ATTEMPTS: integer (nullable = true)
|-- LAST_ACCESS: date (nullable = true)
|-- LASTMAINT: date (nullable = true)
|-- LASTMAINT_OPID: string (nullable = true)
|-- LASTMAINT_TIME: timestamp (nullable = true)
|-- PASSWORD_ESTABLISH_DATE: date (nullable = true)
|-- PASSWORD_ESTABLISH_TIME: timestamp (nullable = true)
|-- QUESTION_TEXT: string (nullable = true)
|-- RESET_ATTEMPTS: integer (nullable = true)
|-- RETRIEVE_ATTEMPTS: integer (nullable = true)
|-- SUPPRESS_UPDATES: string (nullable = true)
|-- TAX_ID_FORMAT_CDE: string (nullable = true)
|-- TAX_ID_NBR: string (nullable = true)
|-- USER_ID_CHG_DATE: date (nullable = true)
|-- USER_ID_CHG_TIME: timestamp (nullable = true)
"""
#
# Scrub and format results
dst_custdirectinterfac_select_df_scrubbed = dst_custdirectinterfac_select_df.select(
  dst_custdirectinterfac_select_df.FUND_SPONSOR_ID.cast(IntegerType()).alias('FUND_SPONSOR_ID'),
  dst_custdirectinterfac_select_df.CDI_USER_ID.cast(LongType()).alias('CDI_USER_ID'),
  dst_custdirectinterfac_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
  dst_custdirectinterfac_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
  str_empty_udf('ACCESS_METHOD').alias('ACCESS_METHOD'),
  str_empty_udf('ACCOUNT').alias('ACCOUNT'),
  str_empty_udf('CDI_CMR_USER_ID').alias('CDI_CMR_USER_ID'),
  dst_custdirectinterfac_select_df.ESTABLISHED_DATE.cast(DateType()).alias('ESTABLISHED_DATE'),
  dst_custdirectinterfac_select_df.ESTABLISHED_TIME.cast(TimestampType()).alias('ESTABLISHED_TIME'),
  dst_custdirectinterfac_select_df.FUND.cast(IntegerType()).alias('FUND'),
  str_empty_udf('INACTIVE_REASON_CD').alias('INACTIVE_REASON_CD'),
  dst_custdirectinterfac_select_df.INVALID_ATTEMPTS.cast(IntegerType()).alias('INVALID_ATTEMPTS'),
  dst_custdirectinterfac_select_df.LAST_ACCESS.cast(DateType()).alias('LAST_ACCESS'),
  dst_custdirectinterfac_select_df.LASTMAINT.cast(DateType()).alias('LASTMAINT'),
  str_empty_udf('LASTMAINT_OPID').alias('LASTMAINT_OPID'),
  dst_custdirectinterfac_select_df.LASTMAINT_TIME.cast(TimestampType()).alias('LASTMAINT_TIME'),
  dst_custdirectinterfac_select_df.PASSWORD_ESTABLISH_DATE.cast(DateType()).alias('PASSWORD_ESTABLISH_DATE'),
  dst_custdirectinterfac_select_df.PASSWORD_ESTABLISH_TIME.cast(TimestampType()).alias('PASSWORD_ESTABLISH_TIME'),
  str_empty_udf('QUESTION_TEXT').alias('QUESTION_TEXT'),
  dst_custdirectinterfac_select_df.RESET_ATTEMPTS.cast(IntegerType()).alias('RESET_ATTEMPTS'),
  dst_custdirectinterfac_select_df.RETRIEVE_ATTEMPTS.cast(IntegerType()).alias('RETRIEVE_ATTEMPTS'),
  str_empty_udf('SUPPRESS_UPDATES').alias('SUPPRESS_UPDATES'),
  str_empty_udf('TAX_ID_FORMAT_CDE').alias('TAX_ID_FORMAT_CDE'),
  str_empty_udf('TAX_ID_NBR').alias('TAX_ID_NBR'),
  dst_custdirectinterfac_select_df.USER_ID_CHG_DATE.cast(DateType()).alias('USER_ID_CHG_DATE'),
  dst_custdirectinterfac_select_df.USER_ID_CHG_TIME.cast(TimestampType()).alias('USER_ID_CHG_TIME')
)
# Coalesce results
dst_custdirectinterfac_select_write = dst_custdirectinterfac_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_shareholder_consent_data_select_write is:") 
dst_custdirectinterfac_select_write.printSchema()

# Potential final output filename(s): customer_direct
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_custdirectinterfac_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
