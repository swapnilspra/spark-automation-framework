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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_CONSENTCUST").load()

#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")

#
# Query: dst_shareholder_consent_data
dst_consentcust_select_df = sqlContext.sql("""select * from t1""")
#
# Print schema of output
dst_consentcust_select_df.printSchema()
#print(dst_consentcust_select_df.count())
print("The number of rows returned by the query is " + str(dst_consentcust_select_df.count()))
#
# schema
"""
root
|-- FUND_SPONSOR_ID: integer (nullable = true)
|-- CLIENT_ID: integer (nullable = true)
|-- CUSTOMER_ID2: string (nullable = true)
|-- CUSTOMER_ID1: string (nullable = true)
|-- LOAD_DATE: date (nullable = true)
|-- LOAD_TIME: timestamp (nullable = true)
|-- CMR_INFD_CSNT: string (nullable = true)
|-- ELC_DVR_ADR_TYP: string (nullable = true)
|-- ELC_STM_FMT_TYP: integer (nullable = true)
|-- EXTERNAL_PLAN: string (nullable = true)
|-- ICU: string (nullable = true)
|-- ICU_TAX_ID: string (nullable = true)
|-- INFD_MNT_DATE: date (nullable = true)
|-- INFD_MNT_TIME: timestamp (nullable = true)
|-- INFD_RCV_DATE: date (nullable = true)
|-- LASTMAINT: date (nullable = true)
|-- LASTMAINT_OPID: string (nullable = true)
|-- LASTMAINT_TIME: timestamp (nullable = true)
|-- OTH_EXTL_CMR_ID: string (nullable = true)
|-- PLAN: string (nullable = true)
|-- REC_ADDED_DATE: date (nullable = true)
|-- REC_ADDED_TIME: timestamp (nullable = true)
|-- SEN_STM_DVR_ISP: string (nullable = true)
|-- STM_EML_ADR_LEN: integer (nullable = true)
|-- STM_EML_ADR_TXT: string (nullable = true)
|-- UDV_ELC_ADR_CDE: string (nullable = true)
"""
#
# Scrub and format results
dst_consentcust_select_df_scrubbed = dst_consentcust_select_df.select(
  dst_consentcust_select_df.FUND_SPONSOR_ID.cast(IntegerType()).alias('FUND_SPONSOR_ID'),
  dst_consentcust_select_df.CLIENT_ID.cast(IntegerType()).alias('CLIENT_ID'),
  str_empty_udf('CUSTOMER_ID2').alias('CUSTOMER_ID2'),
  str_empty_udf('CUSTOMER_ID1').alias('CUSTOMER_ID1'),
  dst_consentcust_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
  dst_consentcust_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
  str_empty_udf('CMR_INFD_CSNT').alias('CMR_INFD_CSNT'),
  str_empty_udf('ELC_DVR_ADR_TYP').alias('ELC_DVR_ADR_TYP'),
  dst_consentcust_select_df.ELC_STM_FMT_TYP.cast(IntegerType()).alias('ELC_STM_FMT_TYP'),
  str_empty_udf('EXTERNAL_PLAN').alias('EXTERNAL_PLAN'),
  str_empty_udf('ICU').alias('ICU'),
  str_empty_udf('ICU_TAX_ID').alias('ICU_TAX_ID'),
  dst_consentcust_select_df.INFD_MNT_DATE.cast(DateType()).alias('INFD_MNT_DATE'),
  dst_consentcust_select_df.INFD_MNT_TIME.cast(TimestampType()).alias('INFD_MNT_TIME'),
  dst_consentcust_select_df.INFD_RCV_DATE.cast(DateType()).alias('INFD_RCV_DATE'),
  dst_consentcust_select_df.LASTMAINT.cast(DateType()).alias('LASTMAINT'),
  str_empty_udf('LASTMAINT_OPID').alias('LASTMAINT_OPID'),
  dst_consentcust_select_df.LASTMAINT_TIME.cast(TimestampType()).alias('LASTMAINT_TIME'),
  str_empty_udf('OTH_EXTL_CMR_ID').alias('OTH_EXTL_CMR_ID'),
  str_empty_udf('PLAN').alias('PLAN'),
  dst_consentcust_select_df.REC_ADDED_DATE.cast(DateType()).alias('REC_ADDED_DATE'),
  dst_consentcust_select_df.REC_ADDED_TIME.cast(TimestampType()).alias('REC_ADDED_TIME'),
  str_empty_udf('SEN_STM_DVR_ISP').alias('SEN_STM_DVR_ISP'),
  dst_consentcust_select_df.STM_EML_ADR_LEN.cast(IntegerType()).alias('STM_EML_ADR_LEN'),
  str_empty_udf('STM_EML_ADR_TXT').alias('STM_EML_ADR_TXT'),
  str_empty_udf('UDV_ELC_ADR_CDE').alias('UDV_ELC_ADR_CDE')
)
# Coalesce results
dst_consentcust_select_write = dst_consentcust_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_shareholder_consent_data_select_write is:") 
dst_consentcust_select_write.printSchema()

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_consentcust_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
