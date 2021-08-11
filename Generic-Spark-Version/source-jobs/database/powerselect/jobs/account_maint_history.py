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
#
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_ACCTMAINTHIST").load()

# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
t1_df.printSchema()

# Query: dst_shareholder_consent_data
dst_acctmainthist_select_df = sqlContext.sql("""select * from t1""")

# Print schema of output
dst_acctmainthist_select_df.printSchema()

print("The number of rows returned by the query is " + str(dst_acctmainthist_select_df.count()))

# schema
"""
root
 |-- MGMTCODE: string (nullable = true)
 |-- FUND: integer (nullable = true)
 |-- ACCOUNT: string (nullable = true)
 |-- VIEW_COLUMN_NM: string (nullable = true)
 |-- SUPER_SHEET_MNT_DT: date (nullable = true)
 |-- ACCT_MNT_SEQ_ID: decimal(3,0) (nullable = true)
 |-- LOAD_DATE: date (nullable = true)
 |-- LOAD_TIME: timestamp (nullable = true)
 |-- AFTER_VALUE: string (nullable = true)
 |-- BATCH_NUMBER: integer (nullable = true)
 |-- BEFORE_VALUE: string (nullable = true)
 """
#
# Scrub and format results
dst_acctmainthist_select_df_scrubbed = dst_acctmainthist_select_df.select(
 str_empty_udf('MGMTCODE').alias('MGMTCODE'),
 dst_acctmainthist_select_df.FUND.cast(IntegerType()).alias('FUND'),
 str_empty_udf('ACCOUNT').alias('ACCOUNT'),
 str_empty_udf('VIEW_COLUMN_NM').alias('VIEW_COLUMN_NM'),
 dst_acctmainthist_select_df.SUPER_SHEET_MNT_DT.cast(DateType()).alias('SUPER_SHEET_MNT_DT'),
 dst_acctmainthist_select_df.ACCT_MNT_SEQ_ID.cast(DecimalType(3,
 0)).alias('ACCT_MNT_SEQ_ID'),
 dst_acctmainthist_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
 dst_acctmainthist_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
 str_empty_udf('AFTER_VALUE').alias('AFTER_VALUE'),
 dst_acctmainthist_select_df.BATCH_NUMBER.cast(IntegerType()).alias('BATCH_NUMBER'),
 str_empty_udf('BEFORE_VALUE').alias('BEFORE_VALUE')
)

# Coalesce results
dst_acctmainthist_select_write = dst_acctmainthist_select_df_scrubbed.repartition(1)

print("The schema of the final output dst_shareholder_consent_data_select_write is:") 
dst_acctmainthist_select_write.printSchema()

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_acctmainthist_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
