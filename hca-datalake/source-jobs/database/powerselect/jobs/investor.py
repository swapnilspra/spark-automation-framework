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
from pyspark.sql.types import DoubleType
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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_INVESTOR").load()
#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
#t1_df.printSchema()
#
# Query: dst_shareholder_consent_data
dst_investor_select_df = sqlContext.sql("""select * from t1""")
#
# Print schema of output
dst_investor_select_df.printSchema()
#print(dst_investor_select_df.count())
print("The number of rows returned by the query is " + str(dst_investor_select_df.count()))
#
# schema
"""
root
 |-- PRODUCT_ID: string (nullable = true)
 |-- REPORTING_GRP: integer (nullable = true)
 |-- FUND: integer (nullable = true)
 |-- ACCOUNT: string (nullable = true)
 |-- LOAD_DATE: date (nullable = true)
 |-- LOAD_TIME: timestamp (nullable = true)
 |-- LINKAGE_TYPE: string (nullable = true)
"""
#
# Scrub and format results
dst_investor_select_df_scrubbed = dst_investor_select_df.select(
 str_empty_udf('PRODUCT_ID').alias('PRODUCT_ID'),
 dst_investor_select_df.REPORTING_GRP.cast(IntegerType()).alias('REPORTING_GRP'),
 dst_investor_select_df.FUND.cast(IntegerType()).alias('FUND'),
 str_empty_udf('ACCOUNT').alias('ACCOUNT'),
 dst_investor_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
 dst_investor_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
 str_empty_udf('LINKAGE_TYPE').alias('LINKAGE_TYPE')
)
# Coalesce results
dst_investor_select_write = dst_investor_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_shareholder_consent_data_select_write is:") 
dst_investor_select_write.printSchema()

# Potential final output filename(s): shareholder_dividend_ytd, account_dividend_ytd
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_investor_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
