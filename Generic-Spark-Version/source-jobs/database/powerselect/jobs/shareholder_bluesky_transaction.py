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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_transmtd").load()
t2_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_transme").load()

#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
t2_df.createOrReplaceTempView("t2")

#
# Query: dst_account_bluesky_transaction
dst_account_bluesky_transaction_select_df = sqlContext.sql("""
SELECT trans_id1,
       trans_id2,
       trim ( (bluesky_new_repeat)) BLUESKY_TXN_FLG,
       trim ( (bluesky_reopen_account_cde)) BLUESKY_REOPEN_ACCOUNT_CDE
  FROM t1
UNION
SELECT trans_id1,
       trans_id2,
       trim ( (bluesky_new_repeat)) BLUESKY_TXN_FLG,
       trim ( (bluesky_reopen_account_cde)) BLUESKY_REOPEN_ACCOUNT_CDE
  FROM t2
""")
#
# Print schema of output
dst_account_bluesky_transaction_select_df.printSchema()

print("The number of rows returned by the query is " + str(dst_account_bluesky_transaction_select_df.count()))

# schema
"""
root
 |-- trans_id1: double (nullable = true)
 |-- trans_id2: double (nullable = true)
 |-- BLUESKY_TXN_FLG: string (nullable = true)
 |-- BLUESKY_REOPEN_ACCOUNT_CDE: string (nullable = true)
"""
#
# Scrub and format results
dst_account_bluesky_transaction_select_df_scrubbed = dst_account_bluesky_transaction_select_df.select(
       dst_account_bluesky_transaction_select_df.trans_id1.cast(DoubleType()).alias('trans_id1'),
       dst_account_bluesky_transaction_select_df.trans_id2.cast(DoubleType()).alias('trans_id2'),
       str_empty_udf('BLUESKY_TXN_FLG').alias('BLUESKY_TXN_FLG'),
       str_empty_udf('BLUESKY_REOPEN_ACCOUNT_CDE').alias('BLUESKY_REOPEN_ACCOUNT_CDE')
)
# Coalesce results
dst_account_bluesky_transaction_select_write = dst_account_bluesky_transaction_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_account_bluesky_transaction_select_write is:") 
dst_account_bluesky_transaction_select_write.printSchema()

# Potential final output filename(s): account_bluesky_transaction
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_account_bluesky_transaction_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
