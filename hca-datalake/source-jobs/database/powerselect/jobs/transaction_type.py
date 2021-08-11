import sys
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

# Create dataframes from connection
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","DBO.HBR_TRANDESC").load()

# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")

# Query: dst_transaction_code
dst_transaction_code_select_df = sqlContext.sql("""
SELECT TRANS_CODE,TRANS_SUFFIX,LASTMAINT,BALANCE_INDICATOR,VOL_TRAN_DESC
 FROM (
    SELECT 
    ROW_NUMBER() OVER (PARTITION BY t1.TRANS_CODE, t1.TRANS_SUFFIX, t1.BALANCE_INDICATOR ORDER BY TIMESTAMP(CONCAT(t1.LASTMAINT,t1.LASTMAINT_TIME)) DESC) AS ROWS,
    t1.TRANS_CODE,
    t1.TRANS_SUFFIX,
    t1.BALANCE_INDICATOR,
    t1.VOL_TRAN_DESC,
    t1.LASTMAINT,
    t1.LASTMAINT_TIME
    FROM t1
 ) 
WHERE ROWS = 1
""")
#
# Print schema of output
dst_transaction_code_select_df.printSchema()
print("The number of rows returned by the query is " + str(dst_transaction_code_select_df.count()))
#
# schema
"""
root
 |-- TRANS_CODE: integer (nullable = true)
 |-- TRANS_SUFFIX: integer (nullable = true)
 |-- LASTMAINT: date (nullable = true)
 |-- BALANCE_INDICATOR: string (nullable = true)
 |-- VOL_TRAN_DESC: string (nullable = true)
"""
#
# Scrub and format results
dst_transaction_code_select_df_scrubbed = dst_transaction_code_select_df.select(
 dst_transaction_code_select_df.TRANS_CODE.cast(IntegerType()).alias('TRANS_CODE'),
 dst_transaction_code_select_df.TRANS_SUFFIX.cast(IntegerType()).alias('TRANS_SUFFIX'),
 dst_transaction_code_select_df.LASTMAINT.cast(DateType()).alias('LASTMAINT'),
 str_empty_udf('BALANCE_INDICATOR').alias('BALANCE_INDICATOR'),
 str_empty_udf('VOL_TRAN_DESC').alias('VOL_TRAN_DESC')
)
# Coalesce results
dst_transaction_code_select_write = dst_transaction_code_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_transaction_code_select_write is:") 
dst_transaction_code_select_write.printSchema()

# Potential final output filename(s): transaction_code
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_transaction_code_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
