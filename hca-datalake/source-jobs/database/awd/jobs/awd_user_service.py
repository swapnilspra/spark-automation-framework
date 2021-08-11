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
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.types import DoubleType
from datetime import datetime

# For the empty CHAR field fix is_empty_string
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from job_utils import get_s3_tmp, move_to_incoming

args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_bucket','s3_tmp_path','s3_dest_path','glue_db','source_db_schema'])
bucket_name = args['s3_bucket']
tmp_path = args['s3_tmp_path']
dest_path = args['s3_dest_path']
db = args['glue_db']
src_schema = args['source_db_schema']

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
job.init(args['JOB_NAME'], args)

#####################################################
#
# THIS SHOULD BE MOVED TO job_utils.py
#
#####################################################

def is_empty_string(s):
    if s.isspace():
        return None
    else:
        return s

str_empty_udf = udf(lambda z: is_empty_string(z), StringType())

#####################################################
#
# THIS SHOULD BE MOVED TO job_utils.py
#
#####################################################

#Table(s)
_W11U999S = glueContext.create_dynamic_frame.from_catalog(database=db, table_name=f"{src_schema}_awd_W11U999S")

# Convert dynamic dataframes to dataframes
W11U999S_df = _W11U999S.toDF()

# Convert dataframes to temp views
W11U999S_df.createOrReplaceTempView("W11U999S")

# awd_user_queue Query
awd_user_service_select_df = sqlContext.sql("""
SELECT 
    W11U999S.USERID, 
    W11U999S.QUEUECD, 
    W11U999S.EXISTFLAG, 
    W11U999S.VIEWFLAG, 
    W11U999S.UPDATEFLAG, 
    W11U999S.WRKSELFLAG, 
    RPAD(W11U999S.MNTDATTIM ,26,'0') AS MNTDATTIM
FROM W11U999S
""")

print("The schema is:")
awd_user_service_select_df.printSchema()

cnt = awd_user_service_select_df.count()
print("The count is " + str(cnt))
# The count is 24
#
#schema
"""
root
 |-- USERID: string (nullable = true)
 |-- QUEUECD: string (nullable = true)
 |-- EXISTFLAG: string (nullable = true)
 |-- VIEWFLAG: string (nullable = true)
 |-- UPDATEFLAG: string (nullable = true)
 |-- WRKSELFLAG: string (nullable = true)
 |-- rpad(CAST(MNTDATTIM AS STRING), 26, 0): string (nullable = true)
"""

awd_user_service_select_df_scrubbed = awd_user_service_select_df.select(
    str_empty_udf('USERID').alias('USERID'),
    str_empty_udf('QUEUECD').alias('QUEUECD'),
    str_empty_udf('EXISTFLAG').alias('EXISTFLAG'),
    str_empty_udf('VIEWFLAG').alias('VIEWFLAG'),
    str_empty_udf('UPDATEFLAG').alias('UPDATEFLAG'),
    str_empty_udf('WRKSELFLAG').alias('WRKSELFLAG'),
    col('MNTDATTIM')
)

_select_write = awd_user_service_select_df_scrubbed.repartition(1).coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
