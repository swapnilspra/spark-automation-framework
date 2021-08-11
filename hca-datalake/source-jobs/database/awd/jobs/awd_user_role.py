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
from datetime import datetime
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

#dg_pwharbo_awd_W52U999S awd DG_PWHARBO.AWD.W52U999S

#Table(s)
_W52U999S = glueContext.create_dynamic_frame.from_catalog(database=db, table_name=f"{src_schema}_awd_W52U999S")

# Convert dynamic dataframes to dataframes
W52U999S_df = _W52U999S.toDF()

# Convert dataframes to temp views
W52U999S_df.createOrReplaceTempView("W52U999S")

# awd_user_role Query
awd_user_role_select_df = sqlContext.sql("""
SELECT 
    W52U999S.ROLENAME, 
    W52U999S.USERID, 
    RPAD(W52U999S.MNTDATTIM ,26,'0') AS MNTDATTIM
FROM W52U999S
""")

print("The schema is:")
awd_user_role_select_df.printSchema()

cnt = awd_user_role_select_df.count()
print("The count is " + str(cnt))
#
#schema
"""
root
 |-- ROLENAME: string (nullable = true)
 |-- USERID: string (nullable = true)
 |-- rpad(CAST(MNTDATTIM AS STRING), 26, 0): string (nullable = true)
"""

awd_user_role_select_df_scrubbed = awd_user_role_select_df.select(col('ROLENAME'),col('USERID'),col('MNTDATTIM'))

_select_write = awd_user_role_select_df_scrubbed.repartition(1).coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=nonsensitive')

print("Done writing")

