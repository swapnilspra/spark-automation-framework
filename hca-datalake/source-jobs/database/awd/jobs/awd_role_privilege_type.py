import sys
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

#dg_pwharbo_awd_W53U999S awd DG_PWHARBO.AWD.W53U999S

#Table(s)
_W53U999S = glueContext.create_dynamic_frame.from_catalog(database=db, table_name=f"{src_schema}_awd_W53U999S")

# Convert dynamic dataframes to dataframes
W53U999S_df = _W53U999S.toDF()

# Convert dataframes to temp views
W53U999S_df.createOrReplaceTempView("W53U999S")

# awd_role_privilege_type Query
awd_role_privilege_type_select_df = sqlContext.sql("""
SELECT 
    W53U999S.ROLENAME, 
    W53U999S.RECORDCD, 
    W53U999S.UNITCD, 
    W53U999S.PRVTYPE, 
    W53U999S.EXISTFLAG, 
    W53U999S.VIEWFLAG, 
    W53U999S.UPDATEFLAG, 
    W53U999S.WRKSELFLAG, 
    RPAD(W53U999S.MNTDATTIM ,26,'0') AS MNTDATTIM
FROM W53U999S
""")

print("The schema is:")
awd_role_privilege_type_select_df.printSchema()

cnt = awd_role_privilege_type_select_df.count()
print("The count is " + str(cnt))
# The count is 1180
#
#schema
"""
root
|-- ROLENAME: string (nullable = true)
|-- RECORDCD: string (nullable = true)
|-- UNITCD: string (nullable = true)
|-- PRVTYPE: string (nullable = true)
|-- EXISTFLAG: string (nullable = true)
|-- VIEWFLAG: string (nullable = true)
|-- UPDATEFLAG: string (nullable = true)
|-- WRKSELFLAG: string (nullable = true)
|-- rpad(CAST(MNTDATTIM AS STRING), 26, 0): string (nullable = true)
"""

awd_role_privilege_type_select_df_scrubbed = awd_role_privilege_type_select_df.select(
    str_empty_udf('ROLENAME').alias('ROLENAME'),
    str_empty_udf('RECORDCD').alias('RECORDCD'),
    str_empty_udf('UNITCD').alias('UNITCD'),
    str_empty_udf('PRVTYPE').alias('PRVTYPE'),
    str_empty_udf('EXISTFLAG').alias('EXISTFLAG'),
    str_empty_udf('VIEWFLAG').alias('VIEWFLAG'),
    str_empty_udf('UPDATEFLAG').alias('UPDATEFLAG'),
    str_empty_udf('WRKSELFLAG').alias('WRKSELFLAG'),
    col('MNTDATTIM')
)

_select_write = awd_role_privilege_type_select_df_scrubbed.repartition(1).coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
