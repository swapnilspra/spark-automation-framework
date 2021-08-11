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

#dg_pwharbo_awd_w06u999s awd DG_PWHARBO.AWD.W06U999S

#Table(s)
_w06u999s = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_awd_w06u999s".format(src_schema))

# Convert dynamic dataframes to dataframes
w06u999s_df = _w06u999s.toDF()

# Convert dataframes to temp views
w06u999s_df.createOrReplaceTempView("w06u999s")

# awd_user_group Query
awd_user_group_select_df = sqlContext.sql("""
SELECT W06U999S.USERID,
       W06U999S.SOCSECNO,
       W06U999S.CLASSIFY,
       W06U999S.LASTNAME,
       W06U999S.FIRSTNAME,
       W06U999S.MIDDLEINIT,
       W06U999S.GROUPCD,
       W06U999S.USERIDSTAT,
       W06U999S.LOCKSTAT,
       W06U999S.QUEUEFLAG,
       W06U999S.ROUTUSERID,
       W06U999S.PHONENBR,
       W06U999S.WRKSEL,
       W06U999S.FAXNBR,
       W06U999S.FAXDEPT,
       W06U999S.FAXLISTID,
       W06U999S.SECLEVEL,
       W06U999S.UNITCD,
       RPAD(W06U999S.MNTDATTIM,26,'0') AS MNTDATTIM,
       W06U999S.EARLYTIM,
       W06U999S.LATETIM,
       RPAD(W06U999S.LASTDATTIM,26,'0') AS LASTDATTIM,
       W06U999S.LASTTOTTIM,
       W06U999S.LOGONCNT,
       W06U999S.PSWDMNTDAT,
       W06U999S.INLOGONCNT,
       W06U999S.COUNTRYCD,
       W06U999S.AWD_DSKTOP_VERSION,
       W06U999S.DATA_REVISION
  FROM W06U999S
""")

awd_user_group_select_df.printSchema()

cnt = awd_user_group_select_df.count()

print("The count is " + str(cnt))

#schema
"""
root
 |-- USERID: string (nullable = true)
 |-- SOCSECNO: decimal(9,0) (nullable = true)
 |-- CLASSIFY: string (nullable = true)
 |-- LASTNAME: string (nullable = true)
 |-- FIRSTNAME: string (nullable = true)
 |-- MIDDLEINIT: string (nullable = true)
 |-- GROUPCD: string (nullable = true)
 |-- USERIDSTAT: string (nullable = true)
 |-- LOCKSTAT: string (nullable = true)
 |-- QUEUEFLAG: string (nullable = true)
 |-- ROUTUSERID: string (nullable = true)
 |-- PHONENBR: string (nullable = true)
 |-- WRKSEL: string (nullable = true)
 |-- FAXNBR: string (nullable = true)
 |-- FAXDEPT: string (nullable = true)
 |-- FAXLISTID: string (nullable = true)
 |-- SECLEVEL: decimal(3,0) (nullable = true)
 |-- UNITCD: string (nullable = true)
 |-- rpad(MNTDATTIM, 26, 0): string (nullable = true)
 |-- EARLYTIM: string (nullable = true)
 |-- LATETIM: string (nullable = true)
 |-- rpad(LASTDATTIM, 26, 0): string (nullable = true)
 |-- LASTTOTTIM: decimal(7,0) (nullable = true)
 |-- LOGONCNT: decimal(5,0) (nullable = true)
 |-- PSWDMNTDAT: string (nullable = true)
 |-- INLOGONCNT: decimal(1,0) (nullable = true)
 |-- COUNTRYCD: decimal(3,0) (nullable = true)
 |-- AWD_DSKTOP_VERSION: string (nullable = true)
 |-- DATA_REVISION: decimal(4,0) (nullable = true)
"""

awd_user_group_select_df_scrubbed = awd_user_group_select_df.select(
       str_empty_udf('USERID').alias('USERID'),
       awd_user_group_select_df.SOCSECNO.cast(DecimalType(9,0)).alias('SOCSECNO'),
       str_empty_udf('CLASSIFY').alias('CLASSIFY'),
       str_empty_udf('LASTNAME').alias('LASTNAME'),
       str_empty_udf('FIRSTNAME').alias('FIRSTNAME'),
       str_empty_udf('MIDDLEINIT').alias('MIDDLEINIT'),
       str_empty_udf('GROUPCD').alias('GROUPCD'),
       str_empty_udf('USERIDSTAT').alias('USERIDSTAT'),
       str_empty_udf('LOCKSTAT').alias('LOCKSTAT'),
       str_empty_udf('QUEUEFLAG').alias('QUEUEFLAG'),
       str_empty_udf('ROUTUSERID').alias('ROUTUSERID'),
       str_empty_udf('PHONENBR').alias('PHONENBR'),
       str_empty_udf('WRKSEL').alias('WRKSEL'),
       str_empty_udf('FAXNBR').alias('FAXNBR'),
       str_empty_udf('FAXDEPT').alias('FAXDEPT'),
       str_empty_udf('FAXLISTID').alias('FAXLISTID'),
       awd_user_group_select_df.SECLEVEL.cast(DecimalType(3,0)).alias('SECLEVEL'),
       str_empty_udf('UNITCD').alias('UNITCD'),
       col('MNTDATTIM'),
       str_empty_udf('EARLYTIM').alias('EARLYTIM'),
       str_empty_udf('LATETIM').alias('LATETIM'),
       col('LASTDATTIM'),
       awd_user_group_select_df.LASTTOTTIM.cast(DecimalType(7,0)).alias('LASTTOTTIM'),
       awd_user_group_select_df.LOGONCNT.cast(DecimalType(5,0)).alias('LOGONCNT'),
       str_empty_udf('PSWDMNTDAT').alias('PSWDMNTDAT'),
       awd_user_group_select_df.INLOGONCNT.cast(DecimalType(1,0)).alias('INLOGONCNT'),
       awd_user_group_select_df.COUNTRYCD.cast(DecimalType(3,0)).alias('COUNTRYCD'),
       str_empty_udf('AWD_DSKTOP_VERSION').alias('AWD_DSKTOP_VERSION'),
       awd_user_group_select_df.DATA_REVISION.cast(DecimalType(4,0)).alias('DATA_REVISION')
)

_select_write = awd_user_group_select_df_scrubbed.repartition(1).coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
