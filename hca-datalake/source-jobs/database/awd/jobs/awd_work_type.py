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

#dg_pwharbo_awd_W08U999S awd DG_PWHARBO.AWD.W08U999S

#Table(s)
_W08U999S = glueContext.create_dynamic_frame.from_catalog(database=db, table_name=f"{src_schema}_awd_W08U999S")

# Convert dynamic dataframes to dataframes
W08U999S_df = _W08U999S.toDF()

# Convert dataframes to temp views
W08U999S_df.createOrReplaceTempView("W08U999S")

# awd_work_type Query
awd_work_type_select_df = sqlContext.sql("""
SELECT W08U999S.UNITCD,
       W08U999S.WRKTYPE,
       W08U999S.CATEGORYCD,
       W08U999S.WRKCLASS,
       W08U999S.CSDSCREEN,
       W08U999S.ICOBJECTID,
       W08U999S.BALFLAG,
       W08U999S.FINFLAG,
       W08U999S.RISKFLAG,
       W08U999S.WRKTYPECD,
       W08U999S.STDELAPDAY,
       W08U999S.STDELAPTIM,
       W08U999S.MAXSUSPDAY,
       W08U999S.AUTOTCKDAY,
       W08U999S.EXTHOST1,
       W08U999S.EXTPARM1,
       W08U999S.EXTDLL1,
       W08U999S.EXTPROC1,
       W08U999S.EXTHOST2,
       W08U999S.EXTPARM2,
       W08U999S.EXTDLL2,
       W08U999S.EXTPROC2,
       W08U999S.EXTHOST3,
       W08U999S.EXTPARM3,
       W08U999S.EXTDLL3,
       W08U999S.EXTPROC3,
       W08U999S.EXTHOST4,
       W08U999S.EXTPARM4,
       W08U999S.EXTDLL4,
       W08U999S.EXTPROC4,
       W08U999S.AUTOFLAGS,
       W08U999S.DATANAME
FROM W08U999S
""")

print("The schema is:")
awd_work_type_select_df.printSchema()

cnt = awd_work_type_select_df.count()
print("The count is " + str(cnt))
# The count is 63
#
#schema
"""
root
|-- UNITCD: string (nullable = true)
|-- WRKTYPE: string (nullable = true)
|-- CATEGORYCD: string (nullable = true)
|-- WRKCLASS: string (nullable = true)
|-- CSDSCREEN: string (nullable = true)
|-- ICOBJECTID: string (nullable = true)
|-- BALFLAG: string (nullable = true)
|-- FINFLAG: string (nullable = true)
|-- RISKFLAG: string (nullable = true)
|-- WRKTYPECD: string (nullable = true)
|-- STDELAPDAY: decimal(5,0) (nullable = true)
|-- STDELAPTIM: decimal(7,0) (nullable = true)
|-- MAXSUSPDAY: decimal(3,0) (nullable = true)
|-- AUTOTCKDAY: decimal(3,0) (nullable = true)
|-- EXTHOST1: string (nullable = true)
|-- EXTPARM1: string (nullable = true)
|-- EXTDLL1: string (nullable = true)
|-- EXTPROC1: string (nullable = true)
|-- EXTHOST2: string (nullable = true)
|-- EXTPARM2: string (nullable = true)
|-- EXTDLL2: string (nullable = true)
|-- EXTPROC2: string (nullable = true)
|-- EXTHOST3: string (nullable = true)
|-- EXTPARM3: string (nullable = true)
|-- EXTDLL3: string (nullable = true)
|-- EXTPROC3: string (nullable = true)
|-- EXTHOST4: string (nullable = true)
|-- EXTPARM4: string (nullable = true)
|-- EXTDLL4: string (nullable = true)
|-- EXTPROC4: string (nullable = true)
|-- AUTOFLAGS: string (nullable = true)
|-- DATANAME: string (nullable = true)
"""

awd_work_type_select_df_scrubbed = awd_work_type_select_df.select(
       str_empty_udf('UNITCD').alias('UNITCD'),
       str_empty_udf('WRKTYPE').alias('WRKTYPE'),
       str_empty_udf('CATEGORYCD').alias('CATEGORYCD'),
       str_empty_udf('WRKCLASS').alias('WRKCLASS'),
       str_empty_udf('CSDSCREEN').alias('CSDSCREEN'),
       str_empty_udf('ICOBJECTID').alias('ICOBJECTID'),
       str_empty_udf('BALFLAG').alias('BALFLAG'),
       str_empty_udf('FINFLAG').alias('FINFLAG'),
       str_empty_udf('RISKFLAG').alias('RISKFLAG'),
       str_empty_udf('WRKTYPECD').alias('WRKTYPECD'),
       awd_work_type_select_df.STDELAPDAY.cast(DecimalType(5,0)).alias('STDELAPDAY'),
       awd_work_type_select_df.STDELAPTIM.cast(DecimalType(7,0)).alias('STDELAPTIM'),
       awd_work_type_select_df.MAXSUSPDAY.cast(DecimalType(3,0)).alias('MAXSUSPDAY'),
       awd_work_type_select_df.AUTOTCKDAY.cast(DecimalType(3,0)).alias('AUTOTCKDAY'),
       str_empty_udf('EXTHOST1').alias('EXTHOST1'),
       str_empty_udf('EXTPARM1').alias('EXTPARM1'),
       str_empty_udf('EXTDLL1').alias('EXTDLL1'),
       str_empty_udf('EXTPROC1').alias('EXTPROC1'),
       str_empty_udf('EXTHOST2').alias('EXTHOST2'),
       str_empty_udf('EXTPARM2').alias('EXTPARM2'),
       str_empty_udf('EXTDLL2').alias('EXTDLL2'),
       str_empty_udf('EXTPROC2').alias('EXTPROC2'),
       str_empty_udf('EXTHOST3').alias('EXTHOST3'),
       str_empty_udf('EXTPARM3').alias('EXTPARM3'),
       str_empty_udf('EXTDLL3').alias('EXTDLL3'),
       str_empty_udf('EXTPROC3').alias('EXTPROC3'),
       str_empty_udf('EXTHOST4').alias('EXTHOST4'),
       str_empty_udf('EXTPARM4').alias('EXTPARM4'),
       str_empty_udf('EXTDLL4').alias('EXTDLL4'),
       str_empty_udf('EXTPROC4').alias('EXTPROC4'),
       str_empty_udf('AUTOFLAGS').alias('AUTOFLAGS'),
       str_empty_udf('DATANAME').alias('DATANAME')
)

_select_write = awd_work_type_select_df_scrubbed.repartition(1).coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
