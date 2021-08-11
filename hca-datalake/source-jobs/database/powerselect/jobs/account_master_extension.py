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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_MASTEREXTENSION").load()
#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
#t1_df.printSchema()
#
# Query: dst_shareholder_consent_data
dst_masterextension_select_df = sqlContext.sql("""select * from t1""")
#
# Print schema of output
dst_masterextension_select_df.printSchema()
#print(dst_masterextension_select_df.count())
print("The number of rows returned by the query is " + str(dst_masterextension_select_df.count()))
#
# schema
"""
root
 |-- FUND: integer (nullable = true)
 |-- ACCOUNT: string (nullable = true)
 |-- LOAD_DATE: date (nullable = true)
 |-- LOAD_TIME: timestamp (nullable = true)
 |-- B_TO_A_ROLL_LAST_ZERO_BALANCE: date (nullable = true)
 |-- CLOSE_TFR_529_OMB: string (nullable = true)
 |-- FIRST_PURCH_AMT: double (nullable = true)
 |-- INITIAL_INVESTMENT: date (nullable = true)
 |-- LASTMAINT: date (nullable = true)
 |-- LASTMAINT_OPID: string (nullable = true)
 |-- LASTMAINT_TIME: timestamp (nullable = true)
 |-- PAY_TO_PLAY: string (nullable = true)
 |-- PURCHASED_THROUGH_MF_SHARES: double (nullable = true)
 |-- SUPPRESS_ACU_INVESTMENT_HST: string (nullable = true)
 |-- SUPPRESS_PROSPECTUS: string (nullable = true)
"""
#
# Scrub and format results
dst_masterextension_select_df_scrubbed = dst_masterextension_select_df.select(
 dst_masterextension_select_df.FUND.cast(IntegerType()).alias('FUND'),
 str_empty_udf('ACCOUNT').alias('ACCOUNT'),
 dst_masterextension_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
 dst_masterextension_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
 dst_masterextension_select_df.B_TO_A_ROLL_LAST_ZERO_BALANCE.cast(DateType()).alias('B_TO_A_ROLL_LAST_ZERO_BALANCE'),
 str_empty_udf('CLOSE_TFR_529_OMB').alias('CLOSE_TFR_529_OMB'),
 dst_masterextension_select_df.FIRST_PURCH_AMT.cast(DoubleType()).alias('FIRST_PURCH_AMT'),
 dst_masterextension_select_df.INITIAL_INVESTMENT.cast(DateType()).alias('INITIAL_INVESTMENT'),
 dst_masterextension_select_df.LASTMAINT.cast(DateType()).alias('LASTMAINT'),
 str_empty_udf('LASTMAINT_OPID').alias('LASTMAINT_OPID'),
 dst_masterextension_select_df.LASTMAINT_TIME.cast(TimestampType()).alias('LASTMAINT_TIME'),
 str_empty_udf('PAY_TO_PLAY').alias('PAY_TO_PLAY'),
 dst_masterextension_select_df.PURCHASED_THROUGH_MF_SHARES.cast(DoubleType()).alias('PURCHASED_THROUGH_MF_SHARES'),
 str_empty_udf('SUPPRESS_ACU_INVESTMENT_HST').alias('SUPPRESS_ACU_INVESTMENT_HST'),
 str_empty_udf('SUPPRESS_PROSPECTUS').alias('SUPPRESS_PROSPECTUS')
)
# Coalesce results
dst_masterextension_select_write = dst_masterextension_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_masterextension_select_write is:") 
dst_masterextension_select_write.printSchema()

# Potential final output filename(s): shareholder_tax_crossref, account_tax_crossref
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_masterextension_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
