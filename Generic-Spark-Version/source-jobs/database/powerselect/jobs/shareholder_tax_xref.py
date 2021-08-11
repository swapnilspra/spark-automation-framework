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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_LOTAXRF").load()
#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
#t1_df.printSchema()
#
# Query: dst_shareholder_tax_xref
dst_shareholder_tax_xref_select_df = sqlContext.sql("""select * from t1""")
#
# Print schema of output
dst_shareholder_tax_xref_select_df.printSchema()
#print(dst_shareholder_tax_xref_select_df.count())
print("The number of rows returned by the query is " + str(dst_shareholder_tax_xref_select_df.count()))
#
# schema
"""
root
 |-- FUND: integer (nullable = true)
 |-- ACCOUNT: string (nullable = true)
 |-- LOAD_DATE: date (nullable = true)
 |-- LOAD_TIME: timestamp (nullable = true)
 |-- BENEF_529_CUST_ID1: string (nullable = true)
 |-- BENEF_529_CUST_ID2: string (nullable = true)
 |-- CMR_CERT_DATE: date (nullable = true)
 |-- CMR_CERT_TIME: timestamp (nullable = true)
 |-- COLLEGE_ENROLL_DT: date (nullable = true)
 |-- CUSTOMER_EXCLUSION: string (nullable = true)
 |-- CUSTOMER_ID1: string (nullable = true)
 |-- CUSTOMER_ID2: string (nullable = true)
 |-- ELEC_AVGCOST_STMT: string (nullable = true)
 |-- ELEC_COMPL_STMT: string (nullable = true)
 |-- ELEC_CONFIRM: string (nullable = true)
 |-- ELEC_INVESTOR_STMT: string (nullable = true)
 |-- ELEC_PRX_VOTE_STMT: string (nullable = true)
 |-- ELEC_TAX_FORM: string (nullable = true)
 |-- EXT_ADDR_CHG_CODE: string (nullable = true)
 |-- FIRST_PURCH_AMT: double (nullable = true)
 |-- LASTMAINT: date (nullable = true)
 |-- LASTMAINT_OPID: string (nullable = true)
 |-- LASTMAINT_TIME: timestamp (nullable = true)
 |-- LEGAL_OWNER_ID1: string (nullable = true)
 |-- LEGAL_OWNER_ID2: string (nullable = true)
 |-- NASU_MARKET_ID: string (nullable = true)
 |-- NON_ROLL_ACCOUNT: string (nullable = true)
 |-- RESSTATECOUNTRY: string (nullable = true)
 |-- SEASON_CONT_ADDR: string (nullable = true)
 |-- SECONDARY_ADDR: string (nullable = true)
 |-- TRANS_ID1: string (nullable = true)
 |-- TRANS_ID2: string (nullable = true)
"""
#
# Scrub and format results
dst_shareholder_tax_xref_select_df_scrubbed = dst_shareholder_tax_xref_select_df.select(
 dst_shareholder_tax_xref_select_df.FUND.cast(IntegerType()).alias('FUND'),
 str_empty_udf('ACCOUNT').alias('ACCOUNT'),
 dst_shareholder_tax_xref_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
 dst_shareholder_tax_xref_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
 str_empty_udf('BENEF_529_CUST_ID1').alias('BENEF_529_CUST_ID1'),
 str_empty_udf('BENEF_529_CUST_ID2').alias('BENEF_529_CUST_ID2'),
 dst_shareholder_tax_xref_select_df.CMR_CERT_DATE.cast(DateType()).alias('CMR_CERT_DATE'),
 dst_shareholder_tax_xref_select_df.CMR_CERT_TIME.cast(TimestampType()).alias('CMR_CERT_TIME'),
 dst_shareholder_tax_xref_select_df.COLLEGE_ENROLL_DT.cast(DateType()).alias('COLLEGE_ENROLL_DT'),
 str_empty_udf('CUSTOMER_EXCLUSION').alias('CUSTOMER_EXCLUSION'),
 str_empty_udf('CUSTOMER_ID1').alias('CUSTOMER_ID1'),
 str_empty_udf('CUSTOMER_ID2').alias('CUSTOMER_ID2'),
 str_empty_udf('ELEC_AVGCOST_STMT').alias('ELEC_AVGCOST_STMT'),
 str_empty_udf('ELEC_COMPL_STMT').alias('ELEC_COMPL_STMT'),
 str_empty_udf('ELEC_CONFIRM').alias('ELEC_CONFIRM'),
 str_empty_udf('ELEC_INVESTOR_STMT').alias('ELEC_INVESTOR_STMT'),
 str_empty_udf('ELEC_PRX_VOTE_STMT').alias('ELEC_PRX_VOTE_STMT'),
 str_empty_udf('ELEC_TAX_FORM').alias('ELEC_TAX_FORM'),
 str_empty_udf('EXT_ADDR_CHG_CODE').alias('EXT_ADDR_CHG_CODE'),
 dst_shareholder_tax_xref_select_df.FIRST_PURCH_AMT.cast(DoubleType()).alias('FIRST_PURCH_AMT'),
 dst_shareholder_tax_xref_select_df.LASTMAINT.cast(DateType()).alias('LASTMAINT'),
 str_empty_udf('LASTMAINT_OPID').alias('LASTMAINT_OPID'),
 dst_shareholder_tax_xref_select_df.LASTMAINT_TIME.cast(TimestampType()).alias('LASTMAINT_TIME'),
 str_empty_udf('LEGAL_OWNER_ID1').alias('LEGAL_OWNER_ID1'),
 str_empty_udf('LEGAL_OWNER_ID2').alias('LEGAL_OWNER_ID2'),
 str_empty_udf('NASU_MARKET_ID').alias('NASU_MARKET_ID'),
 str_empty_udf('NON_ROLL_ACCOUNT').alias('NON_ROLL_ACCOUNT'),
 str_empty_udf('RESSTATECOUNTRY').alias('RESSTATECOUNTRY'),
 str_empty_udf('SEASON_CONT_ADDR').alias('SEASON_CONT_ADDR'),
 str_empty_udf('SECONDARY_ADDR').alias('SECONDARY_ADDR'),
 str_empty_udf('TRANS_ID1').alias('TRANS_ID1'),
 str_empty_udf('TRANS_ID2').alias('TRANS_ID2')
)
# Coalesce results
dst_shareholder_tax_xref_select_write = dst_shareholder_tax_xref_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_shareholder_tax_xref_select_write is:") 
dst_shareholder_tax_xref_select_write.printSchema()

# Potential final output filename(s): shareholder_tax_crossref, account_tax_crossref
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_shareholder_tax_xref_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
