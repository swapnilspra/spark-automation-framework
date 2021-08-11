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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_LOCUST").load()
#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
#t1_df.printSchema()
#
# Query: dst_shareholder_customer_data
dst_shareholder_customer_select_df = sqlContext.sql("""select * from t1""")
#
# Print schema of output
dst_shareholder_customer_select_df.printSchema()
#print(dst_shareholder_customer_select_df.count())
print("The number of rows returned by the query is " + str(dst_shareholder_customer_select_df.count()))
#
# schema
"""
root
 |-- FUND_SPONSOR_ID: integer (nullable = true)
 |-- CUSTOMER_ID2: string (nullable = true)
 |-- CUSTOMER_ID1: string (nullable = true)
 |-- LOAD_DATE: date (nullable = true)
 |-- LOAD_TIME: timestamp (nullable = true)
 |-- BIRTHDATE: date (nullable = true)
 |-- CBA_MHD_ABBREVIATION_NM: string (nullable = true)
 |-- CMR_CDF_CD1: string (nullable = true)
 |-- CMR_CDF_CD2: string (nullable = true)
 |-- CMR_CDF_CD3: string (nullable = true)
 |-- CMR_CDF_CD4: string (nullable = true)
 |-- CMR_CDF_CD5: string (nullable = true)
 |-- CMR_CDF_CD6: string (nullable = true)
 |-- CMR_CDF_CD7: string (nullable = true)
 |-- CMR_CDF_CD8: string (nullable = true)
 |-- CMR_CDF_TX1: string (nullable = true)
 |-- CMR_VD_MFA_CMR_CD: string (nullable = true)
 |-- CUSTOMER_TYPE: string (nullable = true)
 |-- DEATHDATE: date (nullable = true)
 |-- EDU_INST_EMPL: string (nullable = true)
 |-- EDU_INST_ID1: string (nullable = true)
 |-- EDU_INST_ID2: string (nullable = true)
 |-- EXCL_FROM_NEXT_CIP: string (nullable = true)
 |-- EXTERNAL_ID: string (nullable = true)
 |-- FIRM_EMPLOYEE: string (nullable = true)
 |-- FORCE_CIP_CD: string (nullable = true)
 |-- FOREIGN_TAX_RATE: double (nullable = true)
 |-- FULL_PAPER_PROXY_PACKET: string (nullable = true)
 |-- GENDER: string (nullable = true)
 |-- INCL_PERSONAL_RATE_RETURN: string (nullable = true)
 |-- LAST_CONTACT_DT: date (nullable = true)
 |-- LAST_FORCED_CIP_DT: date (nullable = true)
 |-- LAST_LINK_DATE: date (nullable = true)
 |-- LASTMAINT: date (nullable = true)
 |-- LASTMAINT_OPID: string (nullable = true)
 |-- LASTMAINT_TIME: timestamp (nullable = true)
 |-- MARITAL_STATUS: string (nullable = true)
 |-- IDENTITY_PCS_DT: date (nullable = true)
 |-- IDENTITY_RESULT_DT: date (nullable = true)
 |-- IDENTITY_RESULT: string (nullable = true)
 |-- IDENTITY_VERIFY: string (nullable = true)
 |-- MF_CMR_MCH_TYP_CD: string (nullable = true)
 |-- MONITORVIP: string (nullable = true)
 |-- NAME_FIRST: string (nullable = true)
 |-- NAME_LAST: string (nullable = true)
 |-- NAME_MID_INIT: string (nullable = true)
 |-- NAME_MIDDLE: string (nullable = true)
 |-- NAME_SALUT: string (nullable = true)
 |-- NAME_SUFFIX: string (nullable = true)
 |-- NONPURGE: string (nullable = true)
 |-- OCCUPATION_ID: integer (nullable = true)
 |-- RECORD_ADDED_DATE: date (nullable = true)
 |-- RESSTATECOUNTRY: string (nullable = true)
 |-- SEND_MKT_MAIL: string (nullable = true)
 |-- TAX_ID_NBR: string (nullable = true)
 |-- TAX_CBA_METHOD: integer (nullable = true)
 |-- TAX_ID_TYPE: string (nullable = true)
 |-- TAXABLE_STATE: string (nullable = true)
 |-- US_CITIZEN: string (nullable = true)
 |-- ZIP_PLUS4: string (nullable = true)
 |-- ZIPCODE: string (nullable = true)
"""
#
# Scrub and format results
dst_shareholder_customer_select_df_scrubbed = dst_shareholder_customer_select_df.select(
 dst_shareholder_customer_select_df.FUND_SPONSOR_ID.cast(IntegerType()).alias('FUND_SPONSOR_ID'),
 str_empty_udf('CUSTOMER_ID2').alias('CUSTOMER_ID2'),
 str_empty_udf('CUSTOMER_ID1').alias('CUSTOMER_ID1'),
 dst_shareholder_customer_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
 dst_shareholder_customer_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
 dst_shareholder_customer_select_df.BIRTHDATE.cast(DateType()).alias('BIRTHDATE'),
 str_empty_udf('CBA_MHD_ABBREVIATION_NM').alias('CBA_MHD_ABBREVIATION_NM'),
 str_empty_udf('CMR_CDF_CD1').alias('CMR_CDF_CD1'),
 str_empty_udf('CMR_CDF_CD2').alias('CMR_CDF_CD2'),
 str_empty_udf('CMR_CDF_CD3').alias('CMR_CDF_CD3'),
 str_empty_udf('CMR_CDF_CD4').alias('CMR_CDF_CD4'),
 str_empty_udf('CMR_CDF_CD5').alias('CMR_CDF_CD5'),
 str_empty_udf('CMR_CDF_CD6').alias('CMR_CDF_CD6'),
 str_empty_udf('CMR_CDF_CD7').alias('CMR_CDF_CD7'),
 str_empty_udf('CMR_CDF_CD8').alias('CMR_CDF_CD8'),
 str_empty_udf('CMR_CDF_TX1').alias('CMR_CDF_TX1'),
 str_empty_udf('CMR_VD_MFA_CMR_CD').alias('CMR_VD_MFA_CMR_CD'),
 str_empty_udf('CUSTOMER_TYPE').alias('CUSTOMER_TYPE'),
 dst_shareholder_customer_select_df.DEATHDATE.cast(DateType()).alias('DEATHDATE'),
 str_empty_udf('EDU_INST_EMPL').alias('EDU_INST_EMPL'),
 str_empty_udf('EDU_INST_ID1').alias('EDU_INST_ID1'),
 str_empty_udf('EDU_INST_ID2').alias('EDU_INST_ID2'),
 str_empty_udf('EXCL_FROM_NEXT_CIP').alias('EXCL_FROM_NEXT_CIP'),
 str_empty_udf('EXTERNAL_ID').alias('EXTERNAL_ID'),
 str_empty_udf('FIRM_EMPLOYEE').alias('FIRM_EMPLOYEE'),
 str_empty_udf('FORCE_CIP_CD').alias('FORCE_CIP_CD'),
 dst_shareholder_customer_select_df.FOREIGN_TAX_RATE.cast(DoubleType()).alias('FOREIGN_TAX_RATE'),
 str_empty_udf('FULL_PAPER_PROXY_PACKET').alias('FULL_PAPER_PROXY_PACKET'),
 str_empty_udf('GENDER').alias('GENDER'),
 str_empty_udf('INCL_PERSONAL_RATE_RETURN').alias('INCL_PERSONAL_RATE_RETURN'),
 dst_shareholder_customer_select_df.LAST_CONTACT_DT.cast(DateType()).alias('LAST_CONTACT_DT'),
 dst_shareholder_customer_select_df.LAST_FORCED_CIP_DT.cast(DateType()).alias('LAST_FORCED_CIP_DT'),
 dst_shareholder_customer_select_df.LAST_LINK_DATE.cast(DateType()).alias('LAST_LINK_DATE'),
 dst_shareholder_customer_select_df.LASTMAINT.cast(DateType()).alias('LASTMAINT'),
 str_empty_udf('LASTMAINT_OPID').alias('LASTMAINT_OPID'),
 dst_shareholder_customer_select_df.LASTMAINT_TIME.cast(TimestampType()).alias('LASTMAINT_TIME'),
 str_empty_udf('MARITAL_STATUS').alias('MARITAL_STATUS'),
 dst_shareholder_customer_select_df.IDENTITY_PCS_DT.cast(DateType()).alias('IDENTITY_PCS_DT'),
 dst_shareholder_customer_select_df.IDENTITY_RESULT_DT.cast(DateType()).alias('IDENTITY_RESULT_DT'),
 str_empty_udf('IDENTITY_RESULT').alias('IDENTITY_RESULT'),
 str_empty_udf('IDENTITY_VERIFY').alias('IDENTITY_VERIFY'),
 str_empty_udf('MF_CMR_MCH_TYP_CD').alias('MF_CMR_MCH_TYP_CD'),
 str_empty_udf('MONITORVIP').alias('MONITORVIP'),
 str_empty_udf('NAME_FIRST').alias('NAME_FIRST'),
 str_empty_udf('NAME_LAST').alias('NAME_LAST'),
 str_empty_udf('NAME_MID_INIT').alias('NAME_MID_INIT'),
 str_empty_udf('NAME_MIDDLE').alias('NAME_MIDDLE'),
 str_empty_udf('NAME_SALUT').alias('NAME_SALUT'),
 str_empty_udf('NAME_SUFFIX').alias('NAME_SUFFIX'),
 str_empty_udf('NONPURGE').alias('NONPURGE'),
 dst_shareholder_customer_select_df.OCCUPATION_ID.cast(IntegerType()).alias('OCCUPATION_ID'),
 dst_shareholder_customer_select_df.RECORD_ADDED_DATE.cast(DateType()).alias('RECORD_ADDED_DATE'),
 str_empty_udf('RESSTATECOUNTRY').alias('RESSTATECOUNTRY'),
 str_empty_udf('SEND_MKT_MAIL').alias('SEND_MKT_MAIL'),
 str_empty_udf('TAX_ID_NBR').alias('TAX_ID_NBR'),
 dst_shareholder_customer_select_df.TAX_CBA_METHOD.cast(IntegerType()).alias('TAX_CBA_METHOD'),
 str_empty_udf('TAX_ID_TYPE').alias('TAX_ID_TYPE'),
 str_empty_udf('TAXABLE_STATE').alias('TAXABLE_STATE'),
 str_empty_udf('US_CITIZEN').alias('US_CITIZEN'),
 str_empty_udf('ZIP_PLUS4').alias('ZIP_PLUS4'),
 str_empty_udf('ZIPCODE').alias('ZIPCODE')
)
# Coalesce results
dst_shareholder_customer_select_write = dst_shareholder_customer_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_shareholder_customer_select_write is:") 
dst_shareholder_customer_select_write.printSchema()

# Potential final output filename(s): shareholder_customer, account_customer
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_shareholder_customer_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
