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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_MASTER").load()
#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
#t1_df.printSchema()
#
# Query: dst_shareholder_consent_data
dst_master_select_df = sqlContext.sql("""select * from t1""")
#
# Print schema of output
dst_master_select_df.printSchema()
#print(dst_master_select_df.count())
print("The number of rows returned by the query is " + str(dst_master_select_df.count()))
#
# schema
"""
root
 |-- FUND: integer (nullable = true)
 |-- ACCOUNT: string (nullable = true)
 |-- LOAD_DATE: date (nullable = true)
 |-- LOAD_TIME: timestamp (nullable = true)
 |-- ACCT_ENTRY_METHOD: string (nullable = true)
 |-- ACCT_ORIGIN_ID: integer (nullable = true)
 |-- ACCT_SETUP_DTE: date (nullable = true)
 |-- ACCT_TYPE_CD: string (nullable = true)
 |-- ACHFLAG: string (nullable = true)
 |-- AFFIL_BRANCH: string (nullable = true)
 |-- AFFIL_DEALER: integer (nullable = true)
 |-- AFFIL_REP: string (nullable = true)
 |-- AFFIL_ROLE: string (nullable = true)
 |-- ALPHACODE: string (nullable = true)
 |-- AUDIORESP: string (nullable = true)
 |-- BEGSHRSISS: double (nullable = true)
 |-- BEGSHRSUISS: double (nullable = true)
 |-- BLUESKY: string (nullable = true)
 |-- CAPGAINS: string (nullable = true)
 |-- CHECKDIGIT: string (nullable = true)
 |-- CHECKWRITE: string (nullable = true)
 |-- COMMISSION_CALC: string (nullable = true)
 |-- CUMDISC: integer (nullable = true)
 |-- CUSTODIAN_ID: integer (nullable = true)
 |-- CUSTOM_SALUTATION: string (nullable = true)
 |-- DATEEST: date (nullable = true)
 |-- DEALER: integer (nullable = true)
 |-- DEALERBRCH: string (nullable = true)
 |-- DEALERCNTL: string (nullable = true)
 |-- DEBITCARD: string (nullable = true)
 |-- DIVMAIL: string (nullable = true)
 |-- DIVTYPE: string (nullable = true)
 |-- ESCHEAT: string (nullable = true)
 |-- ESCROWSHRS: double (nullable = true)
 |-- EXPACCT: string (nullable = true)
 |-- EXPEXCH: string (nullable = true)
 |-- FIDUCIARY: string (nullable = true)
 |-- FOREIGNTAX: double (nullable = true)
 |-- FRACCHECK: string (nullable = true)
 |-- ISSUECODE: string (nullable = true)
 |-- LASTMAINT: date (nullable = true)
 |-- LINECODE: integer (nullable = true)
 |-- LISTBILL: string (nullable = true)
 |-- LOIFUNDGRP: integer (nullable = true)
 |-- LOINUMBER: integer (nullable = true)
 |-- MAILFLAG: string (nullable = true)
 |-- MGMTCODE: string (nullable = true)
 |-- MONITORVIP: string (nullable = true)
 |-- MULTICWR: string (nullable = true)
 |-- NAVCODE: string (nullable = true)
 |-- NEW_ACCT_RSN_MKT: integer (nullable = true)
 |-- NONPURGE: string (nullable = true)
 |-- OPENCLOSE: string (nullable = true)
 |-- PENALTYWTH: string (nullable = true)
 |-- PREAUTHCHK: string (nullable = true)
 |-- PURCHASERM: string (nullable = true)
 |-- REGLINE1: string (nullable = true)
 |-- REGLINE2: string (nullable = true)
 |-- REGLINE3: string (nullable = true)
 |-- REGLINE4: string (nullable = true)
 |-- REGLINE5: string (nullable = true)
 |-- REGLINE6: string (nullable = true)
 |-- REGLINE7: string (nullable = true)
 |-- REINVESTFD: string (nullable = true)
 |-- REPNAME: string (nullable = true)
 |-- REPNUMBER: string (nullable = true)
 |-- RESSTATECOUNTRY: string (nullable = true)
 |-- SACD1: string (nullable = true)
 |-- SACD2: string (nullable = true)
 |-- SACD3: string (nullable = true)
 |-- SACD4: string (nullable = true)
 |-- SACD5: string (nullable = true)
 |-- SACD6: string (nullable = true)
 |-- SACD7: string (nullable = true)
 |-- SACD8: string (nullable = true)
 |-- SACDTXT: string (nullable = true)
 |-- SACITIZEN: string (nullable = true)
 |-- SADIVPAY: string (nullable = true)
 |-- SAHOUSE: string (nullable = true)
 |-- SALANGUAGE: string (nullable = true)
 |-- SALASTACTD: date (nullable = true)
 |-- SALOI: string (nullable = true)
 |-- SAMGMTEMPL: string (nullable = true)
 |-- SAMIDMTHDV: string (nullable = true)
 |-- SAPHONECHK: string (nullable = true)
 |-- SAPPACNTL: string (nullable = true)
 |-- SAPRICESCH: string (nullable = true)
 |-- SASHRTCAP: string (nullable = true)
 |-- SASTATEWTH: string (nullable = true)
 |-- SAUNVERCRT: double (nullable = true)
 |-- SERVICING_AGENT_ID: integer (nullable = true)
 |-- SFCF_PAY_ACCOUNT_NUMBER: string (nullable = true)
 |-- SFCF_PAY_FUND_CODE: integer (nullable = true)
 |-- SFCF_STATUS: string (nullable = true)
 |-- SOCIALCODE: integer (nullable = true)
 |-- SSN: string (nullable = true)
 |-- SSNCODE: string (nullable = true)
 |-- STOPMAIL: string (nullable = true)
 |-- STOPMAILDATE: date (nullable = true)
 |-- STOPPURCH: string (nullable = true)
 |-- STOPXFER: string (nullable = true)
 |-- SUBACCT: string (nullable = true)
 |-- SWP: string (nullable = true)
 |-- THIRD_PARTY_ADMIN: integer (nullable = true)
 |-- TIMER: string (nullable = true)
 |-- TOTSHRSISS: double (nullable = true)
 |-- TOTSHRSUISS: double (nullable = true)
 |-- TRANSCOUNT: integer (nullable = true)
 |-- ZIPCODE: string (nullable = true)
 |-- ZIPCODE_FUTURE: string (nullable = true)
 |-- ZIPCODE_SUFFIX: string (nullable = true)
"""
#
# Scrub and format results
dst_master_select_df_scrubbed = dst_master_select_df.select(
 dst_master_select_df.FUND.cast(IntegerType()).alias('FUND'),
 str_empty_udf('ACCOUNT').alias('ACCOUNT'),
 dst_master_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
 dst_master_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
 str_empty_udf('ACCT_ENTRY_METHOD').alias('ACCT_ENTRY_METHOD'),
 dst_master_select_df.ACCT_ORIGIN_ID.cast(IntegerType()).alias('ACCT_ORIGIN_ID'),
 dst_master_select_df.ACCT_SETUP_DTE.cast(DateType()).alias('ACCT_SETUP_DTE'),
 str_empty_udf('ACCT_TYPE_CD').alias('ACCT_TYPE_CD'),
 str_empty_udf('ACHFLAG').alias('ACHFLAG'),
 str_empty_udf('AFFIL_BRANCH').alias('AFFIL_BRANCH'),
 dst_master_select_df.AFFIL_DEALER.cast(IntegerType()).alias('AFFIL_DEALER'),
 str_empty_udf('AFFIL_REP').alias('AFFIL_REP'),
 str_empty_udf('AFFIL_ROLE').alias('AFFIL_ROLE'),
 str_empty_udf('ALPHACODE').alias('ALPHACODE'),
 str_empty_udf('AUDIORESP').alias('AUDIORESP'),
 dst_master_select_df.BEGSHRSISS.cast(DoubleType()).alias('BEGSHRSISS'),
 dst_master_select_df.BEGSHRSUISS.cast(DoubleType()).alias('BEGSHRSUISS'),
 str_empty_udf('BLUESKY').alias('BLUESKY'),
 str_empty_udf('CAPGAINS').alias('CAPGAINS'),
 str_empty_udf('CHECKDIGIT').alias('CHECKDIGIT'),
 str_empty_udf('CHECKWRITE').alias('CHECKWRITE'),
 str_empty_udf('COMMISSION_CALC').alias('COMMISSION_CALC'),
 dst_master_select_df.CUMDISC.cast(IntegerType()).alias('CUMDISC'),
 dst_master_select_df.CUSTODIAN_ID.cast(IntegerType()).alias('CUSTODIAN_ID'),
 str_empty_udf('CUSTOM_SALUTATION').alias('CUSTOM_SALUTATION'),
 dst_master_select_df.DATEEST.cast(DateType()).alias('DATEEST'),
 dst_master_select_df.DEALER.cast(IntegerType()).alias('DEALER'),
 str_empty_udf('DEALERBRCH').alias('DEALERBRCH'),
 str_empty_udf('DEALERCNTL').alias('DEALERCNTL'),
 str_empty_udf('DEBITCARD').alias('DEBITCARD'),
 str_empty_udf('DIVMAIL').alias('DIVMAIL'),
 str_empty_udf('DIVTYPE').alias('DIVTYPE'),
 str_empty_udf('ESCHEAT').alias('ESCHEAT'),
 dst_master_select_df.ESCROWSHRS.cast(DoubleType()).alias('ESCROWSHRS'),
 str_empty_udf('EXPACCT').alias('EXPACCT'),
 str_empty_udf('EXPEXCH').alias('EXPEXCH'),
 str_empty_udf('FIDUCIARY').alias('FIDUCIARY'),
 dst_master_select_df.FOREIGNTAX.cast(DoubleType()).alias('FOREIGNTAX'),
 str_empty_udf('FRACCHECK').alias('FRACCHECK'),
 str_empty_udf('ISSUECODE').alias('ISSUECODE'),
 dst_master_select_df.LASTMAINT.cast(DateType()).alias('LASTMAINT'),
 dst_master_select_df.LINECODE.cast(IntegerType()).alias('LINECODE'),
 str_empty_udf('LISTBILL').alias('LISTBILL'),
 dst_master_select_df.LOIFUNDGRP.cast(IntegerType()).alias('LOIFUNDGRP'),
 dst_master_select_df.LOINUMBER.cast(IntegerType()).alias('LOINUMBER'),
 str_empty_udf('MAILFLAG').alias('MAILFLAG'),
 str_empty_udf('MGMTCODE').alias('MGMTCODE'),
 str_empty_udf('MONITORVIP').alias('MONITORVIP'),
 str_empty_udf('MULTICWR').alias('MULTICWR'),
 str_empty_udf('NAVCODE').alias('NAVCODE'),
 dst_master_select_df.NEW_ACCT_RSN_MKT.cast(IntegerType()).alias('NEW_ACCT_RSN_MKT'),
 str_empty_udf('NONPURGE').alias('NONPURGE'),
 str_empty_udf('OPENCLOSE').alias('OPENCLOSE'),
 str_empty_udf('PENALTYWTH').alias('PENALTYWTH'),
 str_empty_udf('PREAUTHCHK').alias('PREAUTHCHK'),
 str_empty_udf('PURCHASERM').alias('PURCHASERM'),
 str_empty_udf('REGLINE1').alias('REGLINE1'),
 str_empty_udf('REGLINE2').alias('REGLINE2'),
 str_empty_udf('REGLINE3').alias('REGLINE3'),
 str_empty_udf('REGLINE4').alias('REGLINE4'),
 str_empty_udf('REGLINE5').alias('REGLINE5'),
 str_empty_udf('REGLINE6').alias('REGLINE6'),
 str_empty_udf('REGLINE7').alias('REGLINE7'),
 str_empty_udf('REINVESTFD').alias('REINVESTFD'),
 str_empty_udf('REPNAME').alias('REPNAME'),
 str_empty_udf('REPNUMBER').alias('REPNUMBER'),
 str_empty_udf('RESSTATECOUNTRY').alias('RESSTATECOUNTRY'),
 str_empty_udf('SACD1').alias('SACD1'),
 str_empty_udf('SACD2').alias('SACD2'),
 str_empty_udf('SACD3').alias('SACD3'),
 str_empty_udf('SACD4').alias('SACD4'),
 str_empty_udf('SACD5').alias('SACD5'),
 str_empty_udf('SACD6').alias('SACD6'),
 str_empty_udf('SACD7').alias('SACD7'),
 str_empty_udf('SACD8').alias('SACD8'),
 str_empty_udf('SACDTXT').alias('SACDTXT'),
 str_empty_udf('SACITIZEN').alias('SACITIZEN'),
 str_empty_udf('SADIVPAY').alias('SADIVPAY'),
 str_empty_udf('SAHOUSE').alias('SAHOUSE'),
 str_empty_udf('SALANGUAGE').alias('SALANGUAGE'),
 dst_master_select_df.SALASTACTD.cast(DateType()).alias('SALASTACTD'),
 str_empty_udf('SALOI').alias('SALOI'),
 str_empty_udf('SAMGMTEMPL').alias('SAMGMTEMPL'),
 str_empty_udf('SAMIDMTHDV').alias('SAMIDMTHDV'),
 str_empty_udf('SAPHONECHK').alias('SAPHONECHK'),
 str_empty_udf('SAPPACNTL').alias('SAPPACNTL'),
 str_empty_udf('SAPRICESCH').alias('SAPRICESCH'),
 str_empty_udf('SASHRTCAP').alias('SASHRTCAP'),
 str_empty_udf('SASTATEWTH').alias('SASTATEWTH'),
 dst_master_select_df.SAUNVERCRT.cast(DoubleType()).alias('SAUNVERCRT'),
 dst_master_select_df.SERVICING_AGENT_ID.cast(IntegerType()).alias('SERVICING_AGENT_ID'),
 str_empty_udf('SFCF_PAY_ACCOUNT_NUMBER').alias('SFCF_PAY_ACCOUNT_NUMBER'),
 dst_master_select_df.SFCF_PAY_FUND_CODE.cast(IntegerType()).alias('SFCF_PAY_FUND_CODE'),
 str_empty_udf('SFCF_STATUS').alias('SFCF_STATUS'),
 dst_master_select_df.SOCIALCODE.cast(IntegerType()).alias('SOCIALCODE'),
 str_empty_udf('SSN').alias('SSN'),
 str_empty_udf('SSNCODE').alias('SSNCODE'),
 str_empty_udf('STOPMAIL').alias('STOPMAIL'),
 dst_master_select_df.STOPMAILDATE.cast(DateType()).alias('STOPMAILDATE'),
 str_empty_udf('STOPPURCH').alias('STOPPURCH'),
 str_empty_udf('STOPXFER').alias('STOPXFER'),
 str_empty_udf('SUBACCT').alias('SUBACCT'),
 str_empty_udf('SWP').alias('SWP'),
 dst_master_select_df.THIRD_PARTY_ADMIN.cast(IntegerType()).alias('THIRD_PARTY_ADMIN'),
 str_empty_udf('TIMER').alias('TIMER'),
 dst_master_select_df.TOTSHRSISS.cast(DoubleType()).alias('TOTSHRSISS'),
 dst_master_select_df.TOTSHRSUISS.cast(DoubleType()).alias('TOTSHRSUISS'),
 dst_master_select_df.TRANSCOUNT.cast(IntegerType()).alias('TRANSCOUNT'),
 str_empty_udf('ZIPCODE').alias('ZIPCODE'),
 str_empty_udf('ZIPCODE_FUTURE').alias('ZIPCODE_FUTURE'),
 str_empty_udf('ZIPCODE_SUFFIX').alias('ZIPCODE_SUFFIX')
)
# Coalesce results
dst_master_select_write = dst_master_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_master_select_write is:") 
dst_master_select_write.printSchema()

# Potential final output filename(s): shareholder_master, account_master
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_master_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
