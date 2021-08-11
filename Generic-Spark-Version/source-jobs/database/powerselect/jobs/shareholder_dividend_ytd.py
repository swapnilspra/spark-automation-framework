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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_DVTRNYTD").load()
#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
#t1_df.printSchema()
#
# Query: dst_shareholder_div_ytd_data
dst_shareholder_div_ytd_select_df = sqlContext.sql("""select * from t1""")
#
# Print schema of output
dst_shareholder_div_ytd_select_df.printSchema()
#print(dst_shareholder_div_ytd_select_df.count())
print("The number of rows returned by the query is " + str(dst_shareholder_div_ytd_select_df.count()))
#
# schema
"""
root
|-- FUND: integer (nullable = true)
|-- ACCOUNT: string (nullable = true)
|-- TRANS_ID1: double (nullable = true)
|-- TRANS_ID2: double (nullable = true)
|-- LOAD_DATE: date (nullable = true)
|-- LOAD_TIME: timestamp (nullable = true)
|-- ACCOUNT_FROM_TO: string (nullable = true)
|-- ACCT_CLOSE_OUT_CDE: string (nullable = true)
|-- ADVANCED_COMM: double (nullable = true)
|-- BALANCE_INDICATOR: string (nullable = true)
|-- BATCH_NUMBER: integer (nullable = true)
|-- BLUESKY_NEW_REPEAT: string (nullable = true)
|-- BLUESKY_REOPEN_ACCOUNT_CDE: string (nullable = true)
|-- CAPGAINS: string (nullable = true)
|-- CERT_NUMBER: string (nullable = true)
|-- CERT_PREFIX: string (nullable = true)
|-- CHECK_NUMBER: string (nullable = true)
|-- CITY_CODE: string (nullable = true)
|-- COM_SCH_ASG_RSN_ID: string (nullable = true)
|-- CONFIRM_DATE: date (nullable = true)
|-- CUMDISC: integer (nullable = true)
|-- DATEEST: date (nullable = true)
|-- DEALER: integer (nullable = true)
|-- DEALER_COMM: double (nullable = true)
|-- DEALER_COMM_CODE: string (nullable = true)
|-- DEALER_CONCESSION: double (nullable = true)
|-- DEALERBRCH: string (nullable = true)
|-- DISCAT: integer (nullable = true)
|-- DIV_RATE: double (nullable = true)
|-- DOWNLOAD_FILE_CODE: string (nullable = true)
|-- EVENT_ID_1: long (nullable = true)
|-- EVENT_ID_2: long (nullable = true)
|-- FIN_TR_CDSC_WV_RSN: string (nullable = true)
|-- FIN_TR_ENT_DT: date (nullable = true)
|-- FIN_TR_ENT_TM: timestamp (nullable = true)
|-- FOREIGNTAX_RATE: double (nullable = true)
|-- FUND_FROM_TO: integer (nullable = true)
|-- GROSS_AMOUNT: double (nullable = true)
|-- INTRADAY_STRIKE_HOUR: integer (nullable = true)
|-- INTRADAY_STRIKE_MINUTE: integer (nullable = true)
|-- KEY_PREFIX: integer (nullable = true)
|-- LASTMAINT: date (nullable = true)
|-- LIQUIDITY_FEE_RESP_PARTY: string (nullable = true)
|-- MGMTCODE: string (nullable = true)
|-- NAVCODE: string (nullable = true)
|-- NEGOT_COMM_RATE: double (nullable = true)
|-- NET_ASSET_VALUE: double (nullable = true)
|-- OFFERING_PRICE: double (nullable = true)
|-- OPERATOR_ID: string (nullable = true)
|-- ORDER_NUMBER: integer (nullable = true)
|-- PAY_OFFICE: string (nullable = true)
|-- PAYMENT_METHOD: string (nullable = true)
|-- PLAN_STATUS_CODE: string (nullable = true)
|-- PRE_POST_NOON: string (nullable = true)
|-- PRICE_STRIKE_HOUR: integer (nullable = true)
|-- PRICE_STRIKE_MINUTE: integer (nullable = true)
|-- PRINT_CONFIRM: string (nullable = true)
|-- QRA_RED_TYP_CDE: string (nullable = true)
|-- REASON_CODE: integer (nullable = true)
|-- RECORD_DATE: date (nullable = true)
|-- REPNAME: string (nullable = true)
|-- REPNUMBER: string (nullable = true)
|-- RESSTATECOUNTRY: string (nullable = true)
|-- RLO_60_DAY_WV_VLD_VAL_ID: integer (nullable = true)
|-- SAMGMTEMPL: string (nullable = true)
|-- SAPPACNTL: string (nullable = true)
|-- SHARES: double (nullable = true)
|-- SOCIALCODE: integer (nullable = true)
|-- SPLIT_COMM: string (nullable = true)
|-- SS_CODE_ALL_TRANS: string (nullable = true)
|-- STATE_CODE: string (nullable = true)
|-- SUPER_SHEET_DATE: date (nullable = true)
|-- TERRITORY: string (nullable = true)
|-- TIMER: string (nullable = true)
|-- TR_HST_STE_CNY_ID: string (nullable = true)
|-- TRADE_DATE: date (nullable = true)
|-- TRADE_ENTRY_DT: date (nullable = true)
|-- TRADE_ENTRY_METHOD: string (nullable = true)
|-- TRADE_ENTRY_TM: timestamp (nullable = true)
|-- TRADE_ORIGIN_ID: string (nullable = true)
|-- TR_DSC_EXT_ID: integer (nullable = true)
|-- TRANS_CODE: integer (nullable = true)
|-- TRANS_DESC: string (nullable = true)
|-- TRANS_PPA_CNTL: string (nullable = true)
|-- TRANS_SUFFIX: integer (nullable = true)
|-- UNCOL_BAL_OVERRIDE: string (nullable = true)
|-- UNDERWRITER_COMM: double (nullable = true)
"""
#
# Scrub and format results
dst_shareholder_div_ytd_select_df_scrubbed = dst_shareholder_div_ytd_select_df.select(
  dst_shareholder_div_ytd_select_df.FUND.cast(IntegerType()).alias('FUND'),
  str_empty_udf('ACCOUNT').alias('ACCOUNT'),
  dst_shareholder_div_ytd_select_df.TRANS_ID1.cast(DoubleType()).alias('TRANS_ID1'),
  dst_shareholder_div_ytd_select_df.TRANS_ID2.cast(DoubleType()).alias('TRANS_ID2'),
  dst_shareholder_div_ytd_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
  dst_shareholder_div_ytd_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
  str_empty_udf('ACCOUNT_FROM_TO').alias('ACCOUNT_FROM_TO'),
  str_empty_udf('ACCT_CLOSE_OUT_CDE').alias('ACCT_CLOSE_OUT_CDE'),
  dst_shareholder_div_ytd_select_df.ADVANCED_COMM.cast(DoubleType()).alias('ADVANCED_COMM'),
  str_empty_udf('BALANCE_INDICATOR').alias('BALANCE_INDICATOR'),
  dst_shareholder_div_ytd_select_df.BATCH_NUMBER.cast(IntegerType()).alias('BATCH_NUMBER'),
  str_empty_udf('BLUESKY_NEW_REPEAT').alias('BLUESKY_NEW_REPEAT'),
  str_empty_udf('BLUESKY_REOPEN_ACCOUNT_CDE').alias('BLUESKY_REOPEN_ACCOUNT_CDE'),
  str_empty_udf('CAPGAINS').alias('CAPGAINS'),
  str_empty_udf('CERT_NUMBER').alias('CERT_NUMBER'),
  str_empty_udf('CERT_PREFIX').alias('CERT_PREFIX'),
  str_empty_udf('CHECK_NUMBER').alias('CHECK_NUMBER'),
  str_empty_udf('CITY_CODE').alias('CITY_CODE'),
  str_empty_udf('COM_SCH_ASG_RSN_ID').alias('COM_SCH_ASG_RSN_ID'),
  dst_shareholder_div_ytd_select_df.CONFIRM_DATE.cast(DateType()).alias('CONFIRM_DATE'),
  dst_shareholder_div_ytd_select_df.CUMDISC.cast(IntegerType()).alias('CUMDISC'),
  dst_shareholder_div_ytd_select_df.DATEEST.cast(DateType()).alias('DATEEST'),
  dst_shareholder_div_ytd_select_df.DEALER.cast(IntegerType()).alias('DEALER'),
  dst_shareholder_div_ytd_select_df.DEALER_COMM.cast(DoubleType()).alias('DEALER_COMM'),
  str_empty_udf('DEALER_COMM_CODE').alias('DEALER_COMM_CODE'),
  dst_shareholder_div_ytd_select_df.DEALER_CONCESSION.cast(DoubleType()).alias('DEALER_CONCESSION'),
  str_empty_udf('DEALERBRCH').alias('DEALERBRCH'),
  dst_shareholder_div_ytd_select_df.DISCAT.cast(IntegerType()).alias('DISCAT'),
  dst_shareholder_div_ytd_select_df.DIV_RATE.cast(DoubleType()).alias('DIV_RATE'),
  str_empty_udf('DOWNLOAD_FILE_CODE').alias('DOWNLOAD_FILE_CODE'),
  dst_shareholder_div_ytd_select_df.EVENT_ID_1.cast(LongType()).alias('EVENT_ID_1'),
  dst_shareholder_div_ytd_select_df.EVENT_ID_2.cast(LongType()).alias('EVENT_ID_2'),
  str_empty_udf('FIN_TR_CDSC_WV_RSN').alias('FIN_TR_CDSC_WV_RSN'),
  dst_shareholder_div_ytd_select_df.FIN_TR_ENT_DT.cast(DateType()).alias('FIN_TR_ENT_DT'),
  dst_shareholder_div_ytd_select_df.FIN_TR_ENT_TM.cast(TimestampType()).alias('FIN_TR_ENT_TM'),
  dst_shareholder_div_ytd_select_df.FOREIGNTAX_RATE.cast(DoubleType()).alias('FOREIGNTAX_RATE'),
  dst_shareholder_div_ytd_select_df.FUND_FROM_TO.cast(IntegerType()).alias('FUND_FROM_TO'),
  dst_shareholder_div_ytd_select_df.GROSS_AMOUNT.cast(DoubleType()).alias('GROSS_AMOUNT'),
  dst_shareholder_div_ytd_select_df.INTRADAY_STRIKE_HOUR.cast(IntegerType()).alias('INTRADAY_STRIKE_HOUR'),
  dst_shareholder_div_ytd_select_df.INTRADAY_STRIKE_MINUTE.cast(IntegerType()).alias('INTRADAY_STRIKE_MINUTE'),
  dst_shareholder_div_ytd_select_df.KEY_PREFIX.cast(IntegerType()).alias('KEY_PREFIX'),
  dst_shareholder_div_ytd_select_df.LASTMAINT.cast(DateType()).alias('LASTMAINT'),
  str_empty_udf('LIQUIDITY_FEE_RESP_PARTY').alias('LIQUIDITY_FEE_RESP_PARTY'),
  str_empty_udf('MGMTCODE').alias('MGMTCODE'),
  str_empty_udf('NAVCODE').alias('NAVCODE'),
  dst_shareholder_div_ytd_select_df.NEGOT_COMM_RATE.cast(DoubleType()).alias('NEGOT_COMM_RATE'),
  dst_shareholder_div_ytd_select_df.NET_ASSET_VALUE.cast(DoubleType()).alias('NET_ASSET_VALUE'),
  dst_shareholder_div_ytd_select_df.OFFERING_PRICE.cast(DoubleType()).alias('OFFERING_PRICE'),
  str_empty_udf('OPERATOR_ID').alias('OPERATOR_ID'),
  dst_shareholder_div_ytd_select_df.ORDER_NUMBER.cast(IntegerType()).alias('ORDER_NUMBER'),
  str_empty_udf('PAY_OFFICE').alias('PAY_OFFICE'),
  str_empty_udf('PAYMENT_METHOD').alias('PAYMENT_METHOD'),
  str_empty_udf('PLAN_STATUS_CODE').alias('PLAN_STATUS_CODE'),
  str_empty_udf('PRE_POST_NOON').alias('PRE_POST_NOON'),
  dst_shareholder_div_ytd_select_df.PRICE_STRIKE_HOUR.cast(IntegerType()).alias('PRICE_STRIKE_HOUR'),
  dst_shareholder_div_ytd_select_df.PRICE_STRIKE_MINUTE.cast(IntegerType()).alias('PRICE_STRIKE_MINUTE'),
  str_empty_udf('PRINT_CONFIRM').alias('PRINT_CONFIRM'),
  str_empty_udf('QRA_RED_TYP_CDE').alias('QRA_RED_TYP_CDE'),
  dst_shareholder_div_ytd_select_df.REASON_CODE.cast(IntegerType()).alias('REASON_CODE'),
  dst_shareholder_div_ytd_select_df.RECORD_DATE.cast(DateType()).alias('RECORD_DATE'),
  str_empty_udf('REPNAME').alias('REPNAME'),
  str_empty_udf('REPNUMBER').alias('REPNUMBER'),
  str_empty_udf('RESSTATECOUNTRY').alias('RESSTATECOUNTRY'),
  dst_shareholder_div_ytd_select_df.RLO_60_DAY_WV_VLD_VAL_ID.cast(IntegerType()).alias('RLO_60_DAY_WV_VLD_VAL_ID'),
  str_empty_udf('SAMGMTEMPL').alias('SAMGMTEMPL'),
  str_empty_udf('SAPPACNTL').alias('SAPPACNTL'),
  dst_shareholder_div_ytd_select_df.SHARES.cast(DoubleType()).alias('SHARES'),
  dst_shareholder_div_ytd_select_df.SOCIALCODE.cast(IntegerType()).alias('SOCIALCODE'),
  str_empty_udf('SPLIT_COMM').alias('SPLIT_COMM'),
  str_empty_udf('SS_CODE_ALL_TRANS').alias('SS_CODE_ALL_TRANS'),
  str_empty_udf('STATE_CODE').alias('STATE_CODE'),
  dst_shareholder_div_ytd_select_df.SUPER_SHEET_DATE.cast(DateType()).alias('SUPER_SHEET_DATE'),
  str_empty_udf('TERRITORY').alias('TERRITORY'),
  str_empty_udf('TIMER').alias('TIMER'),
  str_empty_udf('TR_HST_STE_CNY_ID').alias('TR_HST_STE_CNY_ID'),
  dst_shareholder_div_ytd_select_df.TRADE_DATE.cast(DateType()).alias('TRADE_DATE'),
  dst_shareholder_div_ytd_select_df.TRADE_ENTRY_DT.cast(DateType()).alias('TRADE_ENTRY_DT'),
  str_empty_udf('TRADE_ENTRY_METHOD').alias('TRADE_ENTRY_METHOD'),
  dst_shareholder_div_ytd_select_df.TRADE_ENTRY_TM.cast(TimestampType()).alias('TRADE_ENTRY_TM'),
  str_empty_udf('TRADE_ORIGIN_ID').alias('TRADE_ORIGIN_ID'),
  dst_shareholder_div_ytd_select_df.TR_DSC_EXT_ID.cast(IntegerType()).alias('TR_DSC_EXT_ID'),
  dst_shareholder_div_ytd_select_df.TRANS_CODE.cast(IntegerType()).alias('TRANS_CODE'),
  str_empty_udf('TRANS_DESC').alias('TRANS_DESC'),
  str_empty_udf('TRANS_PPA_CNTL').alias('TRANS_PPA_CNTL'),
  dst_shareholder_div_ytd_select_df.TRANS_SUFFIX.cast(IntegerType()).alias('TRANS_SUFFIX'),
  str_empty_udf('UNCOL_BAL_OVERRIDE').alias('UNCOL_BAL_OVERRIDE'),
  dst_shareholder_div_ytd_select_df.UNDERWRITER_COMM.cast(DoubleType()).alias('UNDERWRITER_COMM')
)
# Coalesce results
dst_shareholder_div_ytd_select_write = dst_shareholder_div_ytd_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_shareholder_div_ytd_select_write is:") 
dst_shareholder_div_ytd_select_write.printSchema()

# Potential final output filename(s): shareholder_dividend_ytd, account_dividend_ytd
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_shareholder_div_ytd_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
