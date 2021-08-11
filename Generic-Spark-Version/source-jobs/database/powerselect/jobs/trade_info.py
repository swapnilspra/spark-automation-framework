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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.HBR_TRADINFO").load()
#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
#t1_df.printSchema()
#
# Query: dst_shareholder_consent_data
dst_tradinfo_select_df = sqlContext.sql("""select * from t1""")
#
# Print schema of output
dst_tradinfo_select_df.printSchema()
#print(dst_tradinfo_select_df.count())
print("The number of rows returned by the query is " + str(dst_tradinfo_select_df.count()))
#
# schema
"""
root
 |-- FUND: integer (nullable = true)
 |-- NSCC_CONTROL_NBR: string (nullable = true)
 |-- NSCC_CORRECT_CODE: string (nullable = true)
 |-- LOAD_DATE: date (nullable = true)
 |-- LOAD_TIME: timestamp (nullable = true)
 |-- CASH_ADJ_AMOUNT: double (nullable = true)
 |-- CASH_ADJ_CREAT_CD: string (nullable = true)
 |-- CNFRM_DLR_COMM_AMT: double (nullable = true)
 |-- CONF_DISC_CAT: string (nullable = true)
 |-- CONF_TO_NSCC_CDE: string (nullable = true)
 |-- CONF_TO_NSCC_DTE: date (nullable = true)
 |-- CUSTOMER_NAME: string (nullable = true)
 |-- DST_ACCT_NBR: string (nullable = true)
 |-- DST_ACCT_NBR_EXT: string (nullable = true)
 |-- DST_REJECT_CODE: string (nullable = true)
 |-- EXCH_FEE_AMT: double (nullable = true)
 |-- EXCH_PURCH_PEND_CD: string (nullable = true)
 |-- EXCH_REG_PEND_CODE: string (nullable = true)
 |-- EXT_CORR_PRCSS_CDE: string (nullable = true)
 |-- EXT_CORR_RECV_DATE: date (nullable = true)
 |-- GROSS_AMT: double (nullable = true)
 |-- LAST_CORRECTION_CD: string (nullable = true)
 |-- LAST_DST_RECORD_CD: string (nullable = true)
 |-- LAST_PROCESS_DATE: date (nullable = true)
 |-- LAST_PROCESS_TIME: timestamp (nullable = true)
 |-- MGMTCODE: string (nullable = true)
 |-- NET_DUE_AMT: double (nullable = true)
 |-- NSCC_ABA_ROUT_NBR: string (nullable = true)
 |-- NSCC_ACH_CODE: string (nullable = true)
 |-- NSCC_AGNT_FIRM_NBR: string (nullable = true)
 |-- NSCC_ANTCP_SETT_DT: date (nullable = true)
 |-- NSCC_ASOF: string (nullable = true)
 |-- NSCC_ASOF_TRADE_DT: date (nullable = true)
 |-- NSCC_BK_PHY_SHR_CD: string (nullable = true)
 |-- NSCC_BRANCH_ID_NBR: string (nullable = true)
 |-- NSCC_CHK_WRT_PRIV: string (nullable = true)
 |-- NSCC_CLRNG_MBR_CDE: string (nullable = true)
 |-- NSCC_CNTRY_OF_ORGN: string (nullable = true)
 |-- NSCC_CONFIRM_NBR: string (nullable = true)
 |-- NSCC_CURRENCY_CODE: string (nullable = true)
 |-- NSCC_CUST_ACCT_CD: string (nullable = true)
 |-- NSCC_FIRM_ACCT_NBR: string (nullable = true)
 |-- NSCC_FIRM_NBR: string (nullable = true)
 |-- NSCC_FRAC_XFR_ECH: string (nullable = true)
 |-- NSCC_FRACTION_CODE: string (nullable = true)
 |-- NSCC_FUND_ACC_CODE: string (nullable = true)
 |-- NSCC_FUND_NUMBER: string (nullable = true)
 |-- NSCC_FUT_TRD_DT_CD: string (nullable = true)
 |-- NSCC_GROSS_SETT_CD: string (nullable = true)
 |-- NSCC_IRA_CODE: string (nullable = true)
 |-- NSCC_IRA_LIQUID_CD: string (nullable = true)
 |-- NSCC_LIQUID_FEE_CD: string (nullable = true)
 |-- NSCC_LOI_NBR_OR_DT: string (nullable = true)
 |-- NSCC_NAV_REASON_CD: string (nullable = true)
 |-- NSCC_NTWRK_CNTL_CD: string (nullable = true)
 |-- NSCC_OMB_ACCT_CD: string (nullable = true)
 |-- NSCC_ORGTNG_TYP_CD: string (nullable = true)
 |-- NSCC_PAC_CODE: string (nullable = true)
 |-- NSCC_PI_DELVRY_CD: string (nullable = true)
 |-- NSCC_REASON_CODE: string (nullable = true)
 |-- NSCC_RECORD_TYP_CD: string (nullable = true)
 |-- NSCC_REJECT_CODE: string (nullable = true)
 |-- NSCC_REL_TRD_GP_ID: string (nullable = true)
 |-- NSCC_REP_ID: string (nullable = true)
 |-- NSCC_REP_NAME: string (nullable = true)
 |-- NSCC_ROA_LOI_VALUE: double (nullable = true)
 |-- NSCC_ROA_SOURCE: string (nullable = true)
 |-- NSCC_SALE_CHRG_PCT: double (nullable = true)
 |-- NSCC_SHORT_SETT_CD: string (nullable = true)
 |-- NSCC_ST_OF_SALE_ID: string (nullable = true)
 |-- NSCC_STTR_WV_ID: string (nullable = true)
 |-- NSCC_SUBMISSION_DT: date (nullable = true)
 |-- NSCC_SWP_CODE: string (nullable = true)
 |-- NSCC_SWP_FREQ_CODE: string (nullable = true)
 |-- NSCC_TEST_CODE: string (nullable = true)
 |-- NSCC_TPA_FIRM_NBR: string (nullable = true)
 |-- NSCC_TRADE_DATE: date (nullable = true)
 |-- NSCC_TRADE_INP_CD: string (nullable = true)
 |-- NSCC_TRADE_MEMO_TX: string (nullable = true)
 |-- NSCC_TRANS_CODE: string (nullable = true)
 |-- NSCC_TRANS_TYPE: string (nullable = true)
 |-- OFFERING_PRICE: double (nullable = true)
 |-- ORDER_ACCEPT_UPON: string (nullable = true)
 |-- ORDER_ACCEPTED_CD: string (nullable = true)
 |-- ORDER_ACCEPTED_DT: date (nullable = true)
 |-- ORDER_CLEARED_DT: date (nullable = true)
 |-- ORDER_DELETE_DATE: date (nullable = true)
 |-- ORDER_DELETED_CD: string (nullable = true)
 |-- ORDER_NUMBER: integer (nullable = true)
 |-- ORDER_SETTLED_DT: date (nullable = true)
 |-- ORGIN_CORRECT_CD: string (nullable = true)
 |-- ORGIN_NSCC_CTL_NBR: string (nullable = true)
 |-- PST_STL_CNC_STA_CD: string (nullable = true)
 |-- PST_STL_CRCT_FIRM: string (nullable = true)
 |-- PST_STL_CRCT_STA: string (nullable = true)
 |-- RE_CNFMTONSCC_CD: string (nullable = true)
 |-- RE_CNFMTONSCC_DT: date (nullable = true)
 |-- REASON_CODE: string (nullable = true)
 |-- REG_CONFM_DATE: date (nullable = true)
 |-- REG_CONFM_TYP_CD: string (nullable = true)
 |-- REG_PEND_AUTOSCRU: string (nullable = true)
 |-- REG_PEND_DATE: date (nullable = true)
 |-- REG_RECV_DATE: date (nullable = true)
 |-- RELATED_TRADE_CDE: string (nullable = true)
 |-- ROA_ACTIVE: string (nullable = true)
 |-- ROA_VALID_DATE: date (nullable = true)
 |-- SEC_ISS_CHK_DIGIT: string (nullable = true)
 |-- SEC_ISS_CNTRY_CDE: string (nullable = true)
 |-- SEC_ISSUE_ID: string (nullable = true)
 |-- SEC_ISSUE_TYPE: string (nullable = true)
 |-- SETTLE_OR_RSN_CDE: string (nullable = true)
 |-- SETTLE_PEND_DATE: date (nullable = true)
 |-- SETTLE_RECV_DATE: date (nullable = true)
 |-- SHARE_CNT: double (nullable = true)
 |-- SHORTTERM_TRD_FEE: double (nullable = true)
 |-- SSN_EIN_NBR: string (nullable = true)
 |-- SSN_EIN_CODE: string (nullable = true)
 |-- TRADE_NUMBER: string (nullable = true)
"""
#
# Scrub and format results
dst_tradinfo_select_df_scrubbed = dst_tradinfo_select_df.select(
 dst_tradinfo_select_df.FUND.cast(IntegerType()).alias('FUND'),
 str_empty_udf('NSCC_CONTROL_NBR').alias('NSCC_CONTROL_NBR'),
 str_empty_udf('NSCC_CORRECT_CODE').alias('NSCC_CORRECT_CODE'),
 dst_tradinfo_select_df.LOAD_DATE.cast(DateType()).alias('LOAD_DATE'),
 dst_tradinfo_select_df.LOAD_TIME.cast(TimestampType()).alias('LOAD_TIME'),
 dst_tradinfo_select_df.CASH_ADJ_AMOUNT.cast(DoubleType()).alias('CASH_ADJ_AMOUNT'),
 str_empty_udf('CASH_ADJ_CREAT_CD').alias('CASH_ADJ_CREAT_CD'),
 dst_tradinfo_select_df.CNFRM_DLR_COMM_AMT.cast(DoubleType()).alias('CNFRM_DLR_COMM_AMT'),
 str_empty_udf('CONF_DISC_CAT').alias('CONF_DISC_CAT'),
 str_empty_udf('CONF_TO_NSCC_CDE').alias('CONF_TO_NSCC_CDE'),
 dst_tradinfo_select_df.CONF_TO_NSCC_DTE.cast(DateType()).alias('CONF_TO_NSCC_DTE'),
 str_empty_udf('CUSTOMER_NAME').alias('CUSTOMER_NAME'),
 str_empty_udf('DST_ACCT_NBR').alias('DST_ACCT_NBR'),
 str_empty_udf('DST_ACCT_NBR_EXT').alias('DST_ACCT_NBR_EXT'),
 str_empty_udf('DST_REJECT_CODE').alias('DST_REJECT_CODE'),
 dst_tradinfo_select_df.EXCH_FEE_AMT.cast(DoubleType()).alias('EXCH_FEE_AMT'),
 str_empty_udf('EXCH_PURCH_PEND_CD').alias('EXCH_PURCH_PEND_CD'),
 str_empty_udf('EXCH_REG_PEND_CODE').alias('EXCH_REG_PEND_CODE'),
 str_empty_udf('EXT_CORR_PRCSS_CDE').alias('EXT_CORR_PRCSS_CDE'),
 dst_tradinfo_select_df.EXT_CORR_RECV_DATE.cast(DateType()).alias('EXT_CORR_RECV_DATE'),
 dst_tradinfo_select_df.GROSS_AMT.cast(DoubleType()).alias('GROSS_AMT'),
 str_empty_udf('LAST_CORRECTION_CD').alias('LAST_CORRECTION_CD'),
 str_empty_udf('LAST_DST_RECORD_CD').alias('LAST_DST_RECORD_CD'),
 dst_tradinfo_select_df.LAST_PROCESS_DATE.cast(DateType()).alias('LAST_PROCESS_DATE'),
 dst_tradinfo_select_df.LAST_PROCESS_TIME.cast(TimestampType()).alias('LAST_PROCESS_TIME'),
 str_empty_udf('MGMTCODE').alias('MGMTCODE'),
 dst_tradinfo_select_df.NET_DUE_AMT.cast(DoubleType()).alias('NET_DUE_AMT'),
 str_empty_udf('NSCC_ABA_ROUT_NBR').alias('NSCC_ABA_ROUT_NBR'),
 str_empty_udf('NSCC_ACH_CODE').alias('NSCC_ACH_CODE'),
 str_empty_udf('NSCC_AGNT_FIRM_NBR').alias('NSCC_AGNT_FIRM_NBR'),
 dst_tradinfo_select_df.NSCC_ANTCP_SETT_DT.cast(DateType()).alias('NSCC_ANTCP_SETT_DT'),
 str_empty_udf('NSCC_ASOF').alias('NSCC_ASOF'),
 dst_tradinfo_select_df.NSCC_ASOF_TRADE_DT.cast(DateType()).alias('NSCC_ASOF_TRADE_DT'),
 str_empty_udf('NSCC_BK_PHY_SHR_CD').alias('NSCC_BK_PHY_SHR_CD'),
 str_empty_udf('NSCC_BRANCH_ID_NBR').alias('NSCC_BRANCH_ID_NBR'),
 str_empty_udf('NSCC_CHK_WRT_PRIV').alias('NSCC_CHK_WRT_PRIV'),
 str_empty_udf('NSCC_CLRNG_MBR_CDE').alias('NSCC_CLRNG_MBR_CDE'),
 str_empty_udf('NSCC_CNTRY_OF_ORGN').alias('NSCC_CNTRY_OF_ORGN'),
 str_empty_udf('NSCC_CONFIRM_NBR').alias('NSCC_CONFIRM_NBR'),
 str_empty_udf('NSCC_CURRENCY_CODE').alias('NSCC_CURRENCY_CODE'),
 str_empty_udf('NSCC_CUST_ACCT_CD').alias('NSCC_CUST_ACCT_CD'),
 str_empty_udf('NSCC_FIRM_ACCT_NBR').alias('NSCC_FIRM_ACCT_NBR'),
 str_empty_udf('NSCC_FIRM_NBR').alias('NSCC_FIRM_NBR'),
 str_empty_udf('NSCC_FRAC_XFR_ECH').alias('NSCC_FRAC_XFR_ECH'),
 str_empty_udf('NSCC_FRACTION_CODE').alias('NSCC_FRACTION_CODE'),
 str_empty_udf('NSCC_FUND_ACC_CODE').alias('NSCC_FUND_ACC_CODE'),
 str_empty_udf('NSCC_FUND_NUMBER').alias('NSCC_FUND_NUMBER'),
 str_empty_udf('NSCC_FUT_TRD_DT_CD').alias('NSCC_FUT_TRD_DT_CD'),
 str_empty_udf('NSCC_GROSS_SETT_CD').alias('NSCC_GROSS_SETT_CD'),
 str_empty_udf('NSCC_IRA_CODE').alias('NSCC_IRA_CODE'),
 str_empty_udf('NSCC_IRA_LIQUID_CD').alias('NSCC_IRA_LIQUID_CD'),
 str_empty_udf('NSCC_LIQUID_FEE_CD').alias('NSCC_LIQUID_FEE_CD'),
 str_empty_udf('NSCC_LOI_NBR_OR_DT').alias('NSCC_LOI_NBR_OR_DT'),
 str_empty_udf('NSCC_NAV_REASON_CD').alias('NSCC_NAV_REASON_CD'),
 str_empty_udf('NSCC_NTWRK_CNTL_CD').alias('NSCC_NTWRK_CNTL_CD'),
 str_empty_udf('NSCC_OMB_ACCT_CD').alias('NSCC_OMB_ACCT_CD'),
 str_empty_udf('NSCC_ORGTNG_TYP_CD').alias('NSCC_ORGTNG_TYP_CD'),
 str_empty_udf('NSCC_PAC_CODE').alias('NSCC_PAC_CODE'),
 str_empty_udf('NSCC_PI_DELVRY_CD').alias('NSCC_PI_DELVRY_CD'),
 str_empty_udf('NSCC_REASON_CODE').alias('NSCC_REASON_CODE'),
 str_empty_udf('NSCC_RECORD_TYP_CD').alias('NSCC_RECORD_TYP_CD'),
 str_empty_udf('NSCC_REJECT_CODE').alias('NSCC_REJECT_CODE'),
 str_empty_udf('NSCC_REL_TRD_GP_ID').alias('NSCC_REL_TRD_GP_ID'),
 str_empty_udf('NSCC_REP_ID').alias('NSCC_REP_ID'),
 str_empty_udf('NSCC_REP_NAME').alias('NSCC_REP_NAME'),
 dst_tradinfo_select_df.NSCC_ROA_LOI_VALUE.cast(DoubleType()).alias('NSCC_ROA_LOI_VALUE'),
 str_empty_udf('NSCC_ROA_SOURCE').alias('NSCC_ROA_SOURCE'),
 dst_tradinfo_select_df.NSCC_SALE_CHRG_PCT.cast(DoubleType()).alias('NSCC_SALE_CHRG_PCT'),
 str_empty_udf('NSCC_SHORT_SETT_CD').alias('NSCC_SHORT_SETT_CD'),
 str_empty_udf('NSCC_ST_OF_SALE_ID').alias('NSCC_ST_OF_SALE_ID'),
 str_empty_udf('NSCC_STTR_WV_ID').alias('NSCC_STTR_WV_ID'),
 dst_tradinfo_select_df.NSCC_SUBMISSION_DT.cast(DateType()).alias('NSCC_SUBMISSION_DT'),
 str_empty_udf('NSCC_SWP_CODE').alias('NSCC_SWP_CODE'),
 str_empty_udf('NSCC_SWP_FREQ_CODE').alias('NSCC_SWP_FREQ_CODE'),
 str_empty_udf('NSCC_TEST_CODE').alias('NSCC_TEST_CODE'),
 str_empty_udf('NSCC_TPA_FIRM_NBR').alias('NSCC_TPA_FIRM_NBR'),
 dst_tradinfo_select_df.NSCC_TRADE_DATE.cast(DateType()).alias('NSCC_TRADE_DATE'),
 str_empty_udf('NSCC_TRADE_INP_CD').alias('NSCC_TRADE_INP_CD'),
 str_empty_udf('NSCC_TRADE_MEMO_TX').alias('NSCC_TRADE_MEMO_TX'),
 str_empty_udf('NSCC_TRANS_CODE').alias('NSCC_TRANS_CODE'),
 str_empty_udf('NSCC_TRANS_TYPE').alias('NSCC_TRANS_TYPE'),
 dst_tradinfo_select_df.OFFERING_PRICE.cast(DoubleType()).alias('OFFERING_PRICE'),
 str_empty_udf('ORDER_ACCEPT_UPON').alias('ORDER_ACCEPT_UPON'),
 str_empty_udf('ORDER_ACCEPTED_CD').alias('ORDER_ACCEPTED_CD'),
 dst_tradinfo_select_df.ORDER_ACCEPTED_DT.cast(DateType()).alias('ORDER_ACCEPTED_DT'),
 dst_tradinfo_select_df.ORDER_CLEARED_DT.cast(DateType()).alias('ORDER_CLEARED_DT'),
 dst_tradinfo_select_df.ORDER_DELETE_DATE.cast(DateType()).alias('ORDER_DELETE_DATE'),
 str_empty_udf('ORDER_DELETED_CD').alias('ORDER_DELETED_CD'),
 dst_tradinfo_select_df.ORDER_NUMBER.cast(IntegerType()).alias('ORDER_NUMBER'),
 dst_tradinfo_select_df.ORDER_SETTLED_DT.cast(DateType()).alias('ORDER_SETTLED_DT'),
 str_empty_udf('ORGIN_CORRECT_CD').alias('ORGIN_CORRECT_CD'),
 str_empty_udf('ORGIN_NSCC_CTL_NBR').alias('ORGIN_NSCC_CTL_NBR'),
 str_empty_udf('PST_STL_CNC_STA_CD').alias('PST_STL_CNC_STA_CD'),
 str_empty_udf('PST_STL_CRCT_FIRM').alias('PST_STL_CRCT_FIRM'),
 str_empty_udf('PST_STL_CRCT_STA').alias('PST_STL_CRCT_STA'),
 str_empty_udf('RE_CNFMTONSCC_CD').alias('RE_CNFMTONSCC_CD'),
 dst_tradinfo_select_df.RE_CNFMTONSCC_DT.cast(DateType()).alias('RE_CNFMTONSCC_DT'),
 str_empty_udf('REASON_CODE').alias('REASON_CODE'),
 dst_tradinfo_select_df.REG_CONFM_DATE.cast(DateType()).alias('REG_CONFM_DATE'),
 str_empty_udf('REG_CONFM_TYP_CD').alias('REG_CONFM_TYP_CD'),
 str_empty_udf('REG_PEND_AUTOSCRU').alias('REG_PEND_AUTOSCRU'),
 dst_tradinfo_select_df.REG_PEND_DATE.cast(DateType()).alias('REG_PEND_DATE'),
 dst_tradinfo_select_df.REG_RECV_DATE.cast(DateType()).alias('REG_RECV_DATE'),
 str_empty_udf('RELATED_TRADE_CDE').alias('RELATED_TRADE_CDE'),
 str_empty_udf('ROA_ACTIVE').alias('ROA_ACTIVE'),
 dst_tradinfo_select_df.ROA_VALID_DATE.cast(DateType()).alias('ROA_VALID_DATE'),
 str_empty_udf('SEC_ISS_CHK_DIGIT').alias('SEC_ISS_CHK_DIGIT'),
 str_empty_udf('SEC_ISS_CNTRY_CDE').alias('SEC_ISS_CNTRY_CDE'),
 str_empty_udf('SEC_ISSUE_ID').alias('SEC_ISSUE_ID'),
 str_empty_udf('SEC_ISSUE_TYPE').alias('SEC_ISSUE_TYPE'),
 str_empty_udf('SETTLE_OR_RSN_CDE').alias('SETTLE_OR_RSN_CDE'),
 dst_tradinfo_select_df.SETTLE_PEND_DATE.cast(DateType()).alias('SETTLE_PEND_DATE'),
 dst_tradinfo_select_df.SETTLE_RECV_DATE.cast(DateType()).alias('SETTLE_RECV_DATE'),
 dst_tradinfo_select_df.SHARE_CNT.cast(DoubleType()).alias('SHARE_CNT'),
 dst_tradinfo_select_df.SHORTTERM_TRD_FEE.cast(DoubleType()).alias('SHORTTERM_TRD_FEE'),
 str_empty_udf('SSN_EIN_NBR').alias('SSN_EIN_NBR'),
 str_empty_udf('SSN_EIN_CODE').alias('SSN_EIN_CODE'),
 str_empty_udf('TRADE_NUMBER').alias('TRADE_NUMBER')
)
# Coalesce results
dst_tradinfo_select_write = dst_tradinfo_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_tradinfo_select_write is:") 
dst_tradinfo_select_write.printSchema()

# Potential final output filename(s): shareholder_trade_info, account_trade_info
tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_tradinfo_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
