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
t1_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_master").load()
t2_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_lotaxrf").load()
t3_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_locstxrf").load()
t4_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_consentcust").load()
t5_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_locust").load()
# no t6 - now an alias - t6_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_consentcust").load()
t7_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_consentstmt").load()
t8_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_investor").load()
t9_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", jdbc_url).option("user", db_username).option("password", db_password).option("dbtable","dbo.hbr_consentcust").load()
#
# Convert dataframe to tables/views
t1_df.createOrReplaceTempView("t1")
t2_df.createOrReplaceTempView("t2")
t3_df.createOrReplaceTempView("t3")
t4_df.createOrReplaceTempView("t4")
t5_df.createOrReplaceTempView("t5")
# no t6 - t6_df.createOrReplaceTempView("t6")
t7_df.createOrReplaceTempView("t7")
t8_df.createOrReplaceTempView("t8")
t9_df.createOrReplaceTempView("t9")
#
# Query: dst_shareholder_consent_data
dst_shareholder_consent_data_select_df = sqlContext.sql("""
select distinct t1.fund,
                ltrim (rtrim (t1.account)) account,
                t1.load_date,
                cast (t1.load_time as char (10)) load_time,
                t1.acct_setup_dte,
                t1.dealercntl,
                ltrim (rtrim (t4.stm_eml_adr_txt)) stm_eml_adr_txt,
                ltrim (rtrim (t5.send_mkt_mail)) send_mkt_mail,
                ltrim (rtrim (t3.customer_id1)) customer_id1,
                ltrim (rtrim (t3.customer_id2)) customer_id2,
                ltrim (rtrim (t5.tax_id_nbr)) tax_id_nbr,
                t6.cmr_infd_csnt,
                t8.reporting_grp investor_id
                from  t1
                inner JOIN t2 on (t1.fund = t2.fund and t1.account = t2.account)
                inner JOIN t3 on (t2.legal_owner_id1 = t3.legal_owner_id1 and t2.legal_owner_id2 = t3.legal_owner_id2)
                left outer join t4 on (t4.customer_id1 = t3.customer_id1 and t4.customer_id2 = t3.customer_id2)
                left outer JOIN 
                    (SELECT t9.* 
                          FROM t9 
                          inner JOIN t7  on ((t7.customer_id1 = t9.customer_id1) and (t7.customer_id2 = t9.customer_id2) and t7.stmt_type_id = 2)
                    ) t6 on (t6.customer_id1 = t2.customer_id1 and t6.customer_id2 = t2.customer_id2)
               left outer join t5 on (t5.customer_id1 = t2.customer_id1 and t5.customer_id2 = t2.customer_id2)
               left outer JOIN t8  on (t1.account = t8.account and t1.fund = t8.fund)
""")
#
# Print schema of output
dst_shareholder_consent_data_select_df.printSchema()
#print(dst_shareholder_consent_data_df.count())
print("The number of rows returned by the query is " + str(dst_shareholder_consent_data_select_df.count()))
#
# schema
"""
root
 |-- fund: integer (nullable = true)
 |-- account: string (nullable = true)
 |-- load_date: date (nullable = true)
 |-- load_time: string (nullable = true)
 |-- acct_setup_dte: date (nullable = true)
 |-- dealercntl: string (nullable = true)
 |-- stm_eml_adr_txt: string (nullable = true)
 |-- send_mkt_mail: string (nullable = true)
 |-- customer_id1: string (nullable = true)
 |-- customer_id2: string (nullable = true)
 |-- tax_id_nbr: string (nullable = true)
 |-- cmr_infd_csnt: string (nullable = true)
 |-- investor_id: integer (nullable = true)
 """
#
# Remove extra scrubbing to address error
# AttributeError: 'NoneType' object has no attribute 'isspace'
# For reference, keeping the scrubbed version for further diagnostics.
"""
  dst_shareholder_consent_data_select_df.fund.cast(IntegerType()).alias('fund'),
  str_empty_udf('account').alias('account'),
  dst_shareholder_consent_data_select_df.load_date.cast(DateType()).alias('load_date'),
  str_empty_udf('load_time').alias('load_time'),
  dst_shareholder_consent_data_select_df.acct_setup_dte.cast(DateType()).alias('acct_setup_dte'),
  str_empty_udf('dealercntl').alias('dealercntl'),
  str_empty_udf('stm_eml_adr_txt').alias('stm_eml_adr_txt'),
  str_empty_udf('send_mkt_mail').alias('send_mkt_mail'),
  str_empty_udf('customer_id1').alias('customer_id1'),
  str_empty_udf('customer_id2').alias('customer_id2'),
  str_empty_udf('tax_id_nbr').alias('tax_id_nbr'),
  str_empty_udf('cmr_infd_csnt').alias('cmr_infd_csnt'),
  dst_shareholder_consent_data_select_df.investor_id.cast(IntegerType()).alias('investor_id')
"""
# Reverting to non-scrubbed pre-empty CHAR string (isspace()) fix
# Format results
dst_shareholder_consent_data_select_df_scrubbed = dst_shareholder_consent_data_select_df.select(
  dst_shareholder_consent_data_select_df.fund.cast(IntegerType()).alias('fund'),
  col('account'),
  dst_shareholder_consent_data_select_df.load_date.cast(DateType()).alias('load_date'),
  col('load_time'),
  dst_shareholder_consent_data_select_df.acct_setup_dte.cast(DateType()).alias('acct_setup_dte'),
  col('dealercntl'),
  col('stm_eml_adr_txt'),
  col('send_mkt_mail'),
  col('customer_id1'),
  col('customer_id2'),
  col('tax_id_nbr'),
  col('cmr_infd_csnt'),
  dst_shareholder_consent_data_select_df.investor_id.cast(IntegerType()).alias('investor_id')
)
# Coalesce results
dst_shareholder_consent_data_select_write = dst_shareholder_consent_data_select_df_scrubbed.repartition(1)
#
print("The schema of the final output dst_shareholder_consent_data_select_write is:") 
dst_shareholder_consent_data_select_write.printSchema()

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dst_shareholder_consent_data_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=pii')

print("Done writing")
