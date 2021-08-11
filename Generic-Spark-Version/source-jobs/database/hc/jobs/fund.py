import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from decimal import Decimal
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType
from pyspark.sql.functions import col
from datetime import datetime
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
sqlContext = SQLContext(sc)

job = Job(glueContext) 
job.init(args['JOB_NAME'], args)

# Table(s)  
fund_fund = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_fund".format(src_schema))
fund_composite = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_composite".format(src_schema))
fund_class = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_class".format(src_schema))
fund_status_code = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_status_code".format(src_schema))
ref_frequency_code = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_ref_frequency_code".format(src_schema))

# Convert dynamic dataframes to dataframes
fund_fund_df = fund_fund.toDF()
fund_composite_df = fund_composite.toDF()
fund_class_df = fund_class.toDF()
fund_status_code_df = fund_status_code.toDF()
ref_frequency_code_df = ref_frequency_code.toDF()

# Convert dataframes to temp views
fund_fund_df.createOrReplaceTempView("fund_fund_tbl")
fund_composite_df.createOrReplaceTempView("fund_composite_tbl")
fund_class_df.createOrReplaceTempView("fund_class_tbl")
fund_status_code_df.createOrReplaceTempView("fund_status_code_tbl")
ref_frequency_code_df.createOrReplaceTempView("ref_frequency_code_tbl")

# Fund Query
fund_select_df = sqlContext.sql("""
select
fnd.cmp_id AS compst_fund_id,
cls.id AS shr_cls_cd,
cls.DESCRIPTION AS shr_cls_desc,
cmp.DESCRIPTION AS fund_nm,
fund_long_nm AS fund_long_desc,
fnd.abbreviation AS fund_abrv,
stc.id AS fund_stat_cd,
initcap(stc.description) AS fund_stat_desc,
inception_date AS fund_incpn_dt,
closed_date AS fund_clos_dt,
fnd.public_fund_nbr AS fund_nbr,
fund_order AS fund_sort_ord,
fnd.address_id AS addr_id,
fnd.cusip AS cusip_id,
fnd.quot_sym AS quot_sym,
state_str_fund_nbr AS st_str_fund_nbr,
fnd.tax_id AS tax_id,
fnd.risk_level AS rsk_lvl,
fq.name AS divd_freq_cd,
fq.description AS divd_freq_desc,
cfq.name AS cap_gain_freq_cd,
cfq.description AS cap_gain_freq_desc,
reg_acct_initial_investment AS reg_acct_init_invmt,
ira_acct_initial_investment AS ira_acct_init_invmt,
ugma_acct_initial_investment AS ugma_acct_init_invmt,
fnd.PERF_INCPN_DT AS perf_incpn_dt,
fnd.perf_calc_flg
from 
fund_fund_tbl AS fnd 
inner join fund_composite_tbl AS cmp on fnd.cmp_id=cmp.id
inner join fund_class_tbl AS cls on fnd.CLS_ID=cls.id
inner join fund_status_code_tbl AS stc on fnd.stc_id=stc.ID
inner join ref_frequency_code_tbl AS fq on fnd.DIVD_FQC_ID=fq.id
inner join ref_frequency_code_tbl AS cfq on fnd.CAP_GAIN_FQC_ID=cfq.id
order by fund_order
""")

fund_select_df_scrubbed = fund_select_df.select(
  fund_select_df.compst_fund_id.cast(IntegerType()).alias('compst_fund_id'),
  fund_select_df.shr_cls_cd.cast(IntegerType()).alias('shr_cls_cd'),
  col('shr_cls_desc'),
  col('fund_nm'),
  col('fund_long_desc'),
  col('fund_abrv'),
  fund_select_df.fund_stat_cd.cast(IntegerType()).alias('fund_stat_cd'),
  col('fund_stat_desc'),
  fund_select_df.fund_incpn_dt.cast(DateType()).alias('fund_incpn_dt'),
  fund_select_df.fund_clos_dt.cast(DateType()).alias('fund_clos_dt'),
  col('fund_nbr'),
  fund_select_df.fund_sort_ord.cast(IntegerType()).alias('fund_sort_ord'),
  fund_select_df.addr_id.cast(IntegerType()).alias('addr_id'),
  col('cusip_id'),
  col('quot_sym'),
  col('st_str_fund_nbr'),
  col('tax_id'),
  fund_select_df.rsk_lvl.cast(IntegerType()).alias('rsk_lvl'),
  col('divd_freq_cd'),
  col('divd_freq_desc'),
  col('cap_gain_freq_cd'),
  col('cap_gain_freq_desc'),
  fund_select_df.reg_acct_init_invmt.cast(IntegerType()).alias('reg_acct_init_invmt'),
  fund_select_df.ira_acct_init_invmt.cast(IntegerType()).alias('ira_acct_init_invmt'),
  fund_select_df.ugma_acct_init_invmt.cast(IntegerType()).alias('ugma_acct_init_invmt'),
  fund_select_df.perf_incpn_dt.cast(DateType()).alias('perf_incpn_dt')
)

fund_select_write = fund_select_df_scrubbed.repartition(1).sort('fund_sort_ord').coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

fund_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=nonsensitive')

print("Done writing")
