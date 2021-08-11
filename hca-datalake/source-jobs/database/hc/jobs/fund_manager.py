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

#Table(s)
fnd_cmp_portfolio_manager = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_cmp_portfolio_manager".format(src_schema))
fnd_portfolio_mgr = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_portfolio_mgr".format(src_schema))
fnd_subadviser = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_subadviser".format(src_schema))

# Convert dynamic dataframes to dataframes
fnd_cmp_portfolio_manager_df = fnd_cmp_portfolio_manager.toDF()
fnd_portfolio_mgr_df = fnd_portfolio_mgr.toDF()
fnd_subadviser_df = fnd_subadviser.toDF()

# Convert dataframes to temp views
fnd_cmp_portfolio_manager_df.createOrReplaceTempView("fnd_cmp_portfolio_manager_tbl")
fnd_portfolio_mgr_df.createOrReplaceTempView("fnd_portfolio_mgr_tbl")
fnd_subadviser_df.createOrReplaceTempView("fnd_subadviser_tbl")

# Fund Manager Query
fund_manager_select_df = sqlContext.sql("""
select
cmp_id,
pm.SBA_ID AS sba_legacy_id,
pm.id AS pm_legacy_id,
sba.EFFECTIVE_DATE AS sba_efftv_strt_dt,
sba.TERMINATE_DATE AS efftv_end_dt,
'' AS pm_efftv_strt_dt,
pm.TERMINATE_DATE AS pm_efftv_end_dt,
fm.EFFECTIVE_DATE AS fund_mgr_efftv_strt_dt,
fm.TERMINATION_DT AS fund_mgr_efftv_end_dt,
sba.ACTIVE_FLAG AS sba_actv_flg,
pm.ACTIVE_FLAG AS pm_actv_flg
from
fnd_cmp_portfolio_manager_tbl AS fm
inner join fnd_portfolio_mgr_tbl AS pm 
on fm.pmg_id=pm.id
inner join fnd_subadviser_tbl AS sba
on sba.ID = pm.sba_id
""")

fund_manager_select_df_scrubbed = fund_manager_select_df.select(
  fund_manager_select_df.cmp_id.cast(IntegerType()).alias('cmp_id'),
  fund_manager_select_df.sba_legacy_id.cast(IntegerType()).alias('sba_legacy_id'),
  fund_manager_select_df.pm_legacy_id.cast(IntegerType()).alias('pm_legacy_id'),
  fund_manager_select_df.sba_efftv_strt_dt.cast(DateType()).alias('sba_efftv_strt_dt'),
  fund_manager_select_df.efftv_end_dt.cast(DateType()).alias('efftv_end_dt'),
  col('pm_efftv_strt_dt'),
  fund_manager_select_df.pm_efftv_end_dt.cast(DateType()).alias('pm_efftv_end_dt'),
  fund_manager_select_df.fund_mgr_efftv_strt_dt.cast(DateType()).alias('fund_mgr_efftv_strt_dt'),
  fund_manager_select_df.fund_mgr_efftv_end_dt.cast(DateType()).alias('fund_mgr_efftv_end_dt'),
  col('sba_actv_flg'),
  col('pm_actv_flg')
)

fund_manager_select_write = fund_manager_select_df_scrubbed.repartition(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

fund_manager_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=nonsensitive')

print("Done writing")
