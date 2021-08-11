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
prf_benchmark_fund = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_prf_benchmark_fund".format(src_schema))
fnd_fund = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_fund".format(src_schema))
prf_benchmark_list = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_prf_benchmark_list".format(src_schema))
fnd_composite = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_composite".format(src_schema))
prf_application_list = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_prf_application_list".format(src_schema))

# Convert dynamic dataframes to dataframes
prf_benchmark_fund_df = prf_benchmark_fund.toDF()
fnd_fund_df = fnd_fund.toDF()
prf_benchmark_list_df = prf_benchmark_list.toDF()
fnd_composite_df = fnd_composite.toDF()
prf_application_list_df = prf_application_list.toDF()

# Convert dataframes to temp views
prf_benchmark_fund_df.createOrReplaceTempView("prf_benchmark_fund_tbl")
fnd_fund_df.createOrReplaceTempView("fnd_fund_tbl")
prf_benchmark_list_df.createOrReplaceTempView("prf_benchmark_list_tbl")
fnd_composite_df.createOrReplaceTempView("fnd_composite_tbl")
prf_application_list_df.createOrReplaceTempView("prf_application_list_tbl")

# Index Rltn Query
fund_index_rltn_select_df = sqlContext.sql("""
select
b.id AS bmk_legacy_id,
cp.id AS compst_fund_id,
p.NAME AS perf_run_type_cd,
link_type AS link_prio,
primary_benchmark_ind AS prim_bmk_flg,
a.INCLUDE_IN_GR_RETS_RPT_IND AS gr_ret_inlsn_flg,
dense_rank() over (order by cp.composite_order,link_type) AS disp_sort_ord
from
prf_benchmark_fund_tbl AS a,
fnd_fund_tbl AS f,
prf_benchmark_list_tbl AS b,
fnd_composite_tbl AS cp,
prf_application_list_tbl AS p
where a.pbl_id=b.id
and a.fnd_id=f.id
and f.CMP_ID=cp.id
and a.PAL_ID=p.id
""")

fund_index_rltn_df_scrubbed = fund_index_rltn_select_df.select(
  fund_index_rltn_select_df.bmk_legacy_id.cast(IntegerType()).alias('bmk_legacy_id'),
  fund_index_rltn_select_df.compst_fund_id.cast(IntegerType()).alias('compst_fund_id'),
  col('perf_run_type_cd'),
  col('link_prio'),
  col('prim_bmk_flg'),
  col('gr_ret_inlsn_flg'),
  col('disp_sort_ord')
)

fund_index_rltn_select_write = fund_index_rltn_df_scrubbed.repartition(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

fund_index_rltn_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=nonsensitive')

print("Done writing")
