import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from decimal import Decimal
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType
from pyspark.sql.types import StringType
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
prf_benchmark_list = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_prf_benchmark_list".format(src_schema))
prf_benchmark_type = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_prf_benchmark_type".format(src_schema))

# Convert dynamic dataframes to dataframes
prf_benchmark_list_df = prf_benchmark_list.toDF()
prf_benchmark_type_df = prf_benchmark_type.toDF()

# Convert dataframes to temp views
prf_benchmark_list_df.createOrReplaceTempView("prf_benchmark_list_tbl")
prf_benchmark_type_df.createOrReplaceTempView("prf_benchmark_type_tbl")

# Benchmark Query
benchmark_select_df = sqlContext.sql("""
SELECT 
bm.id AS bmk_legacy_id,
bm.name AS idx_id,
long_description AS idx_nm,
short_description AS idx_shrt_nm,
df_index_id AS robeco_idx_id,
bloomberg_symbol AS bloomberg_sym,
nvl(bt.id,0) AS idx_type,
bt.description AS idx_type_desc
from prf_benchmark_list_tbl AS bm left outer join prf_benchmark_type_tbl AS bt on nvl(bm.pbt_id,1)=bt.id
order by bmk_legacy_id
""")

benchmark_select_df_scrubbed = benchmark_select_df.select(
  benchmark_select_df.bmk_legacy_id.cast(IntegerType()).alias('bmk_legacy_id'),
  col('idx_id'),
  benchmark_select_df.idx_nm.cast(StringType()).alias('idx_nm'),
  col('idx_shrt_nm'),
  col('robeco_idx_id'),
  col('bloomberg_sym'),
  benchmark_select_df.idx_type.cast(IntegerType()).alias('idx_type'),
  col('idx_type_desc')
)

benchmark_select_write = benchmark_select_df_scrubbed.repartition(1).sort('bmk_legacy_id').coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

benchmark_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=nonsensitive')

print("Done writing")
