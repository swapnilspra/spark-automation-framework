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
fnd_performance_type = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_performance_type".format(src_schema))

# Convert dynamic dataframes to dataframes
fnd_performance_type_df = fnd_performance_type.toDF()

# Convert dataframes to temp views
fnd_performance_type_df.createOrReplaceTempView("fnd_performance_type_tbl")

# Performance Type Query
fnd_performance_type_select_df = sqlContext.sql("""
select 
name AS run_type_cd,
description AS run_type_desc
from fnd_performance_type_tbl
""")

fnd_performance_type_select_write = fnd_performance_type_select_df.repartition(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

fnd_performance_type_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=nonsensitive')

print("Done writing")
