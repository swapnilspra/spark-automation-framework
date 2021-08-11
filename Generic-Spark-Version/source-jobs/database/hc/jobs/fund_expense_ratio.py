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
from pyspark.sql.functions import udf
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

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def cast_string(x):
# check if x is None or empty string: return whichever it is
    if not x and x != 0:
        return x
# check if x comes in like 0E-10
    elif x == 0:
        return f'{0:.12f}'
# convert x to a "decimal string"
    elif is_number(x) and x != 0:
        return f'{x:.12f}'
    else:
        return x

str_udf = udf(lambda z: cast_string(z), StringType())

#Table(s)
actexp_fund = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_actexp_fund".format(src_schema))
fund_fund = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_fund".format(src_schema))
actexp_ratios = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_actexp_ratios".format(src_schema))
actexp_ref_ratio = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_actexp_ref_ratio".format(src_schema))

# Convert dynamic dataframes to dataframes
actexp_fund_df = actexp_fund.toDF()
fund_fund_df = fund_fund.toDF()
actexp_ratios_df = actexp_ratios.toDF()
actexp_ref_ratio_df = actexp_ref_ratio.toDF()

# Convert dataframes to temp views
actexp_fund_df.createOrReplaceTempView("actexp_fund_tbl")
fund_fund_df.createOrReplaceTempView("fund_fund_tbl")
actexp_ratios_df.createOrReplaceTempView("actexp_ratios_tbl")
actexp_ref_ratio_df.createOrReplaceTempView("actexp_ref_ratio_tbl")

# Fund Expense Ratio Query
fund_expense_ratio_select_df = sqlContext.sql("""
select
fnd.fund_nbr,
exp.period_start_date AS efftv_strt_dt,
exp.period_end_date AS efftv_end_dt,
exp_ratio_gross AS gr_expns_ratio,
exp_ratio_net,
exp_waived AS expns_wavr_ratio,
cayman_ratio,
period_type AS per_flg,
final_flag AS manual_entry_flg,
rts.ratio_value AS pass_thru_mgmt_fee_wavr
from
actexp_fund_tbl AS exp
inner join fund_fund_tbl AS fnd
on exp.FND_ID=fnd.id
left outer join
	(SELECT R.*
        FROM actexp_ratios_tbl R inner join actexp_ref_ratio_tbl B
        on R.REF_RATIO_KEY = B.REF_RATIO_KEY
        where B.DISPLAY_NAME = 'Pass-Through Management Fee Waiver') rts on exp.ACTEXP_FUND_KEY = rts.ACTEXP_FUND_KEY
order by period_start_date,period_end_date,fund_order
""")

fund_expense_ratio_select_df_scrubbed = fund_expense_ratio_select_df.select(
	fund_expense_ratio_select_df.fund_nbr.cast(IntegerType()).alias('fund_nbr'),
	fund_expense_ratio_select_df.efftv_strt_dt.cast(DateType()).alias('efftv_strt_dt'),
	fund_expense_ratio_select_df.efftv_end_dt.cast(DateType()).alias('efftv_end_dt'),
	str_udf('gr_expns_ratio').alias('gr_expns_ratio'),
	str_udf('exp_ratio_net').alias('exp_ratio_net'),
	str_udf('expns_wavr_ratio').alias('expns_wavr_ratio'),
	str_udf('cayman_ratio').alias('cayman_ratio'),
	col('per_flg'),
	col('manual_entry_flg'),
	str_udf('pass_thru_mgmt_fee_wavr').alias('pass_thru_mgmt_fee_wavr')
)

cols = ["efftv_strt_dt","efftv_end_dt","fund_nbr"]

fund_expense_ratio_select_write = fund_expense_ratio_select_df_scrubbed.repartition(1).orderBy(cols).coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

fund_expense_ratio_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=nonsensitive')

print("Done writing")
