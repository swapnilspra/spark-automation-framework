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
fund_dividend = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_dividend".format(src_schema))
fund_fund = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_fund".format(src_schema))

# Convert dynamic dataframes to dataframes
fund_dividend_df = fund_dividend.toDF()
fund_fund_df = fund_fund.toDF()

# Convert dataframes to temp views
fund_dividend_df.createOrReplaceTempView("fund_dividend_tbl")
fund_fund_df.createOrReplaceTempView("fund_fund_tbl")

# Dividend Query
dividend_select_df = sqlContext.sql("""
SELECT 
public_fund_nbr AS fund_nbr,
record_date,
reinvest_date,
payable_date,
net_invest_income,
total_gains_short,
total_gains_long,
ord_income,
shrs_of_record,
cap_gains_short,
cap_gains_long,
reinvest_price,
non_taxable_dis,
estimated_dividend,
total_gains_mid,
cap_gains_mid,
foreign_tax_credit,
reinvest_index
FROM
fund_dividend_tbl AS DIV
INNER JOIN fund_fund_tbl AS FND
ON DIV.FND_ID=FND.ID
order by reinvest_date,fund_order
""")

dividend_select_df_scrubbed = dividend_select_df.select(
    col('fund_nbr'),
    dividend_select_df.record_date.cast(DateType()).alias('record_date'),
    dividend_select_df.reinvest_date.cast(DateType()).alias('reinvest_date'),
    dividend_select_df.payable_date.cast(DateType()).alias('payable_date'),
    dividend_select_df.net_invest_income.cast(IntegerType()).alias('net_invest_income'),
    dividend_select_df.total_gains_short.cast(IntegerType()).alias('total_gains_short'),
    dividend_select_df.total_gains_long.cast(IntegerType()).alias('total_gains_long'),
    str_udf('ord_income').alias('ord_income'),
    dividend_select_df.shrs_of_record.cast(IntegerType()).alias('shrs_of_record'),
    dividend_select_df.cap_gains_short.cast(IntegerType()).alias('cap_gains_short'),
    dividend_select_df.cap_gains_long.cast(IntegerType()).alias('cap_gains_long'),
    str_udf('reinvest_price').alias('reinvest_price'),
    dividend_select_df.non_taxable_dis.cast(IntegerType()).alias('non_taxable_dis'),
    col('estimated_dividend'),
    dividend_select_df.total_gains_mid.cast(IntegerType()).alias('total_gains_mid'),
    dividend_select_df.cap_gains_mid.cast(IntegerType()).alias('cap_gains_mid'),
    dividend_select_df.foreign_tax_credit.cast(IntegerType()).alias('foreign_tax_credit'),
    str_udf('reinvest_index').alias('reinvest_index')
)

dividend_select_write = dividend_select_df_scrubbed.repartition(1).sort('reinvest_date').coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

dividend_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=nonsensitive')

print("Done writing")
