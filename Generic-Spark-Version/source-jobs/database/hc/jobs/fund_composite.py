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
fund_composite = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_composite".format(src_schema))
fund_asset_class = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_asset_class".format(src_schema))
fund_asset_subclass = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_asset_subclass".format(src_schema))
fund_inv_style = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_inv_style".format(src_schema))
fund_performance_type = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_performance_type".format(src_schema))
fund_status_code = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_status_code".format(src_schema))
fund_style = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_style".format(src_schema))
fund_cmp_style = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_cmp_style".format(src_schema))
fund_invstmt_vehicle = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_invstmt_vehicle".format(src_schema))
fund_invstmt_advisor = glueContext.create_dynamic_frame.from_catalog(database = db, table_name = "{}_dbo_fnd_invstmt_advisor".format(src_schema))

# Convert dynamic dataframes to dataframes
fund_composite_df = fund_composite.toDF()
fund_asset_class_df = fund_asset_class.toDF()
fund_asset_subclass_df = fund_asset_subclass.toDF()
fund_inv_style_df = fund_inv_style.toDF()
fund_performance_type_df = fund_performance_type.toDF()
fund_status_code_df = fund_status_code.toDF()
fund_style_df = fund_style.toDF()
fund_cmp_style_df = fund_cmp_style.toDF()
fund_invstmt_vehicle_df = fund_invstmt_vehicle.toDF()
fund_invstmt_advisor_df = fund_invstmt_advisor.toDF()

# Convert dataframes to temp views
fund_composite_df.createOrReplaceTempView("fund_composite_tbl")
fund_asset_class_df.createOrReplaceTempView("fund_asset_class_tbl")
fund_asset_subclass_df.createOrReplaceTempView("fund_asset_subclass_tbl")
fund_inv_style_df.createOrReplaceTempView("fund_inv_style_tbl")
fund_performance_type_df.createOrReplaceTempView("fund_performance_type_tbl")
fund_status_code_df.createOrReplaceTempView("fund_status_code_tbl")
fund_style_df.createOrReplaceTempView("fund_style_tbl")
fund_cmp_style_df.createOrReplaceTempView("fund_cmp_style_tbl")
fund_invstmt_vehicle_df.createOrReplaceTempView("fund_invstmt_vehicle_tbl")
fund_invstmt_advisor_df.createOrReplaceTempView("fund_invstmt_advisor_tbl")

# Composite Fund Quary
fund_composite_select_df = sqlContext.sql("""
select a.id as compst_fund_id, 
	ac.name as asset_cls_nm,
	ac.DESCRIPTION as asset_cls_desc,
	sc.name as sub_cls_nm,
	sc.DESCRIPTION as sub_cls_desc,
	fs.STYLE_DESCRIPTION as styl_nm,
	fs.STYLE_DESCRIPTION as styl_desc,
	a.DESCRIPTION as compst_nm,
	pt.NAME as perf_cd ,
	pt.description as perf_nm,
	composite_order as compst_ord,
	st.NAME as compst_stat_cd,
	initcap(st.description) as compst_stat_desc ,
	fst.name as fund_styl,
	a.risk_return_order as risk_ret_ord,
	a.compst_incpn_dt as compst_incpn_dt,
	a.sales_cat,
	a.st_str_fund_nbr,
	veh.VEH_NM,
	adv.ADV_NM,
	product_nm,
	product_rltn,
	a.name as compst_src_id
from 
fund_composite_tbl AS a inner join fund_asset_class_tbl AS ac on a.asc_id=ac.id
inner join fund_asset_subclass_tbl AS sc on a.asb_id=sc.id
inner join fund_inv_style_tbl AS fs on a.inv_style_id=fs.inv_style_id
inner join fund_performance_type_tbl AS pt  on a.pft_id=pt.id
inner join fund_status_code_tbl AS st on a.stc_id=st.id
left outer join fund_cmp_style_tbl AS fcs on  a.id=fcs.CMP_ID
left outer join fund_style_tbl AS fst on fcs.STY_ID=fst.id
left outer join fund_invstmt_vehicle_tbl AS veh on a.INVMT_VEH_ID = veh.INVMT_VEH_ID
left outer join fund_invstmt_advisor_tbl AS adv on a.INVMT_ADV_ID = adv.INVMT_ADV_ID
""")

fund_composite_select_df_scrubbed = fund_composite_select_df.select(
	fund_composite_select_df.compst_fund_id.cast(IntegerType()).alias('compst_fund_id'),
	col('asset_cls_nm'),
	col('asset_cls_desc'),
	col('sub_cls_nm'),
	col('sub_cls_desc'),
	col('styl_nm'),
	col('styl_desc'),
	col('compst_nm'),
	col('perf_cd'),
	col('perf_nm'),
	fund_composite_select_df.compst_ord.cast(IntegerType()).alias('compst_ord'),
	col('compst_stat_cd'),
	col('compst_stat_desc'),
	col('fund_styl'),
	fund_composite_select_df.risk_ret_ord.cast(IntegerType()).alias('risk_ret_ord'),
	fund_composite_select_df.compst_incpn_dt.cast(DateType()).alias('compst_incpn_dt'),
	col('sales_cat'),
	col('st_str_fund_nbr'),
	col('VEH_NM'),
	col('ADV_NM'),
	col('product_nm'),
	col('product_rltn'),
	fund_composite_select_df.compst_src_id.cast(IntegerType()).alias('compst_src_id')
)

fund_composite_select_write = fund_composite_select_df_scrubbed.repartition(1).sort('compst_ord').coalesce(1)

tmp_uri = get_s3_tmp(bucket_name, tmp_path)

fund_composite_select_write.write.option('header', 'true').save(tmp_uri, format='csv')

move_to_incoming(tmp_uri, dest_path, 'hca:dataclassification=nonsensitive')

print("Done writing")
