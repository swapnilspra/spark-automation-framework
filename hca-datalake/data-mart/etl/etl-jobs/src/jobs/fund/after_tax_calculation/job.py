from typing import Dict, List, Any
import pyspark.sql
from common.etl_job import ETLJob  # must be imported after spark has been set up
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import Window
from pyspark.sql import Row
import datetime



class Job(ETLJob):
    target_table = "after_tax_calculation"
    business_key = ["tax_yr", "day_key", "fund_key", "per_key"]
    primary_key = {"atax_calc_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "atwi": {
            "type": "table",
            "source": "after_tax_wealth_index"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "rp": {
            "type": "table",
            "source": "return_periods"
        },
        "fdiv": {
            "type": "table",
            "source": "fund_dividend"
        },
        "cmp": {
            "type": "table",
            "source": "fund_composite"
        }

    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "atwi"
        },
        {
            "source": "cal",
            "conditions": [
                F.col("atwi.day_key") == F.col("cal.day_key")]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("atwi.fund_key") == F.col("fund.fund_key")]
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("day_key"), "target": "day_key"},
        {"source": F.col("reinv_day_key"), "target": "reinv_day_key"},
        {"source": F.col("fund_key"), "target": "fund_key"},
        {"source": F.col("per_key"), "target": "per_key"},
        {"source": F.col("tax_yr"), "target": "tax_yr"},
        {"source": F.col("t_cost_basis"), "target": "cb_amt"},
        {"source": F.col("t_shares"), "target": "shrs_calc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        p_end_date = self._args["p_end_dt"]
        p_target_flg=self._args["p_target_flg"]
       # p_end_date = '2019-12-31 00:00:00'
        p_end_date = datetime.datetime.strptime(p_end_date, '%m/%d/%Y')
        #p_target_flg = 'Y'

        df_joined = super().join(inputs)   # After_tax_wealth_index, Fund, Calendar

        df_joined = df_joined.select("atax_wlth_idx_key", "cal_day", "atwi.fund_key", "cal.day_key", "perf_incpn_dt",
                                     "fund_sort_ord", "atwi.atax_tot_divd", "atwi.day_key", "atwi.reinv_day_key",
                                     "atwi.curr_mo_pr", "atwi.atax_reinv_per_shr")

        # Function to calculate t_date, tax_yr
        def calc_date(p_end_date, p_target_flg):
            if p_target_flg == 'Y':
                o_target_flg = '1%'
            else:
                o_target_flg = 'Target%'

            df = inputs["fund"] \
                .join(inputs["cmp"], [F.col("fund.fund_compst_key") == F.col("cmp.fund_compst_key")]) \
                .where(F.col("fund.fund_nbr").isin(2015, 2215) == False) \
                .where(F.col("cmp.asset_cls_nm").like(o_target_flg) == False) \
                .selectExpr("fund.fund_key as fund_cmp_fund_key")

            t_date,t_yr_nbr = inputs["fdiv"] \
                .join(df, [F.col("fdiv.fund_key") == F.col("fund_cmp_fund_key")]) \
                .join(inputs["cal"], [F.col("fdiv.reinvest_day_key") == F.col("cal.day_key")]) \
                .where(F.col("cal.cal_day") < p_end_date ) \
                .selectExpr("last_day(max(cal.cal_day)) as max_cal", "max(yr_nbr) as yr_nbr").rdd.max()
            return t_date, t_yr_nbr

        t_date = calc_date(p_end_date, p_target_flg)[0]
        t_yr_nbr = calc_date(p_end_date, p_target_flg)[1]

        # t_first_div_dt used to calculate t_shares
        f_div_date = inputs["fdiv"] \
            .join(inputs["cal"], [F.col("reinvest_day_key") == F.col("cal.day_key")]) \
            .where(F.col("cal.cal_day") < p_end_date). \
            groupBy("fdiv.fund_key").agg(F.min(F.col("cal.cal_day")).alias('t_first_div_dt')).alias("fdiv")        

        df_periods = inputs["rp"]\
            .select("per_key","per_nm","annl_fctr","per_sort_ord")\
            .where(F.col("per_nm").isin('YR1', 'YR5', 'YR10', 'INCP'))\
            .orderBy("PER_SORT_ORD").alias("rp")

        dffundcur = df_joined.where(F.col("cal.cal_day") == p_end_date).\
            groupBy("atwi.fund_key","fund.perf_incpn_dt","fund.fund_sort_ord").\
            agg({"atwi.fund_key": "count"}).orderBy("fund.fund_sort_ord") \
            .withColumn("tax_yr", F.lit(t_yr_nbr))\
            .withColumn("t_derived_incp", F.add_months(F.last_day(F.col('fund.perf_INCPN_DT')), -1))\
            .join(f_div_date,[F.col("fdiv.fund_key") == F.col("atwi.FUND_KEY")])\
            .withColumn("t_annl_yr",F.floor(F.months_between(F.to_date(F.lit(t_date)),
                                                             F.to_date(F.col("t_derived_incp"))) / 12))\
            .withColumn("fl", (F.floor(F.months_between(F.lit(t_date), F.col("t_derived_incp")) / 12) * 12)) \
            .withColumn("t_days", F.datediff(F.lit(t_date), F.date_sub(F.expr("add_months(t_derived_incp,fl)"), 1)))\
            .withColumn("t_curr_yr_days", F.datediff(F.to_date(F.lit(t_date)), F.add_months(F.lit(t_date), -12)))\
            .withColumn("t_annl", F.col("t_annl_yr") + (F.col("t_days")/F.col("t_curr_yr_days")))\
            .withColumn("t_incp_flg", F.when(F.col("t_annl_yr") < 10, F.lit("Y")).otherwise(F.lit("N")))\
            .join(df_periods,[F.col("rp.annl_fctr") <= F.col("t_annl_yr")])\
            .withColumn("t_init_date", F.when((F.col("rp.per_nm") == F.lit('INCP')) &
                                              (F.col("t_incp_flg") == F.lit('Y')),
                                              F.date_add(F.col("t_derived_incp"), 1))
                        .otherwise(F.expr("date_add(add_months(to_date('"+str(t_date) +
                                          "'),(12 * floor(rp.annl_fctr) * -1)), 1)")))\
            .withColumn("t_loop_yr", F.when((F.col("per_nm") == 'INCP') & (F.col("t_incp_flg") == 'Y'),
                                            F.col("t_annl_yr"))
                        .otherwise(F.col("rp.annl_fctr")))

        # dividend cursor
        dfdiv = df_joined.select("atwi.atax_tot_divd","atwi.day_key","atwi.reinv_day_key","atwi.fund_key",
                                 "atwi.curr_mo_pr", "atwi.atax_reinv_per_shr","cal.cal_day")\
            .orderBy("cal.cal_day").alias("dfdiv")

        dfout = dffundcur.join(dfdiv,"fund_key")\
            .where(F.col("dfdiv.cal_day").between(F.col("t_init_date"), F.lit(p_end_date)))\
            .select("tax_yr","dfdiv.atax_reinv_per_shr","dfdiv.atax_tot_divd","dfdiv.CAL_DAY","fdiv.fund_key",
                    "dfdiv.REINV_DAY_KEY", "dfdiv.DAY_KEY","rp.per_key","t_first_div_dt","curr_mo_pr","annl_fctr",
                    "t_annl_yr","per_key","t_init_date", "per_nm","t_incp_flg","t_derived_incp")

        window = Window.partitionBy(F.col("fdiv.fund_key"), F.col("rp.per_key")).orderBy("dfdiv.cal_day")

        dfrow = dfout.withColumn("row_num", F.row_number().over(window)) \
            .select("dfdiv.cal_day", "dfdiv.reinv_day_key", "dfdiv.day_key", "fdiv.fund_key", "rp.per_key",
                    "row_num", "dfdiv.atax_reinv_per_shr", "dfdiv.atax_tot_divd", "t_first_div_dt", "curr_mo_pr",
                    "annl_fctr", "tax_yr", "t_annl_yr", "per_key", "t_init_date", "per_nm", "t_incp_flg",
                    "t_derived_incp")

        # get the previous year's price.
        max_cal_df = df_joined.select(F.col("atwi.fund_key"), F.col("cal.cal_day"),
                                     F.col("curr_mo_pr").alias("lkp_curr_mo_pr")).alias("lookup")

        dfmain = dfrow.select(F.col("fdiv.fund_key"), F.col("dfdiv.cal_day")).alias("main")

        dfmclkp = dfmain.join(max_cal_df, [F.col("main.fund_key") == F.col("lookup.fund_key")]) \
                .where(F.col("lookup.cal_day") < F.col("main.cal_day")) \
                .groupBy("main.fund_key", "main.cal_day").agg(F.max(F.col("lookup.cal_day")).alias("max_cal"))\
                .alias("dfmclkp")

        dfinal = dfrow.join(dfmclkp, [(F.col("fdiv.fund_key") == F.col("dfmclkp.fund_key")) &
                                      (F.col("dfdiv.CAL_DAY") == F.col("dfmclkp.CAL_DAY"))], how="left") \
            .join(max_cal_df, [(F.col("lookup.fund_key") == F.col("fdiv.fund_key")) &
                               (F.col("lookup.cal_day") == F.col("dfmclkp.max_cal"))], how="left") \
            .select("dfdiv.cal_day", "dfdiv.reinv_day_key", "dfdiv.day_key", "fdiv.fund_key", "rp.per_key",
                    "row_num", "dfdiv.atax_reinv_per_shr", "dfdiv.atax_tot_divd", "t_first_div_dt", "curr_mo_pr",
                    "annl_fctr", "tax_yr", "t_annl_yr", "per_key", "t_init_date", "per_nm", "t_incp_flg",
                    "t_derived_incp", "max_cal", "lkp_curr_mo_pr") \
            .withColumn("t_shares", (F.when(((F.col("t_annl_yr") == F.col("annl_fctr")) | (F.col("annl_fctr") == 0))
                                            & (F.col("t_first_div_dt") > F.col("t_init_date")), F.lit(100))
                        .otherwise(F.lit(1000) / F.col("lkp_curr_mo_pr"))).cast("Float"))\
            .withColumn("t_shares_1st",
                        F.when(F.col("row_num") == 1,
                               ((F.col("atax_reinv_per_shr").cast("Float") * F.col("t_shares")) + F.col("t_shares")))
                        .otherwise(F.lit(0))) \
            .withColumn("t_cost_basis_1st",
                        F.when(F.col("row_num") == 1,
                               (F.lit(1000) + (F.col("atax_tot_divd").cast("Float") * F.col("t_shares"))))
                        .otherwise(F.lit(0))).alias("dfout")
        ouput = dfinal\
            .select("dfout.t_shares","dfout.row_num", "dfout.per_key", "dfout.cal_day", "dfout.reinv_day_key",
                    "dfout.day_key", "dfout.fund_key", "t_shares_1st", "t_cost_basis_1st", "atax_tot_divd",
                    "atax_reinv_per_shr", "dfout.tax_yr") \
            .orderBy("dfout.fund_key", "dfout.per_key", "dfout.row_num")

#we have to collect and iterate as values are depending on previous calculations and windows functions of Spark didnt seem to handle this.
#These were only 1600 records so collect should not cause issue

        outdata = ouput.rdd.collect()
        l = []
        for i in outdata:
            if i.row_num == 1:
                if i.t_cost_basis_1st is not None or i.t_shares_1st is not None :
                    t_cost_basis = float(i.t_cost_basis_1st)
                    t_shares = float(i.t_shares_1st)
                else:
                    t_cost_basis = i.t_cost_basis_1st
                    t_shares = i.t_shares_1st
            else:
                if t_cost_basis is not None or t_shares is not None:
                    t_cost_basis = t_cost_basis + float(i.atax_tot_divd) * t_shares
                    t_shares = (float(i.atax_reinv_per_shr) * t_shares) + t_shares
                else:
                    t_cost_basis = i.t_cost_basis_1st
                    t_shares = i.t_shares_1st

            row = Row(i.tax_yr, t_cost_basis, t_shares, i.fund_key, i.per_key, i.reinv_day_key, i.day_key)
            l.append(row)
        spark = self._spark

        outrdd = spark.sparkContext.parallelize(l)

        schema = StructType([
            StructField('tax_yr', StringType(), True),
            StructField('t_cost_basis', StringType(), True),
            StructField('t_shares', StringType(), True),
            StructField('fund_key', StringType(), True),
            StructField('per_key', StringType(), True),
            StructField('reinv_day_key', StringType(), True),
            StructField('day_key', StringType(), True)
        ])
        df_output = spark.createDataFrame(outrdd, schema)
        df_output = df_output\
            .selectExpr("fund_key", "per_key", "reinv_day_key","day_key","t_cost_basis", "t_shares","tax_yr")

        return df_output
