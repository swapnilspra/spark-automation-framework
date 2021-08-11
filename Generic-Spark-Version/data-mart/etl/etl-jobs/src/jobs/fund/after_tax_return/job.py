from typing import Dict, List, Any
import pyspark.sql
import datetime
import dateutil.relativedelta
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob  # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "after_tax_return"
    business_key = ["day_key", "fund_key", "per_key"]
    primary_key = {"atax_ret_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "atc": {
            "type": "table",
            "source": "after_tax_calculation"
        },
        "wi": {
            "type": "table",
            "source": "after_tax_wealth_index"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "per": {
            "type": "table",
            "source": "return_periods"
        },
        "dtr": {
            "type": "table",
            "source": "dividend_tax_rate"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "atc",
            "conditions": [
                F.col("atc.day_key") == F.col("wi.day_key"),
                F.col("atc.fund_key") == F.col("wi.fund_key")]
        },
        {
            "source": "wi",
            "conditions": [
                F.col("wi.fund_key") == F.col("atc.fund_key")]
        },
        {
            "source": "cal",
            "conditions": [
                F.col("cal.day_key") == F.col("atc.day_key")]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_key") == F.col("atc.fund_key")
            ]
        },
        {
            "source": "per",
            "conditions": [
                F.col("per.per_key") == F.col("atc.per_key")]
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("per.per_key"), "target": "per_key"},
        {"source": F.col("t_sold_amt"), "target": "sold_amt"},
        {"source": F.col("atc.cb_amt"), "target": "cb_amt"},
        {"source": F.col("t_gls_amt"), "target": "gls_amt"},
        {"source": F.col("t_st_tax"), "target": "st_tax_gls"},
        {"source": F.col("t_lt_tax"), "target": "lt_tax_gls"},
        {"source": F.col("tax_amt"), "target": "tax_amt"},
        {"source": F.col("t_net_proc"), "target": "net_prcd_atax"},
        {"source": F.col("strt_per"), "target": "strt_per"},
        {"source": F.col("t_annl"), "target": "annlzn_fctr"},
        {"source": F.col("t_wo_atax_ret"), "target": "atax_ret_wo_sale"},
        {"source": F.col("t_atax_ret"), "target": "atax_ret"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:
        p_end_date = self._args["p_end_dt"]
        p_end_date = datetime.datetime.strptime(p_end_date, '%m/%d/%Y')
        t_curr_yr_days = (p_end_date - (p_end_date + dateutil.relativedelta.relativedelta(months=-12))).days
        prev_yr_dt = p_end_date + dateutil.relativedelta.relativedelta(months=-12)

        t_init_amt = 1000
        p_year = inputs["cal"] \
            .where(F.col("cal.cal_day").isin(p_end_date)) \
            .select(F.col("cal.yr_nbr")).collect()[0]["yr_nbr"]


        df_joined = super().join(inputs) \
            .where((F.col("cal.cal_day") == F.lit(p_end_date)) & (F.col("atc.tax_yr") == F.lit(p_year)))\
            .select(
            "per.per_key", "fund.fund_key", "fund.perf_incpn_dt", "fund.fund_incpn_dt", "per.per_nm", "per.per_msr",
            "cal.day_key",
            "cal.cal_day", "atc.tax_yr", "atc.cb_amt", "atc.shrs_calc", "wi.curr_mo_pr","wi.reinv_pr") \
            .withColumn('t_annl_yr', F.when(F.col("per.per_nm") == 'INCP', F.floor(
            F.months_between(F.lit(p_end_date), F.to_timestamp(F.col("fund.fund_incpn_dt"))) / 12).cast(
            T.IntegerType())).otherwise(0)) \
            .withColumn('t_days', F.when(F.col("per.per_nm") == 'INCP', F.datediff(F.lit(p_end_date), F.add_months(
            F.col("fund.fund_incpn_dt"), 12)).cast(T.IntegerType())).otherwise(0)) \
            .withColumn('t_annl', F.when(F.col("per.per_nm") == 'INCP',
                                         F.col("t_annl_yr") + (F.col("t_days") / F.lit(t_curr_yr_days))).otherwise(
            F.col("per_msr"))) \
            .withColumn('t_sold_amt', F.col('atc.shrs_calc') * F.col('wi.curr_mo_pr')) \
            .withColumn('t_gls_amt', F.col('t_sold_amt') * F.col('atc.cb_amt')) \
            .withColumn('t_wo_atax_ret',
                        F.pow((F.col('t_sold_amt') / F.lit(t_init_amt)), (F.lit(1) / F.col('t_annl'))) - 1)

        #form division tax rate find latst record for that calendar day
        df_t_rate = inputs["dtr"] \
            .join(inputs["cal"], [F.col('dtr.tax_yr') == F.col('cal.yr_nbr')]) \
            .where(F.col("cal.cal_day").isin(p_end_date)) \
            .select("dtr.ltcg_rt", "dtr.stcg_rt")

        t_ltcg_rt = df_t_rate.collect()[0]["ltcg_rt"]
        t_stcg_rt = df_t_rate.collect()[0]["stcg_rt"]

        #This will calculate shares for previous year
        prev_year_shr = inputs["atc"] \
            .join(inputs["cal"], [F.col('atc.day_key') == F.col('cal.day_key')]) \
            .where(F.col('cal.cal_day') == F.lit(prev_yr_dt)) \
            .selectExpr("atc.fund_key as py_fund_key", "atc.per_key as py_per_key", "atc.tax_yr as py_tax_yr", \
                        "cal.cal_day as py_cal_day", "atc.shrs_calc as py_shrs_calc", "atc.cb_amt as py_cb_amt")

        #using previous year shares values calculated for current
        df_joined = df_joined \
            .join(prev_year_shr, [F.col('fund.fund_key')==F.col('py_fund_key'),F.col('per.per_key') == F.col('py_per_key'),F.col('atc.tax_yr')==F.col('py_tax_yr')])\
            .withColumn('strt_per', F.add_months(F.lit(p_end_date),-12)) \
            .withColumn('t_st_tax', F.when(F.col("py_shrs_calc") > 0, (
                    ((F.col("atc.shrs_calc") - F.col("py_shrs_calc")) * F.col("wi.curr_mo_pr")) - (
                        F.col("atc.cb_amt") - F.col("py_cb_amt"))) * F.lit(t_stcg_rt)).otherwise(F.col('t_gls_amt') * F.lit(t_stcg_rt))) \
            .withColumn('t_lt_tax', F.when(F.col("py_shrs_calc") > 0, (F.col('t_sold_amt')-(((F.col("atc.shrs_calc") - F.col("py_shrs_calc")) * F.col("wi.curr_mo_pr")) - F.col("py_cb_amt"))* F.lit(t_ltcg_rt))).otherwise(0)) \
            .withColumn('t_net_proc', F.col('t_sold_amt') - (F.col('t_st_tax') + F.col('t_lt_tax'))) \
            .withColumn('t_atax_ret', F.pow((F.col('t_net_proc') / F.lit(t_init_amt)), (F.lit(1) / F.col('t_annl'))) - F.lit(1)) \
            .withColumn('tax_amt', F.col('t_st_tax') + F.col('t_lt_tax'))

        return df_joined