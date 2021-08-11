from typing import Dict, List, Any
import pyspark.sql
import datetime
import pyspark.sql.functions as F
from common.etl_job import ETLJob  # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "after_tax_wealth_index"
    business_key = ["day_key", "fund_key"]
    primary_key = {"atax_wlth_idx_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "div": {
            "type": "table",
            "source": "fund_dividend"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "dtr": {
            "type": "table",
            "source": "dividend_tax_rate"
        },
        "fval": {
            "type": "table",
            "source": "fund_valuation"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "cmp": {
            "type": "table",
            "source": "fund_composite"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "div",
            "conditions": [
                F.col("div.reinvest_day_key") == F.col("cal.day_key")]
        },
        {
            "source": "dtr",
            "conditions": [
                F.col("rep.tax_yr") == F.col("cal.yr_nbr")]
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("div.reinvest_day_key"), "target": "reinv_day_key"},
        {"source": F.col("div.fund_key"), "target": "fund_key"},
        {"source": F.col("at_oi"), "target": "atax_ordn_incm"},
        {"source": F.col("at_ltcg"), "target": "atax_ltcg"},
        {"source": F.col("at_stcg"), "target": "atax_stcg"},
        {"source": F.col("at_ftc"), "target": "atax_forgn_tax_cr"},
        {"source": (F.col("at_oi") + F.col("at_stcg") + F.col("at_ltcg") + F.col("at_ftc")), "target": "atax_tot_divd"},
        {"source": F.col("pm_nav"), "target": "prir_mo_pr"},
        {"source": F.col("cm_nav"), "target": "curr_mo_pr"},
        {"source": F.col("div.re_invst_pr"), "target": "reinv_pr"},
        {"source": (((F.col("div.re_invst_pr") + F.col("div.tot_divd")) / F.col("pm_nav")) / (
                    F.col("cm_nav") * F.col("div.re_invst_pr")) - F.lit(1)), "target": "btax_ret"},
        {"source": ((F.col("div.re_invst_pr") + F.col("at_oi") + F.col("at_stcg") + F.col("at_ltcg") + F.col(
            "at_ftc")) / F.col("pm_nav")) * (F.col("cm_nav") / (F.col("div.re_invst_pr")) - F.lit(1)),
         "target": "atax_ret"},
        {"source": (F.col("at_oi") + F.col("at_stcg") + F.col("at_ltcg") + F.col("at_ftc")) / F.col("div.re_invst_pr"),
         "target": "atax_reinv_per_shr"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        p_end_date = self._args["p_end_dt"]
        p_target_flg = self._args["p_target_flg"]

        p_end_date = datetime.datetime.strptime(p_end_date, '%m/%d/%Y')

        #This function calculates t date which is used in below cursors. Its calculated by fund and dividend table joining with calendar to get max date
        def calc_t_date(p_end_date, p_target_flg):
            if p_target_flg == 'Y':
                o_target_flg = '1%'
            else:
                o_target_flg = 'Target%'

            df = inputs["fund"] \
                .join(inputs["cmp"], [F.col("fund.fund_compst_key") == F.col("cmp.fund_compst_key")]) \
                .where(F.col("fund.fund_nbr").isin(2015, 2215) == False) \
                .where(F.col("cmp.asset_cls_nm").like(o_target_flg) == False) \
                .selectExpr("fund.fund_key as fund_cmp_fund_key")

            t_date = inputs["div"] \
                .join(df, [F.col("div.fund_key") == F.col("fund_cmp_fund_key")]) \
                .join(inputs["cal"], [F.col("div.reinvest_day_key") == F.col("cal.day_key")]) \
                .where(F.col("cal.cal_day") <= p_end_date ) \
                .selectExpr("max(cal.cal_day) as max_cal").rdd.max()[0]
            return t_date

        t_date = calc_t_date(p_end_date, p_target_flg)

        #Separate instance of Calendar table for local calculations
        cal_nav = inputs["cal"].selectExpr("cal_day", "cal.day_key as nav_day_key", "yr_nbr as out_yr_nbr",
                                           "mo_ending_dt as out_mo_ending_dt")

        df_cm_nav = inputs["fval"] \
            .join(cal_nav, [F.col("fval.day_key") == F.col("nav_day_key")]) \
            .selectExpr("cal_day as cm_nav_cal_day", "fund_key as cm_nav_fund_key", \
                        "nav_day_key as cm_nav_day_key", "pr_per_shr as cm_nav")

        df_pm_nav = inputs["fval"] \
            .join(cal_nav, [F.col("fval.day_key") == F.col("nav_day_key")]) \
            .selectExpr("cal_day as pm_nav_cal_day", "fund_key as pm_nav_fund_key", \
                        " nav_day_key as pm_nav_day_key", "pr_per_shr as pm_nav")

        df_output = inputs["div"] \
            .join(inputs["cal"], [F.col("div.reinvest_day_key") == F.col("cal.day_key")]) \
            .join(inputs["dtr"], [F.col("dtr.tax_yr") == F.col("cal.yr_nbr")]) \
            .join(df_cm_nav, [F.col("cm_nav_fund_key") == F.col("div.fund_key"),
                              F.col("cm_nav_cal_day") == F.col("cal.mo_ending_dt")]) \
            .join(df_pm_nav, [F.col("pm_nav_fund_key") == F.col("div.fund_key"),
                              F.col("pm_nav_cal_day") == F.col("cal.mo_ending_dt")]) \
            .where(F.col("cal.cal_day").isin(t_date))

        #at_oi column is calculated using conditions which checks qdi amd ord incm amount to calculate
        df_output=df_output.withColumn('at_oi', F.when(F.col('div.qdi') > F.col('div.ordn_incm'), (F.col('div.ordn_incm') * (1 - F.col('dtr.ltcg_rt')))) \
                        .otherwise((F.col('div.ordn_incm') - F.col('div.qdi')) * (1 - F.col('dtr.ordn_incm_rt')) +(F.col('div.qdi') * (1 - F.col('dtr.ltcg_rt')))))

        #at_stcg column is calculated using conditions which checks qdi amd ord incm amount and other columns to calculate
        df_output=df_output.withColumn('at_stcg',\
                    F.when(F.col('div.qdi') > (F.col('div.ordn_incm') + F.col('div.shrt_term_cap_gains')),\
                           F.col('div.shrt_term_cap_gains') * (1 - F.col('dtr.ltcg_rt'))) \
                    .when(F.col('div.qdi') > F.col('div.ordn_incm'), \
                          ((F.col('div.shrt_term_cap_gains') - F.col('div.qdi') - F.col('div.ordn_incm')) *\
                          (1 - F.col('dtr.stcg_rt'))) + (F.col('div.qdi') - F.col('div.ordn_incm')) * (1 - F.col('dtr.ltcg_rt'))) \
                    .otherwise(F.col('div.shrt_term_cap_gains') * (1 - F.col('dtr.stcg_rt'))))

        #at_ftc column is calculated using conditions which checks qdi amd ord incm amount and other columns to calculate
        df_output = df_output.withColumn('at_ftc', F.when(F.col('div.qdi') > (F.col('div.ordn_incm') + F.col('div.shrt_term_cap_gains') + F.col('forgn_tax_cr')),\
                F.col('forgn_tax_cr') * (1 - F.col('dtr.ltcg_rt')))\
                .when(F.col('div.qdi') > (F.col('div.ordn_incm') + F.col('div.shrt_term_cap_gains')),\
                (F.col('forgn_tax_cr') - (F.col('div.qdi') - F.col('div.ordn_incm') - F.col('div.shrt_term_cap_gains'))) * (1 - F.col('dtr.stcg_rt')) +\
                (F.col('div.qdi') - F.col('div.ordn_incm') - F.col('div.shrt_term_cap_gains')) * (1 - F.col('dtr.ltcg_rt')))\
                .otherwise(F.col('forgn_tax_cr') * (1 - F.col('dtr.stcg_rt'))))

        df_output = df_output.withColumn('at_ltcg', (F.col('div.long_term_cap_gains') * (F.lit(4) - F.col('dtr.ltcg_rt')))) \
            .withColumn("curr_mo_pr", F.lit(1)) \
            .select("cm_nav_cal_day", "pm_nav_cal_day", "cal.cal_day", "forgn_tax_cr", "cal.day_key",
                    "cal.mo_ending_dt", "div.fund_key",
                    "div.reinvest_day_key", "div.tot_divd", "div.ordn_incm", "div.shrt_term_cap_gains",
                    "div.long_term_cap_gains", "div.forgn_tax_cr", "div.re_invst_pr", "div.qdi", "at_ltcg","at_stcg",
                    "at_ftc", "tax_yr", "cm_nav", "pm_nav", "at_oi", "curr_mo_pr")

        return df_output
