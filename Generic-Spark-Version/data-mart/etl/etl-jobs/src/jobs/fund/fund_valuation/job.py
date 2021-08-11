from pyspark.sql.types import ArrayType, StringType, MapType, StructField, StructType, FloatType
from typing import Dict, List, Any
import math
import logging
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime
class Job(ETLJob):
    target_table = "fund_valuation"
    business_key = ["fund_key","day_key"]
    primary_key = {"fund_vltn_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "ssbnav": {
            "type": "file",
            "source": "ssbnav"
        },
        "fund_dividend": {
            "type": "table",
            "source": "fund_dividend"
        },
        "fund_composite": {
            "type": "table",
            "source": "fund_composite"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "previous_day_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "previous_day_valuation": {
            "type": "table",
            "source": "fund_valuation"
        },
        "previous_month_end_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "previous_month_end_valuation": {
            "type": "table",
            "source": "fund_valuation"
        },
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "ssbnav"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.when(
                    F.trim(F.col("ssbnav.fund")).cast("int")<F.lit(2000), # handle old legacy fund number
                    F.lit(2000)+F.trim(F.col("ssbnav.fund")).cast("int")
                ).otherwise(F.trim(F.col("ssbnav.fund")).cast("int"))
            ]
        },
        {
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("calendar.cal_day")) == F.to_date(F.col("ssbnav.price_eff_date")) 
            ],
        },
        {
            "source": "fund_dividend",
            "conditions": [
                F.col("fund.fund_key") == F.col("fund_dividend.fund_key"),
                F.col("calendar.day_key")==F.col("fund_dividend.reinvest_day_key")
            ],
            "type":"left_outer"
        },
        {
            "source": "fund_composite",
            "conditions": [
                F.col("fund_composite.st_str_fund_nbr") == F.col("fund.st_str_fund_nbr")
            ]
        },
        # load previous day valuation. this is in two parts - join with calendar to lookup the day key then join by day key
        {
            "source": "previous_day_calendar",
            "conditions": [
                F.col("previous_day_calendar.cal_day")==F.to_date(F.date_sub(F.col("ssbnav.price_eff_date"),1))
            ]
        },
        {
            "source": "previous_day_valuation",
            "conditions": [
                F.col("previous_day_calendar.day_key")==F.col("previous_day_valuation.day_key"),
                F.col("fund.fund_key")==F.col("previous_day_valuation.fund_key")
            ],
            "type":"left_outer"
        },
        # load previous end of month valuation. this is in two parts - join with calendar to lookup the day key then join by day key
        {
            "source": "previous_month_end_calendar",
            "conditions": [
                F.col("previous_month_end_calendar.cal_day")==F.to_date(F.date_sub(F.trunc(F.col("ssbnav.price_eff_date"),"month"),1))
            ]
        },
        {
            "source": "previous_month_end_valuation",
            "conditions": [
                F.col("previous_month_end_calendar.day_key")==F.col("previous_month_end_valuation.day_key"),
                F.col("fund.fund_key")==F.col("previous_month_end_valuation.fund_key")
            ],
            "type":"left_outer"
        },
    ]
    target_mappings:List[Dict[str,Any]] = [

        { "source": F.col("fund.fund_key"), "target": "fund_key" },
        { "source": F.col("calendar.day_key"), "target": "day_key" },
        { "source": F.trim(F.col("ssbnav.total_net_assets")).cast("double"), "target": "nav_amt" },
        { "source": F.trim(F.col("ssbnav.price")).cast("double"), "target": "pr_per_shr" },
        { "source": F.trim(F.col("ssbnav.price")).cast("double"), "target": "asof_pr" },
        { "source": F.trim(F.col("ssbnav.shares_outstndg")).cast("double"), "target": "tot_shrs" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" },
        { "source": F.col("calculated_daily_pr_diff"), "target":"daily_pr_diff" },
        { "source": F.col("calculated_wlth_idx"), "target":"wlth_idx" }, 
        { "source": F.col("calculated_wlth_idx"), "target":"asof_wlth_idx" }, 
        { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key" },        
        { "source": F.lit(None).cast("int"), "target": "hsg_pr_revs" },        
    ]


    # override transform to add forward filling where needed
    def transform(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        # add the money_market_activity - for Performance Code = ‘INTRTE’ fund_composite.perf_cd
        # we load money_market_activity and join with calendar to get the day_key back to a date
        df_money_market_activity = common.utils.read_table_snapshot(
                    table_name="money_market_activity",
                    env=self._env,
                    spark=self._spark)
        df_money_market_calendar = common.utils.read_table_snapshot(
                    table_name="calendar",
                    env=self._env,
                    spark=self._spark)
        df_money_market_activity = df_money_market_activity.alias("money_market_activity").join(df_money_market_calendar.alias("money_market_calendar"),
            [
                F.col("money_market_calendar.day_key") == F.col("money_market_activity.day_key"),
            ]).select("money_market_activity.fund_key","money_market_calendar.cal_day","money_market_activity.intr_w_gain_loss")

        # d = Accrued interest from beginning of the Month until today
        #     (Use SUM(money_market_activity. intr_w_gain_loss) First day of month till today) 
        df_money_market_activity = df_money_market_activity.join(df_joined,
            [
                F.col("fund.fund_key") == F.col("money_market_activity.fund_key"),
                (F.to_date(F.col("money_market_calendar.cal_day")) >= F.trunc(F.col("ssbnav.price_eff_date"),"month")) & (F.col("money_market_calendar.cal_day")<=F.col("ssbnav.price_eff_date")),
            ])
        df_money_market_activity = df_money_market_activity.groupBy("fund.fund_key","ssbnav.price_eff_date").agg(F.sum(F.col("intr_w_gain_loss")).alias("total_intr_w_gain_loss"))

        # join our new column into the joined DF
        df_joined = df_joined.join(df_money_market_activity.alias("money_market_activity"),
            [
                F.col("money_market_activity.fund_key")==F.col("fund.fund_key"),
                F.col("money_market_activity.price_eff_date")==F.col("ssbnav.price_eff_date")
            ],
            how='leftouter'
        )

        df_transformed = self.calculate_wealth_index(df_joined)
        
        df_transformed = super().transform(df_transformed)

        # forward fill
        df_calendar = common.utils.read_table_snapshot(
                    table_name="calendar",
                    env=self._env,
                    spark=self._spark)
        df_ffill = common.utils.forward_fill(df_calendar,df_transformed,"day_key",
            override_values={"daily_pr_diff":F.lit(0)})

        return df_ffill

    def calculate_wealth_index(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        #
        # calculate daily_pr_diff
        #
        # if no previous day exists, should return 0
        df_transformed = df_joined
        
        df_transformed = df_transformed.withColumn(
            "calculated_daily_pr_diff", 
            F.trim(F.col("ssbnav.price")).cast("double")-
            F.coalesce(F.col("previous_day_valuation.pr_per_shr").cast("double"),F.col("ssbnav.price").cast("double"))
        )

        # For Performance Code = ‘PRCDIV’ (from fund_composite.perf_cd)
        # r = Daily Return
        # c = Current Day NAV 
        # p = Previous Day NAV 
        # d = Dividend for the day - fund_dividend.tot_divd for TODAY (this can be null)
        # w = Wealth Index for current day 
        # wp = wealth Index for previous day 
        # r = (c + d) / p - 1 
        # w = 1 * wp * (1 + r)

        # For Performance Code = ‘INTRTE’ (from fund_composite.perf_cd)
        # r = Daily Return 
        # c = Current Day NAV 
        # p = Previous Day NAV 
        # d = Accrued interest from beginning of the Month until today
        #     (Use SUM(money_market_activity. intr_w_gain_loss) First day of month till today) 
        # w = Wealth Index for current day 
        # wp = wealth Index for the last day of previous month 
        # r = (c + d) / p - 1 
        # w = 1 * wp * (1 + r)

        # daily return - if no previous day exists, return 0
        #TODO: what is value if not perf_cd of these two values? 0? -1?
        df_transformed = df_transformed.withColumn(
            "calculated_daily_return", 
            F.when(F.isnull(F.col("previous_day_valuation.pr_per_shr")),F.lit(0)).otherwise(
                F.when(F.lower(F.col("fund_composite.perf_cd"))=="prcdiv",
                    # PRCDIV calculation
                    (F.trim(F.col("ssbnav.price")).cast("double")+
                    F.coalesce(F.col("fund_dividend.ordn_incm"),F.lit(0))+
                    F.coalesce(F.col("fund_dividend.shrt_term_cap_gains"),F.lit(0))+
                    F.coalesce(F.col("fund_dividend.long_term_cap_gains"),F.lit(0))+
                    F.coalesce(F.col("fund_dividend.non_txbl_discnt"),F.lit(0)) ) /
                    F.coalesce(F.col("previous_day_valuation.pr_per_shr").cast("double"),F.lit(1)) - F.lit(1)).\
                when(F.lower(F.col("fund_composite.perf_cd"))=="intrte",
                    # INTRTE calculation
                    (F.trim(F.col("ssbnav.price")).cast("double")+F.coalesce(F.col("total_intr_w_gain_loss"),F.lit(0)) )/
                    F.coalesce(F.col("previous_day_valuation.pr_per_shr").cast("double"),F.lit(1)) - F.lit(1)).\
                otherwise(F.lit(-1))
            )
        )

        # wealth index - if no previous day exists, return 1
        df_transformed = df_transformed.withColumn(
            "calculated_wlth_idx", 
            F.when(F.lower(F.col("fund_composite.perf_cd"))=="prcdiv",
                F.coalesce(F.col("previous_day_valuation.wlth_idx").cast("double"),F.lit(1))).\
            when(F.lower(F.col("fund_composite.perf_cd"))=="intrte",
                F.coalesce(F.col("previous_month_end_valuation.wlth_idx").cast("double"),F.lit(1))).\
            otherwise(F.lit(-1))
            *
            (F.lit(1)+F.col("calculated_daily_return").cast("double"))
        )

        return df_transformed

