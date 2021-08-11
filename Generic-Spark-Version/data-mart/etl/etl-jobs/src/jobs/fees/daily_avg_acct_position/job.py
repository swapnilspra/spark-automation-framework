from typing import Dict, List, Any
from datetime import datetime,timedelta
import pyspark.sql
from datetime import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob  # must be imported after spark has been set up

from pyspark.sql import Row,Window
from collections import namedtuple

class Job(ETLJob):
    target_table = "daily_avg_acct_position"
    business_key = ["acct_key", "day_key", "dlr_key", "fund_key", "tpa_key"]
    primary_key = {"daily_avg_acct_posn_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "account_position": {
            "type": "table",
            "source": "account_position"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "tpa": {
            "type": "table",
            "source": "dealer"
        },
        "account": {
            "type": "table",
            "source": "account"
        },
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "account_position"
        },
        {
            "source": "account",
            "conditions": [
                F.col("account.acct_key")==F.col("account_position.acct_key")
            ]
        },
        {
            "source": "dealer",
            "conditions": [
                F.col("account.dlr_id") == F.col("dealer.dlr_id")
            ],
            "type": "left_outer"
        },
        {
            "source": "tpa",
            "conditions": [
                F.col("account.tpa_id") == F.col("tpa.dlr_id")
            ],
            "type": "left_outer"
        },
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("last_day_key"), "target": "day_key"},
        {"source": F.col("asof_dlr_key"), "target": "dlr_key"},
        {"source": F.col("fund_key"), "target": "fund_key"},
        {"source": F.col("acct_key"), "target": "acct_key"},
        {"source": F.col("avg_daily_shrs"), "target": "avg_daily_shrs"},
        {"source": F.col("avg_daily_bal"), "target": "avg_daily_bal"},
        {"source": F.col("tpa.dlr_key"), "target": "tpa_key"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]


    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:
        if not self._args.get("p_date",None):
            # default value is first day of the current month
            p_date = datetime.today().replace(day=1)
        else:
            p_date = datetime.strptime(self._args["p_date"],"%m/%d/%Y")

        # perform regular joins
        df_joined = super().join(inputs)

        #
        # get the dealer keys as of the third business day of the next month
        #
        # first, get the day_key of the third business day
        third_business_day_key = inputs["calendar"].filter(
            (F.col("bus_day_flg")==F.lit("Y")) &
            (F.col("cal_day").between(F.lit(p_date),F.lit(p_date+timedelta(days=10))))
        ).orderBy("cal_day").collect()[2]["day_key"]

        # filter account positions in the third business day
        ap_asof_third_day = inputs["account_position"].filter(F.col("day_key")==F.lit(third_business_day_key))
        ap_asof_third_day = ap_asof_third_day.alias("ap_asof_third_day")
        # join ap_asof_third_day to account_position
        df_joined = df_joined.join(ap_asof_third_day,[
            F.col("account_position.acct_key")==F.col("ap_asof_third_day.acct_key"),
            F.col("account_position.fund_key")==F.col("ap_asof_third_day.fund_key")
        ],how="leftouter")
        # create a weighted vector of the business days of previous month for computing averages
        business_day_metadata = get_business_day_weights(self._spark,inputs["calendar"],p_date-timedelta(days=1))

        df_account_position = df_joined.filter(F.col("account_position.tot_shrs")>0)

        # join with vector of business days and weights. this filters out only month's business days and provides the weight for each day
        df_account_position_month = df_account_position.join(business_day_metadata.business_day_weights,F.col("weights.day_key")==F.col("account_position.day_key"))

        # note: we cast and use decimal to avoid precision loss 
        df_result = df_account_position_month.groupBy(
            F.coalesce(F.col("ap_asof_third_day.dlr_key"),F.col("dealer.dlr_key")).alias("asof_dlr_key"), # if we don't have a dealer as of third business day, take dealer registered on account
            "tpa.dlr_key",
            "account_position.fund_key",
            "account_position.acct_key"
        ).agg(
            (F.sum(F.col("account_position.tot_shrs")*F.col("weights.weight"))/F.lit(business_day_metadata.month_days_count).cast("decimal")).alias("avg_daily_shrs"),
            (F.sum(F.col("account_position.tot_bal_amt")*F.col("weights.weight"))/F.lit(business_day_metadata.month_days_count).cast("decimal")).alias("avg_daily_bal")
        ).select(
            "asof_dlr_key",
            "tpa.dlr_key",
            "account_position.fund_key",
            "account_position.acct_key",
            F.lit(business_day_metadata.month_days_count).alias("days_in_month"),
            F.lit(business_day_metadata.last_day_key).alias("last_day_key"),
            "avg_daily_shrs",
            "avg_daily_bal"
        )
        return df_result

def get_business_day_weights(spark,df_calendar,p_date:datetime) -> pyspark.sql.DataFrame:
    # to fetch last business day, we take up to last 10 days of prev month, then take last record
    last_business_day_prev_month = df_calendar.filter(
        (F.col("cal_day")>=F.date_add(F.trunc(F.lit(p_date),"month"),-10)) &
        (F.col("cal_day")<F.trunc(F.lit(p_date),"month"))
        ).filter("bus_day_flg='Y'").select("day_key",F.to_date("cal_day").alias("cal_day"),"bus_day_flg").orderBy(F.col("cal_day").desc()).first()
    # filter this month's days
    month_days = df_calendar.filter(
        (F.col("cal_day")>=F.trunc(F.lit(p_date),"month")) &
        (F.col("cal_day")<=F.last_day(F.lit(p_date)))
        )
    # filter only business days in this month
    business_days = month_days.filter("bus_day_flg='Y'").select("day_key",F.to_date("cal_day").alias("cal_day"),"bus_day_flg")
    # calculate weights. each business days has a weight equal to number of business days until next business day.
    # for consecutive days will have weight=1. weekend will have weight=3 etc
    business_day_weights = business_days.select(
        F.col("cal_day"),
        F.col("day_key"),
        F.coalesce(
            F.lag(F.col("cal_day"),-1).over(Window.orderBy("cal_day")),
            F.to_date(F.trunc(F.add_months(F.lit(p_date),1),"month")) # add the first day of next month as last record
        ).alias("next_business_day")
    )
    business_day_weights = business_day_weights.select(
        F.col("cal_day"),
        F.col("day_key"),
        F.datediff(F.col("next_business_day"),F.col("cal_day")).alias("weight")
    )

    # append the last business day of previous month
    # its weight is the number of days from beginning of month until the first business day of the month.
    df_last_business_day = spark.createDataFrame([Row(
        day_key=last_business_day_prev_month["day_key"],
        cal_day=last_business_day_prev_month["cal_day"],
        weight=business_days.first()["cal_day"].day-1
    )])
    business_day_weights = business_day_weights.union(df_last_business_day)

    month_days_rdd = month_days.orderBy("cal_day").collect()

    # mo_days_cnt = calendar.monthrange(p_date.year,p_date.month)[1]
    return_type = namedtuple("Weights","last_day_key month_days_count business_day_weights")
    return return_type(
        last_day_key = month_days_rdd[-1]["day_key"],
        month_days_count = len(month_days_rdd),
        business_day_weights = business_day_weights.alias("weights")
    )
