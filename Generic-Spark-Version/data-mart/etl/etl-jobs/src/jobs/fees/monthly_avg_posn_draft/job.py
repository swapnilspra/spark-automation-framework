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
    target_table = "monthly_avg_posn_draft"
    business_key = ["dlr_tpa_flg", "day_key", "dlr_key", "fund_key"]
    primary_key = {"mthly_avg_posn_drft_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "account_position": {
            "type": "table",
            "source": "account_position"
        },
        "tpa": {
            "type": "table",
            "source": "dealer"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "account": {
            "type": "table",
            "source": "account"
        },
        "eligible_asset": {
            "type": "table",
            "source": "eligible_asset_list"
        },
        "eligible_asset_default": {
            "type": "table",
            "source": "eligible_asset_list"
        }
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
        # join fund for the eligible asset list
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_key") == F.col("account_position.fund_key")
            ],
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("last_day_key"), "target": "day_key"},
        {"source": F.col("asof_dlr_key"), "target": "dlr_key"},
        {"source": F.col("fund_key"), "target": "fund_key"},
        {"source": F.col("dlr_tpa_flg"), "target": "dlr_tpa_flg"},
        {"source": F.col("avg_daily_shrs"), "target": "tot_shrs"},
        {"source": F.col("avg_daily_bal"), "target": "tot_bal_amt"},
        {"source": F.col("tot_amt_excl_tpa"), "target": "tot_amt_excl_tpa"},
        {"source": F.col("tot_eligbl_bal"), "target": "tot_eligbl_bal"},
        {"source": F.col("tot_eligbl_excl_tpa"), "target": "tot_eligbl_excl_tpa"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

    def get_dealer_asof(self,df_account_position,df_calendar,p_date):
        # filter account positions in the selected business day

        # first, get the day_key of the last business day of the month
        asof_business_day_key = df_calendar.filter(
            (F.col("bus_day_flg")==F.lit("Y")) &
            (F.col("cal_day").between(F.lit(p_date-timedelta(days=10)),F.lit(p_date-timedelta(days=1))))
        ).orderBy("cal_day").collect()[-1]["day_key"]

        ap_asof_day = df_account_position.filter(F.col("account_position.day_key")==F.lit(asof_business_day_key)).select("account_position.acct_key","account_position.fund_key","account_position.dlr_key")
        ap_asof_day = ap_asof_day.alias("ap_asof_day")
        return ap_asof_day

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:
        if not self._args.get("p_date",None):
            # default value is first day of the current month
            p_date = datetime.today().replace(day=1)
        else:
            p_date = datetime.strptime(self._args["p_date"],"%m/%d/%Y")

        # create a weighted vector of the business days for computing averages
        business_day_metadata = get_business_day_weights(self._spark,inputs["calendar"],p_date-timedelta(days=1))

        #
        # get the dealer keys as of the last business day of the month
        #
        ap_asof_day = self.get_dealer_asof(inputs["account_position"],inputs["calendar"],p_date)

        # join with vector of business days and weights. this filters out only month's business days and provides the weight for each day
        # filter only positive share counts
        inputs["account_position"] = inputs["account_position"].join(business_day_metadata.business_day_weights,F.col("weights.day_key")==F.col("account_position.day_key")).filter(F.col("account_position.tot_shrs")>0)

        # perform regular joins
        df_joined = super().join(inputs)

        # join ap_asof_day to account_position
        df_joined = df_joined.join(ap_asof_day,[
            F.col("account_position.acct_key")==F.col("ap_asof_day.acct_key"),
            F.col("account_position.fund_key")==F.col("ap_asof_day.fund_key")
        ],how="leftouter")

        # add TPA records
        # add a flag to tpa vs dealer. "D"=Dealer, "T"=TPA
        df_tpa = df_joined.filter(~(
                (F.coalesce(F.col("account.tpa_id"),F.lit(0))==F.lit(0)) | 
                (F.col("account.tpa_id").between(F.lit(700000),F.lit(799999)) & ~F.col("account.tpa_id").isin(707103,707120,709157) )
        ))
        df_tpa = df_tpa.withColumn("dlr_tpa_flg",F.lit("T"))

        df_joined = df_joined.withColumn("dlr_tpa_flg",F.lit("D"))
        df_joined = df_joined.unionAll(df_tpa)

        # set the dealer key
        #
        # if we had a record at end of month, use that dealer key. If not, use the dealer key on the record
        # if this is a TPA, use the tpa dealer key
        df_joined = df_joined.withColumn("asof_dlr_key",
            F.when(F.col("dlr_tpa_flg")==F.lit("D"),
                F.coalesce(F.col("ap_asof_day.dlr_key"),F.col("dealer.dlr_key")))\
            .otherwise(F.col("tpa.dlr_key"))
        )

        # join for eligible assets that have a share class code
        df_joined = df_joined.join(inputs["eligible_asset"],[
            F.col("asof_dlr_key")==F.col("eligible_asset.dlr_key"),
            F.col("fund.shr_cls_cd")==F.col("eligible_asset.shr_cls_cd")
        ],how="left_outer")

        # join for eligible assets that do not have a share class code
        # for any record that does not have an eligible asset, we take the record without a shr_cls_cd
        df_joined = df_joined.join(inputs["eligible_asset_default"],[
            F.col("asof_dlr_key")==F.col("eligible_asset_default.dlr_key"),
            F.col("eligible_asset_default.shr_cls_cd").isNull()
        ],how="left_outer")

        # exclude assets that have a tpa or tpa  in (707103,707120,709157)
        df_joined = df_joined.withColumn(
            "tot_amt_excl_tpa",
            F.when(
                (F.coalesce(F.col("account.tpa_id"),F.lit(0))==F.lit(0)) | 
                (F.col("account.tpa_id").between(F.lit(700000),F.lit(799999)) & ~F.col("account.tpa_id").isin(707103,707120,709157) ),
                F.col("tot_bal_amt"))\
            .otherwise(F.lit(0))
        )

        # add fee eligibility flag
        # we lookup the eligible_asset with exact match on shr_cls_cd. If it is null we take the default which is shr_cls_cd=null record
        df_joined = df_joined.withColumn("fee_eligbl_flg",
            F.when(
                F.coalesce(F.col("eligible_asset.inlsn_excln_flg"),F.col("eligible_asset_default.inlsn_excln_flg"))== F.lit('I'),
                F.when(
                    F.coalesce(F.col("eligible_asset.cum_discnt_nbr"),F.col("eligible_asset_default.cum_discnt_nbr"))==F.col("account.cum_discnt_nbr"),
                    F.lit("Y")
                ).otherwise(F.lit("N"))
            ).when(
                F.coalesce(F.col("eligible_asset.inlsn_excln_flg"),F.col("eligible_asset_default.inlsn_excln_flg"))== F.lit('E'),
                F.when(
                    F.coalesce(F.col("eligible_asset.cum_discnt_nbr"),F.col("eligible_asset_default.cum_discnt_nbr"))==F.col("account.cum_discnt_nbr"),
                    F.lit("N")
                ).otherwise(F.lit("Y"))
            ).otherwise(F.lit("Y"))
        )

        # note: we cast and use decimal to avoid precision loss 
        df_result = df_joined.groupBy(
            "asof_dlr_key",
            "account_position.fund_key",
            "dlr_tpa_flg"
        ).agg(
            (F.sum(F.col("account_position.tot_shrs")*F.col("weights.weight"))/F.lit(business_day_metadata.month_days_count).cast(T.DecimalType(38,18))).alias("avg_daily_shrs"),
            (F.sum(F.col("account_position.tot_bal_amt")*F.col("weights.weight"))/F.lit(business_day_metadata.month_days_count).cast(T.DecimalType(38,18))).alias("avg_daily_bal"),
            (F.sum(F.col("tot_amt_excl_tpa")*F.col("weights.weight"))/F.lit(business_day_metadata.month_days_count).cast(T.DecimalType(38,18))).alias("tot_amt_excl_tpa"),
            (F.sum(
                F.when(F.col("fee_eligbl_flg")==F.lit("Y"),F.col("tot_bal_amt")).otherwise(F.lit(0).cast(T.DecimalType(38,18)))*\
                F.col("weights.weight"))/F.lit(business_day_metadata.month_days_count).cast(T.DecimalType(38,18))
            ).alias("tot_eligbl_bal"),
            (F.sum(
                F.when(F.col("fee_eligbl_flg")==F.lit("Y"),F.col("tot_amt_excl_tpa")).otherwise(F.lit(0).cast(T.DecimalType(38,18)))*\
                F.col("weights.weight"))/F.lit(business_day_metadata.month_days_count).cast(T.DecimalType(38,18))
            ).alias("tot_eligbl_excl_tpa")
        ).select(
            "asof_dlr_key",
            "account_position.fund_key",
            "dlr_tpa_flg",
            F.lit(business_day_metadata.month_days_count).alias("days_in_month"),
            F.lit(business_day_metadata.last_day_key).alias("last_day_key"),
            "avg_daily_shrs",
            "avg_daily_bal",
            "tot_amt_excl_tpa",
            "tot_eligbl_bal",
            "tot_eligbl_excl_tpa"
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
    return_type = namedtuple("Weights","last_day_key month_days_count business_day_weights last_business_day_key")
    return return_type(
        last_day_key = month_days_rdd[-1]["day_key"],
        month_days_count = len(month_days_rdd),
        last_business_day_key = business_day_weights.orderBy("cal_day").collect()[-1]["day_key"],
        business_day_weights = business_day_weights.alias("weights")
    )
