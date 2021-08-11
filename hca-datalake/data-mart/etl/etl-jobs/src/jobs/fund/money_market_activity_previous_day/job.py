from typing import Dict, List, Any
import pyspark.sql
import logging
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
class Job(ETLJob):
    target_table = "money_market_activity"
    business_key = ["fund_key","day_key"]
    primary_key = {"mony_mkt_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "hdw": 
            {
                "type": "file",
                "source": "harbor_mm_web_disclosure"
            },
        "gb05": 
            {
                "type": "file",
                "source": "gb05_comparison_nav_fund_summary"
            },
        "fund":
            {
                "type": "table",
                "source": "fund",
            },
        "calendar":
            {
                "type": "table",
                "source": "calendar"
            }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "hdw"
        },
        {
            "source": "gb05",
            "conditions": [
                F.col("hdw.business_date")==F.col("gb05.period end date"),
                F.col("hdw.basis_ind")==F.col("gb05.dual pricing basis")
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("hdw.fund_code") == F.col("fund.st_str_fund_nbr"),
                F.lower(F.col("hdw.basis_ind"))==   F.when(F.lower(F.col("fund.shr_cls_desc"))=="institutional class","1")\
                                                    .when(F.lower(F.col("fund.shr_cls_desc"))=="administrative class","2")\
                                                    .otherwise("-1")
            ]
        },
        {
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("calendar.cal_day")) == F.to_date(F.col("hdw.business_date"))
            ]
        },
    ]
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("fund.fund_key"), "target": 'fund_key' },
        { "source": F.col("calendar.day_key"), "target": 'day_key' },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" },
        { "source": F.regexp_replace(F.col("gb05.`amortized cost net assets`"),",","").cast("double"), "target": "net_asset_at_cost"},
        # this arrives as a comma separated string for thousands
        { "source": F.regexp_replace(F.col("gb05.`net assets including market value`"),",","").cast("double"), "target": "net_asset_at_mkt"},
        { "source": F.lit(0), "target": "basis_points"},
        { "source": F.lit(None).cast("int"), "target": "invmt_at_cost"}, # MM_INVEST no longer used. set as null.
        { "source": F.lit(None).cast("int"), "target": "invmt_at_val"}, # MM_INVEST no longer used. set as null.
        { "source": F.col("gb05.`current comparison nav`").cast("double"), "target": "mkt_based_pr"},
        { "source": F.col("hdw.`subscriptions_including_divrein`"), "target": "sub_w_drv_in_amt"},
        { "source": F.col("hdw.`redemptions`"), "target": "redmpn_amt"},
        { "source": F.col("hdw.`net_shareholder_flows`"), "target": "net_shrhldr_cash_flow_amt"},
        { "source": F.col("hdw.`daily_liquid_assets_dollar_amount`"), "target": "daily_lqd_asset_amt"},
        { "source": F.col("hdw.`daily_liquid_assets_percent`"), "target": "daily_lqd_asset_rt"},
        { "source": F.col("hdw.`weekly_liquid_assets_dollar_amount`"), "target": "wkl_lqd_asset_amt"},
        { "source": F.col("hdw.`weekly_liquid_assets_percent`"), "target": "wkl_lqd_asset_rt"},
        { "source": F.col("hdw.`shadownav`"), "target": "shdw_nav"},
        { "source": F.col("hdw.`constantnav`"), "target": "cnst_nav"},
        { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key" },        
    ]

    # perform forward filling
    # override transform to forward fill
    def transform(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_transformed = super().transform(df_joined)

        # forward fill
        df_calendar = common.utils.read_table_snapshot(
                    table_name="calendar",
                    env=self._env,
                    spark=self._spark)
        df_ffill = common.utils.forward_fill(df_calendar,df_transformed,"day_key")

        return df_ffill
