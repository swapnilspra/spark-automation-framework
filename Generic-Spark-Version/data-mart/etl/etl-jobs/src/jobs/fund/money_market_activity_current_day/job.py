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
        "mmkt":
            {
                "type": "file",
                "source": "mmkt_info"
            },
        "fund":
            {
                "type": "table",
                "source": "fund",
            },
        "rates_with_gl_y":
            {
                "type": "file",
                "source": "rates"
            },
        "rates_with_gl_n":
            {
                "type": "file",
                "source": "rates"
            },
        "calendar":
            {
                "type": "table",
                "source": "calendar"
            }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "mmkt"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.col("mmkt.alternate fund id")
            ]
        },
        {
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("calendar.cal_day")) == F.to_date(F.col("mmkt.date"))
            ]
        },
        {
            "source": "rates_with_gl_y",
            "conditions": [
                F.col("fund.fund_nbr") == F.col("rates_with_gl_y.ssb_fund"),
                F.col("rates_with_gl_y.with gl")=="Y"
            ]
        },
        {
            "source": "rates_with_gl_n",
            "conditions": [
                F.col("fund.fund_nbr") == F.col("rates_with_gl_n.ssb_fund"),
                F.col("rates_with_gl_n.with gl")=="N"
            ]
        },
    ]
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("fund.fund_key"), "target": 'fund_key' },
        { "source": F.col("calendar.day_key"), "target": 'day_key' },
        { "source": F.col("mmkt.daily mm gain/loss"), "target": 'gain_loss_amt' },
        { "source": F.col("mmkt.`income dist.`"), "target": 'dstrbtn_amt' },
        { "source": F.col("mmkt.`mm dividend payable`"), "target": 'divd_paybl_amt' },
        { "source": F.col("rates_with_gl_y.`exchange rate`"), "target": 'intr_w_gain_loss' },
        { "source": F.col("rates_with_gl_n.`exchange rate`"), "target": 'intr_wo_gain_loss' },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" },
        { "source": F.col("mmkt.`weighted average life`"), "target": "weighted_avg_life"},
        { "source": F.col("mmkt.`average days to maturity`"), "target": "days_to_mtry"},
        { "source": F.lit(0), "target": "basis_points"},
        { "source": F.lit(None).cast("int"), "target": "invmt_at_cost"}, # MM_INVEST no longer used. set as null.
        { "source": F.lit(None).cast("int"), "target": "invmt_at_val"}, # MM_INVEST no longer used. set as null.
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
