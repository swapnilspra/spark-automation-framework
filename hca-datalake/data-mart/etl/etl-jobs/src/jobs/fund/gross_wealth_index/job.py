from typing import Dict, List, Any
from datetime import date
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob  # must be imported after spark has been set up
from datetime import datetime,timedelta

class Job(ETLJob):
    target_table = "gross_wealth_index"
    business_key = ["day_key", "fund_key"]
    primary_key = {"gr_wlth_idx_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "ret": {
            "type": "table",
            "source": "fund_return"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "rep": {
            "type": "table",
            "source": "return_periods"
        },
        "fer": {
            "type": "table",
            "source": "fund_expense_ratio"
        },
        "erh": {
            "type": "table",
            "source": "expense_ratio_history"
        }

    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "rep",
        },
        {
            "source": "ret",
            "conditions": [
                F.col("ret.per_key") == F.col("rep.per_key"),
                F.col("rep.per_nm") == F.lit("1MO")
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_key") == F.col("ret.fund_key")
            ]
        },
        {
            "source": "cal",
            "conditions": [
                F.col("cal.day_key") == F.col("ret.day_key"),
            ]
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("ret.day_key"), "target": "day_key"},
        {"source": F.col("ret.fund_key"), "target": "fund_key"},
        {"source": (F.col("ret.fund_ret") + F.lit(1)) * (F.lit(1) + F.col("view_net_expns_ratio") / F.lit(12)),
         "target": "gr_wlth_idx"},
        {"source": F.col("ret.fund_ret") + F.lit(1), "target": "net_wlth_idx"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "etl_load_cyc_key"},
        {"source": F.lit(4), "target": "src_sys_id"}
    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:
        if not self._args.get("p_end_dt",None):
            # default value is first day of the current month
            p_end_dt = datetime.today().replace(day=1)-timedelta(days=1)
        else:
            p_end_dt = datetime.strptime(self._args["p_end_dt"],"%m/%d/%Y")
        #view is created by joining fund table with fund expense ratio and grouping by day and fund key to calculate net expense ration
        #This again merged/union to expense ratio history , this is actually represented by MV in PL/SQL
        df_view = inputs["fund"] \
            .join(inputs["fer"], [F.col("fund.fund_key") == F.col("fer.fund_key")]) \
            .join(inputs["cal"], [F.col("fer.efftv_end_dt") == F.col("cal.cal_day")]) \
            .select("fer.net_expns_ratio", "cal.day_key", "fund.fund_key", "fer.pass_thru_mgmt_fee_wavr") \
            .withColumn('net_expns_ratio_add',
                        F.col('fer.net_expns_ratio') + F.coalesce("fer.pass_thru_mgmt_fee_wavr", F.lit(0))) \
            .groupBy("cal.day_key", "fund.fund_key").agg(F.sum('net_expns_ratio_add').alias('net_expns_ratio'))\
            .select("net_expns_ratio", "cal.day_key", "fund.fund_key")

        df_view = df_view.union(inputs["erh"].select("net_expns_ratio", "day_key", "fund_key")) \
            .selectExpr("day_key as view_day_key", "fund_key as view_fund_key",
                        "net_expns_ratio as view_net_expns_ratio")

        df_joins = super().join(inputs).where(F.col("cal.cal_day") == F.lit(p_end_dt))
        df_output = df_joins.join(df_view, [F.col("view_day_key") == F.col("ret.day_key"),
                                                   F.col("view_fund_key") == F.col("ret.fund_key")])
        return df_output
