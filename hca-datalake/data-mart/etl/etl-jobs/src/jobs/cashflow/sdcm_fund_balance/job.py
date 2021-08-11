from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob  # must be imported after spark has been set up
import datetime


class Job(ETLJob):
    target_table = "sdcm_fund_balance"
    business_key = ["fund_key", "day_key"]
    primary_key = {"fund_bal_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "cobol": {
            "type": "file",
            "source": "DTO.HCA.SDCM.BALANCE"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "fund_valuation": {
            "type": "table",
            "source": "fund_valuation"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        }
    }

    joins: List[Dict[str, Any]] = [
        {
            "source": "cobol"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("cobol.fund-code") == F.col("fund.fund_nbr")
            ]
        }
    ]

    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("prev_cal_day_key"), "target": "day_key"},
        {"source": F.col("cobol.shares-outstanding-amt"), "target": "tot_fund_shrs"},
        {"source": F.col("tot_fund_asset"), "target": "tot_fund_asset"},
        {"source": F.lit(None).cast("string"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"}
    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        p_end_dt = self._args["p_end_dt"]
        p_end_dt = datetime.datetime.strptime(p_end_dt, '%m/%d/%Y')
        df_joined = super().join(inputs)

        # Create dataframes from tables
        df_calendar = inputs["calendar"]
        df_fund_valuation = inputs["fund_valuation"]

        # get the last buss day
        prev_cal_day_key = common.utils.get_previous_bus_day(df_calendar, p_end_dt, "day_key")
        df_joined = df_joined.withColumn("prev_cal_day_key", F.lit(prev_cal_day_key))

        # get only records that are in status ("Open", "Soft close")
        df_joined = df_joined.where(F.col("fund.fund_stat_desc").isin("Open", "Soft close"))

        df_joined = df_joined\
            .join(df_fund_valuation, [F.col("fund.fund_key") == F.col("fund_valuation.fund_key"),
                                      F.col("fund_valuation.day_key") == F.col("prev_cal_day_key")], how="inner")

        return df_joined

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = self.calc_transformed(df_joined)
        df_transformed = super().transform(df_transformed)
        return df_transformed

    def calc_transformed(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_transformed = df_joined

        df_transformed = df_transformed\
            .withColumn("tot_fund_asset", (F.col("cobol.shares-outstanding-amt") * F.col("fund_valuation.pr_per_shr")))

        return df_transformed
