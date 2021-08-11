from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "cash_holding"
    business_key = ["fund_compst_key","day_key"]
    primary_key = {"cash_holdg_key":"int"}

    SHORT_TERM_COLLATERAL:int = 26
    sources:Dict[str,Dict[str,Any]] = {
        "full_prc": {
            "type": "file",
            "source": "full_prc"
        },
        "fund_composite": {
            "type": "table",
            "source": "fund_composite"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "pta_ssb_position": {
            "type": "file",
            "source": "pta_ssb_position"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "full_prc"
        },
        {
            "source": "fund_composite",
            "conditions": [
                F.col("fund_composite.st_str_fund_nbr") == F.col("full_prc.`fund id`")
            ]
        },
        {
            "source": "calendar",
            "conditions": [
                F.col("calendar.cal_day") == F.col("full_prc.date")
            ]
        },
    ]
    target_mappings:List[Dict[str,Any]] = [

        { "source": F.col("fund_composite.fund_compst_key"), "target": "fund_compst_key" },
        { "source": F.col("calendar.day_key"), "target": "day_key" },
        { "source": F.col("full_prc.`fund cash`"), "target": "cash" },
        { "source": F.col("full_prc.`short term`"), "target": "shrt_term_invmt" },
        { "source": F.col("full_prc.`inv. rec`"), "target": "recbl" },
        { "source": F.col("full_prc.`inv. pay`"), "target": "paybl" },
        { "source": F.col("full_prc.`net pending shares`"), "target": "net_shrs" },
        { "source": F.col("full_prc.`total net assets`"), "target": "net_asset" },
        { "source": F.col("pta_ssb_position_processed.sum_shrpar_qty"), "target": "cash_coltrl" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key" },        
        { "source": F.lit(4), "target": "src_sys_id" },

    ]
    def join(self,inputs:Dict[str,pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:
        # prepare the grouped pta_ssb_position for cash_coltrl

        # SUM shrpar_qty group by fund_id and calen_dt where issue_class_code = 26
        # Lookup fund_composite.st_str_fund_nbr  = fund_id get the fund_compst_key
        # Lookup  calendar.cal_day = calen_dt  get the day_key
        # Use fund_compst_key and day_key to join cash_holdings table and update cash_coltrl = shrpar_qty
        df_joined = super().join(inputs)
        pta_ssb_position_grouped = inputs["pta_ssb_position"].\
            where(F.col("issue_cls_cd")==self.SHORT_TERM_COLLATERAL).\
            groupby("fund_id","calen_dt").\
            agg(F.sum("shrpar_qty").alias("sum_shrpar_qty"))
        # left outer join to the joined dataframe
        df_joined = df_joined.join(pta_ssb_position_grouped.alias("pta_ssb_position_processed"),
            [
                F.col("full_prc.date")==F.col("pta_ssb_position_processed.calen_dt"),
                F.col("full_prc.`fund id`")==F.col("pta_ssb_position_processed.`fund_id`")
            ],
            how="left_outer"
        )
        return df_joined

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
