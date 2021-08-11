from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob  # must be imported after spark has been set up
import datetime


class Job(ETLJob):
    target_table = "dealer_payment_summary"
    business_key = ["dlr_fee_type_key", "day_key", "dlr_key", "fund_key", "dlr_pmt_mthd_key", "wk_ord_nbr"]
    primary_key = {"dlr_pmt_sum_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "cobol": {
            "type": "file",
            "source": "DTO.HCA.MTF.R00858",
            "limit": None,
            "sort": "size",
            "ascending": False
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "dealer_fee_type": {
            "type": "table",
            "source": "dealer_fee_type"
        },
        "day_key_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "pmt_key_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "prev_day_key_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "prev_pmt_day_key_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "dealer_payment_method": {
            "type": "table",
            "source": "dealer_payment_method"
        },
        "dealer_payment_detail": {
            "type": "table",
            "source": "dealer_payment_detail"
        },
        "prev_dealer_payment_summary": {
            "type": "table",
            "source": "dealer_payment_summary"
        }

    }

    joins: List[Dict[str, Any]] = [
        {
            "source": "cobol"
        },
        {
            "source": "dealer_fee_type",
            "conditions": [
                F.col("cobol.tlr-cmpn-fee-cd") == F.col("dealer_fee_type.dlr_fee_type_cd")
            ]
        },
        {
            "source": "day_key_calendar",
            "conditions": [
                F.to_timestamp(F.col("cobol.tlr-pyo-per-end-dt"), "yyyyMMdd") == F.col("day_key_calendar.cal_day")
            ]
        },
        {
            "source": "pmt_key_calendar",
            "conditions": [
                F.last_day(F.add_months(F.to_timestamp(F.col("cobol.tlr-pyo-per-end-dt"), "yyyyMMdd"), 1)) == F.col("pmt_key_calendar.cal_day")
            ]
        },
        {
            "source": "prev_day_key_calendar",
            "conditions": [
                F.add_months(F.to_timestamp(F.col("cobol.tlr-pyo-per-end-dt"), "yyyyMMdd"), -1) == F.col("prev_day_key_calendar.cal_day")
            ]
        },
        {
            "source": "prev_pmt_day_key_calendar",
            "conditions": [
                F.last_day(F.to_timestamp(F.col("cobol.tlr-pyo-per-end-dt"), "yyyyMMdd")) == F.col("prev_pmt_day_key_calendar.cal_day")
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("cobol.fund-code") == F.col("fund.fund_nbr")
            ]
        },
        {
            "source": "dealer",
            "conditions": [
                F.col("cobol.financial-inst-id") == F.col("dealer.dlr_id")
            ]
        },
    ]

    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("dealer_fee_type.dlr_fee_type_key"), "target": "dlr_fee_type_key"},
        {"source": F.col("day_key_calendar.day_key"), "target": "day_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.col("dlr_pmt_mthd_key_agg"), "target": "dlr_pmt_mthd_key"},
        {"source": F.col("cobol.tlr-pyo-wko-nbr-id"), "target": "wk_ord_nbr"},
        {"source": F.col("cobol.cmpn-payee-type-cd"), "target": "compnsn_pyee_type_cd"},
        {"source": F.col("cobol.preagree-cmpn-sch-orr-typ"), "target": "pre_agreed_sched_ovrd_type"},
        {"source": F.col("cobol.preagree-share-rt"), "target": "pre_agree_shr_rt"},
        {"source": F.col("cobol.preagree-average-assets-at"), "target": "pre_agree_avg_asset"},
        {"source": F.col("cobol.preagree-compensation-at"), "target": "pre_agree_compnsn"},
        {"source": F.col("cobol.eligible-cmpn-sch-orr-typ"), "target": "eligbl_compnsn_ovrd_type"},
        {"source": F.col("cobol.eligible-share-rt"), "target": "eligbl_shr_rt"},
        {"source": F.col("cobol.eligible-average-assets-at"), "target": "eligbl_avg_asset"},
        {"source": F.col("cobol.eligible-compensation-at"), "target": "eligbl_compnsn"},
        {"source": F.col("cobol.total-compensation-at"), "target": "tot_compnsn"},
        {"source": F.col("compnsn_diff"), "target": "compnsn_diff"},
        {"source": F.col("prev_tot_compnsn"), "target": "prev_per_compnsn"},
        {"source": F.col("avg_asset_diff"), "target": "avg_asset_diff"},
        {"source": F.col("prev_eligbl_avg_asset"), "target": "prev_per_asset"},
        {"source": F.lit("N"), "target": "trl_mode_flg"},
        {"source": F.col("pmt_key_calendar.day_key"), "target": "pmt_day_key"},
        {"source": F.lit("MTF"), "target": "bus_line"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"},
        {"source": F.lit(None).cast("string"), "target": "curr_row_flg"}

    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        df_joined = super().join(inputs)
        # Create Data frames from tables or dimensions
        df_dealer_payment_detail = inputs["dealer_payment_detail"]
        df_prev_dealer_payment_summary = inputs["prev_dealer_payment_summary"]

        # aggregate dealer_payment_detail in order to get min dlr_pmt_mthd_key
        df_dealer_payment_detail_agg = df_dealer_payment_detail\
            .groupBy(F.col("dealer_payment_detail.wk_ord_id"),
                     F.col("dealer_payment_detail.day_key"),
                     F.col("dealer_payment_detail.dlr_key"),
                     F.col("dealer_payment_detail.fund_key"),
                     F.col("dealer_payment_detail.dlr_fee_type_key"))\
            .agg(F.min(F.col("dealer_payment_detail.dlr_pmt_mthd_key")).alias("dlr_pmt_mthd_key_agg"))

        # join between dealer_payment_detail to current df_joined in oerder to get dlr_pmt_mthd_key
        df_joined = df_joined\
            .join(df_dealer_payment_detail_agg, [F.col("dealer_payment_detail.wk_ord_id") == F.col("cobol.tlr-pyo-wko-nbr-id"),
                                                 F.col("dealer_payment_detail.day_key") == F.col("day_key_calendar.day_key"),
                                                 F.col("dealer_payment_detail.dlr_key") == F.col("dealer.dlr_key"),
                                                 F.col("dealer_payment_detail.fund_key") == F.col("fund.fund_key"),
                                                 F.col("dealer_payment_detail.dlr_fee_type_key") == F.col("dealer_fee_type.dlr_fee_type_key")], how="left_outer")

        # aggregate previous dealer_payment_summary table in order to prevent duplicate records
        df_prev_dealer_payment_summary_agg = df_prev_dealer_payment_summary\
            .groupBy(F.col("prev_dealer_payment_summary.day_key"),
                     F.col("prev_dealer_payment_summary.pmt_day_key"),
                     F.col("prev_dealer_payment_summary.dlr_key"),
                     F.col("prev_dealer_payment_summary.dlr_pmt_mthd_key"),
                     F.col("prev_dealer_payment_summary.fund_key"),
                     F.col("prev_dealer_payment_summary.dlr_fee_type_key"))\
            .agg(F.sum(F.col("prev_dealer_payment_summary.tot_compnsn")).alias("prev_tot_compnsn_agg"),
                 F.sum(F.col("prev_dealer_payment_summary.eligbl_avg_asset")).alias("prev_eligbl_avg_asset_agg"))

        # join prev agg table with cobol current table in order to get previous tot_compnsn, eligbl_avg_asset columns
        df_joined = df_joined\
            .join(df_prev_dealer_payment_summary_agg,
                  [F.col("prev_dealer_payment_summary.day_key") == F.col("prev_day_key_calendar.day_key"),
                   F.col("prev_dealer_payment_summary.pmt_day_key") == F.col("prev_pmt_day_key_calendar.day_key"),
                   F.col("prev_dealer_payment_summary.dlr_key") == F.col("dealer.dlr_key"),
                   F.col("prev_dealer_payment_summary.dlr_pmt_mthd_key") == F.col("dlr_pmt_mthd_key_agg"),
                   F.col("prev_dealer_payment_summary.fund_key") == F.col("fund.fund_key"),
                   F.col("prev_dealer_payment_summary.dlr_fee_type_key") == F.col("dealer_fee_type.dlr_fee_type_key")], how="left_outer")

        return df_joined

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = self.calc_transformed(df_joined)
        df_transformed = super().transform(df_transformed)
        return df_transformed

    def calc_transformed(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = df_joined

        # Null handling
        df_transformed = df_transformed\
            .withColumn("prev_tot_compnsn", F.when(F.col("prev_tot_compnsn_agg").isNull(), F.lit(0))
                        .otherwise(F.col("prev_tot_compnsn_agg")))
        df_transformed = df_transformed\
            .withColumn("prev_eligbl_avg_asset", F.when(F.col("prev_eligbl_avg_asset_agg").isNull(), F.lit(0))
                        .otherwise(F.col("prev_eligbl_avg_asset_agg")))

        # Create calculated column
        df_transformed = df_transformed\
            .withColumn("compnsn_diff", F.col("cobol.total-compensation-at") - F.col("prev_tot_compnsn"))

        df_transformed = df_transformed\
            .withColumn("avg_asset_diff", F.col("cobol.eligible-average-assets-at") - F.col("prev_eligbl_avg_asset"))

        return df_transformed
