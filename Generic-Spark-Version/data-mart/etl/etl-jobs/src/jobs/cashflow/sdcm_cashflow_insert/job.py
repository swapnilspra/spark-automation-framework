from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob  # must be imported after spark has been set up
import datetime


class Job(ETLJob):
    target_table = "sdcm_cashflow"
    business_key = ["reg_key", "dlr_key", "acct_key", "fund_key", "trde_dt_key"]
    primary_key = {"sdcm_cashflow_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "cobol": {
            "type": "file",
            "source": "DTO.HCA.SDCM.POSITION",
            "limit": None,
            "sort": "size",
            "ascending": False
        },
        "region": {
            "type": "dimension",
            "source": "region"
        },
        "dst_st_country": {
            "type": "dimension",
            "source": "dst_st_country"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "account": {
            "type": "table",
            "source": "account"
        },
        "dlr_acc": {
            "type": "table",
            "source": "dealer"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "calendar_trade_dt": {
            "type": "table",
            "source": "calendar"
        },
        "calendar_confirm_dt": {
            "type": "table",
            "source": "calendar"
        },
        "payment_method": {
            "type": "table",
            "source": "payment_method"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
    }

    joins: List[Dict[str, Any]] = [
        {
            "source": "cobol"
        },
        {
            "source": "dealer",
            "conditions": [
                F.col("cobol.financial-inst-id") == F.col("dealer.dlr_id")
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.when(F.col("cobol.load-noload-code") == "1", F.col("cobol.fund-code") == F.col("fund.fund_nbr"))
                .otherwise(F.col("cobol.fund-code") == F.col("fund.fund_nbr"))
            ]
        },
        {
            "source": "calendar_trade_dt",
            "conditions": [
                F.to_timestamp(F.col("cobol.trade-date"), "yyyyMMdd") == F.col("calendar_trade_dt.cal_day")
            ]
        },
        {
            "source": "calendar_confirm_dt",
            "conditions": [
                F.to_timestamp(F.col("cobol.confirm-dt"), "yyyyMMdd") == F.col("calendar_confirm_dt.cal_day")
            ]
        },
        {
            "source": "account",
            "conditions": [
                F.when(F.col("cobol.load-noload-code") == "1",
                       (F.col("cobol.noload-account-number") == F.col("account.acct_nbr")) &
                       (F.col("cobol.fund-code") == F.col("account.fund_nbr")))
            ], "type":'leftouter'
        },
        {
            "source": "dlr_acc",
            "conditions": [
                F.col("account.hbr_dlr_id") == F.col("dlr_acc.dlr_id")
            ], "type":'leftouter'
        }

    ]

    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("region.reg_key"), "target": "reg_key"},
        {"source": F.col("payment_method.pmt_mthd_key"), "target": "pmt_mthd_key"},
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.lit(None).cast("int"), "target": "shrhldr_key"},
        {"source": F.col("account.acct_key"), "target": "acct_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("prev_bus_day_key"), "target": "spr_sheet_dt_key"},
        {"source": F.col("calendar_trade_dt.day_key"), "target": "trde_dt_key"},
        {"source": F.col("calendar_confirm_dt.day_key"), "target": "confirm_dt_key"},
        {"source": F.lit(None).cast("int"), "target": "shrhldr_role_key"},
        {"source": F.col("cobol.noload-account-number"), "target": "acct_nbr"},
        {"source": F.col("sub_shrs"), "target": "sub_shrs"},
        {"source": F.col("sub_amt"), "target": "sub_amt"},
        {"source": F.col("redmpn_shrs"), "target": "redmpn_shrs"},
        {"source": F.col("redmpn_amt"), "target": "redmpn_amt"},
        {"source": F.col("ofrg_pr"), "target": "ofrg_pr"},
        {"source": F.col("nav"), "target": "nav"},
        {"source": F.col("dlr_commsn_amt"), "target": "dlr_commsn_amt"},
        {"source": F.col("undr_wrtr_commsn_amt"), "target": "undr_wrtr_commsn_amt"},
        {"source": F.col("adv_commsn_amt"), "target": "adv_commsn_amt"},
        {"source": F.col("net_shrs"), "target": "net_shrs"},
        {"source": F.col("net_cashflow_amt"), "target": "net_cashflow_amt"},
        {"source": F.col("cobol.cash-control-reconcilement-cd"), "target": "cash_cntl_recon_flg"},
        {"source": F.lit(None).cast("string"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"},
        {"source": F.col("dlr_acc.dlr_key"), "target": "hbr_dlr_key"},
        {"source": F.col("cobol.order-number"), "target": "ord_num"},
        {"source": F.col("gr_amt"), "target": "gr_amt"},
        {"source": F.col("shrs"), "target": "shrs"}

    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        p_end_dt = self._args["p_end_dt"]
        p_end_dt = datetime.datetime.strptime(p_end_dt, '%m/%d/%Y')
        df_joined = super().join(inputs)

        # Create Data frames from tables or dimensions
        df_dst_st_country = inputs["dst_st_country"]
        df_region = inputs["region"]
        df_calendar = inputs["calendar"]
        df_payment_method = inputs["payment_method"]

        df_joined = df_joined\
            .withColumn("pmt_mthd_cd_join", F.when(~(F.col("cobol.payment-method-cde").isin(["A", "C", "D", "Z", "S"])), F.lit(0))
                        .otherwise(F.col("cobol.payment-method-cde")))

        df_joined = df_joined\
            .join(df_payment_method, F.col("pmt_mthd_cd_join") == F.col("payment_method.pmt_mthd_cd"), how="left_outer")

        df_joined = df_joined\
            .join(df_dst_st_country, F.col("dst_st_country.dst_st_cd") == F.col("cobol.resident-state-country"), how="left_outer")

        df_joined = df_joined\
            .join(df_region, F.when(F.col("dst_st_country.st_cd").isNotNull(), F.col("region.st_cd") == F.col("dst_st_country.st_cd"))
                  .otherwise(F.col("region.crty_cd") == F.col("dst_st_country.crty_iso_cd")), how="left_outer")

        # get the last buss day_key
        prev_cal_day_key = common.utils.get_previous_bus_day(df_calendar, p_end_dt, "day_key")
        df_joined = df_joined.withColumn("prev_bus_day_key", F.lit(prev_cal_day_key))

        # get the last buss cal_day
        prev_cal_day = common.utils.get_previous_bus_day(df_calendar, p_end_dt, "cal_day")
        df_joined = df_joined.withColumn("prev_bus_cal_day", F.lit(prev_cal_day))

        return df_joined

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = self.calc_transformed(df_joined)
        df_transformed = super().transform(df_transformed)
        return df_transformed

    def calc_transformed(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_transformed = df_joined

        # Filter the records to identify only the records that need to be entered into the table
        df_transformed = df_transformed\
            .where((F.col("cobol.reason-code") != F.lit(0)) | (F.to_timestamp(F.col("cobol.trade-date"), "yyyyMMdd") == F.col("prev_bus_cal_day")))

        df_transformed = df_transformed\
            .where((F.col("cobol.transaction-type-cd").isin(["0", "1"])) & ~((F.coalesce(F.col("cobol.transaction-code"), F.lit(0)) == F.lit(900)) &
                   (F.coalesce(F.col("cobol.transaction-suffix"), F.lit(0)).isin([11, 16, 17, 21, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 161, 171]))))

        # Create new calculated columns
        df_transformed = df_transformed\
            .withColumn("sub_shrs", F.when(F.col("cobol.transaction-type-cd") == F.lit("0"), F.col("cobol.shares"))
                        .otherwise(F.lit(0)))

        df_transformed = df_transformed\
            .withColumn("sub_amt", F.when(F.col("cobol.transaction-type-cd") == F.lit("0"), F.col("cobol.gross-amount"))
                        .otherwise(F.lit(0)))

        df_transformed = df_transformed\
            .withColumn("redmpn_shrs", F.when(F.col("cobol.transaction-type-cd") == F.lit("1"), F.col("cobol.shares"))
                        .otherwise(F.lit(0)))

        df_transformed = df_transformed\
            .withColumn("redmpn_amt", F.when(F.col("cobol.transaction-type-cd") == F.lit("1"), F.col("cobol.gross-amount"))
                        .otherwise(F.lit(0)))

        df_transformed = df_transformed\
            .groupBy("account.acct_key", "fund.fund_key", "prev_bus_day_key", "calendar_trade_dt.day_key",
                     "region.reg_key", "dealer.dlr_key",
                     "calendar_confirm_dt.day_key", "payment_method.pmt_mthd_key",
                     "cobol.noload-account-number", "dlr_acc.dlr_key", "cobol.order-number", "cobol.cash-control-reconcilement-cd")\
            .agg(F.sum(F.col("sub_shrs")).alias("sub_shrs"),
                 F.sum(F.col("sub_amt")).alias("sub_amt"),
                 F.sum(F.col("redmpn_shrs")).alias("redmpn_shrs"),
                 F.sum(F.col("redmpn_amt")).alias("redmpn_amt"),
                 F.avg(F.col("cobol.offering-price")).alias("ofrg_pr"),
                 F.sum(F.col("cobol.net-asset-value-amt")).alias("nav"),
                 F.sum(F.col("cobol.dealer-commission")).alias("dlr_commsn_amt"),
                 F.sum(F.col("cobol.underwriter-commission")).alias("undr_wrtr_commsn_amt"),
                 F.sum(F.col("cobol.Fin-tr-advanced-commission-at")).alias("adv_commsn_amt"),
                 F.sum(F.col("cobol.gross-amount")).alias("gr_amt"),
                 F.sum(F.col("cobol.shares")).alias("shrs"))

        df_transformed = df_transformed.\
            withColumn("net_shrs", F.col("sub_shrs") - F.abs(F.col("redmpn_shrs")))

        df_transformed = df_transformed.\
            withColumn("net_cashflow_amt", F.col("sub_amt") + F.col("redmpn_amt") -
                       (F.col("dlr_commsn_amt") + F.col("undr_wrtr_commsn_amt") + F.col("adv_commsn_amt")))

        return df_transformed

