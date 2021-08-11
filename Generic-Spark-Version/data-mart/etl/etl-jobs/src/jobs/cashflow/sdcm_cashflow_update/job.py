from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob  # must be imported after spark has been set up
import datetime


class Job(ETLJob):
    target_table = "sdcm_cashflow"
    business_key = ["sdcm_cashflow_key"]
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
        {"source": F.col("sdcm_cashflow.sdcm_cashflow_key"), "target": "sdcm_cashflow_key"},
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
        {"source": F.col("sub_shrs_sum"), "target": "sub_shrs"},
        {"source": F.col("sub_amt_sum"), "target": "sub_amt"},
        {"source": F.col("redmpn_shrs_sum"), "target": "redmpn_shrs"},
        {"source": F.col("redmpn_amt_sum"), "target": "redmpn_amt"},
        {"source": F.col("ofrg_pr_avg"), "target": "ofrg_pr"},
        {"source": F.col("nav_sum"), "target": "nav"},
        {"source": F.col("dlr_commsn_amt_sum"), "target": "dlr_commsn_amt"},
        {"source": F.col("undr_wrtr_commsn_amt_sum"), "target": "undr_wrtr_commsn_amt"},
        {"source": F.col("adv_commsn_amt_sum"), "target": "adv_commsn_amt"},
        {"source": F.col("net_shrs_sum"), "target": "net_shrs"},
        {"source": F.col("net_cashflow_amt_sum"), "target": "net_cashflow_amt"},
        {"source": F.col("cobol.cash-control-reconcilement-cd"), "target": "cash_cntl_recon_flg"},
        {"source": F.lit(None).cast("string"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"},
        {"source": F.col("dlr_acc.dlr_key"), "target": "hbr_dlr_key"},
        {"source": F.col("cobol.order-number"), "target": "ord_num"},
        {"source": F.col("gr_amt_sum"), "target": "gr_amt"},
        {"source": F.col("shrs_sum"), "target": "shrs"}


    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        p_end_dt = self._args["p_end_dt"]
        p_end_dt = datetime.datetime.strptime(p_end_dt, '%m/%d/%Y')
        df_joined = super().join(inputs)

        # Create Data frames from tables or dimensions
        df_dst_st_country = inputs["dst_st_country"]
        df_region = inputs["region"]
        df_dealer = inputs["dealer"]
        df_calendar = inputs["calendar"]
        df_payment_method = inputs["payment_method"]

        df_joined = df_joined\
            .withColumn("pmt_mthd_cd_join", F.when(~(F.col("cobol.payment-method-cde").isin(["A", "C", "D", "Z", "S"])), F.lit(0))
                        .otherwise(F.col("cobol.payment-method-cde")))

        df_joined = df_joined.join(df_payment_method, F.col("pmt_mthd_cd_join") == F.col("payment_method.pmt_mthd_cd"), how="left_outer")

        df_joined = df_joined\
            .join(df_dst_st_country, F.col("dst_st_country.dst_st_cd") == F.col("cobol.resident-state-country"), how="left_outer")

        df_joined = df_joined\
            .join(df_region, F.when(F.col("dst_st_country.st_cd").isNotNull(), F.col("region.st_cd") == F.col("dst_st_country.st_cd"))
                  .otherwise(F.col("region.crty_cd") == F.col("dst_st_country.crty_iso_cd")), how="left_outer")

        # get the last buss day
        cal_day_key = common.utils.get_previous_bus_day(df_calendar, p_end_dt, "day_key")
        df_joined = df_joined.withColumn("prev_bus_day_key", F.lit(cal_day_key))

        return df_joined

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = self.calc_transformed(df_joined)
        df_transformed = super().transform(df_transformed)
        return df_transformed

    def calc_transformed(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = df_joined

        df_transformed = df_transformed\
            .withColumn("sub_shrs", F.when(F.col("cobol.transaction-type-cd") == '0', F.col("cobol.shares"))
                        .otherwise(F.lit(0)))

        df_transformed = df_transformed\
            .withColumn("sub_amt", F.when(F.col("cobol.transaction-type-cd") == '0', F.col("cobol.gross-amount"))
                        .otherwise(F.lit(0)))

        df_transformed = df_transformed\
            .withColumn("redmpn_shrs", F.when(F.col("cobol.transaction-type-cd") == '1', F.col("cobol.shares"))
                        .otherwise(F.lit(0)))

        df_transformed = df_transformed\
            .withColumn("redmpn_amt", F.when(F.col("cobol.transaction-type-cd") == '1', F.col("cobol.gross-amount"))
                        .otherwise(F.lit(0)))

        df_transformed = df_transformed\
            .groupBy("account.acct_key", "fund.fund_key", "prev_bus_day_key", "calendar_trade_dt.day_key",
                     "region.reg_key", "dealer.dlr_key",
                     "calendar_confirm_dt.day_key", "payment_method.pmt_mthd_key",
                     "cobol.noload-account-number", "dlr_acc.dlr_key", "cobol.order-number", "cobol.cash-control-reconcilement-cd")\
            .agg(F.sum(F.col("sub_shrs")).alias("sub_shrs_sum"),
                 F.sum(F.col("sub_amt")).alias("sub_amt_sum"),
                 F.sum(F.col("redmpn_shrs")).alias("redmpn_shrs_sum"),
                 F.sum(F.col("redmpn_amt")).alias("redmpn_amt_sum"),
                 F.avg(F.col("cobol.offering-price")).alias("ofrg_pr_avg"),
                 F.sum(F.col("cobol.net-asset-value-amt")).alias("nav_sum"),
                 F.sum(F.col("cobol.dealer-commission")).alias("dlr_commsn_amt_sum"),
                 F.sum(F.col("cobol.underwriter-commission")).alias("undr_wrtr_commsn_amt_sum"),
                 F.sum(F.col("cobol.Fin-tr-advanced-commission-at")).alias("adv_commsn_amt_sum"),
                 F.sum(F.col("cobol.gross-amount")).alias("gr_amt_sum"),
                 F.sum(F.col("cobol.shares")).alias("shrs_sum"))

        df_transformed = df_transformed.\
            withColumn("net_shrs_sum", F.col("sub_shrs_sum") - F.abs(F.col("redmpn_shrs_sum")))

        df_transformed = df_transformed.\
            withColumn("net_cashflow_amt_sum", F.col("sub_amt_sum") + F.col("redmpn_amt_sum") -
                       (F.col("dlr_commsn_amt_sum") + F.col("undr_wrtr_commsn_amt_sum") + F.col("adv_commsn_amt_sum")))

        # get records from sdcm_cashflow table that acct_key column is null
        df_sdcm_cashflow = common.utils.read_table_snapshot(
            table_name="sdcm_cashflow",
            env=self._env,
            spark=self._spark).alias("sdcm_cashflow")

        df_sdcm_cashflow = df_sdcm_cashflow.where(F.col("sdcm_cashflow.acct_key").isNull())

        df_sdcm_cashflow = df_sdcm_cashflow\
            .withColumn("gr_amt", F.when(F.col("sdcm_cashflow.sub_amt") != F.lit(0), F.col("sdcm_cashflow.sub_amt"))
                        .otherwise(F.col("sdcm_cashflow.redmpn_amt")))

        df_sdcm_cashflow = df_sdcm_cashflow\
            .withColumn("shrs", F.when(F.col("sdcm_cashflow.sub_amt") != F.lit(0), F.col("sdcm_cashflow.sub_shrs"))
                        .otherwise(F.col("sdcm_cashflow.redmpn_shrs")))

        # To identify the records that need to be updated in the table
        # we compering the df_transform to the df_sdcm_cashflow(sdcm_cashflow tbale)
        # that we loaded earlier and get records that the acct_key column from the cobol file is not null
        df_transformed = df_transformed\
            .join(df_sdcm_cashflow, [F.col("sdcm_cashflow.reg_key") == F.col("region.reg_key"),
                                     F.col("sdcm_cashflow.dlr_key") == F.col("dealer.dlr_key"),
                                     F.col("sdcm_cashflow.fund_key") == F.col("fund.fund_key"),
                                     F.col("sdcm_cashflow.trde_dt_key") == F.col("calendar_trade_dt.day_key"),
                                     F.col("shrs") == F.col("shrs_sum"),
                                     F.col("sdcm_cashflow.ord_num") == F.col("cobol.order-number"),
                                     F.col("gr_amt") == F.col("gr_amt_sum")
                                     ], how="inner")\
            .where(F.col("account.acct_key").isNotNull())

        return df_transformed

