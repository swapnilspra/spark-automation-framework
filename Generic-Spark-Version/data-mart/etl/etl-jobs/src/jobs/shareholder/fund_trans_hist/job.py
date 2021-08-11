from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob
import common.utils


class Job(ETLJob):
    target_table = "fund_trans_hist"
    business_key = ["fund_txn_id", "fund_txn_id_2"]
    primary_key = {"txn_hist_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "tip02": {
            "type": "file",
            "source": "DTO.HCA.DFA.TIP02"
        },
        "sbt": {
            "type": "file",
            "source": "shareholder_bluesky_transaction"
        },
        "shareholder_tradeinfo": {
            "type": "file",
            "source": "shareholder_tradeinfo"
        },
        "transaction_type": {
            "type": "table",
            "source": "transaction_type"
        },
        "country": {
            "type": "dimension",
            "source": "dst_st_country"
        },
        "payment_method": {
            "type": "table",
            "source": "payment_method"
        },
        "region": {
            "type": "dimension",
            "source": "region"
        },
        "regionsale": {
            "type": "dimension",
            "source": "region"
        },
        "trade_entry_method": {
            "type": "table",
            "source": "trade_entry_method"
        },
        "dealer_level_control": {
            "type": "table",
            "source": "dealer_level_control"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "dealer_branch": {
            "type": "table",
            "source": "dealer_branch"
        },
        "trade_origin": {
            "type": "table",
            "source": "trade_origin"
        },
        "account": {
            "type": "table",
            "source": "account"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "trade_date_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "confirm_date_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "batch_date_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "spt_sheet_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "reason_code": {
            "type": "table",
            "source": "reason_code"
        },
        "social_security_status": {
            "type": "table",
            "source": "social_security_status"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "tip02"
        },
        {
            "source": "transaction_type",
            "conditions": [
                F.col("transaction_type.txn_sfx_cd") == F.col("tip02.transaction-suffix"),
                F.col("transaction_type.txn_cd") == F.col("tip02.transaction-code")
            ]
        },
        {
            "source": "country",
            "conditions": [
                F.col("country.dst_st_cd") == F.col("tip02.resident-state-code")
            ]
        },
        {
            "source": "region",
            "conditions": [
                F.col("country.st_cd") == F.col("region.st_cd"),
                F.col("country.crty_iso_cd") == F.col("region.crty_cd")
            ]
        },
        {
            "source": "reason_code",
            "conditions": [
                F.col("reason_code.rsn_cd") == F.col("tip02.as-of-code")
            ],
            "type": "leftouter"
        },
        {
            "source": "trade_entry_method",
            "conditions": [
                F.col("trade_entry_method.trde_entry_mthd_cd") == F.col("tip02.trade-entry-method-cde")
            ]
        },
        {
            "source": "dealer",
            "conditions": [
                F.col("dealer.dlr_id") == F.col("tip02.fins-id")
            ]
        },
        {
            "source": "dealer_level_control",
            "conditions": [
                F.col("dealer_level_control.dlr_lvl_cntl_cd") == F.col("tip02.dealer-control-level-code")
            ]
        },
        {
            "source": "payment_method",
            "conditions": [
                F.trim(F.col("payment_method.pmt_mthd_cd")) == F.trim(F.col("tip02.payment-method-cde"))
            ],
            "type": "left"
        },
        {
            "source": "dealer_branch",
            "conditions": [
                F.trim(F.col("dealer_branch.dlr_id")) == F.trim(F.col("tip02.fins-id")),
                F.trim(F.col("dealer_branch.branch_id")) == F.trim(F.col("tip02.dealer-branch-id"))
            ]
        },
        {
            "source": "trade_origin",
            "conditions": [
                F.col("trade_origin.trde_orig_id") == F.col("tip02.trade-origin-id")
            ],
            "type": "left"
        },
        {
            "source": "social_security_status",
            "conditions": [
                F.col("social_security_status.ssn_stat_cd") == F.col("tip02.social-security-nbr-status-cde")
            ]
        },
        {
            "source": "sbt",
            "conditions": [
                F.trim(F.col("sbt.trans_id1")) == F.trim(F.col("tip02.mutual-fund-transaction-id-1")),
                F.trim(F.col("sbt.trans_id2")) == F.trim(F.col("tip02.mutual-fund-transaction-id-2"))
            ],
            "type": "left"
        },
        {
            "source": "account",
            "conditions": [
                F.col("account.fund_nbr") == F.col("tip02.fund-code"),
                F.col("account.acct_nbr") == F.col("tip02.customer-account-number")
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.col("tip02.fund-code")
            ]
        },
        {
            "source": "trade_date_calendar",
            "conditions": [
                F.to_date(F.col("trade_date_calendar.cal_day")) == F.to_date(F.col("tip02.trade-date"), "yyyyMMdd")
            ]
        },
        {
            "source": "confirm_date_calendar",
            "conditions": [
                F.to_date(F.col("confirm_date_calendar.cal_day")) == F.to_date(F.col("tip02.confirm-date"), "yyyyMMdd")
            ]
        },
        {
            "source": "batch_date_calendar",
            "conditions": [
                F.to_date(F.col("batch_date_calendar.cal_day")) == F.to_date(F.col("tip02.batch-date"), "yyyyMMdd")
            ],
            "type": "left"
        },
        {
            "source": "spt_sheet_calendar",
            "conditions": [
                F.to_date(F.col("spt_sheet_calendar.cal_day"), 'MM/dd/yyyy') == F.to_date(F.col("tip02.spt_sheet_dt"),
                                                                                          "yyyyMMdd")
            ],
            "type": "cross"
            # needed because spark thinks it is a 'trivial' join since spt_sheet_dt is the same for all rows
        },
        {
            "source": "shareholder_tradeinfo",
            "conditions": [
                F.trim(F.col("shareholder_tradeinfo.nscc_control_nbr")) == F.coalesce(
                    F.col("tip02.nscc-control-number"), F.col("tip02.filler5"))
            ],
            "type": "left"
        },
        {
            "source": "regionsale",
            "conditions": [
                F.col("shareholder_tradeinfo.nscc_st_of_sale_id") == F.col("regionsale.st_cd")
            ],
            "type": "left"
        }
    ]

    # override extract to get the day key from the file header
    def extract(self, catalog: Dict[str, Any]) -> Dict[str, pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)
        # get the entry from the catalog
        source: Dict[Any, Any] = catalog[self.sources["tip02"]["source"]]
        file_location: str = common.utils.get_file_location(self._env["file_prefix"], source["path"])

        # XXX fix
        spt_sheet_dt = self._spark.read.text(file_location).limit(1).rdd.map(
            lambda line: line.value.split("FINANCIALDIRECT")[1][0:8]
            # the date is after "ACCT POSITION " and is 8 digits, yyyyMMdd
        ).collect()[0]
        print(f"spt_sheet_dt: {spt_sheet_dt}")
        df_inputs["tip02"] = df_inputs["tip02"].withColumn("spt_sheet_dt", F.to_date(F.lit(spt_sheet_dt), "yyyyMMdd"))
        df_inputs["tip02"] = df_inputs["tip02"].alias("tip02")
        return df_inputs

    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("transaction_type.txn_type_key"), "target": "txn_type_key"},
        {"source": F.col("region.reg_key"), "target": "reg_key"},
        {"source": F.coalesce(F.col("regionsale.reg_key"), F.col("region.reg_key")), "target": "reg_of_sale_key"},
        {"source": F.col("tip02.nscc-control-number"), "target": "nscc_cntl_nbr"},
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.lit(None), "target": "shrhldr_key"},
        {"source": F.col("tip02.mutual-fund-transaction-id-1"), "target": "fund_txn_id"},
        {"source": F.col("tip02.mutual-fund-transaction-id-2"), "target": "fund_txn_id_2"},
        {"source": F.col("account.acct_key"), "target": "acct_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("trade_date_calendar.day_key"), "target": "efftv_day_key"},
        {"source": F.col("confirm_date_calendar.day_key"), "target": "sttl_day_key"},
        {"source": F.col("confirm_date_calendar.day_key"), "target": "pmt_day_key"},
        {"source": F.col("batch_date_calendar.day_key"), "target": "batch_day_key"},
        {"source": F.when(F.trim(F.col("trade_origin.trde_orig_key")) == 'null', F.lit(1.0)) \
            .otherwise(F.coalesce(F.col("trade_origin.trde_orig_key"), F.lit(1.0))), "target": "trde_orig_key"},
        {"source": F.col("dealer_branch.dlr_branch_key"), "target": "dlr_branch_key"},
        {"source": F.col("reason_code.rsn_key"), "target": "rsn_key"},
        {"source": F.col("social_security_status.ssn_stat_key"), "target": "ssn_stat_key"},
        {"source": F.col("trade_entry_method.trde_entry_mthd_key"), "target": "trde_entry_mthd_key"},
        {"source": F.col("dealer_level_control.dlr_lvl_cntl_key"), "target": "dlr_lvl_cntl_key"},
        {"source": F.col("payment_method.pmt_mthd_key"), "target": "pmt_mthd_key"},
        {"source": F.col("tip02.customer-account-number"), "target": "acct_nbr"},
        {"source": F.col("tip02.loi-number"), "target": "ltr_of_intend_nbr"},
        {"source": F.col("tip02.batch-number"), "target": "batch_nbr"},
        {"source": F.col("tip02.discount-category"), "target": "discnt_cat"},
        {"source": F.col("tip02.order-number"), "target": "fund_ord_nbr"},
        {"source": F.when(F.trim(F.col("tip02.nav-code")) == 0, F.lit(None)).otherwise(F.col("tip02.nav-code")),
         "target": "nav_cd"},
        {"source": F.trim(F.col("tip02.nscc-nav-reason-code")), "target": "nscc_nav_rsn_cd"},
        {"source": F.col("tip02.shr-acct-price-sched-cde"), "target": "acct_pr_sched"},
        {"source": F.col("tip02.underwriter-comm-effect-cde"), "target": "undr_wrtr_commsn_cd"},
        {"source": F.when(F.trim(F.col("tip02.Fin-tr-cdsc-wv-rsn")) == 'null', F.lit('')) \
            .when(F.trim(F.col("tip02.fin-tr-cdsc-wv-rsn")) == F.lit(None), F.lit('')) \
            .otherwise(F.col("tip02.fin-tr-cdsc-wv-rsn")), "target": "cdsc_wavr_rsn_cd"},
        {"source": F.when(F.trim(F.col("tip02.nscc-sttr-wv-id")) == 0, F.lit('')) \
            .otherwise(F.trim(F.col("tip02.fin-tr-cdsc-wv-rsn"))), "target": "nscc_shrt_term_trader_wavr_rsn"},
        {"source": F.col("tip02.contribution-year"), "target": "contrbn_tax_yr"},
        {"source": F.round(F.col("tip02.transaction-shares-count"), 3), "target": "shrs"},
        {"source": F.round(F.col("tip02.per-share-price-amount"), 4), "target": "shr_pr"},
        {"source": F.round(F.col("tip02.transaction-gross-amount"), 4), "target": "gr_amt"},
        {"source": F.col("tip02.sales-charge-percent"), "target": "sales_pct"},
        {"source": F.col("tip02.dealer-commission-amount"), "target": "dlr_commsn_amt"},
        {"source": F.col("tip02.underwriter-commission-amount"), "target": "undr_wrtr_commsn_amt"},
        {"source": F.when(F.trim(F.col("tip02.net-asset-value-at")) == 'null', F.lit(2.0)) \
            .otherwise(F.coalesce(F.round(F.col("tip02.transaction-gross-amount"), 4), F.lit(2.0))),
         "target": "net_amt"},
        {"source": F.col("tip02.vol-tran-desc"), "target": "txn_desc"},
        {"source": F.col("fund.fund_key"), "target": "frto_fund_key"},
        {"source": F.col("tip02.share-balance-effect-code"), "target": "shr_bal_effct_flg"},
        {"source": F.lit(None), "target": "tpa_dlr_key"},
        {"source": F.col("tip02.operator-id"), "target": "opr_id"},
        {"source": F.col("spt_sheet_calendar.day_key"), "target": "spr_sheet_day_key"},
        {"source": F.col("sbt.bluesky_txn_flg"), "target": "bluesky_txn_flg"},
        {"source": F.col("sbt.bluesky_reopen_account_cde"), "target": "bluesky_reopen_acct_flg"},
        {"source": F.lit(1).cast("int"), "target": "etl_load_cyc_key"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.current_timestamp(), "target": "row_strt_dttm"}
    ]
