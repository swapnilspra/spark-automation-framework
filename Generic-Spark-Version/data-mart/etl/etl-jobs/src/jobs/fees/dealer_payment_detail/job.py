from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob

class Job(ETLJob):
    target_table = "dealer_payment_detail"
    business_key = ["day_key","pmt_day_key","dlr_key","fund_key","dlr_branch_key","dlr_fee_type_key","acct_key","wk_ord_id"]
    primary_key = {"dlr_pmt_det_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "dpd": {
            "type": "file",
            "source": "R00857",
            "limit":None,
            "sort": "size",
            "ascending": False
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "pmt_cal": {
            "type": "table",
            "source": "calendar"
        },
        "db": {
            "type": "table",
            "source": "dealer_branch"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "acc": {
            "type": "table",
            "source": "account"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "dpt": {
            "type": "table",
            "source": "dealer_payout_type"
        },
        "dft": {
            "type": "table",
            "source": "dealer_fee_type"
        },
        "dpm": {
            "type": "table",
            "source": "dealer_payment_method"
        }
    }
    joins:List[Dict[str, Any]] = [
        {
            "source": "dpd"
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.col("dpd.tlr-pyo-per-end-dt"), 'yyyyMMdd') == F.to_date(F.col("cal.cal_day"))
            ]
        },
        {
            "source": "pmt_cal",
            "conditions": [
                F.add_months(F.to_date(F.col("dpd.tlr-pyo-per-end-dt"), 'yyyyMMdd'), 1) == F.to_date(
                    F.col("pmt_cal.cal_day"))
            ]
        },
        {
            "source": "db",
            "conditions": [
                F.trim(F.col("dpd.financial-inst-id")) == F.col("db.dlr_id"),
                F.trim(F.col("dpd.fincl-inst-brch-id")) == F.col("db.branch_id")
            ],
            "type": "left"
        },
        {
            "source": "dealer",
            "conditions": [
                F.trim(F.col("dpd.financial-inst-id")) == F.col("dealer.dlr_id")
            ],
            "type": "left"
        },
        {
            "source": "fund",
            "conditions": [
                F.trim(F.col("dpd.fund-code")) == F.col("fund.fund_nbr")
            ]
        },
        {
            "source": "dft",
            "conditions": [
                F.trim(F.col("dpd.tlr-cmpn-fee-cd")) == F.col("dft.dlr_fee_type_cd")
            ]
        },
        {
            "source": "dpm",
            "conditions": [
                F.trim(F.col("dpd.payment-method-cd")) == F.col("dpm.pmt_mthd_cd")
            ],
            "type": "left"
        },
        {
            "source": "acc",
            "conditions": [
                F.trim(F.col("dpd.account-number")) == F.col("acc.acct_nbr"),
                F.trim(F.col("dpd.fund-code")) == F.col("acc.fund_nbr")
            ]
        },
        {
            "source": "dpt",
            "conditions": [
                F.trim(F.col("dpd.tlr-payout-typ-cd")) == F.col("dpt.payout_type_cd")
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("cal.day_key"), "target": "day_key" },
        { "source": F.col("dealer.dlr_key"), "target": "dlr_key" },
        { "source": F.col("fund.fund_key"), "target": "fund_key" },
        { "source": F.col("db.dlr_branch_key"), "target": "dlr_branch_key" },
        {"source": F.col("dft.dlr_fee_type_key"), "target": "dlr_fee_type_key"},
        { "source": F.coalesce(F.col("dpm.dlr_pmt_mthd_key"), F.lit(1.0)), "target": "dlr_pmt_mthd_key" },
        { "source": F.col("acc.acct_key"), "target": "acct_key" },
        { "source": F.trim(F.col("dpd.tlr-pyo-wko-nbr-id")), "target": "wk_ord_id" },
        { "source": F.col("dpd.nscc-indicator-cd"), "target": "nscc_flg" },
        { "source": F.when(F.trim(F.col("dpd.nscc-name-use-cd"))=='', F.lit(None))\
            .otherwise(F.col("dpd.nscc-name-use-cd")), "target": "nscc_nm_use_flg" },
        { "source": F.trim(F.col("dpd.cmpn-payee-type-cd")), "target": "pmt_ofst_cd" },
        { "source": F.substring(F.col("dpd.tlr-cmpn-fee-cd"),1,3), "target": "payee_type" },
        { "source": F.col("dpd.split-compensation-cd"), "target": "splt_compnsn_flg" },
        { "source": F.when(F.trim(F.col("dpd.prv-cmpn-sch-orr-typ"))=='',F.lit(None) )\
            .otherwise(F.trim(F.col("dpd.prv-cmpn-sch-orr-typ"))), "target": "prev_compnsn_ovrd_type" },
        { "source": F.trim(F.col("dpd.elg-cmpn-sch-orr-typ")), "target": "eligbl_compnsn_ovrd_type" },
        { "source": F.col("dpt.payout_type_key"), "target": "payout_type_key" },
        { "source": F.col("dpd.pre-agree-basis-point-rt"), "target": "pre_agreed_shr_rt" },
        { "source": F.trim(F.col("dpd.pre-agree-asset-at")), "target": "pre_agreed_asset" },
        { "source": F.trim(F.col("dpd.pre-agree-compensation-at")), "target": "pre_agreed_fee" },
        { "source": F.trim(F.col("dpd.prv-split-compensation-at")), "target": "prev_splt_fee" },
        { "source": F.when(F.col("dpd.total-eligible-basis-point-rt")== F.lit(0.0), F.lit(None))
            .otherwise(F.col("dpd.total-eligible-basis-point-rt")), "target": "tot_eligbl_rt" },
        { "source": F.col("dpd.total-eligible-asset-at"), "target": "tot_eligbl_asset" },
        { "source": F.col("dpd.total-eligible-compensation-at"), "target": "tot_eligbl_fee" },
        { "source": F.col("dpd.total-compensation-at"), "target": "tot_compnsn" },
        { "source": F.col("dpd.eligible-basis-point-rt1"), "target": "avg_eligbl_bp" },
        { "source": F.col("dpd.eligible-asset-at1"), "target": "avg_eligbl_asset"},
        { "source": F.col("dpd.eligible-compensation-at1"), "target": "avg_eligbl_fees" },
        { "source": F.col("pmt_cal.day_key"), "target": "pmt_day_key" },
        {"source": F.lit(None), "target": "curr_row_flg"},
        {"source": F.current_timestamp(), "target": "row_strt_dttm"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.lit(1), "target": "etl_load_cyc_key"}
    ]