from typing import Dict, List, Any
import pyspark
import pyspark.sql.functions as F
from common.etl_job import ETLJob


class Job(ETLJob):
    target_table = "dealer_payment_trial"
    business_key = ["invc_day_key","pmt_day_key", "dlr_key", "fund_key", "dlr_branch_key", "dlr_fee_type_key",  "acct_key","wk_ord_id"]
    primary_key = {"dlr_pmt_trl_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "dptrial": {
            "type": "file",
            "source": "R00857_TRIAL",
            "limit": None,
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
    joins: List[Dict[str, Any]] = [
        {
            "source": "dptrial"
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.col("dptrial.tlr-pyo-per-end-dt"), 'yyyyMMdd') == F.to_date(F.col("cal.cal_day"))
            ]
        },
        {
            "source": "pmt_cal",
            "conditions": [
                F.add_months(F.to_date(F.col("dptrial.tlr-pyo-per-end-dt"), 'yyyyMMdd'), 1) == F.to_date(
                    F.col("pmt_cal.cal_day"))
            ]
        },
        {
            "source": "dealer",
            "conditions": [
                F.trim(F.col("dptrial.financial-inst-id")) == F.col("dealer.dlr_id")
            ],
            "type": "left"
        },
        {
            "source": "db",
            "conditions": [
                F.trim(F.col("dptrial.financial-inst-id")) == F.col("db.dlr_id"),
                F.trim(F.col("dptrial.fincl-inst-brch-id")) == F.col("db.branch_id")
            ],
            "type":"left"
        },
        {
            "source": "fund",
            "conditions": [
                F.trim(F.col("dptrial.fund-code")) == F.col("fund.fund_nbr")
            ]
        },
        {
            "source": "dft",
            "conditions": [
                F.trim(F.col("dptrial.tlr-cmpn-fee-cd")) == F.col("dft.dlr_fee_type_cd")
            ]
        },
        {
            "source": "dpm",
            "conditions": [
                F.trim(F.col("dptrial.payment-method-cd")) == F.col("dpm.pmt_mthd_cd")
            ],
            "type": "left"
        },
        {
            "source": "acc",
            "conditions": [
                F.trim(F.col("dptrial.account-number")) == F.col("acct_nbr"),
                F.trim(F.col("dptrial.fund-code")) == F.col("acc.fund_nbr")
            ]
        },
        {
            "source": "dpt",
            "conditions": [
                F.trim(F.col("dptrial.tlr-payout-typ-cd")) == F.col("dpt.payout_type_cd")
            ]
            # F.col("db.dlr_id") == 0]
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("db.dlr_branch_key"), "target": "dlr_branch_key"},
        {"source": F.col("dft.dlr_fee_type_key"), "target": "dlr_fee_type_key"},
        {"source": F.coalesce(F.col("dpm.dlr_pmt_mthd_key"), F.lit(1.0)), "target": "dlr_pmt_mthd_key"},
        {"source": F.col("acc.acct_key"), "target": "acct_key"},
        {"source": F.trim(F.col("dptrial.tlr-pyo-wko-nbr-id")), "target": "wk_ord_id"},
        {"source": F.col("dptrial.nscc-indicator-cd"), "target": "nscc_flg"},
        {"source": F.when(F.trim(F.col("dptrial.nscc-name-use-cd")) == '', F.lit(None)) \
            .otherwise(F.col("dptrial.nscc-name-use-cd")), "target": "nscc_nm_use_flg"},
        {"source": F.col("dptrial.tlr-cmpn-fee-cd"), "target": "pmt_ofst_cd"},
        {"source": F.trim(F.col("dptrial.cmpn-payee-type-cd")), "target": "payee_type"},
        {"source": F.col("dptrial.split-compensation-cd"), "target": "splt_compnsn_flg"},
        {"source": F.when(F.trim(F.col("dptrial.prv-cmpn-sch-orr-typ")) == '', F.lit(None)) \
            .otherwise(F.trim(F.col("dptrial.prv-cmpn-sch-orr-typ"))), "target": "prev_compnsn_ovrd_type"},
        {"source": F.trim(F.col("dptrial.elg-cmpn-sch-orr-typ")), "target": "eligbl_compnsn_ovrd_type"},
        {"source": F.col("dpt.payout_type_key"), "target": "payout_type_key"},
        {"source": F.trim(F.col("dptrial.pre-agree-basis-point-rt")), "target": "pre_agrd_shr_rt"},
        {"source": F.trim(F.col("dptrial.pre-agree-asset-at")), "target": "pre_agrd_asset"},
        {"source": F.trim(F.col("dptrial.pre-agree-compensation-at")), "target": "pre_agrd_fee"},
        {"source": F.trim(F.col("dptrial.prv-split-compensation-at")), "target": "prev_splt_fee"},
        {"source": F.when(F.col("dptrial.total-eligible-basis-point-rt")== F.lit(0.0), F.lit(None))
            .otherwise(F.col("dptrial.total-eligible-basis-point-rt")), "target": "tot_eligbl_rt"},
        {"source": F.col("dptrial.total-eligible-asset-at"), "target": "tot_eligbl_asset"},
        {"source": F.col("dptrial.total-eligible-compensation-at"), "target": "tot_eligbl_fee"},
        {"source": F.col("dptrial.total-compensation-at"), "target": "tot_compnsn"},
        {"source": F.col("dptrial.eligible-basis-point-rt1"), "target": "avg_eligbl_bp"},
        {"source": F.col("dptrial.eligible-asset-at1"), "target": "avg_eligbl_asset"},
        {"source": F.col("dptrial.eligible-compensation-at1"), "target": "avg_eligbl_fees"},
        {"source": F.col("pmt_cal.day_key"), "target": "pmt_day_key"},
        {"source": F.col("cal.day_key"), "target": "invc_day_key"},
        {"source": F.lit(None), "target": "curr_row_flg"},
        {"source": F.current_timestamp(), "target": "row_strt_dttm"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.lit(1), "target": "etl_load_cyc_key"}
    ]