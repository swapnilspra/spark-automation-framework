from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "fund"
    business_key = ["fund_nbr"]
    primary_key = {"fund_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "fund": {
            "type": "file",
            "source": "fund"
        },
        "fund_composite": {
            "type": "table",
            "source": "fund_composite"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "fund"
        },
        {
            "source": "fund_composite",
            "conditions": [
                F.col("fund_composite.st_str_fund_nbr") == F.col("fund.st_str_fund_nbr")
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [

        { "source": F.lit("All"), "target": "all_fund_cd" },
        { "source": F.lit("All Funds"), "target": "all_fund_desc" },
        { "source": F.col("fund_composite.fund_compst_key"), "target": "fund_compst_key" },
        { "source": F.col("fund.shr_cls_cd"), "target": "shr_cls_cd" },
        { "source": F.col("fund.shr_cls_desc"), "target": "shr_cls_desc" },
        { "source": F.col("fund.fund_nm"), "target": "fund_nm" },
        { "source": F.col("fund.fund_long_desc"), "target": "fund_long_desc" },
        { "source": F.col("fund.fund_abrv"), "target": "fund_abrv" },
        { "source": F.col("fund.fund_stat_cd"), "target": "fund_stat_cd" },
        { "source": F.col("fund.fund_stat_desc"), "target": "fund_stat_desc" },
        { "source": F.to_timestamp("fund.fund_incpn_dt","MM/dd/yyyy"), "target": "fund_incpn_dt" },
        { "source": F.to_timestamp("fund.fund_clos_dt","MM/dd/yyyy"), "target": "fund_clos_dt" },
        { "source": F.col("fund.fund_nbr"), "target": "fund_nbr" },
        { "source": F.col("fund.fund_sort_ord"), "target": "fund_sort_ord" },
        { "source": F.col("fund.addr_id"), "target": "addr_id" },
        { "source": F.col("fund.cusip_id"), "target": "cusip_id" },
        { "source": F.col("fund.quot_sym"), "target": "quot_sym" },
        { "source": F.col("fund.st_str_fund_nbr"), "target": "st_str_fund_nbr" },
        { "source": F.col("fund.tax_id"), "target": "tax_id" },
        { "source": F.col("fund.rsk_lvl"), "target": "risk_lvl" },
        { "source": F.col("fund.divd_freq_cd"), "target": "divd_freq_cd" },
        { "source": F.col("fund.divd_freq_desc"), "target": "divd_freq_desc" },
        { "source": F.col("fund.cap_gain_freq_cd"), "target": "cap_gain_freq_cd" },
        { "source": F.col("fund.cap_gain_freq_desc"), "target": "cap_gain_freq_desc" },
        { "source": F.col("fund.reg_acct_init_invmt"), "target": "reg_acct_init_invmt" },
        { "source": F.col("fund.ira_acct_init_invmt"), "target": "ira_acct_init_invmt" },
        { "source": F.col("fund.ugma_acct_init_invmt"), "target": "ugma_acct_init_invmt" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" },
        { "source": F.to_timestamp("fund.perf_incpn_dt","MM/dd/yyyy"), "target": "perf_incpn_dt" }

    ]

