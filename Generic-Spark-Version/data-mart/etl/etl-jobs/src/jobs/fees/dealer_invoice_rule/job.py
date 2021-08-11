from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "dealer_invoice_rule"
    business_key = ["dlr_key","shr_cls_cd","trgt_bus_line","src_bus_line"]
    primary_key = {"invc_dlr_rule_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "dealer_invoice_rule"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "raw"
        },
        {
            "source": "dealer",
            "conditions": [
                F.col("raw.dlr_id") == F.col("dealer.dlr_id")
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.col("raw.shr_cls_cd"), "target": "shr_cls_cd"},
        {"source": F.col("raw.trgt_bus_line"), "target": "trgt_bus_line"},
        {"source": F.col("raw.src_bus_line"), "target": "src_bus_line"},
        {"source": F.col("raw.calc_flg"), "target": "calc_flg"},
        {"source": F.col("raw.mul_src_bl_flg"), "target": "mul_src_bl_flg"},
        {"source": F.col("raw.fee_used_in_fee_flg"), "target": "fee_used_in_fee_flg"},
        {"source": F.col("raw.bp_value"), "target": "bp_value"},
        {"source": F.col("raw.cnt_me_only_me_flg"), "target": "cnt_me_only_me_flg"},
        {"source": F.to_timestamp(F.col("raw.efftv_strt_dt"), "MM/dd/yyyy"), "target": "efftv_strt_dt"},
        {"source": F.to_timestamp(F.col("raw.efftv_end_dt"), "MM/dd/yyyy"), "target": "efftv_end_dt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None), "target": "etl_load_cyc_key"},
        {"source": F.lit(None), "target": "src_sys_id"}
    ]

