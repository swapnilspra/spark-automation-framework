from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "fund_expense_ratio"
    business_key = ["fund_key", "efftv_strt_dt", "efftv_end_dt", "per_flg"]
    primary_key = {"fund_expns_ratio_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "fund_expense_ratio": {
            "type": "file",
            "source": "fund_expense_ratio"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "fund_expense_ratio"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.when(
                    F.col("fund_expense_ratio.fund_nbr").cast("int")<F.lit(2000), # handle old legacy fund number
                    F.lit(2000)+ F.col("fund_expense_ratio.fund_nbr").cast("int")
                ).otherwise( F.col("fund_expense_ratio.fund_nbr").cast("int"))
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [

        {"source": F.lit(None).cast(T.IntegerType()), "target": "expns_ratio_wavr_type_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.lit(None).cast(T.TimestampType()), "target": "semi_annl_rpt_dt"},
        {"source": F.to_timestamp(F.col("fund_expense_ratio.efftv_strt_dt"),"MM/dd/yyyy"), "target": "efftv_strt_dt"},
        {"source": F.to_timestamp(F.col("fund_expense_ratio.efftv_end_dt"),"MM/dd/yyyy"), "target": "efftv_end_dt"},
        {"source": F.col("fund_expense_ratio.cayman_ratio"), "target": "cayman_ratio"},
        {"source": F.col("fund_expense_ratio.gr_expns_ratio"), "target": "gr_expns_ratio"},
        {"source": F.col("fund_expense_ratio.exp_ratio_net"), "target": "net_expns_ratio"},
        {"source": F.col("fund_expense_ratio.expns_wavr_ratio"), "target": "expns_wavr_ratio"},
        {"source": F.col("fund_expense_ratio.manual_entry_flg"), "target": "manual_entry_flg"},
        {"source": F.col("fund_expense_ratio.per_flg"), "target": "per_flg"},
        {"source": F.col("fund_expense_ratio.pass_thru_mgmt_fee_wavr"), "target": "pass_thru_mgmt_fee_wavr"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"}
    ]

