from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "external_fund_balance"
    business_key = ["fund_key"]
    primary_key = {"ext_fund_bal_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "efb": {
            "type": "file",
            "source": "external_fund_balance"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "external_fund_balance"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.col("efb.fund_id")
            ]
        },
        {
            "source": "calendar",
            "conditions": [
                F.col("calendar.cal_day") == F.col("efb.asofdate")
            ]
        },
    ]
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("efb.ext_src"), "target": "ext_src"},
        {"source": F.col("efb.curr_row_flg"), "target": "curr_row_flg"},
        {"source": F.col("efb.ext_fund_bal_key"), "target": "ext_fund_bal_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("calendar.Cal_Key"), "target": "Cal_Key"},
        {"source": F.col("efb.shrs"), "target": "shrs"},
        {"source": F.col("efb.bal_amt"), "target": "bal_amt"},
        { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key" },
        {"source": F.col("efb.src_sys_id"), "target": "src_sys_id"},
        {"source": F.col("efb.row_strt_dttm"), "target": "row_strt_dttm"},
        {"source": F.col("efb.row_stop_dttm"), "target": "row_stop_dttm"},
        {"source": F.col("efb.offr_id"), "target": "offr_id"},
        {"source": F.col("efb.offr_nm"), "target": "offr_nm"}
    ]
