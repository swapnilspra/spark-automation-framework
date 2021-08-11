from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "expense_ratio_history"
    business_key = ["fund_key"]
    primary_key = {"expns_ratio_hist_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "erh": {
            "type": "file",
            "source":"expense_ratio_history"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("erh.day_key"), "target": "day_key" },
        {"source": F.col("erh.fund_key"), "target": "fund_key"},
        { "source": F.to_timestamp("erh.efftv_strt_dt","MM/dd/yyyy"), "target": "efftv_strt_dt" },
        { "source": F.to_timestamp("erh.efftv_end_dt","MM/dd/yyyy"), "target": "efftv_end_dt" },
        {"source": F.col("erh.net_expns_ratio"), "target": "net_expns_ratio"},

    ]
