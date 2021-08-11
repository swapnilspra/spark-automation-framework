from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "dealer_payout_type"
    business_key = ["payout_type_cd"]
    primary_key = {"payout_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "dpt": {
            "type": "file",
            "source": "dealer_payout_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("dpt.payout_type_cd"), "target": "payout_type_cd"},
        {"source": F.col("dpt.payout_type_desc"), "target": "payout_type_desc"}
    ]

