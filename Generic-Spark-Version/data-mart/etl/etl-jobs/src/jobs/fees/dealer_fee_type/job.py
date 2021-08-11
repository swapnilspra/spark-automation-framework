from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "dealer_fee_type"
    business_key = ["dlr_fee_type_cd"]
    primary_key = {"dlr_fee_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "dealer_fee_type": {
            "type": "file",
            "source": "dealer_fee_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("dealer_fee_type.dlr_fee_type_cd"), "target": "dlr_fee_type_cd"},
        {"source": F.col("dealer_fee_type.dlr_fee_type_desc"), "target": "dlr_fee_type_desc"}

    ]

