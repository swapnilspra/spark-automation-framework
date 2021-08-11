from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "cost_type"
    business_key = ["cost_type_id"]
    primary_key = {"cost_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "cost_type": {
            "type": "file",
            "source": "cost_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("cost_type.cost_type_id"), "target": "cost_type_id"},
        {"source": F.col("cost_type.cost_type_desc"), "target": "cost_type_desc"}
    ]