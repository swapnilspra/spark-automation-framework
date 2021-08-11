from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "distribution_type"
    business_key = ["dstrbtn_type_cd"]
    primary_key = {"dstrbtn_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "distribution_type": {
            "type": "file",
            "source": "distribution_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("distribution_type.dstrbtn_type_cd"), "target": "dstrbtn_type_cd"},
        {"source": F.col("distribution_type.dstrbtn_type_desc"), "target": "dstrbtn_type_desc"}
    ]
