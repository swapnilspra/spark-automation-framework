from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "distribution_code"
    business_key = ["dstrbtn_cd"]
    primary_key = {"dstrbtn_cd_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "distribution_code": {
            "type": "file",
            "source": "distribution_code"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("distribution_code.dstrbtn_cd"), "target": "dstrbtn_cd"},
        {"source": F.col("distribution_code.dstrbtn_desc"), "target": "dstrbtn_desc"}
    ]
