from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "exchange_type"
    business_key = ["xchg_type_cd"]
    primary_key = {"xchg_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "exchange_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("raw.xchg_type_cd"), "target": "xchg_type_cd"},
        {"source": F.col("raw.xchg_type_desc"), "target": "xchg_type_desc"}
    ]