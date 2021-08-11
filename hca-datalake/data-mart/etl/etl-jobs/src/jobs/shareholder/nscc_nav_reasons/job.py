from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob

class Job(ETLJob):
    target_table = "nscc_nav_reasons"
    business_key = ["rsn_cd"]
    primary_key = {"rsn_cd": "string"}
    sources:Dict[str,Dict[str,Any]] = {
        "nnr": {
            "type": "file",
            "source": "nscc_nav_reasons"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("nnr.rsn_cd"), "target": "rsn_cd"},
        {"source": F.col("nnr.rsn_desc"), "target": "rsn_desc"}
    ]

