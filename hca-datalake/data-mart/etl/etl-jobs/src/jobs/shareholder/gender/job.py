from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "gender"
    business_key = ["gend_cd"]
    primary_key = {"gend_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "gender": {
            "type": "file",
            "source": "gender"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("gender.gend_cd"), "target": "gend_cd"},
        {"source": F.col("gender.gend_nm"), "target": "gend_nm"}
    ]