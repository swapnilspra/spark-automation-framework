from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "account_category"
    business_key = ["acct_cat_ord"]
    primary_key = {"acct_cat_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "account_category": {
            "type": "file",
            "source": "account_category"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("account_category.acct_cat_desc"), "target": "acct_cat_desc"},
        {"source": F.col("account_category.maj_cat_cd"), "target": "maj_cat_cd"},
        {"source": F.col("account_category.maj_cat_desc"), "target": "maj_cat_desc"},
        {"source": F.col("account_category.acct_cat_ord"), "target": "acct_cat_ord"}
    ]
