from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import common.utils
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_blacklist_country"
    primary_key = {"aml_blklist_crty_key": "int"}
    business_key = ["blklist_crty_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "abc": {
            "type": "file",
            "source": "aml_blacklist_country"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.col("abc.country"), "target": "crty" },  
        { "source": F.to_timestamp(F.col("abc.date added")), "target": "list_add_dt" },  
        { "source": F.to_timestamp(F.col("abc.date removed")), "target": "rmvl_dt" },  
        { "source": F.to_timestamp(F.col("abc.modified")), "target": "chg_dt" },  
        { "source": F.to_timestamp(F.col("abc.created")), "target": "creatn_dt" },  
        { "source": F.col("abc.supporting documentation"), "target": "sprtg_docn_link" },  
        { "source": F.col("abc.status"), "target": "stat" },  
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("abc.commentary"), '\r', ''), '\n', '')), "target": "cmmts" },  
        { "source": F.col("abc.id"), "target": "blklist_crty_id" },
        { "source": F.col("abc.organization"), "target": "org" },
        { "source": F.col("abc.created by"), "target": "creatr" },  
        { "source": F.col("abc.modified by"), "target": "modfr" },  
        { "source": F.lit('Y'), "target": "curr_row_flg" }
    ]