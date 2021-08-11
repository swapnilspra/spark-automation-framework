from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "awd_privilege_type"
    business_key = ["prvl_type"]
    primary_key = {"prvl_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "pt": {
            "type": "file",
            "source": "awd_privilege_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("pt.prvtype"), "target": "prvl_type" },
        { "source": F.col("pt.prvtype"), "target": "prvl_type_desc" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]


    # Override the extraction method to filter null values of PRVTYPE
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        
        inputs = super().extract(catalog)

        inputs["pt"] = inputs["pt"].filter(F.col("prvtype").isNotNull()).select(F.col("prvtype")).distinct()

        return inputs