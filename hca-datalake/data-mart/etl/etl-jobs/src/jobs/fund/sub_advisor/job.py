from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "sub_advisor"
    business_key = ["sba_legacy_id"]
    primary_key = {"sba_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "sub_advisor": {
            "type": "file",
            "source": "sub_advisor"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("sub_advisor.sba_legacy_id"), "target": "sba_legacy_id"},
        {"source": F.col("sub_advisor.sba_nm"), "target": "sba_nm"},
        {"source": F.to_timestamp(F.col("sub_advisor.relshp_strt_dt"),"MM/dd/yy"), "target": "relshp_strt_dt"},
        {"source": F.to_timestamp(F.col("sub_advisor.relshp_end_dt"),"MM/dd/yy"), "target": "relshp_end_dt"},
        {"source": F.col("sub_advisor.actv_relshp_flg"), "target": "actv_relshp_flg"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast(T.TimestampType()), "target": "row_updt_dttm"},
        { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key" },        
        { "source": F.lit(4), "target": "src_sys_id" }
    ]
