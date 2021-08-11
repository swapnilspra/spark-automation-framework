from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "portfolio_manager"
    business_key = ["pm_legacy_id"]
    primary_key = {"pm_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "portfolio_manager": {
            "type": "file",
            "source": "portfolio_manager"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("portfolio_manager.pm_legacy_id"), "target": "pm_legacy_id"},
        {"source": F.col("portfolio_manager.pm_nm"), "target": "pm_nm"},
        {"source": F.to_timestamp(F.col("portfolio_manager.relshp_strt_dt"),"MM/dd/yy"), "target": "relshp_strt_dt"},
        {"source": F.to_timestamp(F.col("portfolio_manager.relshp_end_dt"),"MM/dd/yy"), "target": "relshp_end_dt"},
        {"source": F.col("portfolio_manager.actv_relshp_flg"), "target": "actv_relshp_flg"}
    ]
