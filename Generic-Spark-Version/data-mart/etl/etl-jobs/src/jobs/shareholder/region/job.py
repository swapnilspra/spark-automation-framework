from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "region"
    business_key = ["mktg_reg_id", "reg_type_cd", "st_cd", "glbl_reg_cd", "crty_cd"]
    primary_key = {"reg_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "region": {
            "type": "file",
            "source": "region"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("region.state_code"), "target": "st_cd"},
        {"source": F.col("region.state_name"), "target": "st_nm"},
        {"source": F.col("region.marketing_region_id"), "target": "mktg_reg_id"},
        {"source": F.col("region.marketing_reg_nm"), "target": "mktg_reg_nm"},
        {"source": F.col("region.country_cd"), "target": "crty_cd"},
        {"source": F.col("region.country_name"), "target": "crty_nm"},
        {"source": F.col("region.global_region_cd"), "target": "glbl_reg_cd"},
        {"source": F.col("region.global_region_name"), "target": "glbl_reg_nm"},
        {"source": F.col("region.region_type_cd"), "target": "reg_type_cd"},
        {"source": F.col("region.region_type_name"), "target": "reg_type_nm"},
        {"source": F.lit("ALL"), "target": "top_lvl_reg_cd"},
        {"source": F.lit("All Regions"), "target": "top_lvl_reg_nm"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

