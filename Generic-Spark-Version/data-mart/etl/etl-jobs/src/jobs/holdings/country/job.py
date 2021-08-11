from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "country"
    business_key = ["crty_cd","crty_nm"]
    primary_key = {"crty_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "country"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("raw.crty_nm"), "target": "crty_nm"},
        {"source": F.when(F.col("raw.crty_reg") == F.lit('NULL'), F.lit(None))\
            .otherwise(F.col("raw.crty_reg")), "target": "crty_reg"},
        {"source": F.col("raw.crty_cd"), "target": "crty_cd"},
        {"source": F.when(F.col("raw.crty_factset_reg") == F.lit('NULL'), F.lit(None))\
            .otherwise(F.col("raw.crty_factset_reg")), "target": "crty_factset_reg"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.current_timestamp(), "target": "row_strt_dttm"},
        {"source": F.lit(None), "target": "row_stop_dttm"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.lit(None), "target": "etl_load_cyc_key"}
    ]