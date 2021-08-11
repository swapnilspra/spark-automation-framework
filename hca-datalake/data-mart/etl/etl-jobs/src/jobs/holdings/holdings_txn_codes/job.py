from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "holdings_txn_codes"
    business_key = ["txn_type_cd","txn_cd"]
    primary_key = {"holdg_txn_cd_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "holdings_txn_codes"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("raw.txn_type_cd"), "target": "txn_type_cd"},
        {"source": F.col("raw.txn_type_desc"), "target": "txn_type_desc"},
        {"source": F.col("raw.txn_cd"), "target": "txn_cd"},
        {"source": F.col("raw.txn_cd_desc"), "target": "txn_cd_desc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None), "target": "row_strt_dttm"},
        {"source": F.lit(None), "target": "row_stop_dttm"},
        {"source": F.lit(None), "target": "src_sys_id"},
        {"source": F.lit(None), "target": "etl_load_cyc_key"}
    ]