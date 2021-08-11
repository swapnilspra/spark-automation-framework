from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "transaction_type"
    business_key = ["txn_sfx_cd", "txn_cd"]
    primary_key = {"txn_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "transaction_type": {
            "type": "file",
            "source": "transaction_type"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("transaction_type.txn_sfx_cd"), "target": "txn_sfx_cd"},
        {"source": F.col("transaction_type.txn_cd"), "target": "txn_cd"},
        {"source": F.col("transaction_type.txn_cd_desc"), "target": "txn_cd_desc"},
        {"source": F.col("transaction_type.txn_type_cd"), "target": "txn_type_cd"},
        {"source": F.col("transaction_type.txn_type_desc"), "target": "txn_type_desc"},
        {"source": F.lit("All"), "target": "top_lvl_txn_cd"},
        {"source": F.lit("All Transactions"), "target": "top_lvl_txn_desc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"},
        {"source": F.col("transaction_type.shr_bal_effct_flg"), "target": "shr_bal_effct_flg"}
    ]

