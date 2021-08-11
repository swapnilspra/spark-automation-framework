from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "payment_method"
    business_key = ["pmt_mthd_cd"]
    primary_key = {"pmt_mthd_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "payment_method": {
            "type": "file",
            "source": "payment_method"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.lit("All"), "target": "pmt_mthd_top_lvl_cd"},
        {"source": F.lit("All Payment Methods"), "target": "pmt_mthd_top_lvl_desc"},
        {"source": F.col("payment_method.pmt_mthd_cd"), "target": "pmt_mthd_cd"},
        {"source": F.col("payment_method.pmt_mthd_desc"), "target": "pmt_mthd_desc"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

