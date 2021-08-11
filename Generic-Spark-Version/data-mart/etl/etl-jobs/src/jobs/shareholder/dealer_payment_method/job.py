from typing import Dict, List, Any
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "dealer_payment_method"
    business_key = ["pmt_mthd_cd"]
    primary_key = {"dlr_pmt_mthd_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "dpm": {
            "type": "file",
            "source": "dealer_payment_method"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("dpm.pmt_mthd_cd"), "target": "pmt_mthd_cd"},
        {"source": F.col("dpm.pmt_mthd_desc"), "target": "pmt_mthd_desc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"}
    ]

