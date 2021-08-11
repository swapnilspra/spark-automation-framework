from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "external_fund_list"
    business_key = ["ext_fund_id"]
    primary_key = {"ext_fund_list_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "efl": {
            "type": "file",
            "source": "external_fund_list"
        }
    }

    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("efl.ext_fund_id"), "target": "ext_fund_id"},
        {"source": F.col("efl.ext_fund_desc"), "target": "ext_fund_desc"},
        {"source": F.col("efl.curr_row_flg"), "target": "curr_row_flg"},
        {"source": F.col("efl.fund_tickr_sym"), "target": "fund_tickr_sym"},
        {"source": F.col("efl.ext_fund_list_key"), "target": "ext_fund_list_key"},
        {"source": F.col("efl.fund_key"), "target": "fund_key"},
        {"source": F.col("efl.etl_load_cyc_key"), "target": "etl_load_cyc_key"},
        {"source": F.col("efl.src_sys_id"), "target": "src_sys_id"},
        {"source": F.col("efl.row_strt_dttm"), "target": "row_strt_dttm"},
        {"source": F.col("efl.row_stop_dttm"), "target": "row_stop_dttm"}
    ]
