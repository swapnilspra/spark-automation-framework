from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from itertools import chain

class Job(ETLJob):
    target_table = "eligible_asset_list"
    business_key = ["dlr_key", "shr_cls_cd"]
    primary_key = {"eligbl_asset_list_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "eligible_asset_list"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "raw"
        },
        {
            "source": "dealer",
            "conditions": [
               F.col("dealer.dlr_id") == F.col("raw.dlr_id")
            ]
        }
    ]

    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.col("raw.inlsn_excln_flg"), "target": "inlsn_excln_flg"},
        {"source": F.col("raw.cum_discnt_nbr"), "target": "cum_discnt_nbr"},
        {"source": F.col("raw.shr_cls_cd"), "target": "shr_cls_cd"},
        {"source": F.col("raw.qlfyd_rtrmt_flg"), "target": "qlfyd_rtrmt_flg"},
        {"source": F.lit(None), "target": "efftv_strt_dt"},
        {"source": F.lit(None), "target": "efftv_end_dt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "etl_load_cyc_key"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"}
    ]