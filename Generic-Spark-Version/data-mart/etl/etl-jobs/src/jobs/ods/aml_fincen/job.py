from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import common.utils
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_fincen"
    primary_key = {"aml_fincen_key": "int"}
    business_key = ["fincen_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "af": {
            "type": "file",
            "source": "aml_fincen"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.to_timestamp(F.col("af.created")), "target": "creatn_dt" },
        { "source": F.to_timestamp(F.col("af.modified")), "target": "chg_dt" },
        { "source": F.to_timestamp(F.col("af.date fincen 314(a) list published")), "target": "fincen_list_pbsh_dt" },
        { "source": F.to_timestamp(F.col("af.date reviewed")), "target": "revwed_dt" },
        { "source": F.col("af.quarter"), "target": "qtr" },
        { "source": F.col("af.year"), "target": "yr" },
        { "source": F.col("af.number of fincen subjects"), "target": "fincen_subj_cnt" },
        { "source": F.col("af.number of alerts reviewed"), "target": "alrts_revw_cnt" },
        { "source": F.col("af.false positives"), "target": "false_pos_cnt" },
        { "source": F.col("af.unconfirmed hits"), "target": "uncnrfmd_hits_cnt" },
        { "source": F.col("af.confirmed hits"), "target": "confrmd_hits_cnt" },
        { "source": F.col("af.turn-around time"), "target": "turn_arnd_tm" },
        { "source": F.substring(F.col("af.special"), 1, 1), "target": "spcl_flg" },
        { "source": F.col("af.id"), "target": "fincen_id" },
        { "source": F.col("af.created by"), "target": "creatr" },
        { "source": F.col("af.modified by"), "target": "modfr" },
        { "source": F.lit('Y'), "target": "curr_row_flg" }
    ]

