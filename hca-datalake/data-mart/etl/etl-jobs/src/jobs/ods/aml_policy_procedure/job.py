from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import common.utils
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_policy_procedure"
    primary_key = {"pol_procd_key": "int"}
    business_key = ["aml_pol_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "app": {
            "type": "file",
            "source": "aml_policy_procedure"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.to_timestamp(F.col("created")), "target": "creatn_dt" },
        { "source": F.to_timestamp(F.col("modified")), "target": "chg_dt" },
        { "source": F.to_timestamp(F.col("date approved or rejected")), "target": "apprvl_dt" },
        { "source": F.to_timestamp(F.col("effective date")), "target": "efftv_dt" },
        { "source": F.col("id"), "target": "aml_pol_id" },
        { "source": F.col("created by"), "target": "creatr" },
        { "source": F.col("modified by"), "target": "modfr" },
        { "source": F.col("high level policy name"), "target": "hi_lvl_pol_nm" },
        { "source": F.trim(F.col("recommended changes")), "target": "chg_recmn" },
        { "source": F.col("management review"), "target": "mgmt_revwer" },
        { "source": F.col("executive review"), "target": "exec_revwer" },
        { "source": F.col("status"), "target": "stat" },
        { "source": F.when(F.col("aml related") == True, '1').otherwise('0'), "target": "aml_reld_flg" },
        { "source": F.col("year"), "target": "yr" },
        { "source": F.col("policy #"), "target": "pol_cd" },
        { "source": F.lit('Y'), "target": "curr_row_flg" }
    ]