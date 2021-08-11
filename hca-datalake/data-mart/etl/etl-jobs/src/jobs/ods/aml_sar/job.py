from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import common.utils
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_sar"
    primary_key = {"sar_key": "int"}
    business_key = ["sar_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "as": {
            "type": "file",
            "source": "aml_sar"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.to_timestamp(F.col("created")), "target": "creatn_dt" },
        { "source": F.to_timestamp(F.col("modified")), "target": "chg_dt" },
        { "source": F.to_timestamp(F.col("date account activity was raised as a concern")), "target": "act_crcn_rsd_dt" },
        { "source": F.to_timestamp(F.col("date sar sent to amlco for review")), "target": "sar_amlco_revw_dt" },
        { "source": F.to_timestamp(F.col("date amlco filed sar with fincen")), "target": "sar_fincen_filng_dt" },
        { "source": F.to_timestamp(F.col("date amlcos were notified of activity")), "target": "amlco_act_ntfn_dt" },
        { "source": F.to_timestamp(F.col("deadline for sar filing")), "target": "sar_filng_ddln_dt" },
        { "source": F.col("id"), "target": "sar_id" },
        { "source": F.col("created by"), "target": "creatr" },
        { "source": F.col("modified by"), "target": "modfr" },
        { "source": F.col("name of person/entity under suspicion"), "target": "suspect_nm" },
        { "source": F.col("account number"), "target": "acct_nbr" },
        { "source": F.col("document control number"), "target": "doc_cntl_nbr" },
        { "source": F.substring(F.trim(F.regexp_replace(F.regexp_replace(F.col("reason for suspicion"), '\r', ''), '\n', '')), 1, 4000), "target": "suspicion_rsn" },
        { "source": F.substring(F.col("filed with fincen (yes/no/in progress)"), 1, 1), "target": "fincen_filed_flg" },
        { "source": F.col("year sar filed with fincen"), "target": "sar_fincen_filng_yr" },
        { "source": F.col("sar type"), "target": "sar_type" },
        { "source": F.col("sar category"), "target": "sar_cat" },
        { "source": F.col("supporting documentation (livelink)"), "target": "sprtg_doc_link" },
        { "source": F.col("cyber event").cast("integer"), "target": "cyber_evt_flg" },
        { "source": F.lit('Y'), "target": "curr_row_flg" }
    ]