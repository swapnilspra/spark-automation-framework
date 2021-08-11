from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import common.utils
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_cip_letter"
    primary_key = {"aml_cip_ltr_key": "int"}
    business_key = ["cip_ltr_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "acl": {
            "type": "file",
            "source": "aml_cip_letter"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.to_timestamp(F.col("acl.created")), "target": "creatn_dt" },
        { "source": F.to_timestamp(F.col("acl.modified")), "target": "chg_dt" },
        { "source": F.to_timestamp(F.col("acl.date account opened")), "target": "acct_open_dt" },
        { "source": F.to_timestamp(F.col("acl.date account closed")), "target": "acct_closd_dt" },
        { "source": F.to_timestamp(F.col("acl.date letter sent")), "target": "ltr_sent_dt" },
        { "source": F.to_timestamp(F.col("acl.response deadline")), "target": "rspn_ddln_dt" },
        { "source": F.to_timestamp(F.col("acl.date expire")), "target": "expn_dt" },
        { "source": F.col("acl.id"), "target": "cip_ltr_id" },
        { "source": F.col("acl.year added"), "target": "ltr_yr" },
        { "source": F.col("acl.month added"), "target": "ltr_mo" },
        { "source": F.col("acl.aml or operations requirement"), "target": "ltr_type" },
        { "source": F.col("acl.shareholder(s)"), "target": "shrhldr_nm" },
        { "source": F.col("acl.missing cip information"), "target": "missg_cip_info" },
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("acl.additional comments"), '\r', ''), '\n', '')), "target": "addl_cmmts" },
        { "source": F.col("acl.amount redeemed when closed"), "target": "closg_amt_redmpn" },
        { "source": F.col("acl.created by"), "target": "creatr_nm" },
        { "source": F.col("acl.status"), "target": "stat" },
        { "source": F.col("acl.account number"), "target": "acct_nbr" },
        { "source": F.col("acl.added by"), "target": "creatr" },
        { "source": F.col("acl.modified by"), "target": "modfr" },
        { "source": F.lit('Y'), "target": "curr_row_flg" }
    ]
