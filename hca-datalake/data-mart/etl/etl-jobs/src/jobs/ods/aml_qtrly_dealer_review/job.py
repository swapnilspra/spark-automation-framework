from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_qtrly_dealer_review"
    primary_key = {"aml_qtrly_dlr_revw_key": "int"}
    business_key = ["dlr_revw_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "aqdr": {
            "type": "file",
            "source": "aml_qtrly_dealer_review"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.to_timestamp("Modified"), "target": "chg_dt"},
        { "source": F.to_timestamp("Created"), "target": "creatn_dt"},
        { "source": F.to_timestamp("Due Date"), "target": "due_dt"},
        { "source": F.to_timestamp("Request Date"), "target": "rqst_dt"},
        { "source": F.to_timestamp("Completion Date"), "target": "cpltn_dt"},
        { "source": F.col("Created By"), "target": "creatr"},
        { "source": F.col("Modified By"), "target": "modfr"},
        { "source": F.col("ID"), "target": "dlr_revw_id" },
        { "source": F.col("Firm"), "target": "firm_nm" },
        { "source": F.col("Priority"), "target": "prio" },
        { "source": F.col("Status"), "target": "stat" },
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("Comments"), '\r', ''), '\n', '')), "target": "cmmts" },
        { "source": F.col("Requested By"), "target": "rqstr" },
        { "source": F.col("Assigned To"), "target": "assgne" },
        { "source": F.col("Aging").cast('integer'), "target": "agng" },
        { "source": F.col("Dealer Number"), "target": "dlr_id" },
        { "source": F.col("Reporting Period"), "target": "rptg_qtr" },
        { "source": F.col("Year"), "target": "rptg_yr" },
        { "source": F.when(F.col("Regulatory Violation") == True, 1)
            .otherwise(
                F.when(F.col("Regulatory Violation") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "reglty_vltn_flg" },
        { "source": F.when(F.col("Broker Check") == True, 1)
            .otherwise(
                F.when(F.col("Broker Check") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "brkr_flg" },
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("AML Review"), '\r', ''), '\n', '')), "target": "aml_revw" },
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("Market Timing Review"), '\r', ''), '\n', '')), "target": "mkt_tmng_revw" },
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("Other Regulatory Findings or Events"), '\r', ''), '\n', '')), "target": "othr_reglty_fndg" },
        { "source": F.col("Dealer Group ID"), "target": "dlr_grp_id" },
        { "source": F.when(F.col("AML Re-certification Sent") == True, 1)
            .otherwise(
                F.when(F.col("AML Re-certification Sent") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "aml_recertfn_flg" },
        { "source": F.when(F.col("Other Re-certification Sent") == True, 1)
            .otherwise(
                F.when(F.col("Other Re-certification Sent") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "othr_recertfn_flg" },
        { "source": F.lit("Y"), "target": "curr_row_flg"}
    ]