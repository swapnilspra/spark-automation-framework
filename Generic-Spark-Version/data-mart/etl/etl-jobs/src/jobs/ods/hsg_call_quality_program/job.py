from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "hsg_call_quality_program"
    primary_key = {"cq_prog_key": "int"}
    business_key = ["cq_prog_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "hcqp": {
            "type": "file",
            "source": "hsg_call_quality_program"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.col("Title"), "target": "titl" },
        { "source": F.to_timestamp("Call Review Date"), "target": "call_revw_dt" },
        { "source": F.trim(F.regexp_replace(F.col("Call Reviewer Name"), '\r', ' ')), "target": "call_revwr_nm" },
        { "source": F.col("Call Type"), "target": "call_type" },
        { "source": F.col("Inbound/Outbound"), "target": "call_drctn" },
        { "source": F.col("Representative Name"), "target": "rep_nm" },
        { "source": 
            F.when(F.col("Training with Mentor?") == True, 1)
            .otherwise(
                F.when(F.col("Training with Mentor?") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "mntr_trng_flg" },
        { "source": F.regexp_extract(F.col("VCC Transaction ID"), "\d+", 0).cast('double'), "target": "vcc_txn_id" },
        { "source": F.to_timestamp("Call Date"), "target": "call_dt" },
        { "source": F.substring(F.trim(F.col("Account Number")), 1, 25), "target": "acct_nbr" },
        { "source": F.trim(F.col("Caller Name")), "target": "caller_nm" },
        { "source": F.col("Score - Greeting"), "target": "greetg_scor" },
        { "source": F.col("Score - Presentation-Behavior"), "target": "pres_behvr_scor" },
        { "source": F.col("Score - Information"), "target": "info_scor" },
        { "source": F.col("Score - Questions-Focus Call"), "target": "call_qstn_focus_scor" },
        { "source": F.col("Score - Holds/Transfers"), "target": "hold_xfer_scor" },
        { "source": F.col("Score - Closing"), "target": "closg_scor" },
        { "source": F.trim(F.regexp_replace(F.col("Reviewer Comments"), '\r', ' ')), "target": "revwr_cmmts" },
        { "source": F.trim(F.regexp_replace(F.col("Other Comments"), '\r', ' ')), "target": "othr_cmmts" },
        { "source": F.col("Pre Total Score"), "target": "pre_tot_scor" },
        { "source": F.col("Pre Total Score 2"), "target": "pre_tot_scor_2" },
        { "source": F.col("Total Call Score"), "target": "tot_call_scor" },
        { "source": 
            F.when(F.col("Add to Service Library") == True, 1)
            .otherwise(
                F.when(F.col("Add to Service Library") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "svc_lbry_add_flg" },
        { "source": F.col("Mentor Name"), "target": "mntr_nm" },
        { "source": F.col("Manager Name"), "target": "mgr_nm" },
        { "source": 
            F.when(F.col("Missed Opportunity for Added Value?") == True, 1)
            .otherwise(
                F.when(F.col("Missed Opportunity for Added Value?") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "opty_addd_val_missd_flg" },
        { "source": 
            F.when(F.col("Opportunity for Added Value") == True, 1)
            .otherwise(
                F.when(F.col("Opportunity for Added Value") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "opty_addd_val_flg" },
        { "source": F.trim(F.regexp_replace(F.col("Comm Call Review"), '\r', ' ')), "target": "call_revw_cmmt" },
        { "source": F.to_timestamp("Modified"), "target": "chg_dt" },
        { "source": F.col("ID"), "target": "cq_prog_id" },
        { "source": F.to_timestamp("Created"), "target": "creatn_dt" },
        { "source": F.col("Created By"), "target": "creatr" },
        { "source": F.col("Modified By"), "target": "modfr" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
    ]
