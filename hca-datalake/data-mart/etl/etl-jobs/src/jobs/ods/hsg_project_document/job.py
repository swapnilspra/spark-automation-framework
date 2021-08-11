from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "hsg_project_document"
    primary_key = {"hsg_proj_doc_key": "int"}
    business_key = ["proj_doc_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "hpd": {
            "type": "file",
            "source": "hsg_project_document"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.col("Title"), "target": "proj_titl" },
        { "source": F.col("Project Manager"), "target": "proj_mgr" },
        { "source": F.col("Project Name"), "target": "proj_nm" },
        { "source": F.col("Type of Document"), "target": "doc_type" },
        { "source": 
            F.when(F.col("Agenda Item") == True, 1)
            .otherwise(0), "target": "agnd_item_flg" },  
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Objective"), '\r', ' ')), 1, 4000), "target": "objv" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Change Request"), '\r', ' ')), 1, 4000), "target": "chg_rqst" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Project Deliverables"), '\r', '')), 1, 4000), "target": "proj_dlvbl" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Milestones"), '\r', '')), 1, 4000), "target": "ms" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Resource Responsibilities"), '\r', ' ')), 1, 4000), "target": "rsrc_respy" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Risks"), '\r', ' ')), 1, 4000), "target": "rsks" },
        { "source": F.col("Estimated Hours - Archived").cast('double'), "target": "est_hrs" },
        { "source": F.col("Actual Hours").cast('double'), "target": "actl_hrs" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Project Issues"), '\r', ' ')), 1, 4000), "target": "proj_isss" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Related Business Change Controls"), '\r', ' ')), 1, 4000), "target": "bus_chg_cntl" },
        { "source": F.col("Business Driver"), "target": "bus_drvr" },
        { "source": F.col("Program"), "target": "prog_val" },
        { "source": F.to_timestamp("Date Presented"), "target": "pres_dt" },
        { "source": F.to_timestamp("Project Scope Date Approved"), "target": "proj_scp_apprvl_dt" },
        { "source": F.to_timestamp("Target Project Completion Date"), "target": "trgt_cpltn_dt" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Project Folder Location"), '\r', ' ')), 1, 4000), "target": "proj_foldr_locn" },
        { "source": F.col("Status"), "target": "proj_stat" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Comments"), '\r', ' ')), 1, 4000), "target": "cmmts" },
        { "source": F.col("Project Frequency"), "target": "proj_freq" },
        { "source": F.col("Resource Manager"), "target": "rsrc_mgr" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Closeout Information/Lessons Learned"), '\r', ' ')), 1, 4000), "target": "lsn_lrnd" },
        { "source": F.to_timestamp("Project Change Date Approved"), "target": "proj_chg_apprvl_dt" },
        { "source": F.to_timestamp("Project Closeout Date Approved"), "target": "closout_apprvl_dt" },
        { "source": F.col("Project Life Cycle Type"), "target": "life_cyc_type" },
        { "source": F.col("Estimated Hours - Initiating"), "target": "initn_hrs_est" },
        { "source": F.col("Estimated Hours - Planning"), "target": "plng_hrs_est" },
        { "source": F.col("Estimated Hours - Executing"), "target": "exctn_hrs_est" },
        { "source": F.col("Estimated Hours - Monitoring and Controlling"), "target": "mntrg_hrs_est" },
        { "source": F.col("Estimated Hours - Closing"), "target": "closg_hrs_est" },
        { "source": F.col("Total Estimated Hours"), "target": "tot_hrs_est" },
        { "source": F.col("Project Meeting Frequency"), "target": "proj_mtg_freq" },
        { "source": 
            F.when(F.col("Closeout Meeting Held") == True, 1)
            .otherwise(
                F.when(F.col("Closeout Meeting Held") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "closout_mtg_flg" },  
        { "source": 
            F.when(F.col("Project Documents Archived") == True, 1)
            .otherwise(
                F.when(F.col("Project Documents Archived") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "proj_doc_archv_flg" },  
        { "source": 
            F.when(F.col("Status Meeting Held during Project") == True, 1)
            .otherwise(
                F.when(F.col("Status Meeting Held during Project") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "stat_mtg_flg" },  
        { "source": F.col("Estimated Hours - Change Request 1").cast('double'), "target": "chg_rqst_1_est" },
        { "source": F.col("Estimated Hours - Change Request 2").cast('double'), "target": "chg_rqst_2_est" },
        { "source": F.col("Estimated Hours - Change Request 3").cast('double'), "target": "chg_rqst_3_est" },
        { "source": F.col("Estimated Hours - Change Request 4").cast('double'), "target": "chg_rqst_4_est" },
        { "source": F.col("Estimated Hours - Change Request 5").cast('double'), "target": "chg_rqst_5_est" },
        { "source": F.col("Estimated Hours - Change Request 6").cast('double'), "target": "chg_rqst_6_est" },
        { "source": F.col("Estimated Hours - Change Request 7").cast('double'), "target": "chg_rqst_7_est" },
        { "source": F.col("Estimated Hours - Change Request 8").cast('double'), "target": "chg_rqst_8_est" },
        { "source": F.col("Estimated Hours - Change Request 9").cast('double'), "target": "chg_rqst_9_est" },
        { "source": F.col("Revised Total Estimated Hours"), "target": "rvsd_tot_hrs_est" },
        { "source": F.to_timestamp("Revised Target Project Completion Date - CR1"), "target": "rvsd_trgt_cpltn_dt_1" },
        { "source": F.to_timestamp("Actual Project Completion Date"), "target": "actl_cpltn_dt" },
        { "source": F.col("Year Quarter"), "target": "yr_qtr" },
        { "source": F.regexp_replace(F.col("Reason for Change - Change Request 1"), '\u200b', ''), "target": "chg_rqst_1_rsn" },
        { "source": F.col("Reason for Change - Change Request 2"), "target": "chg_rqst_2_rsn" },
        { "source": F.col("Reason for Change - Change Request 3"), "target": "chg_rqst_3_rsn" },
        { "source": F.col("Reason for Change - Change Request 4"), "target": "chg_rqst_4_rsn" },
        { "source": F.col("Reason for Change - Change Request 5"), "target": "chg_rqst_5_rsn" },
        { "source": F.col("Reason for Change - Change Request 6"), "target": "chg_rqst_6_rsn" },
        { "source": F.col("Reason for Change - Change Request 7"), "target": "chg_rqst_7_rsn" },
        { "source": F.col("Reason for Change - Change Request 8"), "target": "chg_rqst_8_rsn" },
        { "source": F.col("Reason for Change - Change Request 9"), "target": "chg_rqst_9_rsn" },
        { "source": F.to_timestamp("Revised Target Project Completion Date - CR2"), "target": "rvsd_trgt_cpltn_dt_2" },
        { "source": F.to_timestamp("Revised Target Project Completion Date - CR3"), "target": "rvsd_trgt_cpltn_dt_3" },
        { "source": F.to_timestamp("Revised Target Project Completion Date - CR4"), "target": "rvsd_trgt_cpltn_dt_4" },
        { "source": F.to_timestamp("Revised Target Project Completion Date - CR5"), "target": "rvsd_trgt_cpltn_dt_5" },
        { "source": F.to_timestamp("Revised Target Project Completion Date - CR6"), "target": "rvsd_trgt_cpltn_dt_6" },
        { "source": F.to_timestamp("Revised Target Project Completion Date - CR7"), "target": "rvsd_trgt_cpltn_dt_7" },
        { "source": F.to_timestamp("Revised Target Project Completion Date - CR8"), "target": "rvsd_trgt_cpltn_dt_8" },
        { "source": F.to_timestamp("Revised Target Project Completion Date - CR9"), "target": "rvsd_trgt_cpltn_dt_9" },
        { "source": 
            F.when(F.col("Cost Impact Indicator") == True, 1)
            .otherwise(
                F.when(F.col("Cost Impact Indicator") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "cost_impct_ind" },  
        { "source": 
            F.when(F.col("Cost Impact") == True, 1)
            .otherwise(
                F.when(F.col("Cost Impact") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "cost_impct" },  
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Cost Impact Description"), '\r', ' ')), 1, 4000), "target": "cost_impct_desc" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Original Cost Impact Estimate"), '\r', ' ')), 1, 4000), "target": "cost_impct_orig_est" },
        { "source": F.col("Project Documentation Submission"), "target": "proj_docn_prev_subm" },
        { "source": F.col("Created By"), "target": "creatr" },
        { "source": F.col("Modified By"), "target": "modfr" },
        { "source": F.to_timestamp("Modified"), "target": "chg_dt" },
        { "source": F.to_timestamp("Created"), "target": "creatn_dt" },
        { "source": F.col("ID"), "target": "proj_doc_id" },
        { "source": F.lit("Y"), "target": "curr_row_flg" }
    ]

