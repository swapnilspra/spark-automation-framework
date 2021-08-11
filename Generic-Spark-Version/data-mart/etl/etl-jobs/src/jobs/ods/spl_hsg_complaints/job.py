from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "spl_hsg_complaints"
    primary_key = {"cmplnt_key": "int"}
    business_key = ["cmplnt_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "spl_hsg_complaints": {
            "type": "file",
            "source": "spl_hsg_complaints"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.col("Title"), "target": "cmplnt_titl" },
        { "source": F.col("Month"), "target": "cmplnt_mo" },
        { "source": F.col("Status"), "target": "stat" },
        { "source": F.col("Assigned To"), "target": "assgne" },
        { "source": F.col("Complaint/Compliment"), "target": "rec_type" },
        { "source": F.col("Category"), "target": "cat_val" },
        { "source": F.to_timestamp("Date Received"), "target": "recvd_dt" },
        { "source": F.col("Origin"), "target": "orig_dept" },
        { "source": F.col("Written or Verbal"), "target": "cmplnt_type" },
        { "source": F.col("Representative Taking Feedback"), "target": "fdbk_rep" },
        { "source": F.substring(F.trim(F.col("Account Number")), 1, 60), "target": "acct_nbr" },
        { "source": F.trim(F.col("Fund")), "target": "fund_nbr" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Description of Issue"), '\r', ' ')), 1, 4000), "target": "iss_desc" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Description of Resolution"), '\r', ' ')), 1, 4000), "target": "soln_desc" },
        { "source": 
            F.when(F.col("Compromised Account (Y/N)") == "Yes", "Y")
            .otherwise("N"), "target": "acct_comprmed_flg" },
        { "source": F.col("Written Complaint Identifier #"), "target": "wrttn_cmplnt_id" },
        { "source": F.substring(F.trim(F.regexp_replace(F.col("Additional Information"), '\r', ' ')), 1, 4000), "target": "addl_info" },
        { "source": F.to_timestamp("Date Principal Reviewed"), "target": "prnc_revw_dt" },
        { "source": F.col("Remedy Ticket #"), "target": "tckt_id" },
        { "source": F.col("Legal Status"), "target": "lgl_stat" },
        { "source": 
            F.when(F.col("Filed with FINRA?") == "Yes", "Y")
            .otherwise(
                F.when(F.col("Filed with FINRA?") == "No", "N")
                .otherwise(F.lit(None))
            ), "target": "finra_filng_flg" },
        { "source": F.to_timestamp("Date of FINRA Filing"), "target": "finra_filng_dt" },
        { "source": F.substring(F.col("Backup Documentation"), 1, 4000), "target": "bkup_docn_link" },
        { "source": F.col("Aging"), "target": "aging" },
        { "source": 
            F.when(F.col("Escalated to CEO") == "Yes", "Y")
            .otherwise(
                F.when(F.col("Escalated to CEO") == "No", "N")
                .otherwise(F.lit(None))
            ), "target": "ceo_escl_flg" },
        { "source": 
            F.when(F.col("Escalated to CCO") == "Yes", "Y")
            .otherwise(
                F.when(F.col("Escalated to CCO") == "No", "N")
                .otherwise(F.lit(None))
            ), "target": "cco_escl_flg" },
        { "source": F.to_timestamp("Date Escalated to CEO"), "target": "ceo_escl_dt" },
        { "source": F.to_timestamp("Date Escalated to CCO"), "target": "cco_escl_dt" },
        { "source": F.col("Year"), "target": "cmplnt_yr" },
        { "source": 
            F.when(F.col("Escalated to S&M") == "Yes", "Y")
            .otherwise(
                F.when(F.col("Escalated to S&M") == "No", "N")
                .otherwise(F.lit(None))
            ), "target": "cmo_escl_flg" },
        { "source": F.to_timestamp("Date Escalated to S&M"), "target": "cmo_escl_dt" },
        { "source": F.col("State"), "target": "st" },
        { "source": F.col("Address"), "target": "addr_line" },
        { "source": F.col("City"), "target": "city" },
        { "source": F.col("Zip Code"), "target": "zip_cd" },
        { "source": F.col("Push Notif"), "target": "push_ntfn_val" },
        { "source": F.col("FINRA Reporting Period"), "target": "finra_rptg_per" },
        { "source": F.trim(F.regexp_replace(F.col("Principal Review"), '\r', ' ')), "target": "prnc_revwer" },
        { "source": F.col("BMG issue"), "target": "bmg_iss_link" },
        { "source": F.to_timestamp("Date Resolved (HSG)"), "target": "hsg_rsln_dt" },
        { "source": F.col("Created By"), "target": "creatr" },
        { "source": F.col("Modified By"), "target": "modfr" },
        { "source": F.to_timestamp("Modified"), "target": "dt_modfd" },
        { "source": F.to_timestamp("Created"), "target": "dt_creatd" },
        { "source": F.col("ID"), "target": "cmplnt_id" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
    ]