from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "aml_training_history"
    primary_key = {"aml_trng_hist_key": "int"}
    business_key = ["trng_hist_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "ath": {
            "type": "file",
            "source": "aml_training_history"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.col("Title"), "target": "trng_titl" },
        { "source": F.col("Year"), "target": "yr" },
        { "source": F.col("Course Title"), "target": "crse_titl" },
        { "source": F.col("Name of Attendee"), "target": "attnde_nm" },
        { "source": F.col("Type of Session"), "target": "sess_type" },
        { "source": 
            F.when(F.col("Principal") == True, F.lit('Y'))
            .otherwise(
                F.when(F.col("Principal") == False, F.lit('N'))
                .otherwise(F.lit(None))
            ), "target": "prnc_flg" },
        { "source": F.col("Category"), "target": "cat" },
        { "source": F.col("Length of session"), "target": "sess_len" },
        { "source": F.col("Facilitator"), "target": "fclytr" },
        { "source": F.col("Link"), "target": "trng_link" },
        { "source": F.col("Notes"), "target": "ntes" },
        { "source": F.col("Name of Organization"), "target": "org_nm" },
        { "source": F.col("Role"), "target": "role" },
        { "source": F.col("Name of Publication"), "target": "pubn_nm" },
        { "source": F.col("Frequency Read").cast('double'), "target": "read_freq" },
        { "source": 
            F.when(F.col("Complete") == True, 1)
            .otherwise(
                F.when(F.col("Complete") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "cpltn_flg" },
        { "source": F.col("Department"), "target": "dept" },
        { "source": F.col("CEUs"), "target": "ceu" },
        { "source": F.to_timestamp("Membership Start Date"), "target": "mbrs_strt_dt" },
        { "source": F.col("Certification Type"), "target": "certfn_type" },
        { "source": 
            F.when(F.col("ACAMS Recertification Activity") == True, 1)
            .otherwise(
                F.when(F.col("ACAMS Recertification Activity") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "acams_recertfn_flg" },
        { "source": F.to_timestamp("Membership Expiration Date"), "target": "mbrs_expn_dt" },
        { "source": F.col("Cost").cast('double'), "target": "cost" },
        { "source": F.col("Type of Certification"), "target": "certfc_type" },
        { "source": F.to_timestamp("Date"), "target": "trng_dt" },
        { "source": F.col("Employee Status"), "target": "empl_stat" },
        { "source": 
            F.when(F.col("AML Training") == True, 1)
            .otherwise(
                F.when(F.col("AML Training") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "aml_trng_flg" },
        { "source": 
            F.when(F.col("Attachment?") == True, F.lit('Y'))
            .otherwise(
                F.when(F.col("Attachment?") == False, F.lit('N'))
                .otherwise(F.lit(None))
            ), "target": "attchmts_flg" },
        { "source": F.col("New Training Report"), "target": "new_trng_rpt_flg" },
        { "source": F.to_timestamp("Created"), "target": "creatn_dt" },
        { "source": F.col("Created By"), "target": "creatr" },
        { "source": F.col("Modified By"), "target": "modfr" },
        { "source": F.to_timestamp("Modified"), "target": "chg_dt" },
        { "source": F.col("ID"), "target": "trng_hist_id" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
    ]
