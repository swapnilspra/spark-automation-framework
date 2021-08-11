from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import common.utils
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_dealer_contracts"
    primary_key = {"aml_dlr_contrct_key": "int"}
    business_key = ["dlr_contrct_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "adc": {
            "type": "file",
            "source": "aml_dealer_contracts"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.to_timestamp(F.col("date sent to intermediary")), "target": "intrm_sent_dt" },  
        { "source": F.to_timestamp(F.col("request date")), "target": "rqst_dt" },  
        { "source": F.to_timestamp(F.col("aml review request date")), "target": "revw_rqst_dt" },  
        { "source": F.to_timestamp(F.col("aml review completion date")), "target": "revw_cpltn_dt" },  
        { "source": F.to_timestamp(F.col("modified")), "target": "chg_dt" },  
        { "source": F.to_timestamp(F.col("created")), "target": "creatn_dt" },  
        { "source": F.col("id"), "target": "dlr_contrct_id" },  
        { "source": F.col("firm"), "target": "firm_nm" },  
        { "source": F.col("priority"), "target": "prio" },  
        { "source": F.col("status"), "target": "stat" },  
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("comments"), '\r', ''), '\n', '')), "target": "cmmts" },  
        { "source": F.col("requested by"), "target": "rqstr" },  
        { "source": F.substring(F.col("send to marketing"), 1, 1), "target": "ltr_to_mktg_flg" },
        { "source": F.col("legal name/address of company"), "target": "dlr_nm" },  
        { "source": F.col("send agreement to"), "target": "contct_pers" },  
        { "source": F.col("phone"), "target": "phon_nbr" },  
        { "source": F.col("fax"), "target": "fax_nbr" },  
        { "source": F.col("e-mail"), "target": "email_addr" },  
        { "source": F.trim(F.col("type of agreement comments")), "target": "agrmt_type_cmmts" },  
        { "source": F.col("self-clears / clearing firm(s) / clearing number(s)"), "target": "self_clring_dets" },  
        { "source": F.col("clearing firm(s) / clearing number(s) (if applicable)"), "target": "clring_firm_nbr" },  
        { "source": F.col("if no, the dealer of record is:"), "target": "dlr_of_rec" },  
        { "source": F.col("assigned to"), "target": "assgne" },  
        { "source": F.col("Type of Entity"), "target": "enty_type" },  
        { "source": F.col("Fees Paid on Assets Cleared Through the Following Firms (If Applicable)"), "target": "fee_pmt_dets" },  
        { "source": F.when(F.col("will the retirement share class be included in the agreement?") == True, 'Y')
            .otherwise(
                F.when(F.col("will the retirement share class be included in the agreement?") == False, 'N')
                .otherwise(F.lit(None))
            ), "target": "rtrmt_shr_cls_inlsn_flg" },  
        { "source": F.substring(F.col("Will the Intermediary be Listed as Dealer of Record?"), 1, 1), "target": "dlr_of_rec_flg" },  
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("aml review (tarc)"), '\r', ''), '\n', '')), "target": "tarc_aml_revw" },  
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("market timing review (tarc)"), '\r', ''), '\n', '')), "target": "tarc_mkt_tmng_revw" },  
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("other regulatory findings (tarc)"), '\r', ''), '\n', '')), "target": "tarc_othr_fndg" },  
        { "source": 
            F.when(F.col("aml review complete") == True, 1)
            .otherwise(
                F.when(F.col("aml review complete") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "revw_cpltn_flg" },  
        { "source": F.col("aml request aging").cast('integer'), "target": "rqst_dt_agng" },  
        { "source": F.col("created by"), "target": "creatr" },  
        { "source": F.col("modified by"), "target": "modfr" },  
        { "source": F.lit('Y'), "target": "curr_row_flg" }
    ]
