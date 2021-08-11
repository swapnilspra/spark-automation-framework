from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_acam_certification"
    primary_key = {"aml_acam_certfn_key": "int"}
    business_key = ["acam_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "aca": {
            "type": "file",
            "source": "aml_acam_cerfitication"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.col("aca.id"), "target": "acam_id" },
        { "source": F.col("aca.year"), "target": "certfn_titl"},
        { "source": F.col("aca.type of certification"), "target": "certfn_type"},
        { "source": F.regexp_replace(F.regexp_replace(F.col("aca.cost"), '\$', ''), ',', '').cast("double"), "target": "cost"},
        { "source": F.to_timestamp("aca.start date"), "target": "strt_dt"},
        { "source": F.to_timestamp("aca.end/renewal date"), "target": "end_dt"},
        { "source": F.to_timestamp("aca.payment date"), "target": "pmt_dt"},
        { "source": F.trim(F.regexp_replace(F.regexp_replace(F.col("aca.comments"), '\r', ''), '\n', '')), "target": "cmmts"},
        { "source": F.col("name"), "target": "certfn_nm"},
        { "source": F.col("aca.employee status"), "target": "empl_stat"},
        { "source": F.col("aca.ceus required (for certification renewals only)"), "target": "ceu_req_cnt"},
        { "source": F.to_timestamp("aca.modified"), "target": "chg_dt"},
        { "source": F.to_timestamp("aca.created"), "target": "creatn_dt"},
        { "source": F.col("created by"), "target": "creatr"},
        { "source": F.col("modified by"), "target": "modfr"},
        { "source": F.lit("Y"), "target": "curr_row_flg"}
    ]