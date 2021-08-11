from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import common.utils
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_foreign_corr_banks"
    primary_key = {"forgn_corrsp_bnk_key": "int"}
    business_key = ["forgn_corrsp_bnk_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "afcb": {
            "type": "file",
            "source": "aml_foreign_corr_banks"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.to_timestamp(F.col("afcb.created")), "target": "creatn_dt" },
        { "source": F.to_timestamp(F.col("afcb.modified")), "target": "chg_dt" },
        { "source": F.to_timestamp(F.col("afcb.date bank was added")), "target": "bnk_add_dt" },
        { "source": F.to_timestamp(F.col("afcb.date bank was removed")), "target": "bnk_rmvl_dt" },
        { "source": F.col("afcb.id"), "target": "forgn_corrsp_bnk_id" },
        { "source": F.col("afcb.created by"), "target": "creatr" },
        { "source": F.col("afcb.modified by"), "target": "modfr" },
        { "source": F.col("afcb.name of bank"), "target": "bnk_nm" },
        { "source": F.col("afcb.account number"), "target": "acct_nbr" },
        { "source": F.col("afcb.status"), "target": "stat" },
        { "source": F.lit('Y'), "target": "curr_row_flg" }
    ]