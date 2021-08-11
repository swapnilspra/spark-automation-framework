from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from pyspark.sql.types import TimestampType

class Job(ETLJob):
    target_table = "aml_fbar_filings"
    primary_key = {"fbar_filng_key": "int"}
    business_key = ["fbar_filng_id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "aff": {
            "type": "file",
            "source": "aml_fbar_filings"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        }
    }

    joins: List[Dict[str, Any]] = [
        {
            "source": "aff"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("Fund Number") == F.col("fund_nbr")
            ]
        }
    ]
    
    target_mappings:List[Dict[str, Any]] = [
        { "source": F.to_timestamp(F.col("Modified")), "target": "chg_dt"},
        { "source": F.to_timestamp(F.col("Created")), "target": "creatn_dt"},
        { "source": F.to_timestamp(F.col("Date of Last FIling")), "target": "last_filng_dt"},
        { "source": F.to_timestamp(F.col("Date of Fund Closure")), "target": "fund_clos_dt"},
        { "source": F.col("Created By"), "target": "creatr"},
        { "source": F.col("Modified By"), "target": "modfr"},
        { "source": F.col("ID"), "target": "fbar_filng_id" },
        { "source": F.col("Fund Name"), "target": "fund_nm" },
        { "source": F.col("Year"), "target": "yr" },
        { "source": F.when(F.col("Active Fund (Y/N)?") == True, 1)
            .otherwise(
                F.when(F.col("Active Fund (Y/N)?") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "actv_fund_flg" },
        { "source": F.col("fund.fund_key").cast('integer'), "target": "fund_key" },
        { "source": F.when(F.col("Amendment") == True, 1)
            .otherwise(
                F.when(F.col("Amendment") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "amndt_nbr" },
        { "source": F.col("Amendment for Year"), "target": "amndt_yr" },

        { "source": F.lit("Y"), "target": "curr_row_flg"}
    ]