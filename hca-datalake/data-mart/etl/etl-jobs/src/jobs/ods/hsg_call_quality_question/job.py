from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "hsg_call_quality_question"
    # no business key
    primary_key = {"cq_qstn_key": "int"}
    business_key = []

    sources:Dict[str, Dict[str, Any]] = {     
        "hsg_call_quality_question": {
            "type": "file",
            "source": "hsg_call_quality_question"
        }
    }

    # to do
    # replace source column name once csv is done
    target_mappings:List[Dict[str, Any]] = [
        { "source": F.trim(F.col("QSTN")), "target": "qstn" },
        { "source": F.trim(F.col("QSTN_CAT")), "target": "qstn_cat" },
        { "source": F.trim(F.col("QSTN_COL_NM")), "target": "qstn_col_nm" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
    ]