from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import common.utils
from common.etl_job import ETLJob  # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "operator_function"
    primary_key = {"opr_func_key": "int"}
    business_key = ["opr_key", "fac_key", "day_key"]

    sources: Dict[str, Dict[str, Any]] = {
        "opr": {
            "type": "file",
            "source": "oper_status"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "operator": {
            "type": "table",
            "source": "operator"
        },
        "facility": {
            "type": "table",
            "source": "facility"
        }

    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "opr"
        },

        {
            "source": "cal",
            "conditions": [

                F.date_add(F.to_timestamp(F.col("opr.PROCESS_DATE"), "MM/dd/yyyy"), -1) == F.to_date(
                    F.col("cal.cal_day"))

            ]
        },
        {
            "source": "operator",
            "conditions": [
                F.col("operator.opr_id") == F.col("opr.OPERATOR_ID")
            ],
            "type": "cross"
        },
        {
            "source": "facility",
            "conditions": [
                F.col("facility.fac_id") == F.col("opr.FACILITY_ID"),
                F.col("facility.sub_fac_nm") == F.col("DESCRIPTION_TEXT")
            ],
        },
    ]

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = super().transform(df_joined)
        df_joined.show(n=20)
        df_transformed = df_transformed.distinct()
        return df_transformed

    target_mappings: List[Dict[str, Any]] = [

        {"source": F.col("opr.AUTHOR_CDE"), "target": "authzn_flg"},
        {"source": F.col("opr.FUNCTION_ADD_CODE"), "target": "add_func_flg"},
        {"source": F.col("opr.FUNCTION_BROWSE_CODE"), "target": "brws_func_flg"},
        {"source": F.col("opr.FUNCTION_DELETE_CODE"), "target": "del_func_flg"},
        {"source": F.col("opr.FUNCTION_UPDATE_CODE"), "target": "updt_func_flg"},
        {"source": F.col("opr.MINOR_ADD_CODE"), "target": "minr_add_flg"},
        {"source": F.col("opr.MINOR_BROWSE_CODE"), "target": "minr_brws_flg"},
        {"source": F.col("opr.MINOR_DELETE_CODE"), "target": "minr_del_flg"},
        {"source": F.col("opr.MINOR_UPDATE_CODE"), "target": "minr_updt_flg"},
        {"source": F.col("opr.OPERFUNC_LASTMAINT"), "target": "opr_func_last_mntn_dt"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.col("facility.fac_key"), "target": "fac_key"},
        {"source": F.col("operator.opr_key"), "target": "opr_key"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "etl_load_cyc_key"},
        {"source": F.lit(4), "target": "src_sys_id"}
    ]
