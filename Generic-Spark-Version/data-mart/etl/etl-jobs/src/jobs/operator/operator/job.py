from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import common.utils
from common.etl_job import ETLJob  # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "operator"
    primary_key = {"opr_key": "int"}
    business_key = ["opr_id", "day_key"]

    sources: Dict[str, Dict[str, Any]] = {
        "opr": {
            "type": "file",
            "source": "oper_status"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
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
        }
    ]

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = super().transform(df_joined)
        # df_joined.show(n=20)
        df_transformed = df_transformed.distinct()
        return df_transformed

    target_mappings: List[Dict[str, Any]] = [

        {"source": F.col("OPERATOR_ID"), "target": "opr_id"},
        {"source": F.to_date(F.col("FIRST_LOGON_DTE"), 'MM/dd/yyyy'), "target": "first_logon_dt"},
        {"source": F.col("IWS_ACCESS_CDE"), "target": "iws_access_cd"},
        {"source": F.to_date(F.col("LAST_LOGON_DTE"), 'MM/dd/yyyy'), "target": "last_logon_dt"},
        {"source": F.col("OPERSECU_LASTMAINT"), "target": "opr_secr_last_maint"},
        {"source": F.col("OPERSECU_LASTMAINT_ID"), "target": "last_maint_opr"},
        {"source": F.col("NBR_LOGONS"), "target": "logon_cnt"},
        {"source": F.col("OPER_EXPIRED_CDE"), "target": "opr_expn_flg"},
        {"source": F.col("OPER_NAME"), "target": "opr_nm"},
        {"source": F.to_date(F.col("OPR_EXPT_STR_DT"), 'MM/dd/yyyy'), "target": "expctd_strt_dt"},
        {"source": F.to_date(F.col("OPR_LAS_PSW_RS_DT"), 'MM/dd/yyyy'), "target": "last_pswd_rst_dt"},
        {"source": F.to_date(F.col("PASSWORD_CHG_DTE"), 'MM/dd/yyyy'), "target": "pswd_chg_dt"},
        {"source": F.col("PASSWORD_TRIES_CNT"), "target": "pswd_try_cnt"},
        {"source": F.col("PLAN_SPSR_ACCE_CDE"), "target": "ta3270_access_flg"},
        {"source": F.lit("4").cast("int"), "target": "parn_opr_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "etl_load_cyc_key"},
        {"source": F.lit(4), "target": "src_sys_id"}
    ]
