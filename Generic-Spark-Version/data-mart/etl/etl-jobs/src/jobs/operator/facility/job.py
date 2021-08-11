from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob  # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "facility"
    business_key = ["day_key", "fac_id", "fac_func_id"]
    primary_key = {"fac_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "src": {
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
            "source": "src"
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.col("cal.cal_day")) == F.to_date(F.col("src.PROCESS_DATE"), "MM/dd/yyyy")
            ],
            "type": "cross"
        }
    ]

    # override transform to get distinct list of records for facility
    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = df_joined.groupby("FACILITY_ID", "FACILITY_FUNCTION_ID", "day_key").agg(
            F.max(F.col("DESCRIPTION_TEXT")).alias("DESCRIPTION_TEXT"))
        df_transformed = super().transform(df_transformed)
        #   df_transformed.show(n=10)

        return df_transformed

    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("FACILITY_ID"), "target": "fac_id"},
        {"source": F.col("DESCRIPTION_TEXT"), "target": "sub_fac_nm"},
        {"source": F.col("FACILITY_FUNCTION_ID"), "target": "fac_func_id"},
        {"source": F.col("day_key"), "target": "day_key"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.lit(None).cast("int"), "target": "etl_load_cyc_key"}
    ]
