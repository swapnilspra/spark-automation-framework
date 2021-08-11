from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "business_line"
    business_key = ["invc_fee_desc"]
    primary_key = {"bus_line_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "hip": {
            "type": "file",
            "source": "HSG_IntermediaryPaymentInformation"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("bus_line"), "target": "bus_line"},
        {"source": F.col("InvoiceFeeDesc"), "target": "invc_fee_desc"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None), "target": "row_strt_dttm"},
        {"source": F.lit(None), "target": "src_sys_id"},
        {"source": F.lit(None), "target": "etl_load_cyc_key"}
    ]

    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)

        df_inputs["hip"] = df_inputs["hip"].select("BusinessLine", "Comments", "InvoiceFeeDescription") \
            .distinct()

        df_inputs["hip"] = df_inputs["hip"].withColumn("bus_line",
                        F.regexp_replace(
                            F.regexp_replace(F.trim(F.coalesce(F.col("BusinessLine"), F.col("Comments"))), "â€“", "-"),
                            "  ", " ")) \
            .withColumn("InvoiceFeeDesc",
                        F.coalesce(
                            F.regexp_replace(
                                F.regexp_replace(
                                    F.trim(F.upper(
                                        F.coalesce(
                                            F.regexp_replace(F.substring_index(F.col("InvoiceFeeDescription"), "$", 1),
                                                             ";", ""), F.upper(F.col("Comments"))))
                                    ), "â€“", "-")
                                , "  ", " ")
                            , F.upper(F.col("BusinessLine"))))

        df_inputs["hip"] = df_inputs["hip"].select("bus_line", "InvoiceFeeDesc").distinct().where(F.col("InvoiceFeeDesc")!= "-")
        return df_inputs