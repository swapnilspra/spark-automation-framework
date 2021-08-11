from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob

class Job(ETLJob):
    target_table = "dealer_fee_schedule"
    business_key = ["dlr_key","fund_key","dlr_fee_type_key","fee_rt","bus_line"]
    primary_key = {"dlr_fee_sched_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "HSG_IntermediaryPaymentInformation"
        },
        "dft": {
            "type": "table",
            "source": "dealer_fee_type"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "raw"
        },
        {
            "source": "dft",
            "conditions": [
                F.col("fee-type") == F.col("dft.dlr_fee_type_cd")
            ]
        },
        {
            "source": "dealer",
            "conditions": [
                F.col("raw.dealernumber") == F.col("dealer.dlr_id")
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("raw.fundnumber") == F.col("fund.fund_nbr")
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("dft.dlr_fee_type_key"), "target": "dlr_fee_type_key"},
        {"source": F.lit(None), "target": "day_key"},
        {"source": F.col("fee-type-value"), "target": "fee_rt"},
        {"source": F.trim(F.col("raw.businessline")), "target": "bus_line"},
        {"source": F.col("raw.invoicefeedescription"), "target": "invc_fee_desc"},
        {"source": F.col("comments"), "target": "cmmts"},
        {"source": F.lit(None), "target": "efftv_strt_dt"},
        {"source": F.lit(None), "target": "efftv_end_dt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None), "target": "etl_load_cyc_key"},
        {"source": F.lit(None), "target": "src_sys_id"}

    ]

    def extract(self, catalog: Dict[str, Any]) -> Dict[str, pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)

        df_inputs["raw"] = df_inputs["raw"] \
            .withColumn("fee-type-value", F.regexp_replace(F.col("raw.feetypevalue"), "[$]", ""))\
            .where(F.col("fee-type-value").cast("int") > 0)\
            .withColumn("fee-type",
                 F.when(F.col("raw.feetype").like("C_12b1%"), F.lit("12B1")) \
                    .when(F.col("raw.feetype").like("PerAccountFeeAnnual%"), F.lit("PAF")) \
                       .when(F.col("raw.feetype").like("MinimumFeeMonth%"), F.lit("MF")) \
            .otherwise(F.col("raw.feetype")))\
            .withColumn("comments",
                        F.regexp_replace(F.regexp_replace(F.trim(
                            F.when(F.col("raw.comments").like("12b-1 on accounts listing GWFS as dealer of record;%"),
                                   F.regexp_replace(F.substring_index(F.col("raw.comments"),"$",1),";",""))\
                            .otherwise(F.col("raw.comments"))
                                                ), "â€“",""),",",";"))
        return df_inputs