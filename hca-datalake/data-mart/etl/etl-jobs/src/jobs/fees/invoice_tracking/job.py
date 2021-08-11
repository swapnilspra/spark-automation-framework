from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "invoice_tracking"
    business_key = []
    primary_key = {"invc_trckg_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "invoice_tracking_fee_type"
        },
        "its": {
            "type": "table",
            "source": "invoice_tracking_summary"
        },
        "dft": {
            "type": "table",
            "source": "dealer_fee_type"
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
            "source": "its",
            "conditions": [
                F.col("its.invc_trckg_id") == F.col("raw.id")
            ]
        },
        {
            "source": "dft",
            "conditions": [
                F.col("dft.dlr_fee_type_cd") == F.trim(F.col("fee-type-cd"))
            ],
            "type": "left"
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.shr_cls_cd") == F.col("shr-cls-cd")
            ],
            "type": "left"
        }
    ]

    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("its.invc_trckg_sum_key"), "target": "invc_trckg_sum_key"},
        {"source": F.col("dft.dlr_fee_type_key"), "target": "dlr_fee_type_key"},
        {"source": F.col("shr-cls-cd"), "target": "shr_cls_cd"},
        {"source": F.col("fund.shr_cls_desc"), "target": "shr_cls"},
        {"source": F.col("raw.fees"), "target": "fees"},
        { "source": F.lit(None).cast("int"), "target": "src_sys_id" },
        { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key"},
        { "source": F.lit("Y"), "target": "curr_row_flg"}
    ]

    def extract(self, catalog: Dict[str, Any]) -> Dict[str, pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)
        df_inputs["raw"] = df_inputs["raw"].withColumn("shareclassfeetype",
                                                       F.when(F.col("raw.feetype").like("AdminClass12b1Fees%"), F.lit("3-12B1")) \
                                                       .when(F.col("raw.feetype").like("InvestorClass12b1Fees%"), F.lit("2-12B1")) \
                                                       .when(F.col("raw.feetype").like("InstitutionalAccountServiceSPC1Fees%"), F.lit("1-SPC1")) \
                                                       .when(F.col("raw.feetype").like("InstitutionalAccountServiceSPC2Fees%"), F.lit("1-SPC2")) \
                                                       .when(F.col("raw.feetype").like("InstitutionalAccountServiceSPC3Fees%"), F.lit("1-SPC3")) \
                                                       .when(F.col("raw.feetype").like("InvestorClassSubTASPC1Fees%"), F.lit("2-SPC1")) \
                                                       .when(F.col("raw.feetype").like("InvestorClassSubTASPC2Fees%"), F.lit("2-SPC2")) \
                                                       .when(F.col("raw.feetype").like("InvestorClassSubTASPC3Fees%"), F.lit("2-SPC3")) \
                                                       .when(F.col("raw.feetype").like("MinimumFeeAdmin%"), F.lit("3-MF")) \
                                                       .when(F.col("raw.feetype").like("MinimumFeeInstitutional%"), F.lit("1-MF")) \
                                                       .when(F.col("raw.feetype").like("MinimumFeeInvestor%"), F.lit("2-MF")) \
                                                       .when(F.col("raw.feetype").like("PerAccountFeeAdminClass%"), F.lit("3-PAF")) \
                                                       .when(F.col("raw.feetype").like("PerAccountFeeInstitutionalClass%"), F.lit("1-PAF")) \
                                                       .when(F.col("raw.feetype").like("PerAccountFeeInvestorClass%"), F.lit("2-PAF")) \
                                                       .otherwise(F.lit(None)))

        df_inputs["raw"] = df_inputs["raw"].withColumn("fee-type-cd",F.col("shareclassfeetype").substr(3,10)) \
            .withColumn("shr-cls-cd", F.col("shareclassfeetype").substr(1,1))

        df_inputs["fund"] = df_inputs["fund"].select("shr_cls_cd","shr_cls_desc").distinct()
        return df_inputs