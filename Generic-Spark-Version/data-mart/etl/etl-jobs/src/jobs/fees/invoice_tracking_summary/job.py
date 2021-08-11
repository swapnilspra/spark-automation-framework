from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "invoice_tracking_summary"
    business_key = ["invc_trckg_id","dlr_key","pmt_day_key"]
    primary_key = {"invc_trckg_sum_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "raw": {
            "type": "file",
            "source": "invoice_tracking_summary"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "raw"
        },
        {
            "source": "dealer",
            "conditions": [
                F.col("raw.dealerid") == F.col("dealer.dlr_id")
            ]
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.col("raw.datepaymentmade"), "MM/dd/yyyy") == F.to_date(F.col("cal.cal_day"))
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.col("cal.day_key"), "target": "pmt_day_key"},
        {"source": F.col("raw.id"), "target": "invc_trckg_id"},
        {"source": F.col("raw.invoiceyear"), "target": "invc_yr"},
        {"source": F.col("raw.invoiceperiodvalue"), "target": "invc_per"},
        {"source": F.col("raw.datereceived"), "target": "invc_recpt_dt"},
        {"source": F.col("raw.invoiceamounttotal"), "target": "invc_tot_amt"},
        {"source": F.col("raw.paymentamount"), "target": "pmt_tot_amt"},
        {"source": F.col("Analysis"), "target": "anlyst_cmmts"},
        {"source": F.col("raw.invoiceanalysisid"), "target": "invc_anlyst"},
        {"source": F.col("raw.invoiceqc1id"), "target": "invc_qc1"},
        {"source": F.col("raw.invoiceqc2id"), "target": "invc_qc2"},
        {"source": F.col("raw.acceptablethresholdfordifference"), "target": "accptbl_var"},
        {"source": F.col("raw.paymentfrequencyvalue") , "target": "pmt_freq"},
        {"source": F.col("raw.invoiceperiodbeginningdate"), "target": "invc_per_strt_dt"},
        {"source": F.col("raw.invoiceperiodendingdate"), "target": "invc_per_end_dt"},
        {"source": F.col("raw.modifiedbyid"), "target": "modfr"},
        {"source": F.col("raw.created"), "target": "creatn_dt"},
        {"source": F.col("raw.createdbyid"), "target": "creatr"},
        {"source": F.col("raw.modified"), "target": "chg_dt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "etl_load_cyc_key"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"}
    ]

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_target = super().transform(df_joined)

        df_target=df_target.withColumn("comments", F.trim(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(
            F.regexp_replace(F.col("anlyst_cmmts"),'<[^>]+>'," " ),'\n', ' '),'\t', ' '),'â€‹',''),'  ', ''),'&#160;',' '),'&#58;',' - '))).\
            drop("anlyst_cmmts").withColumn("anlyst_cmmts",F.col("comments")).drop("comments")


        return df_target

