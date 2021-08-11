from typing import Dict, List, Any
import pyspark
import pyspark.sql.functions as F
from common.etl_job import ETLJob
from pyspark.sql import Window
import common.utils

class Job(ETLJob):
    target_table = "dealer_invoice_trial"
    business_key = ["dlr_invc_trl_key"]
    primary_key = {"dlr_invc_trl_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "dit": {
            "type": "file",
            "source": "dealer_invoice_trial"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        },
        "pmt_cal": {
            "type": "table",
            "source": "calendar"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "acc": {
            "type": "table",
            "source": "account"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "dft": {
            "type": "table",
            "source": "dealer_fee_type"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "dit"
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.trim(F.col("dit.Invoice Period")), 'MM/dd/yyyy') == F.to_date(F.col("cal.cal_day"))
            ],
            "type":"left"
        },
        {
            "source": "pmt_cal",
            "conditions": [
                F.to_date(F.col("dit.Month Paid"), 'MM/dd/yyyy') == F.to_date(
                    F.col("pmt_cal.cal_day"))
            ],
            "type":"left"
        },
        {
            "source": "dealer",
            "conditions": [
                F.trim(F.col("dit.dealer number")) == F.col("dealer.dlr_id")
            ],
            "type":"left"
        },
        {
            "source": "fund",
            "conditions": [
                F.trim(F.col("dit.fund number")) == F.col("fund.fund_nbr")
            ],
            "type":"left"
        },
        {
            "source": "dft",
            "conditions": [
                F.when(F.substring(F.initcap(F.col("dit.Fee Type")), 1, 5) == F.initcap(F.lit('12B-1')),
                       F.initcap(F.lit('12B1')))
                    .otherwise(F.initcap(F.col("dit.Fee Type"))) == F.initcap(F.col("dft.DLR_FEE_TYPE_CD"))
            ],
            "type":"left"
        },
        {
            "source": "acc",
            "conditions": [
                F.trim(F.col("dit.Harbor Account Number")) == F.col("acct_nbr"),
                F.trim(F.col("dit.Fund Number")) == F.col("acc.fund_nbr")
            ],
            "type":"left"
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("dlr_invc_trl_key"), "target": "dlr_invc_trl_key"},
        {"source": F.col("dft.dlr_fee_type_key"), "target": "dlr_fee_type_key"},
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("cal.day_key"), "target": "invc_day_key"},
        {"source": F.col("pmt_cal.day_key"), "target": "pmt_day_key"},
        {"source": F.col("acc.acct_key"), "target": "acct_key"},
        {"source": F.col("dit.business line"), "target": "bus_line"},
        {"source": F.col("dit.Average Assets (Invoice)"), "target": "invc_avg_asset"},
        {"source": F.col("dit.Basis Points"), "target": "fee_rt"},
        {"source": F.col("dit.Invoice Amount"), "target": "tot_fee_amt"},
        {"source": F.col("dit.Number of Positions"), "target": "posn_cnt"},
        {"source": F.col("dit.Dollars Per Account"), "target": "per_acct_fee"},
        {"source": F.col("dit.Assets from Portal"), "target": "dlr_portal_asset"},
        {"source": F.col("dit.Positions From Portal"), "target": "portal_posn_cnt"},
        {"source": F.substring(F.col("dit.Invoice Frequency"),1,1), "target": "invc_freq_flg"},
        {"source": F.col("dit.Invoice Number"), "target": "invc_nbr"},
        {"source": F.lit(None), "target": "invc_dt"},
        {"source": F.when(F.substring(F.col("dit.Fee Type"),1,5) == '12B-1','N')
           .otherwise(F.lit('Y')), "target": "rec_splt_flg"},
        {"source": F.lit(None), "target": "dlr_cat"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.current_timestamp(), "target": "row_strt_dttm"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.lit(1), "target": "etl_load_cyc_key"}
    ]

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        try:
            spark = self._spark
            df_dlr_invoice_trial = common.utils.read_table_snapshot(
                table_name="dealer_invoice_trial",
                env=self._env,
                spark=self._spark)

            maxlastnum=df_dlr_invoice_trial.groupby().max("dlr_invc_trl_key").collect()[0].asDict().get('max(dlr_invc_trl_key)')

        except:
            maxlastnum=0

        window = Window.orderBy(F.col("cal.day_key"), F.col("dft.dlr_fee_type_key"), F.col("dealer.dlr_key"),\
                                F.col("fund.fund_key"), F.col("pmt_cal.day_key"),F.col("acc.acct_key"),\
                                F.col("dit.business line"))

        df_transformed = df_joined.drop("dlr_invc_trl_key").distinct() \
            .withColumn("dlr_invc_trl_key", maxlastnum + F.row_number().over(window))

        df_target = super().transform(df_transformed)
        return df_target
