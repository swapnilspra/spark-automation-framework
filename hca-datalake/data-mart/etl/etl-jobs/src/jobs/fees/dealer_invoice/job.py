from typing import Dict, List, Any
import pyspark
import pyspark.sql.functions as F
from common.etl_job import ETLJob
from pyspark.sql import Window
import common.utils



class Job(ETLJob):
    target_table = "dealer_invoice"
    business_key = ["dlr_invc_key"]
    primary_key = {"dlr_invc_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "di": {
            "type": "file",
            "source": "dealer_invoice"
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
            "source": "di"
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.trim(F.col("di.Invoice Period")), 'MM/dd/yyyy') == F.to_date(F.col("cal.cal_day"))
            ],
            "type":"leftouter"
        },
        {
            "source": "pmt_cal",
            "conditions": [
                F.to_date(F.col("di.Month Paid"), 'MM/dd/yyyy') == F.to_date(
                    F.col("pmt_cal.cal_day"))
            ],
            "type":"leftouter"
        },
        {
            "source": "dealer",
            "conditions": [
                F.trim(F.col("di.dealer number")) == F.col("dealer.dlr_id")
            ],
            "type":"leftouter"
        },
        {
            "source": "fund",
            "conditions": [
                F.trim(F.col("di.fund number")) == F.col("fund.fund_nbr")
            ],
            "type":"leftouter"
        },
        {
            "source": "dft",
            "conditions": [
                F.when(F.substring(F.initcap(F.col("di.Fee Type")), 1, 5) == F.initcap(F.lit('12B-1')),
                       F.initcap(F.lit('12B1')))
                    .otherwise(F.initcap(F.col("di.Fee Type"))) == F.initcap(F.col("dft.DLR_FEE_TYPE_CD"))
            ],
            "type":"leftouter"
        },
        {
            "source": "acc",
            "conditions": [
                F.trim(F.col("di.Harbor Account Number")) == F.col("acct_nbr"),
                F.trim(F.col("di.Fund Number")) == F.col("acc.fund_nbr")
            ],
            "type":"leftouter"
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("dlr_invc_key"), "target": "dlr_invc_key"},
        {"source": F.col("dft.dlr_fee_type_key"), "target": "dlr_fee_type_key"},
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("cal.day_key"), "target": "invc_day_key"},
        {"source": F.col("pmt_cal.day_key"), "target": "pmt_day_key"},
        {"source": F.col("acc.acct_key"), "target": "acct_key"},
        {"source": F.col("di.business line"), "target": "bus_line"},
        {"source": F.col("di.Average Assets (Invoice)"), "target": "invc_avg_asset"},
        {"source": F.col("di.Basis Points"), "target": "fee_rt"},
        {"source": F.col("di.Invoice Amount"), "target": "tot_fee_amt"},
        {"source": F.col("di.Number of Positions"), "target": "posn_cnt"},
        {"source": F.col("di.Dollars Per Account"), "target": "per_acct_fee"},
        {"source": F.col("di.Assets from Portal"), "target": "dlr_portal_asset"},
        {"source": F.col("di.Positions From Portal"), "target": "portal_posn_cnt"},
        {"source": F.substring(F.col("di.Invoice Frequency"),1,1), "target": "invc_freq_flg"},
        {"source": F.col("di.Invoice Number"), "target": "invc_nbr"},
        {"source": F.lit(None), "target": "invc_dt"},
        {"source": F.lit(None), "target": "invc_cat"},
       {"source": F.when(F.substring(F.col("di.Fee Type"),1,5) == '12B-1','N')
           .otherwise(F.lit('N')), "target": "rec_splt_flg"},
        {"source": F.col("di.category"), "target": "dlr_cat"},
        {"source": F.lit(None), "target": "curr_row_flg"},
        {"source": F.current_timestamp(), "target": "row_strt_dttm"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.lit(1), "target": "etl_load_cyc_key"}
    ]
    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = self.calc_transformed(df_joined)
        df_target = super().transform(df_transformed)
        return df_target

    def calc_transformed(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = df_joined

        try:
            df_dlr_invoice = common.utils.read_table_snapshot(
                table_name="dealer_invoice",
                env=self._env,
                spark=self._spark)

            maxlastnum=df_dlr_invoice.groupby().max('dlr_invc_key').collect()[0].asDict().get('max(dlr_invc_key)')

        except:
            maxlastnum=0

        window = Window.orderBy(F.col('cal.day_key'),F.col("dft.dlr_fee_type_key"), F.col("pmt_cal.day_key"),F.col("dealer.dlr_key"), F.col("fund.fund_key"),F.col("acc.acct_key"))

        df_transformed = df_transformed.drop("dlr_invc_key") \
            .withColumn("dlr_invc_key", maxlastnum+F.row_number().over(window))
        return df_transformed

