from typing import Dict, List, Any
from datetime import datetime,timedelta
import pyspark.sql
import decimal
from datetime import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob  # must be imported after spark has been set up

from pyspark.sql import Row,Window
from collections import namedtuple

class Job(ETLJob):
    target_table = "dealer_invoice_model"
    business_key = ["dlr_fee_type_key","dlr_key","invc_day_key","fund_key","bus_line","invc_freq_flg"]
    primary_key = {"dlr_invc_mdl_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "dealer_invoice": {
            "type": "table",
            "source": "dealer_invoice"
        },
        "dealer_invoice_rule": {
            "type": "table",
            "source": "dealer_invoice_rule"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "dealer_invoice"
        },
        {
            "source": "calendar",
            "conditions": [
                F.col("calendar.day_key") == F.col("dealer_invoice.invc_day_key")
            ],
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_key") == F.col("dealer_invoice.fund_key")
            ],
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("dealer_invoice.dlr_fee_type_key"), "target": "dlr_fee_type_key"},
        {"source":F.col("dealer_invoice.dlr_key"), "target": "dlr_key"},
        {"source":F.col("dealer_invoice.invc_day_key"), "target": "invc_day_key"},
        {"source":F.col("dealer_invoice.fund_key"), "target": "fund_key"},
        {"source":F.col("trgt_bus_line"), "target": "bus_line"},
        {"source":F.col("dealer_invoice.invc_freq_flg"), "target": "invc_freq_flg"},
        {"source":F.col("invc_avg_asset_sum"), "target": "invc_avg_asset"},
        {"source":F.col("tot_fee_amt_final"), "target": "tot_fee_amt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        if not self._args.get("p_date",None):
            # default value is first day of the current month
            p_date = datetime.today().replace(day=1)
        else:
            p_date = datetime.strptime(self._args["p_date"],"%m/%d/%Y")

        p_in_dlr = self._args["p_in_dlr"]

        # get all rows by the given date and dealer, with all bus_line
        # add effective start date and end date for dealer rule, so that more than one mapping(source business line---target business line mapping) can exist in the system
        df_dealer_invoice_keys = inputs["dealer_invoice"].filter( F.col("row_strt_dttm")>p_date).select("invc_day_key","fund_key","dlr_fee_type_key").distinct()

        # join with the distinct keys to filter only those keys
        df_dealer_invoice_agg = inputs["dealer_invoice"].filter(F.col("dlr_key")==p_in_dlr).join(df_dealer_invoice_keys.alias("dealer_invoice_keys"),[
            F.col("dealer_invoice.invc_day_key")==F.col("dealer_invoice_keys.invc_day_key"),
            F.col("dealer_invoice.fund_key")==F.col("dealer_invoice_keys.fund_key"),
            F.col("dealer_invoice.dlr_fee_type_key")==F.col("dealer_invoice_keys.dlr_fee_type_key")
        ]).select("dealer_invoice.*")
        
        df_dealer_invoice_agg = df_dealer_invoice_agg.groupBy("dlr_key","invc_day_key","fund_key","dlr_fee_type_key","bus_line","invc_freq_flg").\
            agg(F.sum("invc_avg_asset").alias("invc_avg_asset_sum"),F.sum("tot_fee_amt").alias("tot_fee_amt_sum"),F.count("dlr_key"))

        inputs["dealer_invoice"] = df_dealer_invoice_agg.alias("dealer_invoice")

        df_joined = super().join(inputs)

        # join with the invoice rule table
        df_joined = df_joined.join(inputs["dealer_invoice_rule"],[
            F.col("dealer_invoice.dlr_key")==F.col("dealer_invoice_rule.dlr_key"),
            F.col("dealer_invoice.bus_line")==F.col("dealer_invoice_rule.src_bus_line"),
            F.col("fund.shr_cls_cd")==F.col("dealer_invoice_rule.shr_cls_cd"),
            F.col("dealer_invoice_rule.curr_row_flg")==F.lit("Y"),
            F.col("calendar.cal_day").between(F.col("dealer_invoice_rule.efftv_strt_dt"),F.col("dealer_invoice_rule.efftv_end_dt"))
        ],how="leftouter")
        print(f"total joined with invoice rule: {df_joined.count()}")

        # UDF to calculate days in year
        @F.udf(T.DoubleType())
        def days_in_year(p_date):
            first_day_of_year:datetime = p_date.replace(month=1,day=1)
            first_day_next_year:datetime = first_day_of_year.replace(year=first_day_of_year.year+1)
            return float( (first_day_next_year-first_day_of_year).days )

        # # UDF to calculate days in year
        # @F.udf(T.DateType())
        # def days_in_year(p_date):
        #     year_ago:datetime = p_date.replace(year=pdate_.year-1)
        #     return float( (today-year_ago).days )

        df_model_raw = df_joined.select(
            "dealer_invoice.dlr_fee_type_key",
            "dealer_invoice.dlr_key",
            "dealer_invoice.invc_day_key",
            "dealer_invoice.fund_key",
            "dealer_invoice.bus_line",
            "dealer_invoice_rule.bp_value",
            "dealer_invoice_rule.calc_flg",
            "dealer_invoice_rule.fee_used_in_fee_flg",
            "dealer_invoice_rule.cnt_me_only_me_flg",
            "fund.shr_cls_cd",
            "calendar.cal_day",
            "dealer_invoice_rule.src_bus_line",
            F.coalesce(F.col("dealer_invoice_rule.trgt_bus_line"),F.col("dealer_invoice.bus_line")).alias("trgt_bus_line"),
            # if CALC_FLG is A, add the assets; if CALC_FLG is S, subtract the assets 
            F.when(F.col("dealer_invoice_rule.calc_flg")==F.lit('A'),F.col("dealer_invoice.invc_avg_asset_sum")).\
                when(F.col("dealer_invoice_rule.calc_flg")==F.lit('S'),F.lit(-1.0)*F.col("dealer_invoice.invc_avg_asset_sum")).\
                otherwise(F.col("dealer_invoice.invc_avg_asset_sum")).alias("invc_avg_asset_final"),
            # fees contains two parts: the first is fee part: if FEE_USED_IN_FEE_FLG is Y, need to add this fee. If it is N, no need to count in this fee, if it is null should add this fee
            # the second part is assets*bp*days_in_month/days_in_year. BP column is positive or negative in dealer_invoice_rule table.So no need to process the addition or subtraction of BP part
            F.when(F.col("dealer_invoice_rule.fee_used_in_fee_flg")==F.lit('Y'),F.col("dealer_invoice.tot_fee_amt_sum")).\
                when(F.col("dealer_invoice_rule.fee_used_in_fee_flg")==F.lit('N'),F.lit(0)).\
                otherwise(F.col("dealer_invoice.tot_fee_amt_sum")).alias("tot_fee_by_flag"),
            (F.col("dealer_invoice.invc_avg_asset_sum")*\
                (F.dayofmonth(F.last_day(F.col("calendar.cal_day")))/days_in_year(F.col("calendar.cal_day")))*\
                F.coalesce(F.col("dealer_invoice_rule.bp_value"),F.lit(0))/F.lit(10000.0)).alias("tot_fee_by_assets"),
            F.col("dealer_invoice.invc_freq_flg")
        )

        print(f"total model raw: {df_model_raw.count()}")
        df_model_grouped = df_model_raw.groupBy(
            "dealer_invoice.dlr_fee_type_key",
            "dealer_invoice.dlr_key",
            "dealer_invoice.invc_day_key",
            "dealer_invoice.fund_key",
            "trgt_bus_line",
            "dealer_invoice.invc_freq_flg"
        ).agg(
            F.sum(F.col("invc_avg_asset_final")).alias("invc_avg_asset_sum"),
            F.sum(F.col("tot_fee_by_flag")+F.col("tot_fee_by_assets")).alias("tot_fee_amt_final"),
            F.count(F.when(F.abs(F.col("cnt_me_only_me_flg"))==F.lit(1000),1)).alias("exclude"),
            F.count(F.lit("1")).alias("number_of_records")
        )
        print(f"total model grouped: {df_model_grouped.count()}")
        # if all records are to be excluded, exclude the entire list. 
        df_model_grouped = df_model_grouped.where(F.col("exclude")<F.col("number_of_records")).orderBy("exclude",ascending=False)

        return df_model_grouped