from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.types as T
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from itertools import chain

class Job(ETLJob):

    target_table = "account_position"
    business_key = ["acct_key","fund_key","day_key"]
    primary_key = {"acct_posn_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "tip03": {
            "type": "file",
            "source": "DTO.HCA.APR.TIP03"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "account": {
            "type": "table",
            "source": "account"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "harbor_dealer": {
            "type": "table",
            "source": "dealer"
        },
        "dealer_branch": {
            "type": "table",
            "source": "dealer_branch"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "tip03"
        },
        {
            "source": "account",
            "conditions": [
                F.col("account.acct_nbr") == F.trim(F.col("tip03.customer-account-number")).cast(T.LongType()),
                F.col("account.fund_nbr") == F.col("tip03.fund-code")
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.col("tip03.fund-code")
            ]
        },
        {
            "source": "dealer",
            "conditions": [
                F.col("dealer.dlr_id") == F.col("tip03.fins-id")
            ]
        },
        {
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("calendar.cal_day"),"yyyyMMdd") == F.col("tip03.spt_sheet_dt")
            ],
            "type":"cross" # needed because spark thinks it is a 'trivial' join since spt_sheet_dt is the same for all rows
        },
        {
            "source": "harbor_dealer",
            "conditions": [
                F.col("account.hbr_dlr_id") == F.col("harbor_dealer.dlr_id")
            ]
        },
        {
            "source": "dealer_branch",
            "conditions": [
                F.col("dealer_branch.dlr_id") == F.trim(F.col("tip03.fins-id")),
                F.col("dealer_branch.branch_id") == F.trim(F.col("tip03.dealer-branch-id"))
            ],
            "type":"leftouter"
        }
    ]

    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("account.acct_key"), "target": "acct_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("fund.fund_nbr"),"target": "fund_nbr"},
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.lit(None).cast("string"), "target": "shrhldr_key"},
        {"source": F.lit(None).cast("int"), "target": "shrhldr_role_key"},
        {"source": F.col("calendar.day_key"), "target": "day_key"},
        {"source": F.col("calendar.yr_nbr"), "target": "partition_yr"},
        {"source": F.trim(F.col("tip03.customer-account-number")).cast(T.LongType()), "target": "acct_nbr"},
        {"source": F.col("tip03.total-shares-count"), "target": "tot_shrs"},
        {"source": F.col("tip03.unissued-shares-count"), "target": "un_issd_shrs"},
        {"source": F.col("tip03.issued-shares-count"), "target": "issd_shrs"},
        {"source": F.col("tip03.net-asset-value"), "target": "nav"},
        {"source": F.col("tip03.reserved-share-cnt"), "target": "rsrvd_shrs"},
        {"source": F.col("tip03.accrued-dividend-amount"), "target": "accrd_divd_amt"},
        {"source": F.col("tip03.escrow-shares-count"), "target": "escrw_shrs"},
        {"source": F.col("tip03.collected-shares-cnt"), "target": "collctd_shrs"},
        {"source": F.col("tip03.collected-balance-amount"), "target": "collctd_bal_amt"},
        {"source": F.col("tip03.cur-income-dividends"), "target": "curr_incm_divd"},
        {"source": F.col("tip03.cur-long-term-cap-gains"), "target": "curr_long_term_cap_gain"},
        {"source": F.col("tip03.cur-short-term-cap-gains"), "target": "curr_shrt_term_cap_gain"},
        {"source": F.col("tip03.cur-foreign-withholding"), "target": "curr_forgn_w_holdg"},
        {"source": F.col("tip03.cur-dividend-withholding"), "target": "curr_divd_w_holdg"},
        {"source": F.col("tip03.cur-foreign-tax-paid-amt"), "target": "curr_forgn_tax_paid_amt"},
        {"source": F.col("tip03.collected-shares-net-chg-cnt"), "target": "collctd_shrs_net_chg_cnt"},
        {"source": F.col("tip03.total-shares-net-chg-cnt"), "target": "tot_shrs_net_chg_cnt"},
        {"source": F.coalesce(F.col("tip03.dsg-wthh-amt"),F.lit(0)), "target": "dsgntd_w_holdg_amt"},
        {"source": F.col("tip03.dsg-wthh-rte"), "target": "dsgntd_w_holdg_rt"},
        {"source": F.coalesce(F.col("tip03.dividend-wth-at"),F.lit(0)), "target": "divd_w_holdg_amt"},
        {"source": F.coalesce(F.col("tip03.dividend-wth-rt"),F.lit(0)), "target": "divd_w_holdg_rt"},
        {"source": F.coalesce(F.col("tip03.stcg-wth-at"),F.lit(0)), "target": "shrt_term_cap_gain_w_holdg_amt"},
        {"source": F.col("tip03.stcg-wth-rt"), "target": "shrt_term_cap_gain_w_holdg_rt"},
        {"source": F.coalesce(F.col("tip03.ltcg-wth-at"),F.lit(0)), "target": "long_term_cap_gain_w_holdg_amt"},
        {"source": F.coalesce(F.col("tip03.ltcg-wth-rt"),F.lit(0)), "target": "long_term_cap_gain_w_holdg_rt"},
        {"source": F.col("dealer_branch.dlr_branch_key"), "target": "dlr_branch_key"},
        {"source": F.col("harbor_dealer.dlr_key"), "target": "hbr_dlr_key"},
        {"source": F.col("account.acct_stat_cd"), "target": "acct_stat_cd"},
        {"source": F.round(F.col("tip03.total-shares-count")*F.col("tip03.net-asset-value"),5), "target": "tot_bal_amt"},
        {"source": F.lit(None), "target": "shrhldr_copy_key"},
        {"source": F.lit(None), "target": "curr_yr_1099b_gr_redmpn_amt"},
        {"source": F.lit(None), "target": "curr_yr_1099b_gr_w_holdg_amt"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"}
    ]

    # override extract to get the day key from the file header
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:

        df_inputs = super().extract(catalog)
        # get the entry from the catalog
        source:Dict[Any,Any] = catalog[self.sources["tip03"]["source"]]
        file_location:str = common.utils.get_file_location(self._env["file_prefix"],source["path"])

        spt_sheet_dt = self._spark.read.text(file_location).limit(1).rdd.map(
                lambda line: line.value.split()[2][0:8] # the date is after "ACCT POSITION " and is 8 digits, yyyyMMdd
            ).collect()[0]
        df_inputs["tip03"] = df_inputs["tip03"].withColumn("spt_sheet_dt",F.to_date(F.lit(spt_sheet_dt),"yyyyMMdd"))
        df_inputs["tip03"] = df_inputs["tip03"].alias("tip03")
        return df_inputs