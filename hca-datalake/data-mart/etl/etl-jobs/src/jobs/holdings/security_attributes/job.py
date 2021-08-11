from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "security_attributes"
    business_key = ["asset_id","ssb_fund_nbr","lot_acct_nbr","posn_type_cd"]
    primary_key = {"secr_attr_key":"int"}
    load_strategy = ETLJob.LoadStrategy.TYPE2

    sources:Dict[str,Dict[str,Any]] = {
        "pta_ssb_position": {
            "type": "file",
            "source": "pta_ssb_position"
        },
        "fund_composite": {
            "type": "table",
            "source": "fund_composite"
        },
        "report_class": {
            "type": "dimension",
            "source": "report_class"
        },
        "issue_class": {
            "type": "dimension",
            "source": "issue_class"
        },
        "investment_indicator": {
            "type": "dimension",
            "source": "investment_ind"
        },
        "asset_group": {
            "type": "dimension",
            "source": "asset_group"
        },
        "market_value": {
            "type": "dimension",
            "source": "market_value"
        },
        "position_type": {
            "type": "dimension",
            "source": "security_position_type"
        },
        "variable_rate_freq": {
            "type": "dimension",
            "source": "variable_rate_freq"
        },
        "market_price_source": {
            "type": "dimension",
            "source": "market_price_source"
        },
        "pool_type": {
            "type": "dimension",
            "source": "pool_type"
        },
        "existing": {
            "type": "table",
            "source": "security_attributes"
        },
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "pta_ssb_position"
        },
        {
            "source": "existing",
            "conditions": [
                F.col("existing.asset_id") == F.col("pta_ssb_position.asset_id"),
                F.col("existing.ssb_fund_nbr") == F.col("pta_ssb_position.fund_id"),
                F.col("existing.lot_acct_nbr") == F.coalesce(F.lpad(F.col("pta_ssb_position.lot_acct_num"),6,"0"),F.lit("XXXXX")),
                F.col("existing.posn_type_cd") == F.col("pta_ssb_position.pos_type_cd"),
                F.col("existing.curr_row_flg") == F.lit("Y")
            ],
            "type":"leftouter"
        },
        {
            "source": "fund_composite",
            "conditions": [
                F.col("pta_ssb_position.fund_id")==F.col("fund_composite.st_str_fund_nbr")
            ],
        },
        {
            "source": "report_class",
            "conditions": [
                F.col("pta_ssb_position.rpt_cls_cd")==F.col("report_class.rpt_cls_cd")
            ],
            "type":"leftouter"
        },
        {
            "source": "issue_class",
            "conditions": [
                F.col("pta_ssb_position.issue_cls_cd")==F.col("issue_class.iss_cls_cd")
            ],
            "type":"leftouter"
        },
        {
            "source": "investment_indicator",
            "conditions": [
                F.col("pta_ssb_position.invest_ind")==F.col("investment_indicator.invmt_ind_cd")
            ],
            "type":"leftouter"
        },
        {
            "source": "asset_group",
            "conditions": [
                F.col("pta_ssb_position.asset_grp_cd")==F.col("asset_group.asset_grp_cd")
            ],
            "type":"leftouter"
        },
        {
            "source": "market_value",
            "conditions": [
                F.col("pta_ssb_position.mkt_val_cd")==F.col("market_value.mkt_val_cd")
            ],
            "type":"leftouter"
        },
        {
            "source": "position_type",
            "conditions": [
                F.col("pta_ssb_position.pos_type_cd")==F.col("position_type.posn_type_cd")
            ],
            "type":"leftouter"
        },
        {
            "source": "variable_rate_freq",
            "conditions": [
                F.col("pta_ssb_position.var_rt_freq_cd")==F.col("variable_rate_freq.varb_rt_freq_cd")
            ],
            "type":"leftouter"
        },
        {
            "source": "market_price_source",
            "conditions": [
                F.col("pta_ssb_position.fasprc_src_cd")==F.col("market_price_source.mkt_pr_src_cd")
            ],
            "type":"leftouter"
        },
        {
            "source": "pool_type",
            "conditions": [
                F.col("pta_ssb_position.pool_type_cd")==F.col("pool_type.pool_type_cd")
            ],
            "type":"leftouter"
        },
    ]
    # override extract to get distinct records
    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_joined = df_joined.filter(F.col("fund_id")!=F.lit("GB35"))
        df_transformed = super().transform(df_joined).distinct()
        df_transformed = df_transformed.withColumn("cusip_id",
            F.when(F.isnull("cash_id") & F.isnull("fwd_id") & F.isnull("spot_id"),F.col("asset_id")).\
                otherwise(F.lit(None))
        )
        # populate security_type
        df_transformed = df_transformed.withColumn("secr_type",
            F.when(~F.isnull("cusip_id"),F.lit("SECURITY")).\
                when(~F.isnull("cash_id"),F.lit("CASH")).\
                when(~F.isnull("spot_id"),F.lit("SPOT")).\
                when(~F.isnull("fwd_id"),F.lit("FORWARD")).\
                otherwise(F.lit(None))
        )
        return df_transformed

    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("pta_ssb_position.asset_id"), "target": "asset_id" },
        { "source": F.col("pta_ssb_position.fund_id"), "target": "ssb_fund_nbr" },
        { "source": F.coalesce(F.lpad(F.col("pta_ssb_position.lot_acct_num"),6,"0"),F.lit("XXXXX")), "target": "lot_acct_nbr" },
        { "source": F.when(F.col("pta_ssb_position.asset_grp_cd")==F.lit("M"),F.col("pta_ssb_position.asset_id")).otherwise(F.lit(None)),
         "target": "cash_id"
        },
        { "source": F.when(F.col("pta_ssb_position.asset_grp_cd")==F.lit("P"),F.col("pta_ssb_position.asset_id")).otherwise(F.lit(None)),
         "target": "spot_id"
        },
        { "source": F.when(F.col("pta_ssb_position.asset_grp_cd")==F.lit("W"),F.col("pta_ssb_position.asset_id")).otherwise(F.lit(None)),
         "target": "fwd_id"
        },
        { "source": F.col("pta_ssb_position.trd_dt"), "target": "shrt_term_purch_dt" },
        { "source": F.col("pta_ssb_position.settle_dt"), "target": "shrt_term_sttl_dt" },
        { "source": F.col("pta_ssb_position.issue_dt"), "target": "orig_iss_dt" },
        { "source": F.col("pta_ssb_position.pos_mtrty_dt"), "target": "mtry_dt" },
        { "source": F.coalesce(F.col("existing.moody_rtng"),F.col("pta_ssb_position.moody_rtg")), "target": "moody_rtng" },
        { "source": F.coalesce(F.col("existing.s_p_rtng"),F.col("pta_ssb_position.snp_rtg")), "target": "s_p_rtng" },
        { "source": F.col("pta_ssb_position.call_put_ind"), "target": "call_put_ind" },
        { "source": F.col("pta_ssb_position.repo_num"), "target": "repo_nbr" },
        { "source": F.col("pta_ssb_position.ssb_trade_id"), "target": "ssb_trde_id" },
        { "source": F.col("pta_ssb_position.brkr_fins"), "target": "fx_brkr_cd" },
        { "source": F.col("pta_ssb_position.rpt_cls_cd"), "target": "rpt_cls_cd" },
        { "source": F.col("report_class.rpt_cls_desc"), "target": "rpt_cls_desc" },
        { "source": F.col("pta_ssb_position.issue_cls_cd"), "target": "iss_cls_cd" },
        { "source": F.col("issue_class.iss_cls_desc"), "target": "iss_cls_desc" },
        { "source": F.col("pta_ssb_position.contracts_qty"), "target": "nbr_of_contrct" },
        { "source": F.col("pta_ssb_position.org_face_pos_qty"), "target": "orig_face_amt" },
        { "source": F.col("pta_ssb_position.sw_closing_fx"), "target": "fx_closg_ind" },
        { "source": F.col("pta_ssb_position.invest_ind"), "target": "invmt_ind" },
        { "source": F.col("investment_indicator.invmt_ind_desc"), "target": "invmt_desc" },
        { "source": F.col("pta_ssb_position.asset_grp_cd"), "target": "asset_grp_cd" },
        { "source": F.col("asset_group.asset_grp_desc"), "target": "asset_grp_desc" },
        { "source": F.col("asset_group.invmt_mkt_val_flg"), "target": "invmt_mkt_val_flg" },
        { "source": F.col("pta_ssb_position.mkt_val_cd"), "target": "mkt_val_cd" },
        { "source": F.col("market_value.mkt_val_desc"), "target": "mkt_val_desc" },
        { "source": F.col("pta_ssb_position.pos_type_cd"), "target": "posn_type_cd" },
        { "source": F.col("position_type.posn_type_desc"), "target": "posn_type_desc" },
        { "source": F.col("pta_ssb_position.orig_cpn_rt"), "target": "orig_coupn_rt" },
        { "source": F.col("pta_ssb_position.var_rt_freq_cd"), "target": "varb_rt_freq_cd" },
        { "source": F.col("variable_rate_freq.varb_rt_freq_desc"), "target": "varb_rt_freq_desc" },
        { "source": F.col("pta_ssb_position.var_rt_chg_dt"), "target": "varb_rt_chg_dt" },
        { "source": F.col("pta_ssb_position.fasprc_src_cd"), "target": "mkt_pr_src_cd" },
        { "source": F.col("market_price_source.mkt_pr_src_vend"), "target": "mkt_pr_src_vend" },
        { "source": F.col("market_price_source.mkt_pr_src_desc"), "target": "mkt_pr_src_desc" },
        { "source": F.col("pta_ssb_position.pool_type_cd"), "target": "pool_type_cd" },
        { "source": F.col("pool_type.pool_type_desc"), "target": "pool_type_desc" },
        { "source": F.col("asset_group.confl_asset_grp_id"), "target": "confl_asset_grp_cd" },
        { "source": F.col("asset_group.confl_asset_grp_nm"), "target": "confl_asset_grp_nm" },
        # { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key" },        
        # { "source": F.lit(4), "target": "src_sys_id" },

    ]

