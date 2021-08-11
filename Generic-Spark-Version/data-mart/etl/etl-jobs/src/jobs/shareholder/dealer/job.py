from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob

class Job(ETLJob):
    target_table = "dealer"
    business_key = ["dlr_id"]
    primary_key = {"dlr_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "dealer": {
            "type": "file",
            "source": "DTO.HCA.FDP.TIP56"
        },
        "db": {
            "type": "table",
            "source": "dealer_branch"
        },
        "hdpd": {
            "type": "dimension",
            "source": "hsg_dealer_profile_database"
        },
        "hdg": {
            "type": "dimension",
            "source": "hsg_dealer_grouping"
        },
        "sf": {
            "type": "dimension",
            "source": "statement_frequency"
        },
        "irh": {
            "type": "dimension",
            "source": "invalid_rep_handling"
        },
        "isa": {
            "type": "dimension",
            "source": "investment_slip_address"
        },
        "dmt": {
            "type": "dimension",
            "source": "dealer_membership_type"
        },
        "pfreq": {
            "type": "dimension",
            "source": "payout_frequency"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "dealer"
        },
        {
            "source": "pfreq",
            "conditions": [
                F.col("dealer.fi-12b1-pay-freq") == F.col("pfreq.freq_cd")
            ],
            "type": "left"
        },
        {
            "source": "sf",
            "conditions": [
                F.col("dealer.fi-stm-frq-cd") == F.col("sf.stmt_freq_cd")
            ]
        },
        {
            "source": "dmt",
            "conditions": [
                F.col("dealer.fi-member-type-cde") == F.col("dmt.mbrs_type_cd")
            ],
            "type": "left"
        },
        {
            "source": "irh",
            "conditions": [
                F.col("dealer.onlin-invld-rep-cd") == F.col("irh.rep_hndlg_cd")
            ],
            "type": "left"
        },
        {
            "source": "isa",
            "conditions": [
                F.col("dealer.ivt-slip-adr-cd") == F.col("isa.slip_addr_cd")
            ],
            "type": "left"
        }
    ]
    target_mappings: List[Dict[str, Any]] = [
        # { "source": F.col("dealer.dlr_key"), "target": "dlr_key" },
        {"source": F.lit("All"), "target": "dlr_top_lvl"},
        {"source": F.lit("All Dealers"), "target": "dlr_top_lvl_desc"},
        {"source": F.col("hdg.dealergroupid"), "target": "dlr_grp_id"},
        {"source": F.upper(F.trim(F.when(((F.upper(F.col("hdg.dealergroupname")) == 'NO GROUPING')| (F.col("hdg.dealergroupname").isNull())),F.col("dealer.fincl-inst-name-1")).\
            otherwise(F.col("hdg.dealergroupname")))), "target": "dlr_grp_nm"},
        {"source": F.col("dealer.financial-inst-id"), "target": "dlr_id"},
        {"source": F.to_timestamp("dealer.fi-established-dte", "yyyy-MM-dd"), "target": "branch_estb_dt"},
        {"source": F.to_timestamp("dealer.fi-closed-dte", "yyyy-MM-dd"), "target": "branch_closed_dt"},
        {"source": F.col("dealer.fi-sls-agmt-reqd"), "target": "sellg_agrmt_rqrd_flg"},
        {"source": F.col("dealer.fi-12b1-agmt-reqd"), "target": "sellg_agrmt_12b1_flg"},
        {"source": F.col("dealer.fi-12b1-pay-freq"), "target": "payout_freq_cd"},
        {"source": F.col("pfreq.freq_desc"), "target": "payout_freq_desc"},
        {"source": F.substring(F.col("dealer.fi-stm-frq-cd"), 1, 2), "target": "stmt_freq_cd"},
        {"source": F.col("sf.freq_desc"), "target": "stmt_freq_desc"},
        {"source": F.col("dealer.fi-12b1-tape-cde"), "target": "recv_12b1_tape_flg"},
        {"source": F.col("dealer.fi-auto-div-chk-cd"), "target": "auto_divd_chk_flg"},
        {"source": F.col("dealer.wire-incl-acrl-cde"), "target": "wire_incl_accrl_flg"},
        {"source": F.col("dealer.sipc-insured-cde"), "target": "sipc_insd_flg"},
        {"source": F.col("dealer.sells-in-fdic-cde"), "target": "dlr_fdic_sell_flg"},
        {"source": F.col("dealer.fins-id"), "target": "sec_id"},
        {"source": F.substring(F.col("dealer.fi-member-type-cde"), 1, 2), "target": "mbr_ship_type_cd"},
        {"source": F.col("dmt.type_desc"), "target": "mbr_ship_type"},
        {"source": F.col("dealer.batch-invld-rep-cd"), "target": "inval_rep_hndlg_cd"},
        {"source": F.col("irh.hndlg_desc"), "target": "inval_rep_hndlg_desc"},
        {"source": F.col("dealer.ivt-slip-adr-cd"), "target": "invmt_slip_addr_cd"},
        {"source": F.col("isa.slip_addr_desc"), "target": "invmt_slip_addr"},
        {"source": F.col("dealer.trustee-dfl-dlr-id"), "target": "trste_dflt_dlr_id"},
        {"source": F.col("dealer.fi-fg-sls-chn-type"), "target": "grp_sales_chnl_id"},
        {"source": F.col("dealer.merged-to-fg-fi-id"), "target": "grp_fincl_inst_id"},
        {"source": F.col("dealer.div-confirm-suppression-cd"), "target": "divd_confirm_sprsn_flg"},
        {"source": F.col("dealer.daily-confirm-suppression-cd"), "target": "daily_confirm_sprsn_flg"},
        {"source": F.col("dealer.onlin-invld-rep-cd") , "target": "onln_inval_rep_hndlg_flg"},
        {"source": F.col("dealer.fi-state-lic-vld"), "target": "dlr_rep_st_lic_vldn_flg"},
        {"source": F.col("dealer.fi-invld-st-lic-cd"), "target": "dlr_inval_rep_st_lic_actn_flg"},
        {"source": F.col("dealer.fi-nasd-lic-vld"), "target": "nasd_regstn_vldn_lic_flg"},
        {"source": F.col("dealer.fi-invld-nasd-lic"), "target": "inval_rep_nasd_regstr_flg"},
        {"source": F.col("dealer.fincl-inst-name-1"), "target": "dlr_nm"},
        {"source": F.col("db.branch_addr_line_1"), "target": "addr_line_1"},
        {"source": F.col("db.branch_addr_line_2"), "target": "addr_line_2"},
        {"source": F.col("db.branch_addr_line_3"), "target": "addr_line_3"},
        {"source": F.col("db.branch_addr_line_4"), "target": "addr_line_4"},
        {"source": F.col("db.branch_city"), "target": "city_nm"},
        {"source": F.substring(F.col("db.branch_st_cd"), 1, 2), "target": "st"},
        {"source": F.substring(F.col("db.branch_zip_cd"), 1, 5).cast(T.FloatType())\
            , "target": "postl_zip_cd"},
        {"source": F.lit("UNITED STATES"), "target": "crty"},
        {"source": F.to_date("hdpd-created", "yyyy-MM-dd"), "target": "intrl_cntl_doc_dt"},
        {"source": F.to_timestamp("hdpd-dateofreceiptofmostrecentcontrolauditreport", "yyyy-MM-dd"),
         "target": "ltst_cntl_audt_recpt_dt"},
        {"source": F.lit(None), "target": "inactv_dt"},
        {"source": F.lit("Y"), "target": "actv_flg"},
        {"source": F.lit(4), "target": "src_sys_id"},
        {"source": F.lit(1), "target": "etl_load_cyc_key"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.current_timestamp(), "target": "row_strt_dttm"},
        {"source": F.when(F.col("hdpd.dealernumber").isNull(),F.lit(None))\
            .when(F.lower(F.col("hdpd-tradessuperomnibusvalue")) == 'yes', F.lit('Y')) \
            .when(F.lower(F.col("hdpd-tradessuperomnibusvalue")) == 'no', F.lit('N')) \
            .otherwise(F.lit('U')), "target": "spr_onbs_flg"},
        {"source": F.when(F.col("hdpd.dealernumber").isNull(),F.lit(None))\
            .when(F.lower(F.col("hdpd-tradesomnibusvalue")) == 'yes', F.lit('Y')). \
            when(F.lower(F.col("hdpd-tradesomnibusvalue")) == 'no', F.lit('N')). \
            otherwise(F.lit('U')),
         "target": "onbs_flg"}
    ]

    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:
        inputs["hdpd"]=inputs["hdpd"].groupBy(F.col("hdpd.dealernumber")).agg(F.max(F.col("hdpd.dealergroupid")).alias("hdpd-dealergroupid"),\
                     F.max(F.col("hdpd.created")).alias("hdpd-created"),\
                     F.max(F.col("hdpd.dateofreceiptofmostrecentcontrolauditreport")).alias("hdpd-dateofreceiptofmostrecentcontrolauditreport"),\
                     F.max(F.col("hdpd.tradessuperomnibusvalue")).alias("hdpd-tradessuperomnibusvalue"),\
                     F.max(F.col("hdpd.tradesomnibusvalue")).alias("hdpd-tradesomnibusvalue"))

        joindf = super().join(inputs)\
            .join(inputs["hdpd"] , [F.col("dealer.financial-inst-id") == F.trim(F.col("hdpd.dealernumber"))] , "left")\
            .join(inputs["hdg"] , [F.col("hdpd-dealergroupid") == F.col("hdg.id")] , "left")

        df1 = inputs["db"].where(F.col("db.branch_id") == '000') \
            .select("db.dlr_id", "db.branch_id", "db.branch_addr_line_1", "db.branch_addr_line_2","db.branch_addr_line_3",
                    "db.branch_addr_line_4", "db.branch_city", "db.branch_st_cd", "db.branch_zip_cd")

        df2 = inputs["db"].where(F.col("db.branch_id") == 'PAYEE') \
            .select("db.dlr_id", "db.branch_id", "db.branch_addr_line_1", "db.branch_addr_line_2","db.branch_addr_line_3",
                    "db.branch_addr_line_4", "db.branch_city","db.branch_st_cd","db.branch_zip_cd")

        df_output = df2.join(df1,"dlr_id","left_anti")
        df_union = df_output.union(df1)

        joindf = joindf.join(df_union,[F.col("db.dlr_id") == F.col("dealer.financial-inst-id")],'left')

        return joindf

    # override extract to get distinct records
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)
        df_inputs["dealer"] = df_inputs["dealer"].distinct()
        return df_inputs
