from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):

    target_table = "shdr_acct_fund"
    business_key = ["dlr_branch_key","fund_key","acct_key", "shrhldr_key","dlr_key","reg_key"]
    primary_key = {"shrhldr_acct_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "cbl_shdr": {
            "type": "file",
            "source": "DTO.HCA.CLO.TIP39"
        },
        "cbl_acct": {
            "type": "file",
            "source": "DTO.HCA.AMP.TIP09"
        },
        "scd": {
            "type": "file",
            "source": "shareholder_consent_data"
        },
        "account": {
            "type": "table",
            "source": "account"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "dealer_branch": {
            "type": "table",
            "source": "dealer_branch"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "shareholder": {
            "type": "table",
            "source": "shareholder"
        },
        "region": {
            "type": "dimension",
            "source": "region"
        },
        "dst_st_country": {
            "type": "dimension",
            "source": "dst_st_country"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "cbl_shdr"
        },
        {
            "source": "cbl_acct",
            "conditions": [
                F.trim(F.col("cbl_acct.fund-code")) == F.trim(F.col("cbl_shdr.fund-code")),
                F.trim(F.col("cbl_acct.customer-account-number")) == F.trim(F.col("cbl_shdr.account-number"))
            ]
        },
        {
            "source": "scd",
            "conditions": [
                F.trim(F.col("scd.customer_id1")) == F.trim(F.col("cbl_shdr.cmr-bpr-id1")),
                F.trim(F.col("scd.customer_id2")) == F.trim(F.col("cbl_shdr.cmr-bpr-id2")),
                F.trim(F.col("scd.fund"))==F.trim(F.col("cbl_shdr.fund-code")),
                F.trim(F.col("scd.account"))==F.trim(F.col("cbl_shdr.account-number"))
            ],
            "type": "left"
        },
        {
            "source": "shareholder",
            "conditions": [
                F.trim(F.col("shareholder.cust_idnfcn_nbr")) == F.trim(F.col("cbl_shdr.cmr-bpr-id1")),
                F.trim(F.col("shareholder.cust_idnfcn_nbr_2")) == F.trim(F.col("cbl_shdr.cmr-bpr-id2"))
            ],
            "type": "left"
        },
        {
            "source": "dealer_branch",
            "conditions": [
                F.trim(F.col("dealer_branch.dlr_id")) == F.trim(F.col("cbl_acct.fins-id")),
                F.trim(F.col("dealer_branch.branch_id")) == F.trim(F.col("cbl_acct.dealer-branch-id"))
            ],
            "type": "left"
        },
        {
            "source": "fund",
            "conditions": [
                F.trim(F.col("fund.fund_nbr"))==  F.trim(F.col("cbl_shdr.fund-code"))
            ],
            "type": "left"
        },
        {
            "source": "account",
            "conditions": [
                F.trim(F.col("account.fund_nbr")) == F.trim(F.col("cbl_shdr.fund-code")),
                F.trim(F.col("account.acct_nbr")) == F.trim(F.col("cbl_shdr.account-number"))
            ],
            "type": "left"
        },
        {
            "source": "dealer",
            "conditions": [
                F.trim(F.col("dealer.dlr_id")) ==  F.trim(F.col("cbl_acct.fins-id"))
            ],
            "type": "left"
        }
    ]

    target_mappings:List[Dict[str,Any]] = [

        {"source": F.col("dealer_branch.dlr_branch_key"), "target": "dlr_branch_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("account.acct_key"), "target": "acct_key"},
        {"source": F.when(F.col("shareholder.shrhldr_key").isNull(),F.lit(-1)).otherwise(F.col("shareholder.shrhldr_key")), "target": "shrhldr_key"},
        {"source": F.col("dealer.dlr_key"), "target": "dlr_key"},
        {"source": F.col("region.reg_key"), "target": "reg_key"},
        {"source": F.col("cbl_shdr.clor-tax-rsp-prty"), "target": "prim_shrhldr_flg"},
        {"source": F.col("scd.stm_eml_adr_txt"), "target": "email_addr"},
        {"source": F.col("scd.cmr_infd_csnt"), "target": "email_cnsnt_flg"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"},
        {"source": F.lit(1), "target": "etl_load_cyc_key"}
    ]

    def join(self,inputs:Dict[str,pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:
        df_joined = super().join(inputs)
        df_region = inputs["region"]
        df_dst_st_country = inputs["dst_st_country"].withColumn("state_cd", F.coalesce(F.col("dst_st_country.dst_st_cd"), F.col("dst_st_country.dst_crty_cd")))

        df_country = df_dst_st_country\
            .join(df_region, [F.col("dst_st_country.st_cd") == F.col("region.st_cd"),
                               F.col("dst_st_country.crty_iso_cd") == F.col("region.crty_cd")], how="inner")

        df_joined = df_joined\
            .join(df_country, [F.col("state_cd") == F.col("cbl_acct.resident-state-country")], how="left")
        return df_joined