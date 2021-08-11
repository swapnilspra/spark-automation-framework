from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from itertools import chain

class Job(ETLJob):

    target_table = "shareholder"
    business_key = ["cust_idnfcn_nbr", "cust_idnfcn_nbr_2"]
    primary_key = {"shrhldr_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "cobol": {
            "type": "file",
            "source": "DTO.HCA.CLO.TIP42"
        },
        "gender": {
            "type": "dimension",
            "source": "gender"
        },
        "dst_st_country": {
            "type": "dimension",
            "source": "dst_st_country"
        },
        "region": {
            "type": "dimension",
            "source": "region"
        },
        "citizenship_status": {
            "type": "dimension",
            "source": "citizenship_status"
        },
        "party_type": {
            "type": "dimension",
            "source": "party_type"
        }

    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "cobol"
        },
        {
            "source": "gender",
            "conditions": [
                F.col("gender.gend_cd") == F.col("cobol.cmr-gnd-cde")
            ]
        },
        {
            "source": "citizenship_status",
            "conditions": [
                F.col("citizenship_status.citizenship_stat_cd") == F.col("cobol.cmr-ctz-cde")
            ]
        },
        {
            "source": "party_type",
            "conditions": [
                F.col("party_type.party_type_cd") == F.col("cobol.cmr-typ-cde")
            ]
        }
    ]

    target_mappings:List[Dict[str,Any]] = [

        {"source": F.lit(None).cast("int"), "target": "shrhldr_role_key"},
        {"source": F.lit("All"), "target": "shrhldr_top_lvl_cd"},
        {"source": F.lit("All Shareholders"), "target": "shrhldr_top_lvl_desc"},
        {"source": F.col("shrhldr_nm_calc"), "target": "shrhldr_nm"},
        {"source": F.col("cobol.cmr-tax-id"), "target": "tax_idnfcn_nbr"},
        {"source": F.lit(None).cast("string"), "target": "ssn_stat_desc"},
        {"source": F.col("cobol.cmr-tax-id-typ-cde"), "target": "tax_idnfcn_type"},
        {"source":F.to_timestamp(F.col("cobol.cmr-lst-lnk-lor"), "yyyyMMdd"), "target": "cust_lgl_ownr_regstn_dt"},
        {"source": F.col("cobol.cmr-bpr-id1"), "target": "cust_idnfcn_nbr"},
        {"source": F.col("cobol.cmr-bpr-id2"), "target": "cust_idnfcn_nbr_2"},
        {"source": F.col("cobol.cmr-mntr-vip-cde"), "target": "vip_cust_flg"},
        {"source": F.col("cobol.cmr-sen-mkt-ml-cde"), "target": "send_mktg_info_flg"},
        {"source": F.col("cobol.cmr-infd-csnt-cd"), "target": "informed_cnsnt_flg"},
        {"source": F.col("party_type.party_type_nm"), "target": "party_type"},
        {"source": F.col("cobol.cmr-fs-eee-cde"), "target": "cust_empl_flg"},
        {"source": F.lit("N"), "target": "mgmt_co_empl_flg"},
        {"source": F.col("citizenship_status.citizenship_stat_nm"), "target": "lgl_stat"},
        {"source": F.col("gender.gend_nm"), "target": "gend"},
        {"source": F.to_timestamp(F.col("cobol.cmr-bth-dte"), "yyyyMMdd"), "target": "brth_dt"},
        {"source": F.to_timestamp(F.col("cobol.cmr-dcs-dte"), "yyyyMMdd"),  "target": "dcsd_dt"},
        {"source": F.lit(None).cast("string"), "target": "emplmt_stat"},
        {"source": F.col("cobol.cmr-mrtl-sts-cde"), "target": "mrtl_stat"},
        {"source": F.col("cobol.cmr-ctz-cde"), "target": "crty_of_citzn_ship"},
        {"source": F.lit("Work"), "target": "addr_type"},
        {"source": F.col("cobol.label-ptl-adr-lin-txt1"), "target": "addr_line_1"},
        {"source": F.col("cobol.label-ptl-adr-lin-txt2"), "target": "addr_line_2"},
        {"source": F.col("cobol.label-ptl-adr-lin-txt3"), "target": "addr_line_3"},
        {"source": F.col("cobol.label-ptl-adr-lin-txt4"), "target": "addr_line_4"},
        {"source": F.trim(F.col("cobol.ptl-adr-cty-nme")), "target": "city"},
        {"source": F.col("region.st_cd"), "target": "st"},
        {"source": F.lit(None).cast("string"), "target": "prov"},
        {"source": F.lit(None).cast("string"), "target": "reg"},
        {"source": F.lit(None).cast("string"), "target": "crty"},
        {"source": F.to_timestamp(F.lit(None), "MM/dd/yyyy"), "target": "purg_prcsd_dt"},
        {"source": F.lit(None).cast("string"), "target": "purg_prcsd_flg"},
        {"source": F.lit("Y").cast("string"), "target": "curr_row_flg"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"}
    ]

    def join(self,inputs:Dict[str,pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        df_joined = super().join(inputs)

        # Create dataframes from tables or dimensions
        df_dst_st_country = inputs["dst_st_country"]
        df_region = inputs["region"]

        df_joined = df_joined\
            .join(df_dst_st_country, F.col("dst_st_country.dst_st_cd") == F.col("cobol.state-country-cde"), how="inner")

        df_joined = df_joined\
            .join(df_region, F.when(F.col("dst_st_country.st_cd").isNotNull(), F.col("region.st_cd") == F.col("dst_st_country.st_cd"))
                  .otherwise(F.col("region.crty_cd") == F.col("dst_st_country.crty_iso_cd")), how="inner")

        return df_joined

    def transform(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_transformed = self.calc_transformed(df_joined)
        df_transformed = super().transform(df_transformed)
        return df_transformed

    def calc_transformed(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_transformed = df_joined

        df_transformed = df_transformed\
            .withColumn("shrhldr_nm_calc", F.when(F.col("cobol.cmr-typ-cde") != 'I', F.col("cobol.customer-corporate-nm"))
                        .otherwise(F.when(F.col("cobol.cmr-frst-nme").isNull() & F.col("cobol.cmr-mdl-nme").isNull() & F.col("cobol.cmr-crp-lst-nme").isNull() & F.col("cobol.cmr-nme-sfx-txt").isNull()
                                          , 'Name withheld. Maybe Omnibus Account')
                        .otherwise(F.trim(F.concat(F.col("cobol.cmr-nme-pre-txt"),
                                                   F.lit(' '),
                                                   F.trim(F.col("cobol.cmr-frst-nme")),
                                                   F.lit(' '),
                                                   F.trim(F.col("cobol.cmr-mdl-nme")),
                                                   F.lit(' '),
                                                   F.trim(F.col("cobol.cmr-crp-lst-nme")),
                                                   F.trim(F.col("cobol.cmr-nme-sfx-txt")))))))

        return df_transformed
