from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob

class Job(ETLJob):
    target_table = "dealer_branch"
    business_key = ["dlr_id", "branch_id"]
    primary_key = {"dlr_branch_key": "int"}
    sources:Dict[str, Dict[str, Any]] = {
        "db": {
            "type": "file",
            "source": "DTO.HCA.FBP.TIP55"
        },
        "state": {
            "type": "dimension",
            "source": "dst_st_country"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "db"
        },
        {
            "source": "state",
            "conditions": [
                F.col("db.fi-brch-st-co-cde") == F.col("state.dst_st_cd")
    ]
        }

    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("db.financial-inst-id"), "target": "dlr_id"},
        {"source": F.when(F.col("db.fincl-inst-brch-id") == '', F.lit(None)) \
            .otherwise(F.col("db.fincl-inst-brch-id")), "target": "branch_id"},
        {"source": F.when(F.trim(F.col("db.fi-brch-name")) == '', F.lit(None)) \
            .otherwise(F.trim(F.col("db.fi-brch-name"))), "target": "branch_nm"},
        {"source": F.when(F.trim(F.col("db.fi-brch-addr-1-txt")) == '', F.lit(None)) \
            .otherwise(F.trim(F.col("db.fi-brch-addr-1-txt"))), "target": "branch_addr_line_1"},
        {"source": F.when(F.trim(F.col("db.fi-brch-addr-2-txt")) == '', F.lit(None)) \
            .otherwise(F.trim(F.col("db.fi-brch-addr-2-txt"))), "target": "branch_addr_line_2"},
        {"source": F.when(F.trim(F.col("db.fi-brch-addr-3-txt")) == '', F.lit(None)) \
            .otherwise(F.trim(F.col("db.fi-brch-addr-3-txt"))), "target": "branch_addr_line_3"},
        {"source": F.when(F.trim(F.col("db.fi-brch-addr-4-txt")) == '', F.lit(None)) \
            .otherwise(F.trim(F.col("db.fi-brch-addr-4-txt"))), "target": "branch_addr_line_4"},
        {"source": F.trim(F.col("db.fi-brch-city-nme")), "target": "branch_city"},
        {"source": F.col("state.st_cd"), "target": "branch_st_cd"},
        {"source": F.col("state.st_nm"), "target": "branch_st"},
        {"source": F.col("state.crty_full_nm"), "target": "branch_crty"},
        {"source": F.col("db.fi-brch-zip-cde"), "target": "branch_zip_cd"},
        {"source": F.to_timestamp("db.fi-brch-estb-dte", "yyyy-MM-dd"), "target": "branch_estb_dt"},
        {"source": F.to_timestamp("db.fi-brch-closed-dte", "yyyy-MM-dd"), "target": "branch_closed_dt"},
        {"source": F.when(F.col("db.fi-brch-pay-ofc-id") == '', F.lit(None)) \
            , "target": "branch_pay_ofc_id"},
        {"source": F.when(F.col("db.fi-fg-br-fn-typ-id") == '', F.lit(None)) \
            , "target": "branch_func_type_id"},
        {"source": F.col("db.fi-br-foreign-tax"), "target": "branch_forgn_tax_cd"},
        {"source": F.lit(1), "target": "etl_load_cyc_key"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]
