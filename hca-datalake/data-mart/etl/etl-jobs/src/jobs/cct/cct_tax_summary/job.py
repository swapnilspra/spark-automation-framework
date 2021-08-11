from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime

class Job(ETLJob):
    target_table = "cct_tax_summary"
    business_key = ["hbr_grp_key","spend_cat_key","suppl_key","key_ref"]
    primary_key = {"tax_sum_key": "int"}    
    sources:Dict[str,Dict[str,Any]] = {
        "tax_sum": {
            "type": "file",
            "source": "cct_tax_summary"
        },
        "supplier" : {
            "type": "table",
            "source": "cct_supplier"
        },
        "spend_category" : {
            "type": "table",
            "source": "cct_spend_category"
        },
        "harbor_group" : {
            "type": "table",
            "source": "cct_harbor_group"
        },
        "calendar" : {
            "type": "table",
            "source": "calendar"
        }
    }
    # JOIN: Calender, User, Queue
    joins:List[Dict[str,Any]] = [
        {
            "source": "tax_sum"
        },
        {
            "source": "supplier",
            "conditions": [
                F.col("supplier.suppl_nm") == F.upper(F.col("tax_sum.Supplier as Worktag"))
            ]
        },
        {
            "source": "spend_category",
            "conditions": [
                F.col("spend_category.spend_cat") == F.upper(F.col("tax_sum.Spend Category as Worktag"))
            ]
        },
        {
            "source": "harbor_group",
            "conditions": [
                F.col("harbor_group.co_nm") == F.upper(F.col("tax_sum.Company"))
            ]
        },
        # Where calender day is date paid
        { 
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("calendar.cal_day"))== F.to_date(F.col("tax_sum.Date Paid"),"MM/dd/yyyy")
            ]
        }
    ]


    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("harbor_group.hbr_grp_key"), "target": "hbr_grp_key" },
        { "source": F.col("spend_category.spend_cat_key"), "target": "spend_cat_key" },
        { "source": F.col("supplier.suppl_key"), "target": "suppl_key" },
        { "source": F.col("calendar.day_key"), "target": "day_key" },
        { "source": F.col("tax_sum.Key"), "target": "key_ref" },
        { "source": F.col("tax_sum.Line Memo"), "target": "line_memo" },
        { "source": F.col("tax_sum.Supplier's Invoice Number"), "target": "suppl_ref_nbr" },
        { "source": F.col("tax_sum.Product / invoice description"), "target": "invc_line_desc" },
        { "source": F.col("tax_sum.Tax Analysis/Conclusion"), "target": "tax_anlys_conclsn" },
        { "source": F.col("tax_sum.Ledger Debit (Credit) Amount"), "target": "invc_amt" },
        { "source": F.col("tax_sum.TOTAL"), "target": "tot_tax_amt" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]

    


