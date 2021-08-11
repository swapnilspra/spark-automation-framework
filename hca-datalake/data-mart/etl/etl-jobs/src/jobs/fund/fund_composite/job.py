from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "fund_composite"
    business_key = ["compst_cd"]
    primary_key = {"fund_compst_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "fc": {
            "type": "file",
            "source": "fund_composite"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.lit("All"), "target": "all_compst_cd" },
        { "source": F.lit("All Composites"), "target": "all_compst_desc"  },
        { "source": F.col("fc.asset_cls_nm"), "target": "asset_cls_nm" },
        { "source": F.col("fc.asset_cls_desc"), "target": "asset_cls_desc" },
        { "source": F.col("fc.sub_cls_nm"), "target": "sub_cls_nm" },
        { "source": F.col("fc.sub_cls_desc"), "target": "sub_cls_desc" },
        { "source": F.col("fc.styl_nm"), "target": "styl_nm" },
        { "source": F.col("fc.styl_desc"), "target": "styl_desc" },
        { "source": F.col("fc.compst_fund_id"), "target": "compst_cd" },
        { "source": F.col("fc.compst_nm"), "target": "compst_nm" },
        { "source": F.col("fc.perf_cd"), "target": "perf_cd" },
        { "source": F.col("fc.perf_nm"), "target": "perf_nm" },
        { "source": F.col("fc.compst_ord"), "target": "compst_ord" },
        { "source": F.col("fc.compst_stat_cd"), "target": "compst_stat_cd" },
        { "source": F.col("fc.compst_stat_desc"), "target": "compst_stat_desc" },
        { "source": F.col("fc.product_nm"), "target": "prod_nm" },
        { "source": F.col("fc.product_rltn"), "target": "invmt_rltn_type" },
        { "source": F.col("fc.adv_nm"), "target": "invmt_advsr" },
        { "source": F.col("fc.veh_nm"), "target": "invmt_veh" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" },
        { "source": F.col("fc.compst_src_id"), "target": "compst_src_id" },
        { "source": F.col("fc.fund_styl"), "target": "fund_styl" },
        { "source": F.col("fc.risk_ret_ord"), "target": "risk_ret_ord" },
        { "source": F.to_timestamp("compst_incpn_dt","MM/dd/yyyy"), "target": "compst_incpn_dt" },
        { "source": F.col("fc.sales_cat"), "target": "sales_cat"},
        { "source": F.col("fc.st_str_fund_nbr"), "target": "st_str_fund_nbr"},
    ]

