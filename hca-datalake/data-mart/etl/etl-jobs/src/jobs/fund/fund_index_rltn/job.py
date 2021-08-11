from pyspark.sql.types import ArrayType, StringType, MapType, StructField, StructType, FloatType
from typing import Dict, List, Any
import math
import logging
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime
class Job(ETLJob):
    target_table = "fund_index_rltn"
    business_key = ["fund_compst_key",  "bmk_idx_key",  "perf_run_type_key"]
    primary_key = {"fund_idx_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "fund_index_rltn": {
            "type": "file",
            "source": "fund_index_rltn"
        },
        "fund_composite": {
            "type": "table",
            "source": "fund_composite"
        },
        "benchmark_index": {
            "type": "table",
            "source": "benchmark_index"
        },
        "perf_run_type": {
            "type": "table",
            "source": "perf_run_type"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "fund_index_rltn"
        },
        {
            "source": "fund_composite",
            "conditions": [
                F.col("fund_composite.compst_cd") == F.col("fund_index_rltn.compst_fund_id")
            ]
        },
        {
            "source": "benchmark_index",
            "conditions": [
                F.col("benchmark_index.bmk_legacy_id") == F.col("fund_index_rltn.bmk_legacy_id")
            ]
        },
        {
            "source": "perf_run_type",
            "conditions": [
                F.col("perf_run_type.run_type_cd") == F.col("fund_index_rltn.perf_run_type_cd")
            ]
        }

    ]
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fund_composite.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("benchmark_index.bmk_idx_key"), "target": "bmk_idx_key"},
        {"source": F.col("perf_run_type.perf_run_type_key"), "target": "perf_run_type_key"},
        {"source": F.col("fund_index_rltn.link_prio"), "target": "link_prio"},
        {"source": F.col("fund_index_rltn.disp_sort_ord"), "target": "rpt_sort_ord"},
        {"source": F.col("fund_index_rltn.prim_bmk_flg"), "target": "prim_bmk_flg"},
        {"source": F.col("fund_index_rltn.gr_ret_inlsn_flg"), "target": "gr_ret_inlsn_flg"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"}
    ]

