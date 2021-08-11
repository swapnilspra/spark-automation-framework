from pyspark.sql.types import ArrayType, StringType, MapType, StructField, StructType, FloatType
from typing import Dict, List, Any
import math
import logging
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T

from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime
class Job(ETLJob):
    target_table = "fund_manager"
    business_key = ["fund_compst_key","sba_key", "pm_key"]
    primary_key = {"fund_mgr_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "fund_manager": {
            "type": "file",
            "source": "fund_manager"
        },
        "sub_advisor": {
            "type": "table",
            "source": "sub_advisor"
        },
        "portfolio_manager": {
            "type": "table",
            "source": "portfolio_manager"
        },
        "fund_composite": {
            "type": "table",
            "source": "fund_composite"
        }

    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "fund_manager"
        },
        {
            "source": "fund_composite",
            "conditions": [
                F.col("fund_composite.compst_cd") == F.col("fund_manager.cmp_id")
            ]
        },
        {
            "source": "sub_advisor",
            "conditions": [
                F.col("sub_advisor.sba_legacy_id") == F.col("fund_manager.sba_legacy_id")
            ]
        },
        {
            "source": "portfolio_manager",
            "conditions": [
                F.col("portfolio_manager.pm_legacy_id") == F.col("fund_manager.pm_legacy_id")
            ]
        }
    ]
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fund_composite.fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("sub_advisor.sba_key"), "target": "sba_key"},
        {"source": F.col("portfolio_manager.pm_key"), "target": "pm_key"},
        {"source": F.col("fund_manager.sba_efftv_strt_dt"), "target": "sba_efftv_strt_dt"},
        {"source": F.col("fund_manager.efftv_end_dt"), "target": "sba_efftv_end_dt"},
        {"source": F.col("fund_manager.pm_efftv_strt_dt"), "target": "pm_efftv_strt_dt"},
        {"source": F.col("fund_manager.pm_efftv_end_dt"), "target": "pm_efftv_end_dt"},
        {"source": F.col("fund_manager.fund_mgr_efftv_strt_dt"), "target": "fund_mgr_efftv_strt_dt"},
        {"source": F.col("fund_manager.fund_mgr_efftv_end_dt"), "target": "fund_mgr_efftv_end_dt"},
        {"source": F.col("fund_manager.sba_actv_flg"), "target": "sba_actv_flg"},
        {"source": F.col("fund_manager.pm_actv_flg"), "target": "pm_actv_flg"}
    ]

