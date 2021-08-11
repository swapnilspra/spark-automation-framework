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
    target_table = "mstr_fund_metrics"
    business_key = ["mstr_fund_key","day_key"]
    primary_key = {"mstr_fund_metrics_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "src": {
            "type": "file",
            "source": "mstr_fund_incremental"
        },
        "fund": {
            "type": "table",
            "source": "mstr_fund"
        },
        "cal": {
            "type": "table",
            "source": "calendar"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "src"
        },
        {
            "source": "cal",
            "conditions": [
                F.to_date(F.col("cal.cal_day")) == F.to_date(F.col("src.Date"), "MM/dd/yyyy")
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("src.FundId") == F.col("fund.fund_id")
            ]
        }
    ] 
    # target mapping   
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fund.mstr_fund_key"), "target": "mstr_fund_key"},
        {"source": F.col("cal.day_key"), "target": "day_key"},
        { "source": F.col("src.FundSize"), "target": "aum" },
        { "source": F.col("src.EstimatedNetFlow"), "target": "net_cashflow" },
        { "source": F.col("src.HoldingsCount"), "target": "holdings_count" },
        { "source": F.col("src.TotalReturn1Year"), "target": "one_yr_return" },
        { "source": F.col("src.TotalReturn3Year"), "target": "three_yr_return" },
        { "source": F.col("src.ExcessReturn1Year"), "target": "one_yr_excess_return" },
        { "source": F.col("src.ExcessReturn3Year"), "target": "three_yr_excess_return" },
        { "source": F.col("src.Beta"), "target": "beta" },
        { "source": F.col("src.TrackingError"), "target": "tracking_error" },
        { "source": F.col("src.MorningstarQRating"), "target": "fund_rtng" },
        { "source": F.col("src.MorningstarOverallRating"), "target": "overall_rating" },
        { "source": F.col("src.MorningstarAnalystRating"), "target": "overall_anlst_rating" },
        { "source": F.col("src.MorningstarParentAnalystRating"), "target": "parent_anlst_rating" },
        { "source": F.col("src.MorningstarParentQRating"), "target": "parent_qt_rating" },
        { "source": F.col("src.MorningstarPeopleAnalystRating"), "target": "people_anlst_rating" },
        { "source": F.col("src.MorningstarPeopleQRating"), "target": "people_qt_rating" },
        { "source": F.col("src.MorningstarPerformanceAnalystRating"), "target": "perf_anlst_rating" },
        { "source": F.col("src.MorningstarPerformanceQRating"), "target": "perf_qt_rating" },
        { "source": F.col("src.MorningstarPriceAnalystRating"), "target": "price_anlst_rating" },
        { "source": F.col("src.MorningstarPriceQRating"), "target": "price_qt_rating" },
        { "source": F.col("src.MorningstarProcessAnalystRating"), "target": "process_anlst_rating" },
        { "source": F.col("src.MorningstarProcessQRating"), "target": "process_qt_rating" },
        { "source": F.col("src.AnnualReportNetExpRatio"), "target": "annual_rpt_net_exp_ratio" },
        { "source": F.col("src.ManagementFee"), "target": "mgmt_fee" },
        { "source": F.lit("Morningstar"), "target": "source_system" },        
        {"source": F.lit("Y"), "target": "current_row_flg"}
    ]

