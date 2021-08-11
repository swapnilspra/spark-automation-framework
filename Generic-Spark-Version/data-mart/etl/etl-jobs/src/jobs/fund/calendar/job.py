from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "calendar"
    business_key = ["day_key"]
    primary_key = {"day_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "calendar": {
            "type": "file",
            "source": "calendar"
        },
    }
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("calendar.day_key"), "target": "day_key" },
        { "source": F.to_timestamp(F.col("calendar.cal_day")), "target": "cal_day" },
        { "source": F.col("calendar.cal_day_shrt_nm"), "target": "cal_day_shrt_nm" },
        { "source": F.col("calendar.cal_day_long_nm"), "target": "cal_day_long_nm" },
        { "source": F.col("calendar.bus_day_flg"), "target": "bus_day_flg" },
        { "source": F.col("calendar.mkt_hol_flg"), "target": "mkt_hol_flg" },
        { "source": F.col("calendar.wk_cd"), "target": "wk_cd" },
        { "source": F.col("calendar.wk_nbr"), "target": "wk_nbr" },
        { "source": F.col("calendar.wk_shrt_nm"), "target": "wk_shrt_nm" },
        { "source": F.col("calendar.wk_long_nm"), "target": "wk_long_nm" },
        { "source": F.col("calendar.wk_ending_dt"), "target": "wk_ending_dt" },
        { "source": F.col("calendar.wk_days_cnt"), "target": "wk_days_cnt" },
        { "source": F.col("calendar.mo_cd"), "target": "mo_cd" },
        { "source": F.col("calendar.mo_nbr"), "target": "mo_nbr" },
        { "source": F.col("calendar.mo_nm"), "target": "mo_nm" },
        { "source": F.col("calendar.mo_shrt_nm"), "target": "mo_shrt_nm" },
        { "source": F.col("calendar.mo_long_nm"), "target": "mo_long_nm" },
        { "source": F.col("calendar.cal_mo_end_flg"), "target": "cal_mo_end_flg" },
        { "source": F.col("calendar.bus_mo_end_flg"), "target": "bus_mo_end_flg" },
        { "source": F.col("calendar.mo_ending_dt"), "target": "mo_ending_dt" },
        { "source": F.col("calendar.mo_days_cnt"), "target": "mo_days_cnt" },
        { "source": F.col("calendar.qtr_cd"), "target": "qtr_cd" },
        { "source": F.col("calendar.qtr_nbr"), "target": "qtr_nbr" },
        { "source": F.col("calendar.qtr_shrt_nm"), "target": "qtr_shrt_nm" },
        { "source": F.col("calendar.qtr_long_nm"), "target": "qtr_long_nm" },
        { "source": F.col("calendar.cal_qtr_end_flg"), "target": "cal_qtr_end_flg" },
        { "source": F.col("calendar.bus_qtr_end_flg"), "target": "bus_qtr_end_flg" },
        { "source": F.col("calendar.qtr_ending_dt"), "target": "qtr_ending_dt" },
        { "source": F.col("calendar.qtr_days_cnt"), "target": "qtr_days_cnt" },
        { "source": F.col("calendar.yr_cd"), "target": "yr_cd" },
        { "source": F.col("calendar.yr_nbr"), "target": "yr_nbr" },
        { "source": F.col("calendar.yr_shrt_nm"), "target": "yr_shrt_nm" },
        { "source": F.col("calendar.yr_long_nm"), "target": "yr_long_nm" },
        { "source": F.col("calendar.yr_ending_dt"), "target": "yr_ending_dt" },
        { "source": F.col("calendar.ye_flg"), "target": "ye_flg" },
        { "source": F.col("calendar.yr_last_bus_day_flg"), "target": "yr_last_bus_day_flg" },
        { "source": F.col("calendar.yr_days_cnt"), "target": "yr_days_cnt" },
        { "source": F.col("calendar.fiscl_wk_cd"), "target": "fiscl_wk_cd" },
        { "source": F.col("calendar.fiscl_wk_nbr").cast("int"), "target": "fiscl_wk_nbr" },
        { "source": F.col("calendar.fiscl_wk_shrt_nm"), "target": "fiscl_wk_shrt_nm" },
        { "source": F.col("calendar.fiscl_wk_long_nm"), "target": "fiscl_wk_long_nm" },
        { "source": F.to_timestamp(F.col("calendar.fiscl_wk_ending_dt")), "target": "fiscl_wk_ending_dt" },
        { "source": F.col("calendar.fiscl_wk_days_cnt").cast("int"), "target": "fiscl_wk_days_cnt" },
        { "source": F.col("calendar.fiscl_mo_cd"), "target": "fiscl_mo_cd" },
        { "source": F.col("calendar.fiscl_mo_nbr"), "target": "fiscl_mo_nbr" },
        { "source": F.col("calendar.fiscl_mo_shrt_nm"), "target": "fiscl_mo_shrt_nm" },
        { "source": F.col("calendar.fiscl_mo_long_nm"), "target": "fiscl_mo_long_nm" },
        { "source": F.col("calendar.fiscl_mo_ending_dt"), "target": "fiscl_mo_ending_dt" },
        { "source": F.col("calendar.fiscl_mo_end_flg"), "target": "fiscl_mo_end_flg" },
        { "source": F.col("calendar.fiscl_mo_last_bus_day_flg"), "target": "fiscl_mo_last_bus_day_flg" },
        { "source": F.col("calendar.fiscl_mo_days_cnt"), "target": "fiscl_mo_days_cnt" },
        { "source": F.col("calendar.fiscl_qtr_cd"), "target": "fiscl_qtr_cd" },
        { "source": F.col("calendar.fiscl_qtr_nbr"), "target": "fiscl_qtr_nbr" },
        { "source": F.col("calendar.fiscl_qtr_shrt_nm"), "target": "fiscl_qtr_shrt_nm" },
        { "source": F.col("calendar.fiscl_qtr_long_nm"), "target": "fiscl_qtr_long_nm" },
        { "source": F.col("calendar.fiscl_qtr_ending_flg"), "target": "fiscl_qtr_ending_flg" },
        { "source": F.col("calendar.fiscl_qtr_last_bus_day_flg"), "target": "fiscl_qtr_last_bus_day_flg" },
        { "source": F.col("calendar.fiscl_qtr_ending_dt"), "target": "fiscl_qtr_ending_dt" },
        { "source": F.col("calendar.fiscl_qtr_days_cnt"), "target": "fiscl_qtr_days_cnt" },
        { "source": F.col("calendar.fiscl_yr_cd"), "target": "fiscl_yr_cd" },
        { "source": F.col("calendar.fiscl_yr_shrt_nm"), "target": "fiscl_yr_shrt_nm" },
        { "source": F.col("calendar.fiscl_yr_long_nm"), "target": "fiscl_yr_long_nm" },
        { "source": F.col("calendar.fiscl_yr_ending_dt"), "target": "fiscl_yr_ending_dt" },
        { "source": F.col("calendar.fiscl_yr_ending_flg"), "target": "fiscl_yr_ending_flg" },
        { "source": F.col("calendar.fiscl_yr_last_bus_day_flg"), "target": "fiscl_yr_last_bus_day_flg" },
        { "source": F.col("calendar.fiscl_yr_nbr"), "target": "fiscl_yr_nbr" },
        { "source": F.col("calendar.fiscl_yr_days_cnt"), "target": "fiscl_yr_days_cnt" },
        { "source": F.col("calendar.curr_row_flg"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" },
        { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key" },        
    ]

