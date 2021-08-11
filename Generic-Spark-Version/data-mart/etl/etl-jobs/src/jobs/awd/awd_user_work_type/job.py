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
    target_table = "awd_user_work_type"
    business_key = ["rec_type_key","usr_key","wk_type_key"]
    primary_key = {"usr_wk_type_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "user_wk_type": {
            "type": "file",
            "source": "awd_user_work_type"
        },
         "rec_type": {
            "type": "table",
            "source": "awd_record_type"
        },
        "wk_type": {
            "type": "table",
            "source": "awd_work_type"
        },
        "user": {
            "type": "table",
            "source": "awd_user"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        }
    }
    # JOIN: Record Type, Work Type, User, and Calendar
    joins:List[Dict[str,Any]] = [
        {
            "source": "user_wk_type"
        },
        {
            "source": "rec_type",
            "conditions": [
                F.col("rec_type.rec_cd") == F.col("user_wk_type.recordcd")
            ]
        },
        {
            "source": "wk_type",
            "conditions": [
                F.col("wk_type.wk_type_nm") == F.col("user_wk_type.wrktype")
            ]
        },
        {
            "source": "user",
            "conditions": [
                 F.col("user.usr_id") == F.col("user_wk_type.userid")
            ]
        },
        {   # Where calender day is yesterday's date (PERMSN_DT)
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("calendar.cal_day")) == F.to_date(F.col("permsn_dt"))

            ],
            "type": "cross"
        }
    ]
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("rec_type.rec_type_key"), "target": "rec_type_key" },
        { "source": F.col("wk_type.wk_type_key"), "target": "wk_type_key" },
        { "source": F.col("user.usr_key"), "target": "usr_key" },
        { "source": F.col("calendar.day_key"), "target": "day_key" },
        { "source": F.col("user.ut_cd"), "target": "ut_cd" },
        { "source": F.col("user_wk_type.existflag"), "target": "exist_flg" },
        { "source": F.col("user_wk_type.viewflag"), "target": "view_flg" },
        { "source": F.col("user_wk_type.updateflag"), "target": "updt_flg" },
        { "source": F.col("user_wk_type.wrkselflag"), "target": "wk_slctn_flg" },
        { "source": F.to_timestamp(F.col("user_wk_type.mntdattim"),"yyyy-MM-dd-HH.mm.ss"), "target": "last_maint_dt" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]

    


    # Override the extraction method to add PERMSN_DT column during CSV extraction
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        
        inputs = super().extract(catalog)

        inputs["user_wk_type"] = inputs["user_wk_type"].withColumn("permsn_dt",F.date_sub(F.current_timestamp(),1))

        return inputs