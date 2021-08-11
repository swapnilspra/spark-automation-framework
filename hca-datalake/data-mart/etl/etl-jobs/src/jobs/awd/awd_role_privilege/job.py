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
    target_table = "awd_role_privilege"
    business_key = ["rec_type_key","role_key","prvl_type_key"]
    primary_key = {"role_prvl_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "role_prvl": {
            "type": "file",
            "source": "awd_role_privilege"
        },
        "record_type": {
            "type": "table",
            "source": "awd_record_type"
        },
        "role": {
            "type": "table",
            "source": "awd_role"
        },
        "prvl_type": {
            "type": "table",
            "source": "awd_privilege_type"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "role_prvl"
        },
        {
            "source": "prvl_type",
            "conditions": [
                F.col("prvl_type") == F.col("prvtype")
            ]
        },
        {
            "source": "record_type",
            "conditions": [
                F.col("rec_cd") == F.col("recordcd")
            ]
        },
        {
            "source": "role",
            "conditions": [
                F.col("role_nm") == F.col("rolename")
            ]
        },
        {
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("cal_day")) == F.to_date(F.col("permsn_dt"))
            ],
            "type": "cross"
        }
    ]
  
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("record_type.rec_type_key"), "target": "rec_type_key" },
        { "source": F.col("role.role_key"), "target": "role_key" },
        { "source": F.col("prvl_type.prvl_type_key"), "target": "prvl_type_key" },
        { "source": F.col("calendar.day_key"), "target": "day_key" },    
        { "source": F.col("role_prvl.unitcd"), "target": "ut_cd" },
        { "source": F.col("role_prvl.existflag"), "target": "exist_flg" },
        { "source": F.col("role_prvl.viewflag"), "target": "view_flg" },
        { "source": F.col("role_prvl.updateflag"), "target": "updt_flg" },
        { "source": F.col("role_prvl.wrkselflag"), "target": "wk_slctn_flg" },
        { "source": F.to_timestamp(F.col("role_prvl.mntdattim"),"yyyy-MM-dd-HH.mm.ss"), "target": "last_maint_dt" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]


    # Override the extraction method to add PERMSN_DT column during CSV extraction
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        
        inputs = super().extract(catalog)

        inputs["role_prvl"] = inputs["role_prvl"].withColumn("PERMSN_DT",F.date_sub(F.current_timestamp(),1))

        return inputs
