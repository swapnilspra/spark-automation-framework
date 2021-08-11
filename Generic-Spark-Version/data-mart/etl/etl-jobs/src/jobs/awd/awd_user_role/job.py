from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "awd_user_role"
    business_key = ["role_key","usr_key"]
    primary_key = {"usr_role_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "user_role": {
            "type": "file",
            "source": "awd_user_role"
        },
        "user": {
            "type": "table",
            "source": "awd_user"
        },
        "role": {
            "type": "table",
            "source": "awd_role"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        }
    }
    # JOIN: Calendar, User, and Role
    joins:List[Dict[str,Any]] = [
        {
            "source": "user_role"
        },
        {
            "source": "user",
            "conditions": [
                F.col("user.usr_id") == F.col("user_role.userid")
            ]
        },
        {
            "source": "role",
            "conditions": [
                F.col("role.role_nm") == F.col("user_role.rolename")
            ]
        },  
        {   # Where calender day is yesterday's date (PERMSN_DT)
            "source": "calendar",
            "conditions": [
                 F.to_date(F.col("calendar.cal_day")) ==  F.to_date(F.col("permsn_dt")) 
            ],
            "type":"cross"
        }
    ]

    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("role.role_key"), "target": "role_key" },
        { "source": F.col("user.usr_key"), "target": "usr_key" },
        { "source": F.col("calendar.day_key"), "target": "day_key" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]

    # Override the extraction method to add PERMSN_DT column during CSV extraction
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        
        inputs = super().extract(catalog)

        inputs["user_role"] = inputs["user_role"].withColumn("permsn_dt",F.date_sub(F.current_timestamp(),1))

        return inputs