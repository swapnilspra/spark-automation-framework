from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "awd_user_queue"
    business_key = ["q_key","usr_key"]
    primary_key = {"usr_q_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "user_q": {
            "type": "file",
            "source": "awd_user_queue"
        },
        "user": {
            "type": "table",
            "source": "awd_user"
        },
        "queue": {
            "type": "table",
            "source": "awd_queue"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        }
    }
    # JOIN: Calender, User, Queue
    joins:List[Dict[str,Any]] = [
        {
            "source": "user_q"
        },
        {
            "source": "user",
            "conditions": [
                F.col("user.usr_id") == F.col("user_q.userid")
            ]
        },
        {
            "source": "queue",
            "conditions": [
                F.col("queue.q_cd") == F.col("user_q.queuecd")
            ]
        },
        # Where calender day is yesterday's date (PERMSN_DT)
        { 
            "source": "calendar",
            "conditions": [
                F.to_date(F.col("calendar.cal_day"))==  F.to_date(F.col("permsn_dt"))
            ],
            "type":"cross"
        }
    ]

    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("queue.q_key"), "target": "q_key" },
        { "source": F.col("user.usr_key"), "target": "usr_key" },
        { "source": F.col("calendar.day_key"), "target": "day_key" },
        { "source": F.col("user.ut_cd"), "target": "ut_cd" },
        { "source": F.col("user_q.existflag"), "target": "exist_flg" },
        { "source": F.col("user_q.viewflag"), "target": "view_flg" },
        { "source": F.col("user_q.updateflag"), "target": "updt_flg" },
        { "source": F.col("user_q.wrkselflag"), "target": "wk_slctn_flg" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
]

      # Override the extraction method to add PERMSN_DT column during CSV extraction
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        
        inputs = super().extract(catalog)

        inputs["user_q"] = inputs["user_q"].withColumn("permsn_dt",F.date_sub(F.current_timestamp(),1))

        return inputs