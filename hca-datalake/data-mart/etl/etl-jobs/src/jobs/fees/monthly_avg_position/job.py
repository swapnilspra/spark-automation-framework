from typing import Dict, List, Any
from datetime import datetime,timedelta
import pyspark.sql
from datetime import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob  # must be imported after spark has been set up
import jobs.fees.monthly_avg_posn_draft.job

class Job(jobs.fees.monthly_avg_posn_draft.job.Job):
    target_table = "monthly_avg_position"
    business_key = ["dlr_tpa_flg", "day_key", "dlr_key", "fund_key"]
    primary_key = {"mthly_avg_posn_key": "int"}

    # only difference vs the monthly draft is that we take the dealer as of the third business day. everything else is inherited
    def get_dealer_asof(self,df_account_position,df_calendar,p_date):
        # filter account positions in the selected business day

        # first, get the day_key of the third business day
        asof_business_day_key = df_calendar.filter(
            (F.col("bus_day_flg")==F.lit("Y")) &
            (F.col("cal_day").between(F.lit(p_date),F.lit(p_date+timedelta(days=10))))
        ).orderBy("cal_day").collect()[2]["day_key"]

        ap_asof_day = df_account_position.filter(F.col("account_position.day_key")==F.lit(asof_business_day_key)).select("account_position.acct_key","account_position.fund_key","account_position.dlr_key")
        ap_asof_day = ap_asof_day.alias("ap_asof_day")
        return ap_asof_day

