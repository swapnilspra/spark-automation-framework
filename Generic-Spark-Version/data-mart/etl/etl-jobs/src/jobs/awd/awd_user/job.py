from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime

class Job(ETLJob):
    target_table = "awd_user"
    business_key = ["usr_id","grp_nm","rout_usr_id"]
    primary_key = {"usr_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "usr": {
            "type": "file",
            "source": "awd_user"
        }
    }

    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("usr.groupcd"), "target": "grp_nm" },
        { "source": F.col("usr.useridstat"), "target": "usr_stat_cd" },
        { "source": F.col("usr.useridstat"), "target": "usr_stat_desc" },
        { "source": F.col("usr.userid"), "target": "usr_id" },
        { "source": F.col("usr.firstname"), "target": "first_nm" },
        { "source": F.col("usr.middleinit"), "target": "mint" },
        { "source": F.col("usr.lastname"), "target": "last_nm" },
        { "source": F.col("usr.socsecno"), "target": "tax_id" },
        { "source": F.col("usr.classify"), "target": "clsfcn" },
        { "source": F.col("usr.lockstat"), "target": "lck_stat" },
        { "source": F.col("usr.queueflag"), "target": "q_flg" },
        { "source": F.col("usr.phonenbr"), "target": "phon_nbr" },
        { "source": F.col("usr.faxnbr"), "target": "fax_nbr" },
        { "source": F.col("usr.wrksel"), "target": "wk_slctn_cd" },
        { "source": F.col("usr.faxdept"), "target": "fax_dept" },
        { "source": F.col("usr.routuserid"), "target": "rout_usr_id" },
        { "source": F.col("usr.seclevel"), "target": "sec_lvl_cd" },
        { "source": F.col("usr.unitcd"), "target": "ut_cd" },
        { "source": F.to_timestamp(F.col("usr.mntdattim"),"yyyy-MM-dd-HH.mm.ss"), "target": "last_maint_dt" },
        { "source": F.to_timestamp(F.col("usr.lastdattim"),"yyyy-MM-dd-HH.mm.ss"), "target": "last_dt" },
        { "source": F.to_timestamp(F.col("usr.pswdmntdat"), "MM/dd/yy"), "target": "last_pswd_maint_dt" }, 
        { "source": F.col("usr.awd_dsktop_version"), "target": "awd_desktop_vrsn" },
        { "source": F.col("usr.data_revision"), "target": "data_revs" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" }
    ]

