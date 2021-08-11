from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "spl_hsg_daily_tasks_lookup"
    # no primary key exists
    primary_key = {"": "int"}
    business_key = ["id"]

    sources:Dict[str, Dict[str, Any]] = {     
        "spl_hsg_daily_tasks_lookup": {
            "type": "file",
            "source": "spl_hsg_daily_tasks_lookup"
        }
    }

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.col("Department"), "target": "department" },
        { "source": F.col("Priority"), "target": "priority" },
        { "source": F.col("Deadline"), "target": "deadline" },
        { "source": F.col("Source"), "target": "source" },
        { "source": F.col("Description"), "target": "description" },
        { "source": 
            F.when(F.col("Level I Task") == True, 1)
            .otherwise(0), 
          "target": "level_i_task" },
        { "source": 
            F.when(F.col("Level II Task") == True, 1)
            .otherwise(0), 
          "target": "level_ii_task" },
        { "source": 
            F.when(F.col("Level III Task -BFDS") == True, 1)
            .otherwise(
                F.when(F.col("Level III Task -BFDS") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "bfds_task_in_level_iii" },
        { "source": 
            F.when(F.col("Level III Task -HSG") == True, 1)
            .otherwise(0), 
          "target": "hsg_task_level_iii" },
        { "source": F.col("Task-Title"), "target": "task_title" },
        { "source": F.col("Area of Responsibility"), "target": "area_of_responsibility" },
        { "source": F.col("Task Status"), "target": "task_status" },
        { "source": F.to_timestamp("Modified"), "target": "modified" },
        { "source": F.col("ID"), "target": "id" },
        { "source": F.to_timestamp("Created"), "target": "created" },
        { "source": F.col("Created By"), "target": "created_by" },
        { "source": F.col("Modified By"), "target": "modified_by" },
    ]
