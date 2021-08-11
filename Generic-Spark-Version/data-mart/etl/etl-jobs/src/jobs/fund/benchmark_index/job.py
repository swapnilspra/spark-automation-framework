from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "benchmark_index"
    business_key = ["idx_id"]
    primary_key = {"bmk_idx_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "benchmark": {
            "type": "file",
            "source": "benchmark"
        }
    }
    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("benchmark.idx_id"), "target": "idx_id"},
        {"source": F.col("benchmark.idx_nm"), "target": "idx_nm"},
        {"source": F.col("benchmark.idx_shrt_nm"), "target": "idx_shrt_nm"},
        {"source": F.col("benchmark.robeco_idx_id").cast(T.IntegerType()), "target": "robeco_idx_id"},
        {"source": F.col("benchmark.bloomberg_sym"), "target": "bloomberg_sym"},
        {"source": F.col("benchmark.idx_type"), "target": "idx_type"},
        {"source": F.col("benchmark.idx_type_desc"), "target": "idx_type_desc"},
        {"source": F.col("benchmark.bmk_legacy_id"), "target": "bmk_legacy_id"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4), "target": "src_sys_id"}

    ]

