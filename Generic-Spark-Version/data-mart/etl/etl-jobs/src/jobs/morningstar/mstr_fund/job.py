from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "mstr_fund"
    business_key = ["fund_id"]
    primary_key = {"mstr_fund_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "mf": {
            "type": "file",
            "source": "mstr_fund_incremental"
        }
    }
    # override transform to get distinct list of records for facility
    def transform(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_transformed = df_joined.groupBy("FundId").agg(F.max(F.col("GlobalCategory")).alias("GlobalCategory"),   
            F.max(F.col("FirmName")).alias("FirmName"),  
            F.max(F.col("MorningstarCategory")).alias("MorningstarCategory"),   
            F.max(F.col("Name")).alias("Name"), 
            F.max(F.col("InceptionDate")).alias("InceptionDate"), 
            F.max(F.col("USCategory")).alias("USCategory"),
            F.max(F.col("InvestmentType")).alias("InvestmentType"),
            F.max(F.col("ObsoleteType")).alias("ObsoleteType"),
            F.max(F.col("ObsoleteDate")).alias("ObsoleteDate"),
            F.max(F.col("EnhancedIndex")).alias("EnhancedIndex"),
            F.max(F.col("FundofFunds")).alias("FundofFunds"),
            F.max(F.col("IndexFund")).alias("IndexFund"),
            F.max(F.col("FundStandardName")).alias("FundStandardName")
        )

        df_transformed = super().transform(df_transformed)
        return df_transformed

    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("FundId"), "target": "fund_id" },
        { "source": F.col("GlobalCategory"), "target": "global_cat" },
        { "source": F.col("FirmName"), "target": "firm_nm" },
        { "source": F.col("MorningstarCategory"), "target": "mstar_cat" },
        { "source": F.col("Name"), "target": "fund_nm" },
        { "source": F.to_timestamp("InceptionDate","MM/dd/yyyy"), "target": "inception_date" },
        { "source": F.col("USCategory"), "target": "us_cat_group" },
        { "source": F.col("InvestmentType"), "target": "investment_type" },
        { "source": F.col("ObsoleteType"), "target": "fund_status" },
        { "source": F.to_timestamp("ObsoleteDate","MM/dd/yyyy"),  "target": "fund_close_date" },
        { "source": F.col("EnhancedIndex").substr(1,1), "target": "enhanced_idx_flg" },
        { "source": F.col("FundofFunds").substr(1,1), "target": "fund_of_funds_flg" },
        { "source": F.col("IndexFund").substr(1,1), "target": "idx_fund_flg" },
        { "source": F.lit("Y"), "target": "current_row_flg" },
        { "source": F.lit("Morningstar"), "target": "source_system" },
        { "source": F.col("FundStandardName"), "target": "fund_standard_nm" },
    ]

