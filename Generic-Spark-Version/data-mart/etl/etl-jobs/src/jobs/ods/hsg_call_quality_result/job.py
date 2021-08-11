from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
import re

class Job(ETLJob):
    target_table = "hsg_call_quality_result"
    primary_key = {"cq_rslt_key": "int"}
    # no business key
    business_key = []

    sources:Dict[str, Dict[str, Any]] = {     
        "result": {
            "type": "file",
            "source": "hsg_call_quality_result"
        },
        "program": {
            "type": "table",
            "source": "hsg_call_quality_program"
        },
        "question": {
            "type": "table",
            "source": "hsg_call_quality_question"
        }
    }

    joins: List[Dict[str, Any]] = [
        {
            "source": "result"
        },
        {
            "source": "program",
            "conditions": [
                F.col("result.id") == F.col("program.cq_prog_id")
            ]
        },
        {
            "source": "question",
            "conditions": [
                F.lower(F.col("result.question")) == F.lower(F.col("question.qstn_col_nm"))
            ]
        }
    ]

    target_mappings:List[Dict[str, Any]] = [
        { "source": F.col("program.cq_prog_key"), "target": "cq_prog_key"},
        { "source": F.col("question.cq_qstn_key"), "target": "cq_qstn_key"},
        { "source": F.col("program.cq_prog_id"), "target": "cq_prog_id"},
        { "source": 
            F.when(F.col("question_result") == True, 1)
            .otherwise(
                F.when(F.col("question_result") == False, 0)
                .otherwise(F.lit(None))
            ), "target": "qstn_rslt" },
        { "source": F.col("score"), "target": "qstn_scor" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
    ]

    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)
        df_transformed = df_inputs["result"]
        
        # find columns for unpivot, whose names are like "Score (question 1 "This" is...), Score (question 2; some / chars), ..."
        # score: column name for score value
        # question: question string from score column, by removing "Score ()" and eplacing special chars into ""
        # question_value: questino value column name from score volumn, by removing "Score ()"
        # create list
        # [
        #   {"score": "Score (question 1 "This" is /; question)", "question": "question1Thisisquestion", "question_result": "question 1 "This" is /; question"},
        #   {"score": "Score (question 1 "This" is /; question)", "question": "question1Thisisquestion", "question_result": "question 1 "This" is /; question"},
        #   {"score": "Score (question 1 "This" is /; question)", "question": "question1Thisisquestion", "question_result": "question 1 "This" is /; question"},
        #   ...
        # ]
        cols = [
            {
                "score": col, 
                "question": re.sub(r"^Score \(", "", col).replace(" ", "").replace("\"", "").replace("\'", "").replace("/", "").replace(",", "").replace("-", "").replace("(", "").replace(")", "").replace(";", ""), 
                "question_result": re.sub(r"\)$", "", re.sub(r"^Score \(", "", col))
            } for col in df_transformed.columns if 'Score (' in col
        ]

        # dataframe for questions and scores
        # create new rows for each element in the given array
        unpivot = F.explode(
            # create array column
            F.array(
                [
                    # create new struct columns from cols list
                    F.struct(
                        F.lit(col["question"]).alias("question"), 
                        F.col(col["question_result"]).alias("question_result"), 
                        F.col(col["score"]).alias("score")
                    ) for col in cols
                ]
            )
        ).alias("unpivot")
        
        df_transformed = df_transformed.select(["id"] + [unpivot]).select(["id"] + ["unpivot.question", "unpivot.score", "unpivot.question_result"])

        df_inputs["result"] = df_transformed.alias("result")
        
        return df_inputs
