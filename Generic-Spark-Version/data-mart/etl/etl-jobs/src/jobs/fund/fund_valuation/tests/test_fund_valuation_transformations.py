from pyspark.sql.types import *
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
import logging
import os
import common.pytest_utils

def test_first_day_weekday(request,tmpdir):
    #
    # set up spark
    #
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "../lib/postgresql-42.2.8.jar") \
        .getOrCreate()
    # spark.sparkContext.addFile('./common',recursive=True)
    env = {
        "file_prefix":os.path.join(request.fspath.dirname,"data"),
        # use parquet files at the job level
        "folders": {
            "datamart":os.path.join(request.fspath.dirname,"data","datamart")
        },
    }
    logging.basicConfig()
    logger = logging.getLogger("test_fund_valuation")


    import jobs.fund.fund_valuation.job
    job = jobs.fund.fund_valuation.job.Job(spark,env,logger)

    df_test = common.pytest_utils.load_test_csv(spark,os.path.join(request.fspath.dirname,"data","test_calculations.csv"))

    df_result = job.calculate_wealth_index(df_test)

    common.pytest_utils.assert_df_matches_expected(df_result,"calculated_daily_return",precision=1e-12)

    common.pytest_utils.assert_df_matches_expected(df_result,"calculated_daily_pr_diff",precision=1e-12)

    common.pytest_utils.assert_df_matches_expected(df_result,"calculated_wlth_idx",precision=1e-12)
