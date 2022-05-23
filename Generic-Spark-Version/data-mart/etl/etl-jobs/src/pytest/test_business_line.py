import pytest
import pyspark
from pyspark.sql import SparkSession
import importlib
from typing import Dict, List, Any
import common.utils
import os
import logging
import sys


class test_sample:

    @pytest.mark.usefixtures("spark_session")
    def test_sample_transform():
        spark = SparkSession \
            .builder \
            .appName("etltest") \
            .getOrCreate()
        import configparser
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'D:\\Spark_automation_framework\\spark-automation-framework\\Generic-Spark-Version\\data-mart\\etl\\etl-jobs\\scripts\\environment.ini'))

        catalog: Dict[str, Any] = common.utils.get_catalog()

        env: Dict[str, Any] = {
            "jdbc":
                {"url": config['db']['url'],
                 "database": config['db']['database'],
                 "user": config['db']['user'],
                 "password": config['db']['password'],
                 "schema": 'hdm'
                 },
            "folders": {},
            "catalog": catalog
        }
        env["file_prefix"] = "D:\\Spark_automation_framework\\spark-automation-framework\\Generic-Spark-Version\\data-mart\\etl\\etl-jobs\\src\\jobs\\%s\\%s\\tests\\data" % ('fees','business_line')
        logging.basicConfig()
        logger = logging.getLogger('fees.business_line')
        # level = logging.getLevelName(loglevel)
        # logger.setLevel(loglevel)
        print("hello")

        print(catalog)

        from common.etl_job import ETLJob  # must be imported after spark has been set up
        job_module = importlib.import_module("jobs.%s.job" % 'fees.business_line')
        job_class = getattr(job_module, "Job")
        job: ETLJob = job_class(spark, env, logger, [])

        # read the Raw file
        inputs: Dict[str, pyspark.sql.DataFrame] = job.extract(env["catalog"])
        # join the dataframe
        df_joined: pyspark.sql.DataFrame = job.join(inputs)
        # transform to target structure
        df_target: pyspark.sql.DataFrame = job.transform(df_joined)

        #Output parquet file count
        out_df = spark.read.parquet("D:\\Spark_automation_framework\\spark-automation-framework\\Generic-Spark-Version\\data-mart\\etl\\etl-jobs\\src\\jobs\\fees\\business_line\\tests\\data\\datamart\\business_line")
        assert (df_target.count() == out_df.count()),"Raw and target count not matching"
        print("Count test passed")


    test_sample_transform()