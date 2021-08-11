import pyspark.sql.types as T
from pyspark.sql import SparkSession
import common.utils
import pytest
import os
import logging
from common.etl_job import ETLJob

@pytest.fixture(scope="session")
def spark(request):
    """Fixture for creating a spark context."""
    spark = SparkSession \
        .builder \
        .appName("SuperGlue") \
        .getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    return spark

@pytest.fixture(scope="session")
def env(request):
    test_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'data','test_multiple')
    # point to our test data folder
    env = {
        "file_prefix": test_path
    }
    return env

def test_multiple_files_csv(spark,env):

    catalog = {
        "test": {
            "path":".*\.csv",
            "format":"csv",
            "header":True
        }
    }
    # define dummy job class
    class testJob(ETLJob):
        sources = {
            "test": {
                "type":"file",
                "source":"test",
                "limit":None,
                "sort":"size",
                "ascending":False
            }
        }
    testJobInstance = testJob(spark,env,logging.getLogger(),{})
    inputs = testJobInstance.extract(catalog)

    df_expected = spark.createDataFrame([
        ("U3","Unknown3"),
        ("U31","Bla"),
        ("U2","Unknown2"),
        ("U","Unknown")
    ],["GEND_CD","GEND_NM"])

    assert inputs["test"].orderBy("GEND_CD").collect()==df_expected.orderBy("GEND_CD").collect()

def test_multiple_files_csv_by_size(spark,env):
    # get only the largest file
    catalog = {
        "test": {
            "path":".*\.csv",
            "format":"csv",
            "header":True
        }
    }
    # define dummy job class
    class testJob(ETLJob):
        sources = {
            "test": {
                "type":"file",
                "source":"test",
                "limit":1,
                "sort":"size",
                "ascending":False
            }
        }
    testJobInstance = testJob(spark,env,logging.getLogger(),{})
    inputs = testJobInstance.extract(catalog)

    df_expected = spark.createDataFrame([
        ("U3","Unknown3"),
        ("U31","Bla")
    ],["GEND_CD","GEND_NM"])

    assert inputs["test"].orderBy("GEND_CD").collect()==df_expected.orderBy("GEND_CD").collect()

def test_multiple_files_vsam(spark,env):

    catalog = {
        "test": {
            "path":".*\.vsam",
            "format":"cobol",
            "copybook": "CLO.TIP42.CUST_POSITION.CBL",
            "skip_header_rows":1
        }
    }
    class testJob(ETLJob):
        sources = {
            "test": {
            "type":"file",
            "source":"test",
            "limit":None,
            "sort":"size",
            "ascending":False
            }
        }
    testJobInstance = testJob(spark,env,logging.getLogger(),{})
    inputs = testJobInstance.extract(catalog)

    df_expected = spark.createDataFrame([
        (11111111111,),
        (22222222222,),
        (33333333333,),
        (44444444444,),
    ],["CMR-BPR-ID2"])

    assert inputs["test"].select("CMR-BPR-ID2").orderBy("CMR-BPR-ID2").collect()==df_expected.orderBy("CMR-BPR-ID2").collect()

def test_multiple_files_vsam_by_size(spark,env):

    catalog = {
        "test": {
            "path":".*\.vsam",
            "format":"cobol",
            "copybook": "CLO.TIP42.CUST_POSITION.CBL",
            "skip_header_rows":1
        }
    }
    class testJob(ETLJob):
        sources = {
            "test": {
            "type":"file",
            "source":"test",
            "limit":1,
            "sort":"size",
            "ascending":False
            }
        }
    testJobInstance = testJob(spark,env,logging.getLogger(),{})
    inputs = testJobInstance.extract(catalog)

    df_expected = spark.createDataFrame([
        (33333333333,),
        (44444444444,),
    ],["CMR-BPR-ID2"])

    assert inputs["test"].select("CMR-BPR-ID2").orderBy("CMR-BPR-ID2").collect()==df_expected.orderBy("CMR-BPR-ID2").collect()
