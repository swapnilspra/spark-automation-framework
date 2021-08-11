import pytest
import common.utils
from pyspark.sql import SparkSession
import pandas as pd

@pytest.fixture(scope="session")
def spark(request):
    """Fixture for creating a spark context."""
    spark = SparkSession \
        .builder \
        .appName("SuperGlue") \
        .getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    return spark

# test add primary key
# def test_add_primary_key():
#     spark = SparkSession \
#         .builder \
#         .appName("SuperGlue") \
#         .getOrCreate()

#     df_existing = spark.createDataFrame([ (1,"one"),(2,"two")],["pk","bk"])
#     df_new = spark.createDataFrame([ ("two","name2"),("three","name3")],["bk","nm"])
#     df_merged = utils.add_primary_key(df_existing,df_new,"bk","pk")

#     actual_df = df_merged
#     expected_df = spark.createDataFrame([ ("two","name2",2),("three","name3",4)],["bk","nm","pk"])
#     expected = expected_df.orderBy("pk").collect()
#     actual = actual_df.orderBy("pk").collect() 
#     assert expected == actual

# # test upsert
# def test_upsert():
#     spark = SparkSession \
#         .builder \
#         .appName("SuperGlue") \
#         .getOrCreate()
#     df_existing = spark.createDataFrame([ (1,"one","name1"),(2,"two","name2")],["pk","bk","nm"])
#     df_new = spark.createDataFrame([ ("two","name2 new"),("three","name3 new")],["bk","nm"])
#     df_merged = utils.upsert(df_existing,df_new,"bk","pk")

#     actual_df = df_merged
#     expected_df = spark.createDataFrame([ ("one","name1",1),("two","name2 new",2),("three","name3 new",4)],["bk","nm","pk"])
#     expected = expected_df.orderBy("pk").select("pk","bk","nm").collect()
#     actual = actual_df.orderBy("pk").collect() 
#     assert expected == actual

from datetime import datetime
from pandas.testing import assert_frame_equal

def test_forward_fill(spark):

    df_calendar = spark.read\
        .option("inferSchema", "true")\
        .option("header","true")\
        .option("delimiter",",")\
        .csv("./common/tests/data/calendar.csv")
    df_source = spark.createDataFrame([ 
        ("friday",datetime.strptime("2019-08-02","%Y-%m-%d"),6479),
        ("saturday",datetime.strptime("2019-08-03","%Y-%m-%d"),6480),
        ("sunday",datetime.strptime("2019-08-04","%Y-%m-%d"),6481)
                                    ],["val","date","day_key"])
    df_source = df_source.alias('source')
    df_result = common.utils.forward_fill(df_calendar,df_source,"day_key")
    assert df_result.count()==6

    expected_result = pd.DataFrame([
        ["friday",pd.to_datetime("2019-08-02"),6479],
        ["friday",pd.to_datetime("2019-08-02"),6480],
        ["friday",pd.to_datetime("2019-08-02"),6481],
        ["saturday",pd.to_datetime("2019-08-03"),6480],
        ["saturday",pd.to_datetime("2019-08-03"),6481],
        ["sunday",pd.to_datetime("2019-08-04"),6481]
    ],columns=["val","date","day_key"])
    assert_frame_equal(df_result.toPandas()[["day_key","val"]].sort_values(["day_key"]),expected_result[["day_key","val"]].sort_values(["day_key"]), check_like=True ,check_dtype=False)

def test_find_delta(spark):

    df_existing = spark.createDataFrame([
        (1,"one",1,1),
        (2,"two",2,1),
    ],["pk","bkey1","bkey2","val"])
    df_incoming = spark.createDataFrame([
        ("one",1,1),
        ("two",2,1),
        ("three",3,1),
    ],["bkey1","bkey2","val"])

    df_expected_insert = spark.createDataFrame([
        ("three",3,1),
    ],["bkey1","bkey2","val"])
    df_insert,df_update = common.utils.find_delta(
        df_existing,df_incoming,["bkey1","bkey2"],
        primary_key={"pk":"int"})
    assert df_update.count()==0,"should have no updates"
    assert df_insert.select("bkey1","bkey2","val").collect()==df_expected_insert.select("bkey1","bkey2","val").collect(),"should have exactly 1 insert"

def test_find_delta_missing_column_incoming(spark):

    df_existing = spark.createDataFrame([
        (1,"one",1,1,11),
        (2,"two",2,2,22),
    ],["pk","bkey1","bkey2","col1","col2"])
    df_incoming = spark.createDataFrame([
        ("one",1,1),
        ("two",2,1),
        ("three",3,1),
    ],["bkey1","bkey2","col1"])

    df_expected_insert = spark.createDataFrame([
        ("three",3,1),
    ],["bkey1","bkey2","col1"])
    df_expected_update = spark.createDataFrame([
        (2,"two",2,1),
    ],["pk","bkey1","bkey2","col1"])
    df_insert,df_update = common.utils.find_delta(
        df_existing,df_incoming,["bkey1","bkey2"],
        primary_key={"pk":"int"})
    assert df_update.select(sorted(df_update.columns)).collect()==df_expected_update.select(sorted(df_expected_update.columns)).collect(),"should have exactly 1 update"
    df_insert = df_insert.drop("pk")
    assert df_insert.select(sorted(df_insert.columns)).collect()==df_expected_insert.select(sorted(df_expected_insert.columns)).collect(),"should have exactly 1 insert"

def test_find_delta_missing_column_existing(spark):

    df_existing = spark.createDataFrame([
        (1,"one",1,1),
        (2,"two",2,2),
    ],["pk","bkey1","bkey2","col1"])
    df_incoming = spark.createDataFrame([
        ("one",1,1,11),
        ("two",2,2,22),
        ("three",3,1,33),
    ],["bkey1","bkey2","col1","col2"])

    df_expected_insert = spark.createDataFrame([
        ("three",3,1,33),
    ],["bkey1","bkey2","col1","col2"])
    df_expected_update = spark.createDataFrame([
        (1,"one",1,1,11),
        (2,"two",2,2,22),
    ],["pk","bkey1","bkey2","col1","col2"])
    df_insert,df_update = common.utils.find_delta(
        df_existing,df_incoming,["bkey1","bkey2"],
        primary_key={"pk":"int"})
    assert df_update.orderBy("pk").select(sorted(df_update.columns)).collect()==df_expected_update.orderBy("pk").select(sorted(df_expected_update.columns)).collect(),"should have exactly 1 update"
    df_insert = df_insert.drop("pk")
    assert df_insert.select(sorted(df_insert.columns)).collect()==df_expected_insert.select(sorted(df_expected_insert.columns)).collect(),"should have exactly 1 insert"

def test_find_delta_case_sensitive(spark):

    df_existing = spark.createDataFrame([
        (1,"One",1,1),
        (2,"two",2,1),
    ],["pk","bkey1","bkey2","val"])
    df_incoming = spark.createDataFrame([
        ("one",1,20),
        ("two",2,1),
        ("three",3,1),
    ],["bkey1","bkey2","val"])

    df_expected_update = spark.createDataFrame([
        (1,"one",1,20),
    ],["pk","bkey1","bkey2","val"])
    df_insert,df_update = common.utils.find_delta(
        df_existing,df_incoming,["bkey1","bkey2"],
        primary_key={"pk":"int"})
    assert df_update.count()==0,"business key should be case sensitive by default"

def test_find_delta_case_insensitive(spark):

    df_existing = spark.createDataFrame([
        (1,"One",1,1),
        (2,"two",2,1),
    ],["pk","bkey1","bkey2","val"])
    df_incoming = spark.createDataFrame([
        ("one",1,20),
        ("two",2,1),
        ("three",3,1),
    ],["bkey1","bkey2","val"])

    df_expected_update = spark.createDataFrame([
        (1,"one",1,20),
    ],["pk","bkey1","bkey2","val"])
    df_insert,df_update = common.utils.find_delta(
        df_existing,df_incoming,["bkey1","bkey2"],
        primary_key={"pk":"int"},
        business_key_props={"bkey1":{"case_sensitive":False}})
    assert df_update.select("bkey1","bkey2","val").collect()==df_expected_update.select("bkey1","bkey2","val").collect(),"should have exactly 1 insert"

# add test to align_columns

# add tests to upsert- running twice

def test_upsert_dataframe(spark):

    df_existing = spark.createDataFrame([
        (1,"one",1),
        (2,"two",2),
    ],["pk","val_no_change","val_change"])
    df_incoming_insert = spark.createDataFrame([
        (3,3),
    ],["pk","val_change"])

    df_incoming_update = spark.createDataFrame([
        (1,11),
    ],["pk","val_change"])

    df_expected_merged = spark.createDataFrame([
        (1,"one",11),
        (3,None,3),
    ],["pk","val_no_change","val_change"])
    df_merged = common.utils.upsert_dataframe(
        df_existing,
        df_incoming_insert,
        df_incoming_update,
        primary_key={"pk":"int"}
    )
    assert df_merged.orderBy("pk").collect()==df_expected_merged.orderBy("pk").select(df_merged.columns).collect(),"merge should match expected"

