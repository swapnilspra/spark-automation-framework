# additional utilities to test dataframes
import unittest
import json
import sys
from datetime import datetime
import pandas as pd
import pyspark.sql.types as T
from typing import Dict, List, Any, Tuple
import pyspark.sql
import logging
import pyspark.sql.functions as F
import importlib
import glob
import os
import common.utils
from functools import reduce


# helper method to set nested dict by dot notation
def set_nested(my_dict, key_string, value):
    """Given `foo`, 'key1.key2.key3', 'something', set foo['key1']['key2']['key3'] = 'something'"""

    # Start off pointing at the original dictionary that was passed in.
    here = my_dict

    # Turn the string of key names into a list of strings.
    keys = key_string.split(".")

    # For every key *before* the last one, we concentrate on navigating through the dictionary.
    for key in keys[:-1]:
        # Try to find here[key]. If it doesn't exist, create it with an empty dictionary. Then,
        # update our `here` pointer to refer to the thing we just found (or created).
        here = here.setdefault(key, {})

    # Finally, set the final key to the given value
    here[keys[-1]] = value


def load_test_csv(spark, path: str, parse_dates=[]) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
    df = pd.read_csv(path, parse_dates=parse_dates)
    df = df.where((pd.notnull(df)), None)

    # create the schema based on the headers of the file.
    # The naming convention of the columns should be <table alias>.<column name> matching the input
    colnames: Dict[str, List[str]] = {}
    for header in df.columns:
        split_header = header.split(".")
        # handle columns without aliases
        if len(split_header) == 1:
            alias = ""
            column = header
        else:
            alias = split_header[0]
            column = split_header[1]

        collist = colnames.get(alias, [])
        collist.append(column)
        colnames[alias] = collist

    # create the schema for pyspark df
    schema_list = []
    for alias, columns in colnames.items():
        if (alias == ""):
            # regular field
            table_schema = [T.StructField(col, T.StringType()
                                          ) for col in columns]
            schema_list.extend(table_schema)
        else:
            # add as struct to mimic aliases
            table_schema = T.StructField(
                alias,
                T.StructType([T.StructField(col, T.StringType()) for col in columns])
            )
            schema_list.append(table_schema)

    schema = T.StructType(schema_list)

    # load the rows of the dataframe as dicts for the nested pyspark dataframe structure
    rows = []
    for idx, row in df.iterrows():
        rec: Dict[str, Any] = {}
        for col in df.columns:
            set_nested(rec, col, row[col])
        rows.append(rec)
    test_df: pyspark.sql.DataFrame = spark.createDataFrame(rows, schema)

    return test_df


def cast_nested_col(df: pyspark.sql.DataFrame,
                    col_name: str, col_type: str,
                    alias: str = None, date_format: str = None) -> pyspark.sql.DataFrame:
    if alias:
        # get columns names
        alias_columns = [F.col(f"{alias}.{col.name}") for col in df.schema[alias].dataType if col.name != col_name]

        if col_type == 'timestamp':
            return df.withColumn(alias, F.struct([*alias_columns,
                                                  F.to_timestamp(F.col(f"{alias}.{col_name}"), date_format).alias(
                                                      col_name)]))
        elif col_type == 'date':
            return df.withColumn(alias, F.struct([*alias_columns,
                                                  F.to_date(F.col(f"{alias}.{col_name}"), date_format).alias(
                                                      col_name)]))
        else:
            return df.withColumn(alias, F.struct([*alias_columns,
                                                  F.col(f"{alias}.{col_name}").cast(col_type).alias(col_name)]))
    else:
        if col_type == 'timestamp':
            return df.withColumn(col_name, F.to_timestamp(F.col(col_name), date_format))
        elif col_type == 'date':
            return df.withColumn(col_name, F.to_date(F.col(col_name), date_format))
        else:
            return df.withColumn(col_name, F.col(col_name).cast(col_type))


def assert_df_matches_expected(
        df: pyspark.sql.DataFrame,
        column_name: str,
        column_type: str = "float",
        precision: float = 1e-15):
    # compare the column requested to the same one with alias of "expected" and see that all are under certain precision
    df_compare = df.select("*",
                           F.col(f"expected.{column_name}").alias("expected_output"),
                           F.col(column_name).alias("actual_output"))
    # subtract and compare diff
    if (column_type == "float"):
        df_compare = df_compare.withColumn("diff",
                                           F.abs(F.col("expected_output") - F.col("actual_output"))
                                           )
        df_compare = df_compare.withColumn("identical",
                                           F.col("diff") < F.lit(precision)
                                           )
    else:
        df_compare = df_compare.withColumn("diff",
                                           F.concat(F.lit("expected:"), F.col("expected_output"), F.lit(" vs actual:"),
                                                    F.col("actual_output")))
        df_compare = df_compare.withColumn("identical",
                                           F.col("expected_output") == F.col("actual_output")
                                           )

    # find ones that are different
    df_diff = df_compare.filter(F.coalesce(F.col("identical"), F.lit(False)) != True)
    if (df_diff.count() > 0):
        df_diff.show()
    assert df_diff.count() == 0, f"All actual values match expected for column {column_name}"


def run_integration_test(
        jobname: str,
        spark,
        datafolder: str,
        job_arguments: Dict[str, str],
        ignore_cols: str = '',
        stop_on_failure: bool = False,
        loglevel: str = "DEBUG"):
    # initialize catalog
    catalog: Dict[str, Any] = common.utils.get_catalog()
    # initialize env
    env: Dict[str, Any] = {
        "jdbc": {},
        "folders": {

        },
        "file_prefix": os.path.join(datafolder, "raw"),
        "catalog": catalog
    }

    # initialize logger
    logging.basicConfig()

    logger = logging.getLogger("test")
    logger.setLevel(loglevel)

    # initialize job class
    from common.etl_job import ETLJob  # must be imported after spark has been set up
    job_module = importlib.import_module("jobs.%s.job" % jobname)
    job_class = getattr(job_module, "Job")
    job: ETLJob = job_class(spark, env, logger, job_arguments)

    global_ignore_cols = ['etl_load_cyc_key','row_stop_dttm','row_strt_dttm','curr_row_flg','src_sys_id']
    # load the 'before' files into dataframes
    dfs = {}
    for filename in glob.glob(os.path.join(datafolder, "before", "*.csv")):
        base = os.path.basename(filename)
        tablename = os.path.splitext(base)[0]
        logger.info(f"loading {tablename}")
        # check if there is a schema file to describe this table. If so, use it instead of inferSchema
        # if job.load_strategy==ETLJob.LoadStrategy.TYPE2:
        #     # load with the row_strt_dttm since we may need these for loading the existing records
        #     df = load_table_from_csv(spark,os.path.join(datafolder,"before",f"{tablename}.csv"),list(set(global_ignore_cols)-{'row_stop_dttm','row_strt_dttm','curr_row_flg'}))
        # else:
        #     # load with ignore cols
        #     df = load_table_from_csv(spark,os.path.join(datafolder,"before",f"{tablename}.csv"),global_ignore_cols)
        df = load_table_from_csv(spark,os.path.join(datafolder,"before",f"{tablename}.csv"),[])
        dfs[tablename] = df.alias(tablename)

    df_after_filename = os.path.join(datafolder,"after",job.target_table+".csv")
    df_after_full = load_table_from_csv(spark,df_after_filename)
    df_after = df_after_full.drop(*global_ignore_cols)

    # create dataframes of expected output. we match by primary key
    df_before_full = dfs[job.target_table]
    df_before = df_before_full.drop(*global_ignore_cols)
    df_insert_expected, df_update_expected = common.utils.find_delta(
        df_before_full,
        df_after_full,job.primary_key,job.primary_key, spark
    )
    # drop ignore cols
    df_insert_expected = df_insert_expected.drop(*global_ignore_cols)
    df_insert_expected.cache()
    df_update_expected = df_update_expected.drop(*global_ignore_cols)
    df_update_expected.cache()

    logger.info(f"expected inserts: {df_insert_expected.count()}")
    logger.info(f"expected updates: {df_update_expected.count()}")

    # mock upsert method.
    # check expected vs actual
    def mock_upsert(spark, env,
        target_table_name: str,
        pk: List,
        df:pyspark.sql.DataFrame,
        logger):

        df_insert = df.join(df_before.select(*pk),pk,how="leftanti")
        df_update = df.join(df_before.select(*pk),pk,how="inner")

        all_ignore_cols = global_ignore_cols + ignore_cols.split(",")
        logger.info(f"ignoring cols: {','.join(all_ignore_cols)}")
        all_ignore_cols = list( set(all_ignore_cols+list(job.primary_key.keys())+job.business_key) )

        # check insert
        # we compare by business key
        df_full = df_after
        if job.load_strategy==ETLJob.LoadStrategy.TYPE2:
            # take only curr_row_flg=Y rows
            df_full = df_after.filter(F.col("curr_row_flg")==F.lit("Y"))
        logger.info("----------------")
        logger.info("checking inserts")
        compare_pyspark_df(
            df_insert,
            df_insert_expected,
            all_ignore_cols,
            df_full,
            job.business_key,logger,stop_on_failure=stop_on_failure)

        # check update
        # we compare by primary key since for updates this should already exist + business k
        logger.info("----------------")
        logger.info("checking updates")
        compare_pyspark_df(
            df_update,
            df_update_expected,
            all_ignore_cols,
            df_after,
            list(job.primary_key.keys())+job.business_key,
            logger,
            stop_on_failure=stop_on_failure
        )

    # ignore writing table snapshot
    def mock_write_table_snapshot(df:pyspark.sql.DataFrame,table_name:str,business_keys:List[str],primary_key:Dict[str,Any],env:Dict,spark,logger, table_tags:Dict[str,str]=None):
        pass

    def mock_read_table_snapshot(table_name, env, spark):
        return dfs[table_name]

    # run the job with patched methods
    with unittest.mock.patch.multiple('common.utils', create=True,
                                      read_table_snapshot=unittest.mock.Mock(wraps=mock_read_table_snapshot),
                                      upsert=unittest.mock.Mock(wraps=mock_upsert),
                                      write_table_snapshot=unittest.mock.Mock(wraps=mock_write_table_snapshot)
                                      ):

        job.run()
        # if upsert was not called at all we can assume that no new or updated records were found. assert that
        assert (common.utils.upsert.called or (
                    df_update_expected.count() == 0 and df_insert_expected.count() == 0)), "expected updates or inserts but no upsert done"  # type: ignore
        if not stop_on_failure:
            logger.info("done")


def load_table_from_csv(spark, filename, ignore_cols: List[str] = []):
    tablename = os.path.basename(filename).split(".")[0]
    schemaFile: str = os.path.join(os.path.dirname(filename), f"{tablename}.schema.json")
    if os.path.exists(schemaFile):
        # use schemafile
        with open(schemaFile, 'r') as schemaFileHandler:
            schema_fields = json.load(schemaFileHandler)
            df_schema = {'fields': [], 'type': 'struct'}
            for field in schema_fields["fields"]:
                df_schema["fields"].append({  # type:ignore
                    "name": field["name"],
                    "type": field["type"],
                    "nullable": field.get("nullable", True),
                    "metadata": field.get("metadata", {})
                })
        df = spark.read.option("header", True). \
            option("inferSchema", False). \
            option("ignoreTrailingWhiteSpace", True). \
            option("quote", "\""). \
            option("escape", "\""). \
            option("multiLine", "true"). \
            csv(filename)
        df = df.select(
            *[F.col(details["name"]).cast(details["type"]).alias(details["name"])  # type: ignore
              for details in df_schema["fields"]])
    else:
        # for performance, we load only a subset of rows to inferSchema, then use that schema when loading full file.
        # otherwise, spark tries to read in the whole file to inferSchema
        sample_rows = spark.read.text(filename).limit(500)
        file_schema = spark.read.option("header", True).option("inferSchema", True).csv(
            sample_rows.rdd.map(lambda x: x[0])).schema
        df = spark.read.option("header", True). \
            option("inferSchema", False). \
            option("ignoreTrailingWhiteSpace", True). \
            option("quote", "\""). \
            option("escape", "\""). \
            option("multiLine", "true"). \
            csv(filename, schema=file_schema)
        # convert all double to decimal
        df = df.select(
            *[F.col(field.name).cast(T.DecimalType(38, 18)) if field.dataType == T.DoubleType() else F.col(field.name)
              for field in df.schema])
    # make column names lowercase
    df = df.toDF(*[c.lower() for c in df.columns])
    df = df.drop(*ignore_cols)
    return df

def compare_pyspark_df(df_actual,df_expected,ignore_cols,df_full,business_key,logger,stop_on_failure=False):
    # check all columns exist
    assert len(set(df_expected.columns)-set(df_actual.columns)-set(ignore_cols))==0,\
        "Following columns expected but do not exist. Either add to ignore-cols or fix: %s" % ",".join(set(df_expected.columns)-set(df_actual.columns)-set(ignore_cols))

    precision = 5
    # loop over all the columns and convert to appropriate datatypes
    converted_cols_expected = []
    for col in df_expected.columns:
        if (type(df_expected.schema[col].dataType) in [T.DoubleType, T.FloatType, T.DecimalType]):
            # round to specified precision
            converted_cols_expected.append(F.round(col, precision).alias(col))
        elif df_expected.schema[col].dataType == T.IntegerType():
            # we convert integers to double for consistency
            converted_cols_expected.append(F.col(col).cast("double"))
        else:
            converted_cols_expected.append(F.col(col))

    # join by key
    df_col_comparison = (df_expected.select(converted_cols_expected).alias("expected")).join(
        df_actual.select(converted_cols_expected).alias("actual"),
        [F.col(f"expected.{col}").eqNullSafe(F.col(f"actual.{col}")) for col in business_key],
        how='full'
    )

    df_col_comparison.cache()
    #
    # check for missing insert records. These are records where actual business key is null
    #
    df_expected_not_met = df_col_comparison.where(
        reduce(lambda x, y: x & y, (F.col(f"actual.{x}").isNull() for x in business_key))
    )
    if df_expected_not_met.count() > 0:
        logger.error(f"{df_expected_not_met.count()} expected records not found in actual")
        df_expected_not_met.select(*[F.col(f"expected.{col}") for col in business_key]).show(10)
        if stop_on_failure:
            raise AssertionError(f"{df_expected_not_met.count()} expected records exist that are not in actual df")

    #
    # check for redundant records. These are records where expected business key is null
    #
    df_actual_redundant = df_col_comparison.where(
        reduce(lambda x, y: x & y, (F.col(f"expected.{x}").isNull() for x in business_key))
    )
    if df_actual_redundant.count() > 0:
        print(f"{df_actual_redundant.count()} extra records not found in expected")
        df_actual_redundant.select(*[F.col(f"actual.{col}") for col in business_key]).show(10)
        if stop_on_failure:
            raise AssertionError("extra records exist that are not in expected df")

    # compare column by column
    # join by business key
    df_col_by_col = (df_actual.select(converted_cols_expected).alias("actual")).join(
        df_full.select(converted_cols_expected).alias("expected"),
        [F.col(f"expected.{col}").eqNullSafe(F.col(f"actual.{col}")) for col in business_key],
        how='leftouter'
    )
    df_col_by_col.cache()

    for col in set(df_expected.columns)-set(ignore_cols):
        df_compare_col = df_col_by_col.where(
            ~F.col(f"expected.{col}").eqNullSafe(F.col(f"actual.{col}")) &
            ~(
                    reduce(lambda x, y: x & y, (F.col(f"actual.{x}").isNull() for x in business_key)) |
                    reduce(lambda x, y: x & y, (F.col(f"expected.{x}").isNull() for x in business_key))
            )  # if all business keys are null this is a missing or extra insert. ignore.
        ).select(
            *[F.col(f"actual.{bkey}") for bkey in business_key],
            F.col(f"expected.{col}").alias("expected"),
            F.col(f"actual.{col}").alias("actual")
        )

        if (df_compare_col.count() > 0):
            print(f"column {col} failed. {df_compare_col.count()} rows with differences")
            df_compare_col.show(10, False)
            if stop_on_failure:
                raise AssertionError(f"column {col} failed. {df_compare_col.count()} rows with differences")
        else:
            logger.info(f"column {col} passed")