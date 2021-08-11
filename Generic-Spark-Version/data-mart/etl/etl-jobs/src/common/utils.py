import pyspark.sql.types as T
import logging
import yaml
import pkgutil
from itertools import chain
from datetime import timedelta
import json
import pg8000
import pyspark.sql
import shutil
from pyspark.sql.types import ArrayType, StringType, MapType, StructField, StructType, FloatType
import pyspark.sql.types as T
from typing import Dict, List, Any, Tuple, Pattern, Match, Optional, Set
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from itertools import islice
from datetime import datetime
import pandas as pd
import pdb
import glob, os, re
import boto3
import unittest.mock
import jobs
import sys
import importlib
import pkg_resources
from urllib.parse import urlencode


def rename_columns(df, columns_dict):
    # replace columns from dict mapping
    for key, item in columns_dict.items():
        result_df = result_df.withColumnRenamed(key, item)
    return result_df
# an alternate way to add unique id. more performant
def df_zipwithindex(df, offset=1, col_name="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe 
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    new_schema = StructType(
                    [StructField(col_name,T.LongType(),True)]        # new added field in front
                    + df.schema.fields                            # previous schema
                )

    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(lambda args: ([args[1] + offset] + list(args[0])))

    return new_rdd.toDF(new_schema)


def fill_auto_increment(
        df_existing: pyspark.sql.DataFrame,
        df_new: pyspark.sql.DataFrame,
        autoincrement_column: str):
    # add primary key from existing to new by shared business key
    # returns: df_new with a new primary key column
    #           for new entries, returns an autoincrement value
    max_id = get_max_value(df_existing,autoincrement_column,0)+1

    df_combined = df_new.orderBy(autoincrement_column)
    df_combined = df_zipwithindex(df_combined,offset=max_id)
    df_combined = df_combined.withColumn(autoincrement_column,
        F.when( F.col(autoincrement_column).isNull(), 
                F.col("rowId"))\
                .otherwise(F.col(autoincrement_column))).drop("rowId")
    return df_combined


#
# transform df_source so that columns match df_target:
# first, missing columns are added with null values
# then, extra columns are dropped
#
# how: source/target - determines which dataframe columns to align to
# add_missing: if columns are missing in the target dataframe, should we add NULL columns? default False
#
def align_columns(df_source_raw, df_target_raw, add_missing=False, ignore_columns=[]):
    if add_missing:
        # add NULL columns to align the df
        df_source = df_source_raw
        for column in df_target_raw.columns:
            if column not in df_source.columns and column not in ignore_columns:
                df_source = df_source.withColumn(column, F.lit(None).cast(df_target_raw.schema[column].dataType))
    else:
        df_source = df_source_raw

    # return only shared columns
    shared_existing_columns:List[str] = list(
        set(df_source.columns).\
        intersection(set(df_target_raw.columns).\
        union(set(ignore_columns)))
    )
    df_source = df_source.select(*shared_existing_columns)

    return df_source


def get_max_value(df, column, default=0):
    max_value = df.select(F.max(F.col(column)).alias("MAX")).limit(1).collect()[0].MAX
    if (max_value is None):
        return default
    else:
        return max_value


#
# validate that all columns exist
#
def validate_new_existing(df_existing, df_new):
    missing_columns = set(df_existing.columns) - set(df_new.columns)
    if len(missing_columns) > 0:
        raise ValueError("missing columns in new df: %s" % ",".join(missing_columns))


def find_delta(df_existing_raw, df_incoming_raw, business_key, primary_key, spark,
               business_key_props: Dict[str, Dict[str, Any]] = {}):
    #
    # find diffs between two dataframes by business key. adds primary key to updated records
    #

    # only look at common columns
    df_incoming = df_incoming_raw
    df_existing = align_columns(df_existing_raw,df_incoming,ignore_columns=primary_key.keys(),add_missing=True)
    # left outer join to existing to find primary keys
    # we coalesce to '' to handle null comparisons
    join_condition = [
        F.coalesce(df_incoming[business_key_column], F.lit('')) == F.coalesce(df_existing[business_key_column],
                                                                              F.lit(''))
        if business_key_props.get(
            business_key_column, {}).get("case_sensitive", True)
        else F.lower(F.coalesce(df_incoming[business_key_column], F.lit(''))) == F.lower(
            F.coalesce(df_existing[business_key_column], F.lit('')))
        for business_key_column in business_key]
    if len(join_condition) != 0:
        df_merged = df_incoming.alias('incoming').join(df_existing.alias('existing'), join_condition, 'leftouter')
        # subtract to find only records that have changed

        #
        # find inserts
        # ones without existing.business_key are inserts
        #
        df_insert = df_merged
        for business_key_col in business_key:
            df_insert = df_insert.filter("existing.%s is null" % business_key_col)

        df_insert = df_insert.select("incoming.*")
        if set(business_key) != set(primary_key.keys()):
            # add empty column for primary key
            for primary_key_col, primary_key_type in primary_key.items():
                df_insert = df_insert.withColumn(
                    primary_key_col,
                    F.lit(None).cast(primary_key_type))
        #
        # find update
        #
        df_merged_updates = df_merged.dropna(subset=["existing.%s" % col for col in business_key], how='any')

        # handle the special case where the business key = primary key
        if set(business_key) == set(primary_key.keys()):
            # we fetch the columns from incoming and don't take PK from existing
            df_merged_incoming = df_merged_updates.select("incoming.*")
        elif set(business_key) != set(primary_key.keys()):
            # we fetch the columns from incoming (without PK) and PK from existing
            df_merged_incoming = df_merged_updates.select("incoming.*",
                                                          *["existing.%s" % col for col in primary_key.keys()])

        # reorder the columns so they align with the other df
        df_merged_incoming = df_merged_incoming.select(sorted(df_merged_incoming.columns))
        df_merged_existing = df_merged_updates.select("existing.*")
        # reorder the columns so they align with the other df
        df_merged_existing = df_merged_existing.select(sorted(df_merged_existing.columns))

        # subtract the two dataframes to find delta
        df_update = df_merged_incoming.subtract(df_merged_existing)

        # insert are the ones with no primary key. updates are the rest.
        # if set(business_key)==set(primary_key.keys()):
        #     df_update = df_delta.select("incoming.*")
        # else:
        #     df_update = df_delta.select(*["existing.%s" % col for col in primary_key.keys()],"incoming.*")
    # else:
    #     # we fetch the columns from incoming (without PK) and PK from existing
    #     df_merged_incoming = df_merged_updates.select("incoming.*",*["existing.%s" % col for col in primary_key.keys()])

    # # reorder the columns so they align with the other df
    # df_merged_incoming = df_merged_incoming.select(sorted(df_merged_incoming.columns))
    # df_merged_existing = df_merged_updates.select("existing.*")
    # # reorder the columns so they align with the other df
    # df_merged_existing = df_merged_existing.select(sorted(df_merged_existing.columns))
    #
    # # subtract the two dataframes to find delta
    # df_update = df_merged_incoming.subtract(df_merged_existing)

    return df_insert,df_update

def get_file_location(folder,path):
    # loads either from local FS or S3, searching for the file by regexp. returns first found
    file_locations, _ = get_file_locations(folder, path, limit=1)
    return file_locations[0]


def get_file_locations(folder, path, limit=1, sort='last_modified', ascending=True) -> Tuple[List[str], List[str]]:
    """
        loads either from local FS or S3, searching for the file by regexp. returns first found
        folder - input folder. prefixed with s3:// if s3 otherwise local
        path - regexp with path info
        limit - max number of files to fetch. use None to load all files
        sort - if files are limited, how to sort? will return the first from the list based on the sorting. default 'last_modified'. possible values - 'last_modified' | 'size'
        ascending - if files are limited, how to sort? will return the first from the list based on the sorting
    Returns
        files to process.
        all files found in folder matching regexp
    """
    match_file = re.compile(os.path.basename(path), flags=re.IGNORECASE).match
    files_list: List[str] = []
    if folder.startswith("s3"):
        # load from s3
        parsedFolder = folder.split("://")
        s3 = boto3.resource('s3')
        # Bucket to use
        bucket = s3.Bucket(parsedFolder[1].split("/")[0])

        # list objects within a given prefix
        path_without_filename: str = '/'.join(parsedFolder[1].split('/')[1:])
        bucket_objects = bucket.objects.filter(Delimiter='/', Prefix=path_without_filename)

        # list files sorted by sort order
        def get_s3_obj_property(obj, property):
            if property == 'last_modified':
                return obj.last_modified
            elif property == 'size':
                return obj.size
            else:
                return obj.key

        files_list = [obj.key.split("/")[-1] for obj in
                      sorted(bucket_objects, key=lambda x: get_s3_obj_property(x, sort), reverse=(not ascending))]
    else:
        # load from file system
        files_list = os.listdir(folder)
        # we add full path to get properties of the file
        full_path_files = [os.path.join(folder, f) for f in files_list]

        def get_file_obj_property(obj: str, property: str):
            if property == 'last_modified':
                return os.path.getmtime(obj)
            elif property == 'size':
                return os.path.getsize(obj)
            else:
                return obj

        full_path_files.sort(key=lambda x: get_file_obj_property(x, sort), reverse=(not ascending))
        # strip the folder name
        files_list = [os.path.basename(filename) for filename in full_path_files]

    # filter by regexp
    files = list(filter(match_file, files_list))

    if (len(files) > 0):
        all_matched_files = [os.path.join(folder, filename) for filename in files]
        return all_matched_files[0:limit], all_matched_files
    else:
        raise FileNotFoundError("can't find input file %s at %s" % (path, folder))


# read table from datalake
def read_table_snapshot(table_name: str, env: Dict, spark) -> pyspark.sql.DataFrame:
    print(f'reading table snapshot => {table_name}')
    folder = env["folders"]["datamart"]
    filename = os.path.join(folder, table_name)
    print(f"folder => {folder}, filename => {filename}")

    if folder.startswith("s3"):
        parsedFolder = folder.split("://")
        parquet_prefix = os.path.join('/'.join(parsedFolder[1].split('/')[1:]),
                                      table_name) + '/'  # need the trailing slash here
        print(f"s3 filename for parquet file => {parquet_prefix}")
        # load from s3
        s3 = boto3.resource('s3')
        # Bucket to use
        bucket = s3.Bucket(parsedFolder[1].split("/")[0])
        parquet_objs = [obj.key.split('/')[-1] for obj in bucket.objects.filter(Prefix=parquet_prefix, Delimiter='/') if
                        obj.key.split('/')[-1].endswith('.parquet')]

        # check if we have such a parquet file
        if len(parquet_objs) == 0:
            raise FileNotFoundError("can't find parquet file for %s table in %s" % (table_name, filename))
    else:
        # filesystem
        if not os.path.isdir(filename):
            raise FileNotFoundError("can't find parquet file for %s table in %s" % (table_name, filename))

    return spark.read.parquet(filename)


def sync_jdbc_to_parquet(table_name: str, write_mode: str, data_classfication: str, env: Dict, spark, logger):
    # syncs the current table to the parquet file
    logger.info(f"syncing table {table_name}")
    filename = os.path.join(env["folders"]["datamart"], table_name)
    df_from_db = spark.read \
        .format("jdbc") \
        .option("url", env["jdbc"]["url"]) \
        .option("dbtable", "{}.{}".format(env["jdbc"]["schema"], table_name)) \
        .option("user", env["jdbc"]["user"]) \
        .option("password", env["jdbc"]["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    logger.info(f"total records in DB {df_from_db.count()}")
    df_from_db.write.mode(write_mode).parquet(filename)

    logger.info("sync complete")

    tags = {
        'hca:dataclassification': data_classfication,
        'hca:target_table': table_name,
    }
    s3_tag_recursive(filename, tags)

    logger.info("tagging complete")


def write_table_snapshot(df:pyspark.sql.DataFrame,table_name:str,business_keys:List[str],primary_key:Dict[str,Any],env:Dict,spark,logger, table_tags:Dict[str,str]=None):
    filename = os.path.join(env["folders"]["datamart"], table_name)
    tmp_parquet_filename = '%s.tmp.parquet' % table_name
    filename_tmp = os.path.join(env["folders"]["datamart"],tmp_parquet_filename)
    logger.debug("writing parquet snapshot to %s" % filename)
    # TODO improve performance by selectively rewriting updated partitions or using delta lake
    try:
        # we try to read the current table just to see if it exists. If it doesn't we'll create an empty parquet
        df_existing = read_table_snapshot(table_name,env,spark)
        # we add the existing records that don't exist in the incoming delta and then add the delta records so we can rewrite the entire file
        df_out = align_and_union(df,df_existing.join(df,list(primary_key.keys()),how="leftanti"))

        df_out = df_out.repartition(*[F.col(col) for col in business_keys]) # TODO verify that this does not have an ordering issue because of parallelization

        # check if we need to update anything
        if (df.count()>0):

            # we write to tmp folder since otherwise we run into issues with concurrency. in the future, use Deltalake when available on AWS glue
            df_out.write.mode("overwrite").parquet(filename_tmp)
            # rename the temp parquet file to permanent one

            if env["folders"]["datamart"].startswith("s3"):
                # handle s3
                bucket_name = env["folders"]["datamart"].split("//")[1].split("/")[0]
                # delete the original parquet file
                s3 = boto3.resource('s3')
                s3.Bucket(bucket_name).objects.filter(Prefix=f"{table_name}/").delete()
                # rename the parquet folder. s3 doesn't have rename (sigh), so we copy. recursively. since there is also no recursive copy...
                s3_copy_recursive(bucket_name, tmp_parquet_filename+"/", f"{table_name}/", table_tags)
            else:
                # local fs
                # delete the original parquet file
                shutil.rmtree(filename)
                # Renaming the parquet folder.
                os.rename(filename_tmp,filename)
    except FileNotFoundError:
        # write a new parquet. first write.
        logger.debug(f"existing parquet file not found, writing new file to {filename}")
        df.write.parquet(filename)

        # if writing new parquet file to s3, add tags
        if env["folders"]["datamart"].startswith("s3"):
            s3_tag_recursive(filename, table_tags) # type: ignore
# write type 2

#
# upsert a delta dataframe into the database
#
def upsert(spark, env,
    target_table_name: str,
    pk: List,
    df:pyspark.sql.DataFrame,
    logger):

    #TODO: adding a new column needs to add it to parquet file
    # connection to pgsql db
    try:
        connection = pg8000.connect(
            user=env["jdbc"]["user"],
            password=env["jdbc"]["password"],
            host=env["jdbc"]["host"],
            database=env["jdbc"]["database"],
            port=int(env["jdbc"]["port"])
        )

        db = connection.cursor()
        db.execute("SELECT version();")
        row = db.fetchone()
        logger.debug(f"You are connected to {row}")

    except Exception as connection_faild:
        logger.debug(f"Error while connecting to PostgreSQL, {connection_faild}")
        raise

    # set pk columns, temp table name
    pk_columns = ', '.join(str(x) for x in pk)
    temp_table_name = f"temp_{target_table_name}"

    # DROP TEMP TABLE IF EXISTS
    drop_temp_table = f"""DROP TABLE IF EXISTS {env["jdbc"]["schema"]}.{temp_table_name};"""
    db.execute(drop_temp_table)
    connection.commit()

    # create temp table schema with no data
    create_temp_table = f"""CREATE TABLE {env["jdbc"]["schema"]}.{temp_table_name} AS TABLE {env["jdbc"]["schema"]}.{target_table_name} WITH NO DATA;"""
    db.execute(create_temp_table)
    connection.commit()

    # read temp table for setting variables
    df_temp = spark.read \
        .format("jdbc") \
        .option("url", env["jdbc"]["url"]) \
        .option("dbtable", "{}.{}".format(env["jdbc"]["schema"], temp_table_name)) \
        .option("user", env["jdbc"]["user"]) \
        .option("password", env["jdbc"]["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # set variables for dynamic sql queries
    temp_column_list = set(df_temp.columns)
    shared_columns = set(temp_column_list).intersection(df.columns)

    # regular upsert
    update_columns_list = [str(x) + "=" + "excluded." + str(x) for x in shared_columns if x not in pk_columns]
    update_columns = ', '.join(update_columns_list)
    columns = ', '.join(str(x) for x in shared_columns)
    values = ', '.join(["incoming." + str(x) for x in shared_columns])

    # columns check
    exclude_from_col_check = {"row_stop_dttm", "row_strt_dttm"}
    if (shared_columns - exclude_from_col_check).difference(temp_column_list):
        raise ValueError(f'The following columns are missing from DB but found in the dataFrame {shared_columns.difference(temp_column_list)}')
    elif temp_column_list.difference(shared_columns):
        logger.warning(f'The following columns are missing from dataFrame but found in the DB : {temp_column_list.difference(shared_columns)}')

    # write df_union to temp table on pgsql
    df.write \
        .format("jdbc") \
        .mode('overwrite') \
        .option("url", env["jdbc"]["url"]) \
        .option("user", env["jdbc"]["user"]) \
        .option("password", env["jdbc"]["password"]) \
        .option("dbtable", "{}.{}".format(env["jdbc"]["schema"], temp_table_name)) \
        .option("driver", "org.postgresql.Driver") \
        .save()

    # create dynamic upsert sql script
    upsert_query = f""" insert into {env["jdbc"]["schema"]}.{target_table_name} ({columns}) select {values} from {env["jdbc"]["schema"]}.{temp_table_name} AS incoming
                        on conflict ({pk_columns}) 
                        do update set {update_columns};
                    """
    logger.debug(upsert_query)

    # execute merge query on target table name
    db.execute(upsert_query)
    connection.commit()

    # closing database connection
    db.close()
    connection.close()

def upsert_dataframe(df_existing,df_insert,df_update,primary_key:Dict[str,Any]):
    # join by primary key
    df_update_joined = (df_update.alias("incoming")).join(df_existing.alias("existing"),
    [
        (F.col(f"incoming.{key}")==F.col(f"existing.{key}")) 
        # &
        # (~F.isnull(F.col(f"incoming.{key}"))) 
        for key in primary_key.keys()
    ],
    how="inner")
    # we want to update, so populate the missing columns with existing data
    missing_cols_update = set(df_existing.columns)-set(df_update.columns)
    df_update_joined = df_update_joined.select("incoming.*",*[F.col(f"existing.{col}") for col in missing_cols_update])
    # add blank columns for any missing columns in the insert
    missing_cols_insert = set(df_existing.columns)-set(df_insert.columns)
    df_insert_joined = df_insert
    for column in missing_cols_insert:
        df_insert_joined = df_insert_joined.withColumn(column,F.lit(None).cast(df_existing.schema[column].dataType))

    # add blank columns for any missing columns in the update
    missing_cols_update = set(df_insert_joined.columns)-set(df_update_joined.columns)
    for column in missing_cols_update:
        df_update_joined = df_update_joined.withColumn(column,F.lit(None).cast(df_insert_joined.schema[column].dataType))

    return align_and_union(df_insert_joined,df_update_joined)

def align_and_union(df1: pyspark.sql.DataFrame, df2: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    # union requries that df columns are aligned. this function aligns and unions by sorting the columns
    return df1.select(sorted([colname.lower() for colname in df1.columns])).\
        union(df2.select(sorted([colname.lower() for colname in df2.columns])))


def parse_jdbc_url(url: str) -> Tuple[str, int, str]:
    url_list = url.split("/")
    port_list: List[str] = url_list[-2].split(":")
    host = port_list[0]
    port: int = 0
    if len(port_list) < 2:
        # no port. use default.
        port = 5432
    else:
        port = int(port_list[1])

    database = url_list[-1]
    return host, port, database


#
# forward fill
#
def forward_fill(
        df_calendar: pyspark.sql.DataFrame,
        df_source: pyspark.sql.DataFrame,
        day_key_column: str,
        override_values: Dict[str, Any] = {},
) -> pyspark.sql.DataFrame:
    # receives a source dataframe and forward fills the next days after day_key until a business day is found
    # returns dataframe with duplicated records with new day_key

    # fetch 7 days ahead into a pandas dataframe to keep sorted in one partition
    INTERVAL: int = 7

    # create a dict map for each unique date value in source dataframe.
    # For each date, we will have an array of the next non-business days
    # this is needed in case we have mixed records with different dates
    # we only expect few distinct dates so we are not concerned about parallelization
    source_days = df_source.alias("source").join(df_calendar.alias("tbl_calendar"),
                                                 F.col("tbl_calendar.day_key") == F.col(f"source.{day_key_column}"),
                                                 "inner").select("tbl_calendar.cal_day",
                                                                 "tbl_calendar.day_key").distinct().collect()
    ffill_map: Dict[Any, List[int]] = {}
    for row in source_days:
        # fetch the next X days to check if business day
        row_day_key: int = row[1]  # we use position indexing to avoid dealing with column names with aliases
        row_date: datetime = row[0]
        ffill_days: List[int] = [row_day_key]
        from_date = row_date + timedelta(days=1)
        to_date = from_date + timedelta(days=INTERVAL)
        days = df_calendar.where(F.col('cal_day').between(*(F.lit(from_date),
                                                            F.lit(to_date)))).toPandas().sort_values("cal_day")
        # add all of the non-business days until the first business day
        for key, day in days.iterrows():
            if (day["bus_day_flg"] == 'N'):
                ffill_days.append(day["day_key"])
            else:
                break
        ffill_map[row_day_key] = F.array([F.lit(day) for day in ffill_days])

    # use the map to transform the source and explode
    # duplicate the records with the new day key.
    # we use an array with Explode function to duplicate rows
    mapping_expr = F.create_map([F.lit(x) for x in chain(*(ffill_map.items()))])

    df_source = df_source.withColumn("duplicated_day_key", F.explode(mapping_expr.getItem(F.col(day_key_column))))

    # use the override values. This is for entries like daily_return where we want an explicit value for the forward filled days
    for colname, override_value in override_values.items():
        df_source = df_source.withColumn(colname, F.when(F.col("duplicated_day_key") == F.col(day_key_column),
                                                         F.col(colname)).otherwise(override_value))

    # move the duplicated day key value to be the day key itself and drop the duplicated day key
    df_source = df_source.withColumn(day_key_column, F.col("duplicated_day_key")).drop("duplicated_day_key")
    return df_source


def get_catalog() -> Dict[str, Any]:
    catalog: Dict[str, Any] = {}
    jsonCatalog: List[Dict[str, Any]] = []

    # load all catalogs from the directory
    catalogs = pkg_resources.resource_listdir('metadata.catalogs', '')
    for catalog_file in filter(lambda x: x.endswith(".json"), catalogs):
        moduleCatalog = json.loads(pkgutil.get_data("metadata.catalogs", catalog_file))  # type: ignore
        jsonCatalog.extend(moduleCatalog)

    # load yaml files
    for catalog_file in filter(lambda x: x.endswith(".yaml"), catalogs):
        moduleCatalog = yaml.safe_load(pkgutil.get_data("metadata.catalogs", catalog_file))  # type: ignore
        jsonCatalog.extend(moduleCatalog)

    for source in jsonCatalog:
        catalog[source["Name"]] = {
            "path": source["StorageDescriptor"].get("Location"),
            "type": source["TableType"],
            "delimiter": source["StorageDescriptor"].get("Delimiter", ","),
            "skip_header_rows": source["StorageDescriptor"].get("SkipHeaderRows", 0),
            "skip_footer_rows": source["StorageDescriptor"].get("SkipFooterRows", 0),
            "header": source["StorageDescriptor"].get("Header", True),
            "format": source["StorageDescriptor"].get("Format", "csv"),
            # copybook specs
            "copybook": source["StorageDescriptor"].get("Copybook", None),
            "record_selector_field": source["StorageDescriptor"].get("RecordSelectorField", None),
            "record_types": source["StorageDescriptor"].get("RecordTypes", None),
            "row_prefix": source["StorageDescriptor"].get("RowPrefix", None),
            "multiline": source["StorageDescriptor"].get("Multiline", False),
            "use_header": source["StorageDescriptor"].get("UseHeader", False),
            "trim": source["StorageDescriptor"].get("Trim", False),
            "columns": {}
        }
        # add column definitions
        for column in source["StorageDescriptor"].get("Columns", []):
            catalog[source["Name"]]["columns"][column["Name"]] = {
                "type": column.get("Type"),
                "format": column.get("Format"),
                "cobol_format": column.get("CobolFormat")
            }
    return catalog


# utility to split bucket, key from file uri
# s3://some-bucket-name/folder/key => some-bucket-name, folder/key
def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    bucket = s3_uri.split("//")[1].split("/")[0]
    key = '/'.join(s3_uri.split('//')[1].split('/')[1:])
    return bucket, key


# utility to archive raw files upon job success
def s3_archive_raw_files(processed_files: List[str], archive_prefix: str):
    s3 = boto3.resource('s3')

    for raw_file in processed_files:
        raw_bucket_name, raw_key = parse_s3_uri(raw_file)
        raw_filename = os.path.basename(raw_key)

        archive_bucket_name, archive_key = parse_s3_uri(archive_prefix)
        archive_key = os.path.join(archive_key, raw_filename)

        print(f"copying from {raw_bucket_name}/{raw_key} to {archive_bucket_name}/{archive_key}")
        s3.Object(archive_bucket_name, archive_key).copy_from(
            CopySource={'Bucket': raw_bucket_name, 'Key': raw_key},
            TaggingDirective='COPY')

        s3.Object(raw_bucket_name, raw_key).delete()


# utility to copy entire folder within s3 bucket. amazingly AWS sdk does not provide this functionality
def s3_copy_recursive(bucket_name: str, from_folder: str, to_folder: str, tags: Dict[str, str] = None):
    # api wants tags as encoded url to url encode tags dict
    tags_url = urlencode(tags) if tags else None

    if not from_folder.endswith('/'):
        from_folder = from_folder + '/'

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=from_folder):
        old_source = {'Bucket': bucket_name, 'Key': obj.key}

        # replace the prefix
        new_key = os.path.join(to_folder, obj.key[len(from_folder):])
        new_obj = bucket.Object(new_key)

        logging.getLogger("common.utils").info(f"copying {obj.key} to {new_obj.key} with tags {tags_url}")

        # if tags supplied replace, otherwise copy from source
        if tags_url:
            new_obj.copy_from(
                CopySource=old_source,
                TaggingDirective='REPLACE',
                Tagging=tags_url)
        else:
            new_obj.copy_from(
                CopySource=old_source,
                TaggingDirective='COPY')

        logging.getLogger("common.utils").info("s3 copy complete")


# utility to tag new parquet files in s3 after writing
def s3_tag_recursive(s3_folder_uri: str, tags: Dict[str, str]):
    # parse bucket and prefix from s3_folder_uri
    bucket_name = s3_folder_uri.split("//")[1].split("/")[0]
    # use trailing slash to make sure only files in the folder are picked up
    folder_prefix = '/'.join(s3_folder_uri.split("//")[1].split("/")[1:]) + '/'
    logging.getLogger("common.utils").info(f"preparing to tag files in bucket {bucket_name}, prefix {folder_prefix}")

    s3 = boto3.resource('s3')
    s3client = boto3.client('s3')

    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=folder_prefix):
        logging.getLogger("common.utils").info(f"tagging parquet file {obj.key} with tags {tags}")

        s3client.put_object_tagging(
            Bucket=bucket_name,
            Key=obj.key,
            Tagging={
                'TagSet': [{'Key': key, 'Value': value} for key, value in tags.items()]
            })


def get_catalog_dependencies() -> Dict[str, Dict[str, Any]]:
    # this traverses all jobs and creates a dictionary mapping between a file and the jobs that depend on it

    # since many of the jobs import pyspark, we have to mock it to be able to import without a SparkContext
    # we need to do this since we may need this in a lambda or unittest where spark is not available
    class PySparkMock(unittest.mock.MagicMock):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # add < > <= >= operators to mock
            self.__lt__ = lambda a, b: True
            self.__gt__ = lambda a, b: True
            self.__le__ = lambda a, b: True
            self.__ge__ = lambda a, b: True

    mocked_pyspark = PySparkMock()
    sys.modules['pyspark'] = mocked_pyspark

    job_metadata: Dict[str, Dict[str, Any]] = {}

    # load all job classes
    for importer, modname, ispkg in pkgutil.walk_packages(path=jobs.__path__, prefix='jobs.'):  # type: ignore
        if not ispkg:
            # find the job class
            job_name = ".".join(
                modname.split(".")[1:-1])  # take the middle part of the job package (without prefix or suffix)
            job_module = importlib.import_module("jobs.%s.job" % job_name)
            job_class = getattr(job_module, "Job")
            job_sources = getattr(job_class, "sources")

            job_metadata[job_name] = {
                "target_table": getattr(job_class, "target_table"),
                "source_files": set(),
                "source_tables": set(),
                "source_dimensions": set(),
                "data_sensitivity": getattr(job_class, "data_classification", None),
                "trigger_on_file_arrival": getattr(job_class, "trigger_on_file_arrival", False),
                "processed_files_retention_period": getattr(job_class, "processed_files_retention_period", 0)
            }

            for key, props in job_sources.items():
                if props["type"] == "file":
                    job_metadata[job_name]["source_files"].add(props["source"])
                elif props["type"] == "table":
                    job_metadata[job_name]["source_tables"].add(props["source"])
                elif props["type"] == "dimension":
                    job_metadata[job_name]["source_dimensions"].add(props["source"])
    # remove the mocked entries
    del sys.modules['pyspark']

    return job_metadata


def get_previous_bus_day(df_calendar: pyspark.sql.DataFrame, p_end_dt, col_name: str):
    """
    this function return the previous business day key from the current day (p_end_dt)
    """
    df_calendar = df_calendar \
        .select(col_name) \
        .where(F.col("bus_day_flg") == F.lit("Y")) \
        .filter(F.col("cal_day").between(F.to_timestamp(F.date_sub(F.lit(p_end_dt), 7), "yyyy-MM-dd"),
                                         F.to_timestamp(F.date_sub(F.lit(p_end_dt), 1), "yyyy-MM-dd"))) \
        .orderBy(F.col("cal_day").desc()).collect()

    return df_calendar[0][col_name]