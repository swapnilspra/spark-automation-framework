import common.utils
import pyspark.sql.functions as F
from typing import Dict, List, Any
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
import importlib
import boto3
import sys
import base64

# assumptions:
# - table to sync is in postgres connection called "Postgres" (same used by glue jobs)
# - sync table glue job only has 1 glue connection (db settings pulled from here)
# - if parquet table already exists under arg datamart_location, by default job will error unless arg write_mode = overwrite
# - etl job exists, pull data_classification tag to add to parquet table

# arguments:
# - table_names = comma delimited list of {schema}.{table name} to sync, must include schema
# - write_mode = spark dataframe write mode (error|overwrite), default=error which will raise exeception if parquet table already exists
# - datamart_location = root s3 uri to save parquet tables *set by pulumi, do not modify
# - aws_region = aws region of current glue job for pulling db connection info *set by pulumi, do not modify
args = getResolvedOptions(sys.argv, ['JOB_NAME','table_names', 'datamart_location','aws_region','write_mode'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job_name = args['JOB_NAME']
table_names = args['table_names'].split(',')
datamart_location = args['datamart_location']
aws_region = args['aws_region']
write_mode = args['write_mode']

#
# initialize glue job with glue security config supplied in params
# to encrypt cloudwatch logs
#
job = Job(glueContext) 
job.init(job_name, args)

#
# read the catalog
#
catalog:Dict[str,Any] = common.utils.get_catalog()

#
# get the jdbc connection from the glue catalog
#
client = boto3.client('glue', region_name=aws_region)
glue_job = client.get_job(JobName=args['JOB_NAME'])['Job']
db_name = next(iter(glue_job['Connections']['Connections'])) if 'Connections' in glue_job else None

# for now lets assume every job has a db connection
db_connection = client.get_connection(Name=db_name)

# 
# db password will be stored encrypted, so we need to decrypt
#
kms = boto3.client('kms', region_name=aws_region)
ecrypted_db_password = db_connection['Connection']['ConnectionProperties']['ENCRYPTED_PASSWORD']
db_password = kms.decrypt(CiphertextBlob=bytes(base64.b64decode(ecrypted_db_password)))['Plaintext']

env:Dict[str,Any] = {
    "jdbc":
        {"url": db_connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'],
        "user": db_connection['Connection']['ConnectionProperties']['USERNAME'],
        "password": db_password.decode('utf-8'),
        #"schema": set later based off table_names parameter
        },
    "folders": {},
    "catalog": catalog
}
host,port,database = common.utils.parse_jdbc_url(env["jdbc"]["url"])
env["jdbc"]["host"] = host
env["jdbc"]["port"] = port
env["jdbc"]["database"] = database

#
# set s3 folders
#
env["folders"]["datamart"] = datamart_location

#
# set the logger
#
# patch the logger to conform to standard logging interface
# logger = glueContext.get_logger()
# logger.warning = logger.warn

import logging
logging.basicConfig()

# 
# import jobs module for bringing in job attrs
from common.etl_job import ETLJob # must be imported after spark has been set up
job_dependencies = common.utils.get_catalog_dependencies()

for table_with_schema in table_names:
    schema, table_name = table_with_schema.split('.')

    # assume etl job exists for table, pull dataclassification for tagging
    data_classification = next((properties['data_sensitivity'] for job, properties in job_dependencies.items() if properties['target_table'] == table_name), 'nonsensitive')
    print(f"data_classifiction => {data_classification}")

    # schema can differ by table
    env['jdbc']['schema'] = schema

    logger = logging.getLogger(table_name)
    logger.setLevel("DEBUG")

    print("syncing table %s" % table_name)

    common.utils.sync_jdbc_to_parquet(table_name, write_mode, data_classification, env, spark, logger)
