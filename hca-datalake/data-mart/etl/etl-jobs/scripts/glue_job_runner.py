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
import configparser
import base64

args = getResolvedOptions(sys.argv, ['JOB_NAME','hca_job_name','raw_location','datamart_location','delta_location','archive_location','aws_region'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job_name = args["hca_job_name"]
raw_location = args['raw_location']
datamart_location = args['datamart_location']
delta_location = args['delta_location']
archive_location = args['archive_location']
aws_region = args['aws_region']

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
        "schema": "hdm" #TODO get schema from job config
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
env["file_prefix"] = f"{raw_location}/{job_name}/"
#TODO add delta_location, archive_location to env

#
# set the logger
#
# patch the logger to conform to standard logging interface
# logger = glueContext.get_logger()
# logger.warning = logger.warn

import logging
logging.basicConfig()
logger = logging.getLogger(job_name)
# level = logging.getLevelName(loglevel)
logger.setLevel("DEBUG")

print("job name is %s" % job_name)

#
# run job
#
from common.etl_job import ETLJob # must be imported after spark has been set up
job_module = importlib.import_module("jobs.%s.job" % job_name)
job_class = getattr(job_module, "Job")
job:ETLJob = job_class(spark,env,logger)

job.run()

# move file to archive and wipe raw folder
job.archive(f"{archive_location}/{job_name}")