import pulumi
import os
import json
import glob
from datalake import Datalake, DatalakeFolderConfig
from distribution import EtlJobDist
from typing import Dict, List
from datetime import datetime
from utils import get_tables_by_project, etl_job_names

# tag everything with pulumi stack + project
basetags = {
    'hca:pulumi_stack': pulumi.get_stack(),
    'hca:pulumi_project': pulumi.get_project(),
    'hca:managed_by': 'pulumi',
}

# relative paths for folders used in distribution
dist_dir = os.path.abspath('../dist')
workflow_dir = os.path.abspath('../../src/metadata/workflows')
glue_notification_dist_dir = os.path.join(dist_dir, 'lambda-glue-notification')

# TODO figure out better sanity check for aws -> pulumi mapping

# create kms key, s3 buckets for datalake
# name with pulumi stack for easy id of resources belonging to this stack
datalake = Datalake(f"datalake-{pulumi.get_stack()}", 
    folders=DatalakeFolderConfig(), 
    tags=basetags)

# create glue notification lambda and attach to fileproc bucket
glue_notification = datalake.create_glue_notification_lambda(glue_notification_dist_dir)
datalake.fileproc_bucket.attach_notifications('fileproc-lambda-notifications', [{
    'events': ['s3:ObjectCreated:*'],
    'lambda_function_arn': glue_notification.function.arn,
    'filterPrefix': 'incoming/'
}])

# create script archive function to unzip dist from working/ and move to scripts/
scripts_archive = datalake.create_script_archive_lambda()
datalake.scripts_bucket.attach_notifications('scripts-lambda-notifications', [{
    'events': ['s3:ObjectCreated:*'],
    'lambda_function_arn': scripts_archive.function.arn,
    'filterPrefix': 'working/'
}])

# prepare distribution and copy to s3 bucket
SCRIPTS_VERSION = os.environ.get('SCRIPTS_VERSION', f"local-{datetime.now().strftime('%Y%m%d%H%M%S')}")

# pulumi uploads to {scripts_bucket}/working as zip, 
# script-archive function then unzips and moves to /scripts
# where etljobs look for script files
etldist = EtlJobDist('distribution', 
    scripts_bucket=datalake.scripts_bucket.bucket,
    scripts_version=SCRIPTS_VERSION,
    dist_dir=dist_dir,
    tags=basetags)

# get job dependencies for building iam policies
with open(os.path.join(glue_notification_dist_dir,'job_dependencies.json')) as f:
    job_dependencies = json.load(f)

# create glue etl jobs
connection_name = 'Postgres'
for jobname in etl_job_names():
    job_dependency = job_dependencies[jobname] if jobname in job_dependencies else None
    if not job_dependency:
        raise Exception(f"job dependency not found for job => {jobname} from catalog => {job_dependencies.keys()}")

    datalake.add_etl_job(jobname, job_dependency, etldist, [connection_name])

# create glue job for sync table script in etl-jobs/scripts/glue_sync_table.py
datalake.add_sync_table_job('glue_sync_table.py', etldist, [connection_name])

#TODO we should probably create separate roles for each project/db rather than assume pii
crawler_role = datalake.create_crawler_role()

# create glue database and crawler to bring mart parquet files into glue catalog
for project, tables in get_tables_by_project(job_dependencies).items():
    datalake.add_db_project(project, tables, crawler_role, 'cron(0 12 * * ? *)') # cron uses utc time

# create glue workflows and triggers
print(f'reading workflow definitions from directory => {workflow_dir}')
for filepath in glob.glob(workflow_dir + '/*.yaml'):
    datalake.add_workflow(filepath)

# exports for other pulumi projects in repo
pulumi.export('datalake_bucket', datalake.datalake_bucket.bucket.bucket)
pulumi.export('fileproc_bucket', datalake.fileproc_bucket.bucket.bucket)
pulumi.export('scripts_bucket', datalake.scripts_bucket.bucket.bucket)
pulumi.export('kms_key', datalake.kms_key.arn)
pulumi.export('glue_security_config', datalake.glue_security_config.name)

pulumi.export('policy_kms_full_usage', datalake.policy_kms_full_usage.arn)
pulumi.export('policy_kms_encrypt_only', datalake.policy_kms_encrypt_only.arn)
pulumi.export('policy_glue_service', datalake.policy_glue_service.arn)

pulumi.export('raw_location', datalake.s3_raw_uri)
pulumi.export('mart_location', datalake.s3_mart_uri)
pulumi.export('archive_location', datalake.s3_archive_uri)
pulumi.export('delta_location', datalake.s3_delta_uri)