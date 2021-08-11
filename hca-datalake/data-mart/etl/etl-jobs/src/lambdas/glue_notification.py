import re
import boto3
import json
import os
from typing import List
from urllib.parse import urlparse

# files needed to run:
# {LAMBDA_TASK_ROOT}/
#   catalog.json
#   job_dependencies.json

# from pulumi
S3_DATALAKE_BUCKET = os.environ['S3_DATALAKE_BUCKET']
S3_RAW_PATH = os.environ['S3_RAW_PATH']
PULUMI_PROJECT = os.environ['PULUMI_PROJECT']
PULUMI_STACK = os.environ['PULUMI_STACK']

# from aws
AWS_REGION = os.environ['AWS_REGION']
WORK_DIR = os.environ.get('LAMBDA_TASK_ROOT','.')

s3 = boto3.resource('s3',region_name=AWS_REGION)
glue = boto3.client('glue', region_name=AWS_REGION)
resources = boto3.client('resourcegroupstaggingapi', region_name=AWS_REGION)
ACCOUNT_ID = boto3.client('sts').get_caller_identity().get('Account')


def find_glue_job_by_name(job_name:str) -> str:
    items = resources.get_resources(
        TagFilters=[
            {
                'Key': 'hca:job_name',
                'Values': [ job_name ],
            },{
                'Key': 'hca:pulumi_project',
                'Values': [ PULUMI_PROJECT ],
            },{
                'Key': 'hca:pulumi_stack',
                'Values': [ PULUMI_STACK ]
            }
        ],
        ResourceTypeFilters=['glue:job'])

    jobs = [ r['ResourceARN'] for r in items['ResourceTagMappingList'] ]

    # expect to find exactly 1 job
    if len(jobs) == 1:
        return os.path.basename(jobs[0])
    else:
        #TODO keep an eye on this, maybe ok to find 0?
        raise Exception(f"expected 1 job with name [{job_name}] for project={PULUMI_PROJECT}, stack={PULUMI_STACK}, found ===> {jobs}")


def is_job_running(job:str) -> bool:
    job_history = glue.get_job_runs(JobName=job, MaxResults=1)['JobRuns']

    return len(job_history) == 1 and job_history[0]['JobRunState'] in ['RUNNING','STARTING']


def main(event, context):
    # save event to logs
    print(json.dumps(event))

    # loop over every entry and see if we have a match
    source_bucket_name = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    filename = source_key.split("/")[-1] # take only filename, without folder name

    # make sure event is a file and not a folder
    if source_key.endswith('/'):
        print("key is a folder, skipping...")
        return

    # match sources that match the filename pattern
    with open(os.path.join(WORK_DIR, 'catalog.json'), 'r') as catalog_file:
        catalog = json.load(catalog_file)

    matched_sources = [source for source,properties in catalog.items() if re.match(properties['path'],filename)]
    print(f"following sources match: {matched_sources}")

    # find jobs that depend on this source
    with open(os.path.join(WORK_DIR, 'job_dependencies.json'), 'r') as job_dependencies_file:
        job_dependencies = json.load(job_dependencies_file)

    matched_jobs = [(job, properties) for job,properties in job_dependencies.items() if set(matched_sources).intersection(properties["source_files"]) ]
    print(f"following jobs match: {matched_jobs}")

    # move the file to the target folder
    source_obj = { 'Bucket': source_bucket_name, 'Key': source_key }
    for job_name, job_props in matched_jobs:
        new_key = os.path.join(S3_RAW_PATH.strip('/'), job_name, filename)
        print(f"copying from => {source_bucket_name}/{source_key} to => {S3_DATALAKE_BUCKET}/{new_key}")

        # copy object with tags
        s3.Object(S3_DATALAKE_BUCKET, new_key).copy_from(
            CopySource=source_obj,
            TaggingDirective='COPY')

        # find datalake specific job based off tags
        actual_job_name = find_glue_job_by_name(job_name)

        # if we need to trigger immediately, trigger the job if not already running
        if job_props['trigger_on_file_arrival'] and not is_job_running(actual_job_name):
            print(f'starting job ===> {actual_job_name}')

            run_id = glue.start_job_run(JobName=actual_job_name)

            print(f"started job => {actual_job_name} with run => {run_id}")

    # remove original file if copied to >= 1 job
    if len(matched_jobs) > 0:
        s3.Object(source_bucket_name, source_key).delete()