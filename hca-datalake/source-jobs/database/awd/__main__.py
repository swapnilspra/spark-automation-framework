import pulumi
from pulumi_aws import ec2, s3, glue, iam
import os, sys
from datetime import datetime
import pkgutil
import glob
import mimetypes
import json
sys.path.insert(0, '../')
from crawl import LegacyDbGlueCrawler
from workflow import WorkflowSchedule

project = pulumi.get_project()
env = pulumi.get_stack()

config = pulumi.Config()
awsconfig = pulumi.Config('aws')

tags = {
    'hca:pulumi_stack': env,
    'hca:pulumi_project': project,
    'hca:managed_by': 'pulumi',
    'hca:source_job': 'awd'
}

# get reference to datalake infra stack if specified in config
infra_stack_name = pulumi.Config().get('infra_stack')
if not infra_stack_name:
    infra_stack_name = f"harborcapital/datalake/{env}"

infra = pulumi.StackReference('infra', infra_stack_name)

fileproc_bucket = s3.Bucket.get('fileproc-bucket', infra.get_output('fileproc_bucket'))
scripts_bucket = s3.Bucket.get('scripts-bucket', infra.get_output('scripts_bucket'))
glue_security_config = glue.SecurityConfiguration.get('security-config', infra.get_output('glue_security_config'))

# NOTE 
# - prior to running, need to set password encryption either manually or with script
# - aws glue put-data-catalog-encryption-settings --data-catalog-encryption-settings \
#     "ConnectionPasswordEncryption={ReturnConnectionPasswordEncrypted=true,AwsKmsKeyId=alias/hca/datalake-[env]"
# - track progress for doing with pulumi => https://github.com/terraform-providers/terraform-provider-aws/issues/9736
# - otherwise connection password will be stored in plain text

scripts_version = os.environ.get('SCRIPTS_CONTEXT', f"local-{datetime.now().strftime('%Y%m%d%H%M%S')}")
print(f"scripts version => {scripts_version}")

# prepare scripts prefix for pulumi managed scripts
versioned_scripts_prefix = f"working/{scripts_version}/{project}/{env}"

# jobs will point to scripts/ to keep consistent paths
scripts_prefix = f"scripts/{project}/{env}"

source_job_name = f'{project}_{env}'

# for s3 objects include tag for version
objtags = tags.copy() if tags else {}
objtags.update({
    'hca:datalake_version': scripts_version
})

# for glue connection
jdbc_url = config.require('jdbc_url')
jdbc_driver = config.require('jdbc_driver')
username = config.require('username')
password = config.require_secret('password')
db_subnet_id = config.require('db_subnet_id')
db_security_groups = config.require('db_security_groups').split(',')

# for glue crawler
crawl_path = config.require('crawl_path')

# define folder name for exports in fileproc bucket 
export_folder_name = config.require('export_folder_name') if config.get('export_folder_name') else 'awd'

subnet = ec2.Subnet.get('db-subnet', db_subnet_id)

# upload jdbc jars to s3
jdbc_jar_uris = []
for jar in glob.glob('jars/*.jar'):
    jar_name = os.path.basename(jar)

    mime_type, _ = mimetypes.guess_type(jar)
    s3_jar = s3.BucketObject(jar_name,
        bucket=scripts_bucket,
        source=pulumi.FileAsset(jar),
        content_type=mime_type,
        key=os.path.join(versioned_scripts_prefix, jar_name),
        tags=objtags,
        opts=pulumi.ResourceOptions(ignore_changes=['kmsKeyId']))

    jdbc_jar_uris.append(s3_jar.key)

# don't use bucketobjects directly, jobs will point to centrally located files
s3_jar_uris = pulumi.Output.all(
    scripts_bucket.bucket, 
    jdbc_jar_uris).apply(lambda args, scripts_prefix=scripts_prefix: ','.join([f"s3://{os.path.join(args[0],scripts_prefix,os.path.basename(a))}" for a in args[1]]))

# create connection to store jdbc params and allow us to run inside harbor vpc
connection = glue.Connection("db-connection",
    connection_type='JDBC',
    connection_properties={
        'JDBC_CONNECTION_URL': jdbc_url,
        'USERNAME': username,
        'PASSWORD': password,
        'JDBC_DRIVER_CLASS_NAME': jdbc_driver,
    },
    description=f"glue db connection to awd db",
    physical_connection_requirements={
        'availability_zone': subnet.availability_zone,
        'securityGroupIdLists': db_security_groups,
        'subnet_id': subnet.id
    },
    name=f'awd_{env}')

# create shared iam role for jobs
roletags = tags.copy() if tags else {}
roletags.update({
    'hca:dataclassification': 'pii'
})
role = iam.Role(
    resource_name=f"{project}-{env}-role",
    path="/glue/",
    description=f"shared role for glue source job => {project}",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Principal": {
                "Service": "glue.amazonaws.com"
            }
        }]
    }),
    force_detach_policies=True,
    tags=roletags)

rolepolicy = iam.RolePolicy("inline-policy",
    policy=fileproc_bucket.bucket.apply(lambda b, job_name=source_job_name, export_name=export_folder_name: json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject*",
            "Resource": f"arn:aws:s3:::{b}/tmp/{job_name}/*",
        },{
            # allow delete from tmp job folder
            "Effect": "Allow",
            "Action": [
                "s3:DeleteObject*",
                "s3:PutObject*",
            ],
            "Resource": f"arn:aws:s3:::{b}/tmp/{job_name}/*"
        },{
            # allow put to actual dest prefix
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectTagging"
            ],
            "Resource": f"arn:aws:s3:::{b}/incoming/{export_name}/*"
        },{
            # allow encrypt cloudwatch logs
            "Effect": "Allow",
            "Action": [
                "logs:AssociateKmsKey"
            ],
            "Resource": "arn:aws:logs:*:*:log-group:/aws-glue/*"
        }]
    })),
    role=role)

scriptspolicy = iam.RolePolicy("scripts-policy",
    policy=scripts_bucket.bucket.apply(lambda b: json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject*",
            "Resource": f"arn:aws:s3:::{b}/*"
        }]
    })),
    role=role)

iam.RolePolicyAttachment("kms-policy",
    policy_arn=infra.get_output('policy_kms_full_usage'),
    role=role)

iam.RolePolicyAttachment("glue-service-policy",
    policy_arn=infra.get_output('policy_glue_service'),
    role=role)

# create crawler
crawler = LegacyDbGlueCrawler("awd-crawler",
    db_name=f"awd_{env}",
    connection=connection,
    crawl_path=crawl_path,
    iam_policies=[
        infra.get_output('policy_kms_full_usage'),
        infra.get_output('policy_glue_service'),
    ],
    tags=tags)

# upload job_utils.py
s3_job_utils = s3.BucketObject("job-utils",
    bucket=scripts_bucket,
    source=pulumi.FileAsset('../job_utils.py'),
    content_type='text/x-python',
    key=os.path.join(versioned_scripts_prefix, 'job_utils.py'),
    tags=objtags,
    opts=pulumi.ResourceOptions(ignore_changes=['kmsKeyId']))

# create glue jobs from scripts jobs/
jobs = {}
scripts = [ f for f in glob.glob('jobs/*') if os.path.isfile(f) and os.path.splitext(f)[1] == '.py' ]
for script in scripts:
    scriptname = os.path.splitext(os.path.basename(script))[0]
    print(f"creating glue export job for => {scriptname}")

    # put script to s3
    mime_type, _ = mimetypes.guess_type(script)
    s3_script = s3.BucketObject(scriptname,
        bucket=scripts_bucket,
        source=pulumi.FileAsset(script),
        content_type=mime_type,
        key=os.path.join(versioned_scripts_prefix, os.path.basename(script)),
        tags=objtags,
        opts=pulumi.ResourceOptions(ignore_changes=['kmsKeyId']))

    # create glue job
    jobs[scriptname] = glue.Job(f"awd_{env}_{scriptname}",
        glue_version='1.0',
        command={
            'name': 'glueetl',
            'pythonVersion': '3',
            'scriptLocation': s3_script.bucket.apply(lambda b, scripts_prefix=scripts_prefix, script=os.path.basename(script): f"s3://{os.path.join(b, scripts_prefix, script)}"),
        },
        role_arn=role.arn,
        default_arguments={
            "--s3_bucket": fileproc_bucket.bucket,
            "--s3_tmp_path": f"tmp/{source_job_name}/{scriptname}/", # will get suffixed with yyyymmddHHMMss/ in job
            "--s3_dest_path": f"incoming/{export_folder_name}/{scriptname}.csv",
            "--glue_db": f"awd_{env}",
            "--source_db_schema": crawl_path,
            "--extra-py-files": s3_script.bucket.apply(lambda b: f"s3://{os.path.join(b, scripts_prefix,'job_utils.py')}"),
            "--extra-jars": s3_jar_uris,
            "--enable-metrics": True
        },
        tags=tags,
        connections=[connection.name],
        security_configuration=glue_security_config.name,
        timeout=60)

# create workflow with schedule to run jobs daily
schedule_wf = WorkflowSchedule(f"awd_daily_{env}",
    source_jobs=jobs.values(),
    schedule_expr="0 0 * * ? *",
    enabled=(env in ['qa','datamart']))