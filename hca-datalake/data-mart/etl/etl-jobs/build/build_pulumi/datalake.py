import pulumi
from pulumi_aws import s3, kms, iam, get_caller_identity, glue, cloudwatch
from s3 import DatalakeBucket, datalake_bucket_policy, fileproc_bucket_policy, scripts_bucket_policy
from kms import key_policy
from iam import kms_usage_policy, kms_encrypt_policy
import json
import os
from typing import Dict, Callable, List, Set
from utils import arn_concat
from functions import glue_notification, scripts_archive
from distribution import EtlJobDist
from etljob import GlueEtlJob, etljob_policy
from scripts import sync_table
from jobworkflow import JobWorkflow, WorkflowDefinition


class DatalakeFolderConfig(object):
    mart: str
    deltas: str
    archive: str
    raw: str

    def __init__(self, mart='/mart',deltas='/deltas',archive='/archive',raw='/raw'):
        self.mart = self._strip_path(mart)
        self.deltas = self._strip_path(deltas)
        self.archive = self._strip_path(archive)
        self.raw = self._strip_path(raw)

    # paths should begin with slash and not end with slash
    def _strip_path(self, path:str) -> str:
        result = path if path.startswith('/') else '/'+path
        return result if not result.endswith('/') else result[:-1]


class Datalake(pulumi.ComponentResource):
    datalake_bucket: DatalakeBucket
    fileproc_bucket: DatalakeBucket
    scripts_bucket: DatalakeBucket

    kms_key: kms.Key
    glue_security_config: glue.SecurityConfiguration

    # folder prefixes of datalake bucket
    folder_config: DatalakeFolderConfig

    #TODO revisit kms policy, see if we can flip it
    policy_kms_full_usage: iam.Policy
    policy_kms_encrypt_only: iam.Policy
    policy_glue_service: iam.Policy
    policy_glue_etljob: iam.Policy

    s3_raw_uri:pulumi.Output[str]
    s3_mart_uri:pulumi.Output[str]
    s3_archive_uri:pulumi.Output[str]
    s3_delta_uri:pulumi.Output[str]

    tags:Dict[str,str]

    jobs:Dict[str,GlueEtlJob]


    def __init__(self, name, 
            folders:DatalakeFolderConfig=None,
            tags:Dict[str,str]=None, 
            opts:pulumi.ResourceOptions=None):
        super().__init__('hca:Datalake', name, None, opts)

        aws_region = pulumi.Config('aws').get('region')
        account_id = get_caller_identity().account_id
        folders = folders if folders else DatalakeFolderConfig()

        self.folder_config = folders
        self.tags = tags if tags else {}
        self.jobs = {}
        
        self.kms_key = kms.Key(f"{name}-kms-key",
            description="kms key for encryption of datalake",
            policy=key_policy(account_id, aws_region),
            tags=self.tags,
            opts=pulumi.ResourceOptions(parent=self))

        alias = kms.Alias(f"{name}-kms-key-alias",
            target_key_id=self.kms_key.id,
            name=f"alias/hca/{name}",
            opts=pulumi.ResourceOptions(parent=self, delete_before_replace=True))

        # create main datalake bucket for raw, mart, archive, etc
        self.datalake_bucket = DatalakeBucket(
            f"{name}-bucket",
            self.kms_key,
            lifecycle_rules=[{
                "id": "transition-archive-to-glacier",
                "enabled": True,
                "prefix": folders.archive,
                "transitions": [{
                    "days": 90,
                    "storage_class": "GLACIER"
                }]
            }],
            bucket_policy=datalake_bucket_policy,
            tags=self.tags,
            opts=pulumi.ResourceOptions(parent=self))

        # create fileproc bucket for pre-raw from infra account
        self.fileproc_bucket = DatalakeBucket(
            f"{name}-fileproc-bucket",
            self.kms_key,
            lifecycle_rules=[{
                "id": "purge-temporary-files",
                "enabled": True,
                "prefix": "/tmp",
                "expiration": {
                    "days": 1
                },
                "noncurrentVersionExpiration":{
                    "days": 1
                }
            }],
            bucket_policy=fileproc_bucket_policy,
            tags=self.tags,
            opts=pulumi.ResourceOptions(parent=self))

        # create scripts bucket for storing etljob scripts
        self.scripts_bucket = DatalakeBucket(
            f"{name}-script-bucket",
            self.kms_key,
            bucket_policy=scripts_bucket_policy,
            tags=self.tags,
            opts=pulumi.ResourceOptions(parent=self))

        # create kms policies
        self.policy_kms_full_usage = iam.Policy(f"{name}-iam-key-full-usage",
            description="allow encrypt/decrypt with datalake kms key",
            policy=self.kms_key.arn.apply(kms_usage_policy),
            path='/',
            opts=pulumi.ResourceOptions(parent=self))

        self.policy_kms_encrypt_only = iam.Policy(f"{name}-iam-key-encrypt-only",
            description="allow encrypt only with datalake kms key",
            policy=self.kms_key.arn.apply(kms_encrypt_policy),
            path='/',
            opts=pulumi.ResourceOptions(parent=self))
        
        # get glue service policy (create custom one later)
        self.policy_glue_service = iam.Policy.get(f"{name}-glue-service", 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole', opts=pulumi.ResourceOptions(parent=self))

        # create etljob policy 
        #TODO should try to merge with policy_glue_service, some duplication here
        self.policy_glue_etljob = iam.Policy(f"{name}-etljob-policy",
            description=f"shared policy for datalake glue etl jobs => {name}",
            path='/glue/',
            policy=pulumi.Output.all(
                self.datalake_bucket.bucket.arn, 
                self.scripts_bucket.bucket.arn).apply(lambda buckets: etljob_policy(buckets[0], buckets[1], folders)),
            opts=pulumi.ResourceOptions(parent=self))

        # create glue security config
        # use specific name as any changes will trigger replacement of resource
        self.glue_security_config = glue.SecurityConfiguration(f"{name}-security-config",
            name=name,
            encryption_configuration={
                'cloudwatchEncryption': {
                    'cloudwatchEncryptionMode': 'SSE-KMS',
                    'kms_key_arn': self.kms_key.arn
                },
                's3Encryption': {
                    's3EncryptionMode': 'SSE-KMS',
                    'kms_key_arn': self.kms_key.arn
                },
                'jobBookmarksEncryption': {
                    'jobBookmarksEncryptionMode': 'DISABLED'
                }
            },
            opts=pulumi.ResourceOptions(parent=self))

        self.s3_raw_uri = pulumi.Output.concat('s3://', self.datalake_bucket.bucket.bucket, self.folder_config.raw)
        self.s3_mart_uri = pulumi.Output.concat('s3://', self.datalake_bucket.bucket.bucket, self.folder_config.mart)
        self.s3_archive_uri = pulumi.Output.concat('s3://', self.datalake_bucket.bucket.bucket, self.folder_config.archive)
        self.s3_delta_uri = pulumi.Output.concat('s3://', self.datalake_bucket.bucket.bucket, self.folder_config.deltas)
        
        self.register_outputs({})


    def create_glue_notification_lambda(self, package_dir:str) -> glue_notification.GlueNotificationLambda:
        glue_notification_lambda = glue_notification.GlueNotificationLambda('glue-notification', 
            datalake_bucket=self.datalake_bucket.bucket,
            datalake_raw_path=self.folder_config.raw,   #'raw/',
            fileproc_bucket=self.fileproc_bucket.bucket,
            managed_policy_arns=[
                self.policy_kms_full_usage.arn,
                'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
            ],
            package_dir=package_dir,
            tags=self.tags,
            opts=pulumi.ResourceOptions(parent=self))
        
        return glue_notification_lambda
    
    def create_script_archive_lambda(self) -> scripts_archive.ScriptArchiveLambda:
        script_archive_lambda = scripts_archive.ScriptArchiveLambda('scripts-archive',
            scripts_bucket=self.scripts_bucket.bucket,
            managed_policy_arns=[
                self.policy_kms_full_usage.arn,
                'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
            ],
            tags=self.tags,
            opts=pulumi.ResourceOptions(parent=self))

        return script_archive_lambda


    def add_etl_job(self, jobname:str, job_dependency:Dict[str,str], etl_distribution:EtlJobDist, glue_connections:List[str]) -> GlueEtlJob:
        job = GlueEtlJob(jobname, 
            iam_policies=[
                self.policy_kms_full_usage.arn,
                self.policy_glue_service.arn, #TODO
                self.policy_glue_etljob.arn
            ],
            data_classification=job_dependency['data_sensitivity'],
            target_table=job_dependency['target_table'],
            security_config=self.glue_security_config,
            script_location=etl_distribution.get_prefixed_s3_uri('glue_job_runner.py'),
            job_args={
                '--hca_job_name': jobname,
                '--raw_location': self.s3_raw_uri,
                '--datamart_location': self.s3_mart_uri,
                '--archive_location': self.s3_archive_uri,
                '--delta_location': self.s3_delta_uri, 
                '--aws_region': pulumi.Config('aws').get('region'),
                '--extra-py-files': etl_distribution.get_extra_py_files(),
                '--enable-metrics': True
            },
            job_db_connections=glue_connections,
            tags=self.tags,
            opts=pulumi.ResourceOptions(parent=self))

        self.jobs[jobname] = job
        

    def add_sync_table_job(self, script_filename:str, etl_distribution:EtlJobDist, glue_connections:List[str]):
        jobname = os.path.splitext(script_filename)[0]

        job = GlueEtlJob(jobname,
            iam_policies=[
                self.policy_kms_full_usage,
                self.policy_glue_service.arn  #TODO 
            ],
            inline_policy=pulumi.Output.all(
                self.datalake_bucket.bucket.arn,
                self.scripts_bucket.bucket.arn).apply(lambda args, mart_folder=self.folder_config.mart: sync_table.inline_policy(args[0], args[1], mart_folder)),
            script_location=etl_distribution.get_prefixed_s3_uri(script_filename),
            job_args={
                '--table_names': 'hdm.some_table,hdm.some_other_table',
                '--aws_region': pulumi.Config('aws').get('region'),
                '--datamart_location': self.s3_mart_uri,
                '--write_mode': 'error', #error or overwrite, see spark docs for values of df.write.mode({write_mode})
                '--extra-py-files': etl_distribution.get_extra_py_files(),
                '--enable-metrics': True
            },
            security_config=self.glue_security_config,
            job_db_connections=glue_connections,
            job_name_prefix='sync',
            tags=self.tags)
        
        self.jobs[jobname] = job


    def add_db_project(self, project_name:str, table_names:Set[str], crawler_role:iam.Role, cron_schedule:str):
        glue_db = glue.CatalogDatabase(f"{project_name}-db",
            name=project_name,
            description=f"datalake db => {project_name}",
            opts=pulumi.ResourceOptions(parent=self))

        glue_crawler = glue.Crawler(f"{project_name}-crawler",
            description=f"crawler for mart files project => {project_name}",
            database_name=glue_db.name,
            role=crawler_role.arn,
            s3_targets=self.datalake_bucket.bucket.bucket.apply(lambda p, mart_folder=self.folder_config.mart: [{'path': arn_concat(p, mart_folder, table)} for table in sorted(table_names)]),
            security_configuration=self.glue_security_config.name,
            schedule=cron_schedule,
            schema_change_policy={
                'deleteBehavior': 'DEPRECATE_IN_DATABASE',
                'updateBehavior': 'UPDATE_IN_DATABASE'
            },
            opts=pulumi.ResourceOptions(parent=self))

    def add_workflow(self, filepath:str):
        wf_name = os.path.splitext(os.path.basename(filepath))[0]

        # read workflow definition from file
        wf_map = WorkflowDefinition(filepath)

        # create workflow using file name as name,
        # pass map of glue jobs as it holds actual job names
        wf = JobWorkflow(wf_name, wf_definition=wf_map, glue_jobs_map=self.jobs)


    def create_crawler_role(self):
        # create iam role for crawlers to assume
        crawler_role = iam.Role('datalake-crawler-role',
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
            description="datalake mart crawler",
            force_detach_policies=True,
            path='/glue/',
            opts=pulumi.ResourceOptions(parent=self))

        # attach policies to crawler role
        iam.RolePolicyAttachment('crawler-kms-policy', 
            role=crawler_role, 
            policy_arn=self.policy_kms_full_usage.arn, 
            opts=pulumi.ResourceOptions(parent=self))

        iam.RolePolicyAttachment('crawler-glue-service-policy', 
            role=crawler_role, 
            policy_arn=self.policy_glue_service.arn, 
            opts=pulumi.ResourceOptions(parent=self))

        iam.RolePolicy('crawler-read-mart-policy', 
            role=crawler_role, 
            policy=self.datalake_bucket.bucket.arn.apply(lambda b: json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": arn_concat(b, self.folder_config.mart, '*')
                },{
                    "Effect": "Allow",
                    "Action": "logs:AssociateKmsKey",
                    "Resource": "*"
                }]
            })),
            opts=pulumi.ResourceOptions(parent=self))

        return crawler_role