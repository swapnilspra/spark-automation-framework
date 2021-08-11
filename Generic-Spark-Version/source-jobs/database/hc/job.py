import pulumi
from pulumi_aws import iam, glue, s3, ec2
from typing import Dict, List, Union
import os
import glob
import mimetypes
import json
import sys
sys.path.insert(0, '../')
from crawl import LegacyDbGlueCrawler

class HcDatabaseExport(pulumi.ComponentResource):
    connection: glue.Connection
    dbcrawler: LegacyDbGlueCrawler
    jobs: Dict[str,glue.Job]

    def __init__(self, name, 
            config:pulumi.Config=None,
            fileproc_bucket:s3.Bucket=None,
            scripts_bucket:s3.Bucket=None,
            scripts_version:str=None,
            security_config:glue.SecurityConfiguration=None,
            iam_policies:List[str]=None,
            tags:Dict[str,str]=None,
            opts:pulumi.ResourceOptions=None):
        super().__init__('hca:HcDatabaseExport', name, None, opts)

        # upload scripts to working/ for versioning/archiving
        versioned_scripts_prefix = f"working/{scripts_version}/{pulumi.get_project()}/{pulumi.get_stack()}"

        # jobs will point to scripts/ to keep consistent paths
        scripts_prefix = f"scripts/{pulumi.get_project()}/{pulumi.get_stack()}"

        objtags = tags.copy() if tags else {}
        objtags.update({
            'hca:datalake_version': scripts_version
        })
        
        # for glue connection
        jdbc_url = config.require('jdbc_url')
        username = config.require('username')
        password = config.require_secret('password')
        db_subnet_id = config.require('db_subnet_id')
        db_security_groups = config.require('db_security_groups').split(',')

        # for glue crawler
        crawl_path = config.require('crawl_path')
        
        # define where exports go
        export_folder_name = config.require('export_folder_name') if config.get('export_folder_name') else 'fund'

        subnet = ec2.Subnet.get('crawl-subnet', db_subnet_id)

        self.connection = glue.Connection(f"{name}-connection",
            connection_type='JDBC',
            connection_properties={
                'JDBC_CONNECTION_URL': jdbc_url,
                'USERNAME': username,
                'PASSWORD': password
            },
            description=f"glue db connection to hc db",
            physical_connection_requirements={
                'availability_zone': subnet.availability_zone,
                'securityGroupIdLists': db_security_groups,
                'subnet_id': subnet.id
            },
            name=name,
            opts=pulumi.ResourceOptions(parent=self))

        self.crawler = LegacyDbGlueCrawler(f"{name}-crawler",
            db_name=name,
            connection=self.connection,
            crawl_path=crawl_path,
            iam_policies=iam_policies,
            tags=tags,
            opts=pulumi.ResourceOptions(parent=self))

        roletags = tags.copy() if tags else {}
        roletags.update({
            'hca:dataclassification': 'confidential'
        })
        jobrole = iam.Role(
            resource_name=f"{name}-job-role",
            path="/glue/",
            description=f"shared role for glue source job => {name}",
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
            tags=roletags,
            opts=pulumi.ResourceOptions(parent=self))     

        rolepolicy = iam.RolePolicy(f"{name}-role-policy",
            policy=fileproc_bucket.bucket.apply(lambda b: get_inline_policy(b, name, export_folder_name)),
            role=jobrole,
            opts=pulumi.ResourceOptions(parent=self))

        scriptspolicy = iam.RolePolicy(f"{name}-scripts-policy",
            policy=scripts_bucket.bucket.apply(read_scripts_policy),
            role=jobrole,
            opts=pulumi.ResourceOptions(parent=self))

        for i, iam_policy in enumerate(iam_policies):
            iam.RolePolicyAttachment(f"{name}-policy-{i}",
                policy_arn=iam_policy,
                role=jobrole,
                opts=pulumi.ResourceOptions(parent=self))

        # upload job_utils.py
        s3_job_utils = s3.BucketObject(f"{name}-job-utils",
            bucket=scripts_bucket,
            source=pulumi.FileAsset('../job_utils.py'),
            content_type='text/x-python',
            key=os.path.join(versioned_scripts_prefix, 'job_utils.py'),
            tags=objtags,
            opts=pulumi.ResourceOptions(parent=self, ignore_changes=['kmsKeyId']))

        # create glue jobs from scripts under hc/
        self.jobs = {}
        scripts = [ f for f in glob.glob('jobs/*') if os.path.isfile(f)]
        for script in scripts:
            scriptname = os.path.splitext(os.path.basename(script))[0]
            print(f"creating glue export job for => {scriptname}")

            mime_type, _ = mimetypes.guess_type(script)
            s3_script = s3.BucketObject(scriptname,
                bucket=scripts_bucket,
                source=pulumi.FileAsset(script),
                content_type=mime_type,
                key=os.path.join(versioned_scripts_prefix, os.path.basename(script)),
                tags=objtags,
                opts=pulumi.ResourceOptions(parent=self, ignore_changes=['kmsKeyId']))

            self.jobs[scriptname] = glue.Job(f"{name}_{scriptname}", # hc_fund_composite
                glue_version='1.0',
                description=f"source job to export data from legacy oracle hc db table => {scriptname}",
                command={
                    'name': 'glueetl',
                    'pythonVersion': '3',
                    'scriptLocation': s3_script.bucket.apply(lambda b, scripts_prefix=scripts_prefix, script=os.path.basename(script): f"s3://{os.path.join(b, scripts_prefix, script)}"),
                },
                role_arn=jobrole.arn,
                default_arguments={
                    "--s3_bucket": fileproc_bucket,
                    "--s3_tmp_path": f"tmp/{name}/{scriptname}/", # will get resolved to tmp/{name}/yyyymmddHHMMss/ in job
                    "--s3_dest_path": f"incoming/{export_folder_name}/{scriptname}.csv",
                    "--glue_db": name,
                    "--source_db_schema": crawl_path,
                    "--extra-py-files": s3_script.bucket.apply(lambda b: f"s3://{os.path.join(b, scripts_prefix, 'job_utils.py')}"),
                    "--enable-metrics": True
                },
                tags=tags,
                connections=[self.connection.name],
                security_configuration=security_config,
                timeout=30,
                opts=pulumi.ResourceOptions(parent=self))       
    

def read_scripts_policy(scripts_bucket):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject*",
            "Resource": f"arn:aws:s3:::{scripts_bucket}/*"
        }]
    })

def get_inline_policy(bucket_name, job_name, export_folder_name):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject*",
            "Resource": f"arn:aws:s3:::{bucket_name}/tmp/{job_name}/*"
        },{
            # allow put/delete from tmp job folder
            "Effect": "Allow",
            "Action": [
                "s3:DeleteObject*",
                "s3:PutObject*"
            ],
            "Resource": f"arn:aws:s3:::{bucket_name}/tmp/{job_name}/*"
        },{
            # allow put to actual dest prefix
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectTagging"
            ],
            "Resource": f"arn:aws:s3:::{bucket_name}/incoming/{export_folder_name}/*"
        },{
            "Effect": "Allow",
            "Action": [
                "logs:AssociateKmsKey"
            ],
            "Resource": "arn:aws:logs:*:*:log-group:/aws-glue/*"
        }]
    })
