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

class DstAwdDatabaseExport(pulumi.ComponentResource):
    connection: glue.Connection
    dbcrawler: LegacyDbGlueCrawler
    jobs: Dict[str,glue.Job]

    def __init__(self, name, 
            config:pulumi.Config=None,
            fileproc_bucket:s3.Bucket=None,
            scripts_bucket:s3.Bucket=None,
            scripts_prefix:str=None,
            iam_policies:List[Union[str,iam.Policy]]=[],
            tags:Dict[str,str]={},
            opts:pulumi.ResourceOptions=None):
        super().__init__('hca:DstAwdDatabaseExport', name, None, opts)
        
        # for glue connection
        jdbc_url = config.require('jdbc_url')
        jdbc_driver = ''
        username = config.require('username')
        password = config.require_secret('password')
        db_subnet_id = config.require('db_subnet_id')
        db_security_groups = config.require('db_security_groups').split(',')

        # for glue crawler
        crawl_path = config.require('crawl_path')
        
        # define where exports go
        export_folder_name = config.require('export_folder_name') if config.get('export_folder_name') else 'awd'

        subnet = ec2.Subnet.get('crawl-subnet', db_subnet_id)

        # upload jar to s3, note it is looking for specific version
        jar_filepath = 'jars/ojdbc6.jar'
        mime_type, _ = mimetypes.guess_type(jar_filepath)
        s3_jar_obj = s3.BucketObject('dbdriver',
            bucket=scripts_bucket,
            source=pulumi.FileAsset(jar_filepath),
            content_type=mime_type,
            key=os.path.join(scripts_prefix, 'jars', os.path.basename(jar_filepath)),
            opts=pulumi.ResourceOptions(parent=self, ignore_changes=['kmsKeyId']))

        s3_jar_uri = pulumi.Output.all(scripts_bucket.bucket, s3_jar_obj.key).apply(lambda args: f"s3://{args[0]}/{args[1]}")

        self.connection = glue.Connection(f"{name}-connection",
            connection_type='JDBC',
            connection_properties={
                'JDBC_CONNECTION_URL': jdbc_url,
                'USERNAME': username,
                'PASSWORD': password,
                'JDBC_DRIVER_CLASS_NAME': jdbc_driver,
                'JDBC_DRIVER_JAR_URI': s3_jar_uri,
            },
            description=f"glue db connection to dst awd db (oracle)",
            physical_connection_requirements={
                'availability_zone': subnet.availability_zone,
                'securityGroupIdLists': db_security_groups,
                'subnet_id': subnet.id
            },
            name=name,
            opts=pulumi.ResourceOptions(parent=self))

        self.crawler = LegacyDbGlueCrawler(f"{name}-crawler",
            connection=self.connection,
            crawl_path=crawl_path,
            iam_policies=[], #TODO
            tags=tags,
            opts=pulumi.ResourceOptions(parent=self))

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
            tags=tags,
            opts=pulumi.ResourceOptions(parent=self))     

        rolepolicy = iam.RolePolicy(f"{name}-role-policy",
            policy=fileproc_bucket.bucket.apply(lambda b: get_inline_policy(b, name, export_folder_name)),
            role=jobrole,
            opts=pulumi.ResourceOptions(parent=self))
        
        # create glue jobs from scripts under hc/
        jobs = {}
        scripts = [ f for f in glob.glob('jobs/*') if os.path.isfile(f)]
        for script in scripts:
            scriptname = os.path.splitext(os.path.basename(script))[0]
            print(f"creating glue export job for => {scriptname}")

            mime_type, _ = mimetypes.guess_type(script)
            s3_script = s3.BucketObject(scriptname,
                bucket=scripts_bucket,
                source=pulumi.FileAsset(script),
                content_type=mime_type,
                key=os.path.join(scripts_prefix, name, os.path.basename(script)),
                opts=pulumi.ResourceOptions(parent=self, ignore_changes=['kmsKeyId']))

            jobs[scriptname] = glue.Job(f"{name}_{scriptname}", # hc_fund_composite
                glue_version='1.0',
                command={
                    'name': 'glueetl',
                    'pythonVersion': '3',
                    'scriptLocation': pulumi.Output.all(s3_script.bucket, s3_script.key).apply(lambda args: f"s3://{os.path.join(args[0],args[1])}"),
                },
                role_arn=jobrole.arn,
                default_arguments={
                    "--s3-bucket": fileproc_bucket,
                    "--s3-tmp-path": f"tmp/{name}/", # will get resolved to tmp/{name}/yyyymmddHHMMss/ in job
                    "--s3-dest-path": f"incoming/{export_folder_name}/{scriptname}.csv",
                    "--extra-jars": s3_jar_uri
                },
                tags=tags,
                connections=[self.connection.id],
                opts=pulumi.ResourceOptions(parent=self))
        
    

def get_inline_policy(bucket_name, job_name, export_folder_name):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject*",
            "Resource": f"arn:aws:s3:::{bucket_name}/tmp/{job_name}/*",
            "Condition": {
                "ForAnyValue:StringEquals": {
                    "s3:ExistingObjectTag/hca:dataclassification":"pii"
                }
            }
        },{
            # allow delete from tmp job folder
            "Effect": "Allow",
            "Action": "s3:DeleteObject*",
            "Resource": f"arn:aws:s3:::{bucket_name}/tmp/{job_name}/*"
        },{
            # allow put to actual dest prefix
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectTagging"
            ],
            "Resource": f"arn:aws:s3:::{bucket_name}/{export_folder_name}/*"
        }]
    })
