import pulumi, json
from pulumi_aws import glue, iam, s3
from typing import Dict, List, Union
from datetime import datetime


class LegacyDbGlueSourceJob(pulumi.ComponentResource):
    glue_job: glue.Job
    role: iam.Role

    def __init__(self, name,
            bucket:s3.Bucket=None,
            connection:glue.Connection=None,
            script_location:str=None,
            iam_policies:List[Union[str,iam.Policy]]=[],
            incoming_folder:str=None,
            tags:Dict[str,str]={},
            opts:pulumi.ResourceOptions=None):
        super().__init__('hca:LegacyDbGlueSourceJob', name, None, opts)

        self.role = iam.Role(
            resource_name=f"{name}-role",
            path="/glue/",
            description=f"role for glue source job => {name}",
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
            opts=pulumi.ResourceOptions(parent=self),
            tags=tags)       

        rolepolicy = iam.RolePolicy(f"{name}-role-policy",
            policy=bucket.id.apply(lambda b: get_inline_policy(b, name)),
            role=self.role,
            opts=pulumi.ResourceOptions(parent=self))

        for i, iam_policy in enumerate(iam_policies):
            iam.RolePolicyAttachment(f"{name}-policy-{i}",
                policy_arn=iam_policy,
                role=self.role,
                opts=pulumi.ResourceOptions(parent=self))

        self.glue_job = glue.Job(f"{incoming_folder}_{name}",
            glue_version='1.0',
            command={
                'name': 'glueetl',
                'pythonVersion': '3',
                'scriptLocation': script_location, #TODO etl_distribution.glue_job_runner
            },
            role_arn=self.role.arn,
            default_arguments={
                "--s3-bucket": bucket,
                "--s3-tmp-path": f"tmp/{name}/", # will get resolved to tmp/{name}/yyyymmddHHMMss/ in job
                "--s3-dest-path": f"incoming/{incoming_folder}/{name}.csv"
            },
            tags=tags,
            connections=[connection.id],
            opts=pulumi.ResourceOptions(parent=self))


def get_inline_policy(bucket_name, job_name):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject*",
            "Resource": f"arn:aws:s3:::{bucket_name}/tmp/{job_name}/*",
            "Condition": {
                "ForAnyValue:StringEquals": {
                    "s3:ExistingObjectTag/hca:dataclassification":"nonsensitive"
                }
            }
        },{
            # allow delete from job folder
            "Effect": "Allow",
            "Action": "s3:DeleteObject*",
            "Resource": f"arn:aws:s3:::{bucket_name}/tmp/hc/{job_name}/*"
        },{
            # allow put to archive and mart folders
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectTagging"
            ],
            "Resource": f"arn:aws:s3:::{bucket_name}/fund/{job_name}/*"
        }]
    })