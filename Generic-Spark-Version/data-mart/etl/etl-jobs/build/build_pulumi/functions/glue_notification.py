import configparser, os, json
import pulumi
from pulumi_aws import iam, s3, lambda_
from typing import List, Dict, Callable
from utils import arn_concat


class GlueNotificationLambda(pulumi.ComponentResource):
    function: lambda_.Function

    def __init__(self, name, 
            datalake_bucket:s3.Bucket=None,
            datalake_raw_path:pulumi.Input[str]=None,
            fileproc_bucket:s3.Bucket=None,
            managed_policy_arns:List[pulumi.Input[str]]=[],
            package_dir:str=None,
            tags:Dict[str,str]=None,
            opts:pulumi.ResourceOptions=None):
        super().__init__('hca:GlueNotificationLambda', name, None, opts)
        
        merged_tags = tags.copy() if tags else {}
        merged_tags.update({
            'hca:dataclassification': 'pii'
        })

        role = iam.Role(f"{name}-role",
            path="/lambda/",
            description=f"role for glue notification lambda",
            assume_role_policy=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Principal": {
                        "Service": "lambda.amazonaws.com"
                    }
                }]
            }),
            force_detach_policies=True,
            tags=merged_tags,
            opts=pulumi.ResourceOptions(parent=self))

        # attach policies
        for index, policy in enumerate(managed_policy_arns):
            iam.RolePolicyAttachment(f"{name}-attach-policy-{index}",
                policy_arn=policy,
                role=role,
                opts=pulumi.ResourceOptions(parent=self))
        
        iam.RolePolicy(f"{name}-inline-policy",
            role=role,
            policy=pulumi.Output.all(
                datalake_bucket.arn, 
                fileproc_bucket.arn).apply(lambda b: inline_policy(b[0], b[1], datalake_raw_path)),
            opts=pulumi.ResourceOptions(parent=self))

        self.function = lambda_.Function(f"{name}-function",
            runtime='python3.6',
            description='copy files from fileproc bucket to datalake raw bucket and trigger glue jobs',
            handler='glue_notification.main',
            environment={
                'variables': {
                    'S3_DATALAKE_BUCKET': datalake_bucket,
                    'S3_RAW_PATH': datalake_raw_path,
                    'PULUMI_STACK': pulumi.get_stack(),
                    'PULUMI_PROJECT': pulumi.get_project()
                }
            },
            memory_size=256,
            timeout=60,
            code=pulumi.AssetArchive({
                # use lambda-glue-notification created with build.py
                '.': pulumi.FileArchive(package_dir),
            }),
            role=role.arn,
            tags=merged_tags,
            opts=pulumi.ResourceOptions(parent=self))

        lambda_.Permission(f"{name}-permission",
            action='lambda:InvokeFunction',
            principal='s3.amazonaws.com',
            function=self.function,
            source_arn=fileproc_bucket.arn,
            opts=pulumi.ResourceOptions(parent=self))
        
        self.register_outputs({})


def inline_policy(datalake_bucket_arn, fileproc_bucket_arn, raw_folder) -> str:
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectTagging"
            ],
            "Resource": fileproc_bucket_arn + '/*'
        },{
            "Effect": "Allow",
            "Action": "s3:DeleteObject*",
            "Resource":fileproc_bucket_arn + '/*'
        },{
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectTagging"
            ],
            "Resource": arn_concat(datalake_bucket_arn, raw_folder, '*')
        },{
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "*"
        },{
            "Effect": "Allow",
            "Action": [
                "glue:StartJobRun",
                "glue:List*",
                "glue:Get*"
            ],
            "Resource":"*"
        },{
            "Effect": "Allow",
            "Action": "tag:GetResources",
            "Resource": "*"
        },{
            "Effect": "Allow",
            "Action": [
                "s3:ListBucketMultipartUploads",
                "s3:ListMultipartUploadParts",
                "s3:AbortMultipartUpload"
            ],
            "Resource": [
                datalake_bucket_arn,
                datalake_bucket_arn + '/*'
            ]
        }]
    })