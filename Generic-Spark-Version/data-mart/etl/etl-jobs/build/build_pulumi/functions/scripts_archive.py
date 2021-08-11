import configparser, os, json
import pulumi
from pulumi_aws import iam, s3, lambda_
from typing import List, Dict


class ScriptArchiveLambda(pulumi.ComponentResource):
    function: lambda_.Function

    def __init__(self, name, 
            scripts_bucket:s3.Bucket=None,
            managed_policy_arns:List[str]=[],
            tags:Dict[str,str]=None,
            opts:pulumi.ResourceOptions=None):
        super().__init__('hca:ScriptArchiveLambda', name, None, opts)
        
        merged_tags = tags.copy() if tags else {}
        merged_tags.update({
            'hca:dataclassification': 'pii'
        })

        role = iam.Role(f"{name}-role",
            path="/lambda/",
            description=f"role for script archive lambda",
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

        # attach managed policies
        if managed_policy_arns:
            for index, policy in enumerate(managed_policy_arns):
                iam.RolePolicyAttachment(f"{name}-attach-policy-{index}",
                    policy_arn=policy,
                    role=role,
                    opts=pulumi.ResourceOptions(parent=self))
        
        fileprocpolicy = iam.RolePolicy(f"{name}-inline-policy",
            role=role,
            policy=scripts_bucket.arn.apply(inline_policy),
            opts=pulumi.ResourceOptions(parent=self))

        print(f"archive function => {os.path.abspath(os.path.join(os.getcwd(),'../../src/lambdas/scripts_archive.py'))}")
        self.function = lambda_.Function(f"{name}-function",
            runtime='python3.6',
            description='copy files from fileproc bucket to datalake raw bucket and trigger glue jobs',
            handler='index.main',
            memory_size=128,
            timeout=30,
            code=pulumi.AssetArchive({
                # NOTE use relative path from pulumi root
                'index.py': pulumi.FileAsset(os.path.abspath(os.path.join(os.getcwd(),'../../src/lambdas/scripts_archive.py'))),
            }),
            role=role.arn,
            tags=merged_tags,
            opts=pulumi.ResourceOptions(parent=self))

        lambda_.Permission(f"{name}-permission",
            action='lambda:InvokeFunction',
            principal='s3.amazonaws.com',
            function=self.function,
            source_arn=scripts_bucket.arn,
            opts=pulumi.ResourceOptions(parent=self))
        
        self.register_outputs({})


def inline_policy(scripts_bucket_arn):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectTagging"
            ],
            "Resource": f"{scripts_bucket_arn}/working/*"
        },{
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectTagging"
            ],
            "Resource": [
                f"{scripts_bucket_arn}/scripts/*",
                f"{scripts_bucket_arn}/archive/*"
            ]
        },{
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "*"
        }]
    })