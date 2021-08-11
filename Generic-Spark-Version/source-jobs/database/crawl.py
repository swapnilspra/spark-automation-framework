import pulumi
import json
from pulumi_aws import glue, iam
from typing import List, Union, Dict

class LegacyDbGlueCrawler(pulumi.ComponentResource):
    role: iam.Role
    database: glue.CatalogDatabase
    crawler: glue.Crawler

    def __init__(self, name, 
            connection:glue.Connection=None,
            db_name:str=None,
            iam_policies:List[str]=[],
            crawl_path:str=None,
            tags:Dict[str,str]={}, 
            opts:pulumi.ResourceOptions=None):
        super().__init__('hca:LegacyDbGlueCrawler', name, None, opts)

        #db_name = name if db_name is None else db_name
        print(f"iam policies from crawler => {iam_policies}")

        self.role = iam.Role(
            resource_name=f"{name}-role",
            path="/glue/",
            description=f"role for glue crawler for legacy db => {name}",
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

        # TODO refine default aws policy
        # requires datalake kms policy for decrypting db connection password
        # include policy similar to AWSGlueServiceRole
        for i, iam_policy in enumerate(iam_policies):
            iam.RolePolicyAttachment(f"{name}-crawl-policy-{i}",
                policy_arn=iam_policy,
                role=self.role,
                opts=pulumi.ResourceOptions(parent=self))

        self.database = glue.CatalogDatabase(f"{name}-database",
            name=db_name,
            description=f"glue database for source job => {name}",
            opts=pulumi.ResourceOptions(parent=self))

        self.crawler = glue.Crawler(f"{name}-crawler",
            name=db_name,
            description=f"glue crawler for legacy db => {db_name}",
            database_name=self.database.name,
            jdbc_targets=[{
                'connectionName': connection.name,
                'path': crawl_path
            }],
            #schema_change_policy={},
            role=self.role.arn,
            tags=tags,
            opts=pulumi.ResourceOptions(parent=self))

        self.register_outputs({})


#NOT USED
# jhagen - adapted from here => https://docs.aws.amazon.com/glue/latest/dg/create-service-policy.html
def get_inline_policy():
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                #TODO may be more glue:* needed here
                #TODO check if tag conditions work here for deletetable
                "glue:Get*",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:SearchTables",
                "glue:ImportCatalogToGlue"
            ],
            "Resource": "*"
        },{
            "Effect": "Allow",
            "Action": [
                #TODO i don't think it requires these, but need to confirm
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "cloudwatch:PutMetricData",

                # role must be able to provision eni to connect to db on private subnet
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",				
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
            ],
            "Resource": "*",
        },{
            #TODO required?
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey"                
            ],
            "Resource": [
                "arn:aws:logs:*:*:/aws-glue/*"
            ]
        },{
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": [
                        "aws-glue-service-resource"
                    ]
                }
            },
            "Resource": [
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*"
            ]
        }]
    })