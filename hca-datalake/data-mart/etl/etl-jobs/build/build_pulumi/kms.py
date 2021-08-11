import json

def key_policy(account_id, region):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "kms:*",
            "Resource": "*",
            "Principal": {
                "AWS": f"arn:aws:iam::{account_id}:root"
            }
        },{
            "Effect":"Allow",
            "Action":[
                "kms:Encrypt",
                "kms:DescribeKey",
                "kms:GenerateDataKey"
            ],
            "Resource": "*",
            "Principal": {
                "AWS": f"arn:aws:iam::964890379153:root"
            }

        },{
            "Effect": "Allow",
            "Principal": { "Service": f"logs.{region}.amazonaws.com" },
            "Action": [ 
                "kms:Encrypt*",
                "kms:Decrypt*",
                "kms:ReEncrypt*",
                "kms:GenerateDataKey*",
                "kms:Describe*"
            ],
            "Resource": "*"
        }]
    })