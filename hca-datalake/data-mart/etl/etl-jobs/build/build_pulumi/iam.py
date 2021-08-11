import json
from typing import List

def kms_encrypt_policy(kms_arn):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "kms:Encrypt",
                "kms:GenerateDataKey*",
                "kms:DescribeKey"
            ],
            "Resource": kms_arn
        }]
    })

def kms_usage_policy(kms_arn):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:GenerateDataKey*",
                "kms:DescribeKey"
            ],
            "Resource": kms_arn
        },{
            "Effect": "Allow",
            "Action": [
                "kms:List*",
                "kms:Get*",
                "kms:Describe*",
            ],
            "Resource": "*"
        }]
    })