import json
from utils import arn_concat

def inline_policy(datalake_bucket_arn, scripts_bucket_arn, mart_folder):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            # allow spark to write dumb _$folder$ files
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": arn_concat(datalake_bucket_arn,'*_$folder$')
        },{
            # allow write/delete to mart folder, note this is kind of dangerous...
            "Effect": "Allow",
            "Action": [
                "s3:GetObject*",
                "s3:PutObject*",
                "s3:DeleteObject*"
            ],
            "Resource": arn_concat(datalake_bucket_arn, mart_folder, '*')
        },{
            # allow list whole bucket
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:GetObjectTagging",
                "s3:ListBucket*",
                "s3:ListMultipartUploadParts",
                "s3:AbortMultipartUpload"
            ],
            "Resource": [
                datalake_bucket_arn,
                datalake_bucket_arn+'/*',
            ]
        },{
            # allow get glue connection (postgres)
            "Effect": "Allow",
            "Action": "glue:GetConnection",
            "Resource": "*"
        },{
            # allow encrypt cloudwatch logs with kms key
            "Effect": "Allow",
            "Action": "logs:AssociateKmsKey",
            "Resource": "arn:aws:logs:*:*:log-group:/aws-glue/*"
        },{
            # allow get all from scripts bucket
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": [
                scripts_bucket_arn,
                scripts_bucket_arn+'/*'
            ]
        }]
    })