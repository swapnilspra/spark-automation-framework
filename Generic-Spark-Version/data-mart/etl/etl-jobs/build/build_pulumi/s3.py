import pulumi, json


def datalake_bucket_policy(bucket_arn, kms_key_arn):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            # deny if trying to use non-datalake kms key
            "Effect": "Deny",
            "Principal": "*",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": [
                bucket_arn + '/*'
            ],
            "Condition": {
                "StringNotLikeIfExists": {
                    "s3:x-amz-server-side-encryption-aws-kms-key-id": kms_key_arn,
                },
            }
            #TODO only process putting things in /mart & /archive should be glue jobs
        },{
            # deny access to pii unless principaltag=pii
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": [
                bucket_arn,
                bucket_arn + '/*'
            ],
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/hca:dataclassification": "pii"
                },
                "StringNotEquals": {
                    "aws:PrincipalTag/hca:dataclassification": "pii"
                }
            }
        },{
            # deny access to confidential unless principaltag=pii,confidential
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": [
                bucket_arn,
                bucket_arn + '/*'
            ],
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/hca:dataclassification": "confidential"
                },
                "StringNotEquals": {
                    "aws:PrincipalTag/hca:dataclassification": ["pii","confidential"]
                }
            }
        },{
            # deny access to delete any object tag when objecttag=pii|confidential
            "Effect": "Deny",
            "Principal": "*",
            "Action": [
                "s3:DeleteObjectTagging",
            ],
            "Resource": bucket_arn + '/*',
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/hca:dataclassification": ["pii","confidential"]
                },
            }
        },{
            # deny downgrading access to tag when objecttag=pii
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutObjectTagging",
            "Resource": bucket_arn + '/*',
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/hca:dataclassification": "pii",
                },
                "StringEqualsIfExists": {
                    "s3:RequestObjectTag/hca:dataclassification": ["nonsensitive","confidential"]
                }
            }
        
        }, {
            # deny any dataclassification tag that's not pii, nonsensitive, confidential
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutObjectTagging",
            "Resource": bucket_arn + '/*',
            "Condition": {
                "ForAnyValue:StringNotEquals": {
                    "s3:RequestObjectTag/hca:dataclassification": ["pii", "nonsensitive", "confidential"]
                }
            }
        }
        ]
    })

def fileproc_bucket_policy(bucket_arn, kms_key_arn):
    policy = json.loads(datalake_bucket_policy(bucket_arn, kms_key_arn))

    #TODO revise fileproc-datalake to assume role in datalake account
    policy['Statement'].append({
        "Effect": "Allow",
        "Principal": { "AWS": "arn:aws:iam::964890379153:root" }, #TODO create iam role for infra to assume and kill this
        "Action": [
            "s3:PutObject*"
        ],
        "Resource": [
            f"{bucket_arn}/incoming/*"
        ]
    })
    return json.dumps(policy)

def scripts_bucket_policy(bucket_arn, kms_key_arn):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": [
                bucket_arn + '/*'
            ],
            "Condition": {
                "StringNotLikeIfExists": {
                    "s3:x-amz-server-side-encryption-aws-kms-key-id": kms_key_arn
                }
            }
        }]
        #TODO deny access to modify scripts via console,
        #only script-archive lambda can do this
    })


from pulumi_aws import kms, s3
from typing import Dict, Callable, List

class DatalakeBucket(pulumi.ComponentResource):
    bucket: s3.Bucket

    def __init__(self,name,
            kms_key: kms.Key,
            lifecycle_rules: List[Dict[str,object]]=[],
            versioning_enabled=True,
            bucket_policy: Callable[[str,str],str]=None,
            block_cross_account_access=True,
            tags:Dict[str,str]=None,
            opts:pulumi.ResourceOptions=None):

        super().__init__('hca:DatalakeBucket', name, None, opts)

        self.bucket = s3.Bucket(name,
            lifecycle_rules=lifecycle_rules,
            server_side_encryption_configuration={
                'rule': {
                    'applyServerSideEncryptionByDefault': {
                        'kmsMasterKeyId': kms_key.arn,
                        'sseAlgorithm': 'aws:kms'
                    }
                }
            },
            versioning={ 'enabled': versioning_enabled },
            tags=tags,
            opts=pulumi.ResourceOptions(parent=self))

        if bucket_policy:
            s3.BucketPolicy(f"{name}-policy",
                bucket=self.bucket,
                policy=pulumi.Output.all(self.bucket.arn, kms_key.arn).apply(lambda p: bucket_policy(p[0],p[1])),
                opts=pulumi.ResourceOptions(parent=self))

        s3.BucketPublicAccessBlock(f"{name}-access-block",
            bucket=self.bucket,
            block_public_acls=True,
            block_public_policy=True,
            ignore_public_acls=True,
            restrict_public_buckets=True,
            opts=pulumi.ResourceOptions(parent=self))
        
        self.register_outputs({})
    

    # only call this once, to attach multiple functions include them in lambd_event_notifications
    def attach_notifications(self, name, lambda_event_notifications):
        s3.BucketNotification(name,
            bucket=self.bucket,
            lambda_functions=lambda_event_notifications,
            opts=pulumi.ResourceOptions(parent=self))