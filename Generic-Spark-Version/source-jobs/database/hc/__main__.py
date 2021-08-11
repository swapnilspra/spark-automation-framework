import pulumi
from pulumi_aws import ec2, s3, glue
import os, sys
from datetime import datetime
import pkgutil
import glob
import job
from workflow import WorkflowSchedule

project = pulumi.get_project()
env = pulumi.get_stack()

config = pulumi.Config()

tags = {
    'hca:pulumi_stack': env,
    'hca:pulumi_project': project,
    'hca:source_job': 'hc',
    'hca:managed_by': 'pulumi'
}

# get reference to datalake infra stack if specified in config
infra_stack_name = pulumi.Config().get('infra_stack')
if not infra_stack_name:
    infra_stack_name = f"harborcapital/datalake/{env}"
    #infra_stack_name = f"jason-hagen/datalake/{env}"

infra = pulumi.StackReference('infra', infra_stack_name)

fileproc_bucket = s3.Bucket.get('fileproc-bucket', infra.get_output('fileproc_bucket'))
scripts_bucket = s3.Bucket.get('scripts-bucket', infra.get_output('scripts_bucket'))
glue_security_config = glue.SecurityConfiguration.get('security-config', infra.get_output('glue_security_config'))

# NOTE 
# - prior to running, need to set password encryption either manually or with script
# - aws glue put-data-catalog-encryption-settings --data-catalog-encryption-settings \
#     "ConnectionPasswordEncryption={ReturnConnectionPasswordEncrypted=true,AwsKmsKeyId=alias/hca/datalake-[env]"
# - track progress for doing with pulumi => https://github.com/terraform-providers/terraform-provider-aws/issues/9736
# - otherwise connection password will be stored in plain text

SCRIPTS_VERSION = os.environ.get('SCRIPTS_VERSION', f"local-{datetime.now().strftime('%Y%m%d%H%M%S')}")

hcexport = job.HcDatabaseExport(f"hc_{env}",
    config=config,
    fileproc_bucket=fileproc_bucket,
    scripts_bucket=scripts_bucket,
    scripts_version=SCRIPTS_VERSION,
    security_config=glue_security_config,
    iam_policies=[
        infra.get_output('policy_kms_full_usage'),
        infra.get_output('policy_glue_service'),
        #'arn:aws:iam::232930948780:policy/fileproc1-kmspolicy-GU87ZLKMZU5V' #TODO REMOVE THIS - temporary for glue in dev us-west-2
    ],
    tags=tags)

# create workflow with schedule to run jobs daily
schedule_wf = WorkflowSchedule(f"hc_daily_{env}",
    source_jobs=hcexport.jobs.values(),
    schedule_expr="0 5 * * ? *",
    enabled=(env in ['qa','datamart','dev']))