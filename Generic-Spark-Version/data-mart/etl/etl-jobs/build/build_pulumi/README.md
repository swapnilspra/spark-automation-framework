# Pulumi preview/deploy #

## Local setup (linux) ##
Install pulumi (https://www.pulumi.com/docs/get-started/install/)
```text
$ curl -fsSL https://get.pulumi.com | sh
```
## Building ##
Prior to running pulumi it is assumed the datalake etljob project has been built:
```text
$ cd data-mart/etl/etl-jobs/build
$ python3 build.py bdist_egg
```
This will create dist dir under `data-mart/etl/etl-jobs/build/dist` with contents similar to the following:
```text
├── hca_etl-0.1-py3.6.egg
├── glue_job_runner.py
├── glue_sync_table.py
└── lambda-glue-notification
    ├── catalog.json
    ├── glue_notification.py
    └── job_dependencies.json
└── extra-packages
    ├── pg8000-1.13.2-py3-none-any.whl
    ├── python_dateutil-2.8.1-py2.py3-none-any.whl
    └── scramp-1.1.0-py3-none-any.whl
```
####Notes####
- Pulumi packaging process expects these files to exist in dist dir, all will be uploaded to s3 with an version prefix and attached to glue jobs or used to package lambda functions
- Beyond this point, pulumi is executing as an entirely different process, any of the util functions declared in datalake will be unavailable as the python dependencies will not be installed

## Running pulumi infrastructure project locally##
Pulumi will create the following infrastructure:

- **s3 buckets**
    - scripts bucket - pulumi adds job scripts and dependencies
    - fileproc bucket - incoming pre-processing bucket for fileproc to dump pre-raw files
    - datalake bucket - stores actual datalake including raw, archive, deltas and mart folders
- **glue job** for each module under `data-mart/etl/etl-jobs/src/jobs`
- **kms key** for encrypting all datalake files in fileproc and datalake bucket
- **iam managed policies**
    - policy_kms_full_usage - allow encrypt/decrypt with datalake kms key
    - policy_kms_encrypt_only - allow encrypt only with datalake kms key
    - policy_get_object_pii - allow get objects in datalake bucket tagged hca:dataclassification=[pii, confidential, nonsensitive]
    - policy_get_object_confidential - allow get objects in datalake bucket tagged hca:dataclassification=[confidential, nonsensitive]
    - policy_get_object_nonsensitive - allow get objects in datalake bucket tagged hca:dataclassification=nonsensitive
    - policy_get_scripts - allow get all objects from scripts bucket (should make sure this is attached to anything reading from scripts ie glue jobs)
    - policy_glue_service - currently hardcoded to aws glue service policy, but will create a harbor specific one later and replace it here so we don't have to make changes to things that need it
- **lambdas**
    - glue_notification - monitor for new objects in fileproc bucket and copy into dependent jobs raw folder in datalake bucket, also trigger glue job if necessary
    - scripts_archive - copy pulumi managed files into consistent folder for etl jobs and also archive into a permanent versioned archive for ability to revert to prior version of script



###Create stack###
To work locally, you should be using your personal pulumi workspace and NOT the harborcapital workspace, by not specifying an organization pulumi will create a stack in your personal workspace. This isolates it from stacks with same name in the organization workspace `harborcapital` which should only be executed from cicd. To connect to aws you'll need to set config value aws:region and specify a default aws profile along with telling pulumi to assume roles.
```text
$ cd data-mart/etl/etl-jobs/build/build_pulumi
$ pulumi stack init jhagen
$ pulumi config set aws:region us-east-2
$ export AWS_PROFILE=identity-dev
$ export AWS_SDK_LOAD_CONFIG=1
```

###Preview###
When you run pulumi the first time pulumi will ask for a token, either get the from the ui or select a local backend. Running pulumi preview will connect to aws and identify changes from last known state as perceived by pulumi.  See pulumi help for full list of options.
```text
$ pulumi preview
```

###Deploy###
If everything looks good, run pulumi deploy to deploy to aws using AWS_PROFILE env var.
```text
$ pulumi up
```