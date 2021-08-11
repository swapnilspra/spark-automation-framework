# Directories

place all required whl files of package dependencies under build/pkg

Must be pure python. Glue will not accept packages with C dependencies

# Building

1. Create dist artifacts:

```
cd build
python build.py bdist_egg --dist-dir ../dist
```
Creates:

- hca_etl-xxx.egg : egg file with code artifacts
- glue_job_runner.py : copied from scripts folder

2. Deploy cloudformation stack

```
cd build_cdk
cdk deploy
```

Alternatively, to just create the cloudformation template use `cdk synth`

## Notes

If you are using a security role, run the following to assume the role:

```
source ./assume_role.sh
```
