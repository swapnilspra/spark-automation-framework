# Spark version

Data Lake creation .
    This framework is created for the ETL implementation. It extracts data from csv, COBOL and PostgreSQL table then transform using pyspark and load to PostgreSQL with loading strategies Append, Upsert, overwrite and Type 2.
    This is automated ETL framework can be useful for any ETL based project, we need to create .py job file for inputs which contains source details, Join details like sources, transformations and final column name in target tables and pass this input to the framework and load the target tables in the PostgreSQL.

## Repo Structure

This repository contains several top-level folders, each representing a specific area of work.

Artifacts contained herein represent technical and working code artifacts only.

| Folder | Purpose |
|--- |--- |
| [data-lake](data-lake) | Data lake and S3 handling artifacts |
| [data-mart](data-mart) | Data mart and running ETL artifacts, schema creation and migration |
| [tools](tools) | Miscellaneous tools |

Subdirectory structure within the topics will follow the general structure:

```text
|-- Module1
    |-- README.md
    |-- doc
    |-- src
    |-- test
    |-- scripts
```

Where

- `Module1` is the name of the module or area
- `README.md` describes the module in general
- `doc` is a directory containing documentation
- `src` is a directory containing code artifacts
- `test` is a directory containing testing code artifacts
- `scripts` is a directory containing build, deployment, or other construction artifacts used for producing the solution

# HCA Powersave tags
Use the following tags for resources EC2, RDS, etc. to control how the resources are shutdown or powered up for development pruposes. These tags control how resources are powered down during off hours
- missing tag hca:powersave will power off instance at night and leave it off until manually started
- tag hca:powersave=off will disable powersave and leave resource alone (ideally used in production environment or resources that you want to be up all the time
- tag hca:powersave=on  will run instance per business hours schedule, stopping/starting M-F
- override schedule with tag hca:powersave=off=(M-F,19);on=(M-F,7);tz=ct
- => full specs https://www.capitalone.io/docs/quickstart/offhours.html#tag-based-configuration
