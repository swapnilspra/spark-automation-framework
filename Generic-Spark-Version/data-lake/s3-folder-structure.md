# Data Lake Structure

Data lake consists of data files intended for arbitrary processing in the future by a variety of application.

Key features of a data lake include:

- Arbitrary file size
- Arbitrary number of files
- Arbitrary file ingestion cadence
- Arbitrary read patterns
- Heterogenous file format

While the above characteristics hold true, some organizational structure is both desirable and necessary. The structure described below aims primarily to satisfy the goals:

- That related sources are easy to identify
- That programmatic access to sources is easy and uniform
- That organization is human readable
- That time and archiving of data is straightforward

## Folder organization

The data lake is built on S3 bucket technology. Paths in described below are assumed to be relative to the bucket root URI.

Folder naming shall follow the pattern:

`<source-name>/YYYY/MM/DD/<table-name>.<version-ordinal>[.<time-specifier>].<type-suffix>`

Where:

|Part | Description|
|--- |--- |
|**source-name**| Name of data source. This may be an organization or division |
|**table-name**| Table name. This identify the dataset name |
|**version-ordinal**| Zero-based version of the file. In case of multiple versions of the same dataset, each upload would increment the ordinal. |
|**time-specifier**| A timestamp representing the snapshot time of the data in ISO 8601 format, UTC. The timestamp shall describe the time the data was established, not the time of writing into the bucket. The timestamp shall conform to `YYYYMMDDHHmmss` formatting representing UTC time zone. |


Examples:

`acme-bonds/2019/07/03/bond-a.0.csv`

`acme-bonds/2019/07/03/bond-b.0.csv`

`funder-co/2019/07/03/lighthouse-a.0.20190630180000.csv`

`funder-co/2019/07/03/lighthouse-a.0.20190630190000.csv`

`funder-co/2019/07/03/bay-product.0.csv`

`funder-co/2019/07/03/bay-product.1.csv`

```bash

|-- acme-bonds
|    |-- 2019
|        |-- 07
|            |-- 03
|                |-- bond-a.0.csv
|                |-- bond-b.0.csv
|-- funder-co
|    |-- 2019
|        |-- 07
|            |-- 03
|                |-- lighthouse-a.0.20190630180000.csv
|                |-- lighthouse-a.0.20190630190000.csv
|                |-- bay-product.0.csv
|                |-- bay-product.1.csv


```

## Implementation Rules

1. File and folder naming shall conform to the template above.

1. Leaf folders shall not contain artifacts other than data files.

1. A table name shall be uniquely associated to the schema of the data in the file. If a new schema is required it shall reside in a newly named file with a differing `table-name` portion of the file name.

1. Timestamping the file name should be chosen to represent multiple discrete data sets. When more than one file is expected in a day, a timestamp should be utilized to differentiate the content of the files in the same folder. The content of the files in a single day when using timestamp notation shall be considered additive.

1. Versioning the filename shall represent replacement or update to the dataset in part or in whole. No file shall be overwritten with the same file unless the file is corrupt. A versioned file shall be considered complete replacement of the data set contained in the previous version. This holds true when any of the data entities in the file is updated.

1. Clients consuming the data files should consider the highest version of a file as the authoritative data source among other versioned files of the same name save for the version.

1. Clients consuming data file where timestamp is present shall consume all files for that day for a complete data set.

1. Different timestamped files containing same entity shall be considered an error.

1. No meaning shall be assigned to the order in which files were uploaded into a folder.

1. No meaning shall be assigned to the timestamp order save for differentiating time of capture of the data.
