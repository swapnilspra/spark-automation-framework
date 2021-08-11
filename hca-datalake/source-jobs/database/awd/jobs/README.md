# hca-datalake_dst_awd-glue-extracts

Peered with hca-datalake_dst_powerselect-glue-extracts, hca-datalake_dst-db2-extracts (deprecated: standalone, non-Glue, Lambda approach) and, for the internal database Harbor Central, hca-datalake_harbor-central-glue-extracts.

AWD was originally a DB2 database ported to a schema in the database DG_PWHARBO. DG_PWHARBO is crawlable by Glue but only the AWD schema is populated.

However, the other schema are easily viewed through a standard RDBMS client like DBeaver. It is not clear why Glue will not crawl the other schema, numerous attempts were made including with and without wildcard searches.

Most of the scripts in this repo are simple, single-table pulls. The notable exception is the first script, DST_AWD_01_shareholder_email_outbound.

This is the only script that uses tables from the "BI" schema and it does not use tables from the AWD schema.

As a result, this script is similar to the scripts used for PowerSelect (DB2) in that a more manual, explicit handling of the table declarations has to be used since there are no corresponding entries in the Glue Data Catalog.

Additionally, the original SQL script for this process used several Oracle-specific functions to define the fields 'KEY_DATE', 'KEY_TIME' and 'KEY_MILSEC'.

While KEY_DATE could have been easily defined in PySparkSQL, separating the other fields would have made the query much less readable so the decision was to break this processing out into a method (split_date_time_millisecond) which is turned into a udf (explode_ts_udf). 


