# GLUE JOB OVERVIEW

#### The three sub-folders of the hca-datalake/source-jobs/database folder correspond to the three source databases described below.

* awd: Oracle schema at DST (n.b., AWD was originally a DB2 database but was converted into an Oracle schema inside the DG_PWHARBO Oracle database - also see notes below on the BI schema in the section on the "non-crawlable case")

* hc: RDS Oracle database in Harbor's AWS environment

* powerselect: IBM DB2 database at DST

The purpose of the Glue jobs is to run extraction queries that produce flat files from the corresponding databases. The differences between the jobs correspond to factors based on the database platform (Oracle vs DB2 and whether or not AWS provides native driver support for the platform), the "crawlability" of the database (i.e., databases without native drivers can't be crawled) and some further variability (cf. DST AWD jobs below, as the Oracle database, PG_DWHARBO, is crawlable but the BI schema required "manual" scripting).

All scripts also include a process which converts the Glue/Spark output into a "named" file. That is, by default, Spark lets the user specify the name of the output location (e.g., S3 bucket/folder) of the script process but not the names of the output files which, due to the parallel architecture of Spark, are written as "part" files. The Nayatech processes were based on (named CSV) sample files that were provided and these processes expect single, named CSV files as input. To accommodate this the Glue scripts "coalesce" ("repartition") the output into a single file but this file still cannot be named by Spark. In order to be amenable with the original source files provided to Nayatech, a method is run at the end of each job to pick up the part file, rename it, and put it in a designated folder where it will be picked up by the next scripting stage (e.g., Nayatech process, etc.).

**WARNING:** *scripts run against DST databases (i.e., AWD and PowerSelect jobs) almost universally pull PII data. Care should be exercised to avoid writing data anywhere insecure (e.g., make sure that any logging, if used for debug, is encrypted via KMS).*

## ORIGINAL SOURCE QUERIES (DST):

#### Queries for AWD and PowerSelect can be found in the files in the files section under "General" under Data Lake Migration in Teams.

* PowerSelect:  
https://harborcapitaladvisors.sharepoint.com/sites/DataLakeMigration/Shared%20Documents/General/PowerSelect%20Queries.txt

* AWD:  
https://harborcapitaladvisors.sharepoint.com/sites/DataLakeMigration/Shared%20Documents/General/AWD%20Queries.txt

## CRAWLABLE CASE:

#### When the source database (schema) is crawlable, Glue creates a "Data Catalog" for the schema which simplifies the script (i.e., field types are "known" by Glue). In this case, the jobs have a uniform structure:
1. Specify the name of the "dynamic frame" created by Glue that corresponds to a crawled table.  
*Note: "db" and "src_schema" are values passed to the script, "source_table" is the name of the table in the schema.*  
   * source_table = glueContext.create_dynamic_frame.from_catalog(database = db, table_name =  "{}_dbo_source_table".format(src_schema))
1. Cast the dynamic frame as a (Spark) dataframe via the ".toDF()" method in Pyspark.  
   * source_table_df = source_table.toDF()
1. Cast the dataframe as a table/view.  
   * source_table_df.createOrReplaceTempView("source_table_tbl")
1. The job SQL is executed by the Spark SQLContext and returned as a dataframe.  
   * This dataframe is then run through a "scrubber" for additional formatting, coalesced/repartitioned (occasionally with ordering on fields) into a single file and written to disk. The "renaming function" is then run which places the "named file" in an S3 folder where it can be picked up by other jobs.
   * Also, in this case, all authentication is handled by the connection and crawler and thus does not need to handled by the extraction script (cf. the connection strings in the non-crawlable example below).

## NON-CRAWLABLE CASE:

In contrast to the crawlable case, where the original Oracle scripts used internally at Harbor needed little adaptation to be run in Glue, when the schema/database is not crawlable either due to lack of AWS driver support (e.g., IBM DB2) or is not "discovered" by Glue when crawling the database (e.g., the BI schema in the DB_PWHARBO database described below), a more manual approach is required. This is because corresponding entries in the Glue Data Catalog are not created so the scripts cannot reference the Glue Data Catalog.

This "manual approach" did not involve a significant departure from the outline above for crawlable datasources (though the steps are slightly different) but the absence of the Glue Data Catalog meant that Glue had no information about field types which subsequently required some additional casting. Additionally, there was a need to adapt some of the SQL queries. That is, in contrast to sending a script "across the wire" to be executed by the RDBMS engine, Spark creates an execution plan which run on the cluster; the absence of the Data Catalog means that Glue (Spark) "knows less" about the database. All of the AWD queries with the exception of "shareholder_email_outbound" only use the AWD schema, which was crawlable, and follow the pattern above. The "shareholder_email_outbound" process uses tables from the BI schema which, like AWD, is on the DG_PWHARBO database server but, in contrast to AWD, BI was not crawled and follows the pattern below. All of the DST PowerSelect (IBM DB2) scripts follow this pattern as well.
1. A Glue JDBC connection parameters are passed by the caller:
   * jdbc_driver_name = oracle.jdbc.driver.OracleDriver (Oracle) or com.ibm.db2.jcc.DB2Driver (DB2). These are class names in the driver jar file provided to Glue on S3. The driver jar file location is specified in the Glue Job description (as is the license jar file in the case of IBM DB2, which is provided as a "referenced file" - see link below on connnection and crawler information).
   * db_url = jdbc:oracle:thin://@123.456.789.012:1528/DATABASE (Oracle AWD) or jdbc:db2://MACHINE.DOMAIN.TLD:50000/DATABASE (DB2 PowerSelect)
   * Authentication = db_username + db_password
1. A (Spark) dataframe is constructed from a JDBC connection using the parameters listed in #1 above:
   * SOURCE_TABLE_df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", db_url).option("user", db_username).option("password", db_password).option("dbtable","SCHEMA_NAME.SOURCE_TABLE").load()  
*n.b., the schema did not need to be specified in the PowerSelect scripts but was needed to specify tables in the BI schema on DG_PWHARBO. Note also that the OJDBC driver (and, for DB2, the license driver) has to be specified and made available in S3. Thus, even though BI is a schema in Oracle, in contrast to AWD which uses the Glue-provided Oracle driver, an Oracle driver has to be made available.*
1. As in the crawlable case, the dataframe is then cast as a table/view for use by the sqlContext.
   * SOURCE_TABLE_df.createOrReplaceTempView("SOURCE_TABLE")
1. Exactly like step #4 in the crawlable case.

## FORMATTING AND ADDITIONAL SCRIPTING:

1. Formatting functions are written as Python methods but then cast as user-defined functions (UDFs) in Spark to be executed on the cluster. Various formatting and utility scripts are being extracted from the job scripts and collected in include files.
1. Dates in the script output was standardized on yyyy-mm-dd as opposed to m/dd/yyyy.
1. For decimal format fields, Pyspark outputs "0" as "0E-n" where "n" is the scale. With a mind to making the scripts as agnostic as possible, methods are included ("is_number" and "cast_string") used in the UDF "str_udf" that outputs "0.0" with prescribed precision as a literal string (i.e., so that the output is NOT in scientific notation: 0E-12 = 0.000000000000). Since the output is delivered serialized as an unstructured flat file, there is no loss of precision or information. In particular, programs (e.g., Excel) that don't necessarily know how to interpret scientific notation should be able to read and parse the files.
1. The ouput dataframe is then run through various "scrubbing" transformations to prep the columns to match the expected field format for consumption by downstream scripts. It is here that various type-casting (in Pyspark data types) and the aforementioned methods are used.
1. The AWD job "shareholder_email_outbound" (the only job using the BI schema) included a number of Oracle-specific functions in the SQL. Since this job was of "non-crawlable" type, the SQL had to be adjusted from the original source. Specifically, the Oracle SQL leveraged functions to extract the date, time and millisecond components from a timestamp. The date part could be easily extracted via the Spark to_date() method but it would have been clumsy to extract the time and millisecond components in the SQL (e.g., via the split() method, etc.). To make the process more readable, a method was written (split_date_time_millisecond) that "exploded" the timestamp into an array ([date,time,millisecond]) which was used by the UDF "explode_ts_udf". The SQL was then modified to drop the KEY_DATE, KEY_TIME and KEY_MILSEC fields which were then added via the explode_ts_udf function and the withColumn() dataframe operation.

## CONNECTION AND CRAWLER INFORMATION:

#### Can be found in the file:

https://github.com/harborcapital/hca-datalake/blob/dev/source-jobs/database/connections.txt

