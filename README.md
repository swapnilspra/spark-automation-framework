# Spark-Autoamation-framework
When the requirement is to prepare the data for business analytics or reporting based huge number of underlined tables. Then there should be some automated way to the process of extracting, transforming, and loading to avoid repetitive efforts for developing the huge number of pipelines for all the number of tables. If you are looking for minimizing cost, and in the lesser timelines also low code approach then these data bricks automated notebooks are best for your use case.

Here consider we transferred on-premises data to the azure data lake service already and now we need to process the data and load it to the Azure data lake storage.

Now the question comes, every table has different transformations, load strategies, and business requirements. Identify all the use cases and functionality and add reusable functions to the function notebook which can be used in stage, transform and join notebook. These notebooks require JSON files to be passed which will have all the details such as table name, stage, transform container, load strategies, etc. We will see it all in detail. The developer has to create a JSON file for all tables.

Architecture:


Raw data: Raw data can be extracted using Azure data factory from databases using copy tool to Azure database storage raw container.

Stage Notebook:

Once data is loaded to the raw container, reading data from file formats like CSV, Excel, and JSON (Mongodb data extracted in JSON), data cleaning, Datatype conversion from raw to expected schema process can be done using Stage notebook and data will be loaded to stage container in the delta format. In Stage, container data is stored in partitions and the partitions are getting overwritten just to load only incremental data. But existing partitions are also saved in the same stage container.

Inputs to Notebook:

Inputs are provided to the notebook using Widget.

Input_file: path to json file

input_partition: partitions to Raw container

out_partition: partition for stage container

1. Read raw files: CSV, Excel, and JSON files can be read into this framework. For CSV format options like inferSchema, quote character, and escape character can be pass-through input JSON.

2. Data cleaning: If any unwanted characters need to be removed from the dataframe then use

3. Data type conversion: If the structure of the data needs to be converted into the expected format then create the table with expected data types and then load data to Stage. The convert_datatype function was created to identify the data type of the table and convert the raw data type into an expected data type. If the staging table is not already created then the staging table will be created in the same raw format.

4. Add partitions: It also has a feature to add partitions or change partitions while loading data to the stage container.

Transform notebook:

Once data is loaded to the stage then it will be transformed and loaded to the transformed container as per the given load strategies- append, update, upsert, and SCD type 2.

For transform notebook, there are some parameters that are mandatory for loading data to transform container.

Sort key: Data will be sorted as per the given sort key for getting the latest records and avoiding duplicates.

business_key: The business key is used to find the delta records newly inserted, updated existing records i.e transform. business_key parameter is not mandatory for load strategies APPEND and OVERWRITE unless you want to remove duplicates in the same load. For UPSERT, business_key is mandatory.

Partitions can be identified from the staging table and transformed data can be stored the same as the stage container

Data can be loaded to different outputs like Azure datalake service in CSV or XML format and SQL Server Database.

Load strategies:

Append: Data will be appended to existing data.

Upsert: Existing source data will be stored as a temp table. And create dataframe for queries used for transformation. Then updated existing records, newly inserted records, and existing records that have no change using business keys. Identify merge join, whenMatchedUpdateset, whenNotMatchedUpdateset using the business key and existing data frame. As Delta format is used upsert is possible.

Overwrite: Data will be simply overwritten entire existing table.

Join notebook:

Join notebook was created for joining multiple sources or source queries and then run transform notebook again for loading data to datamart container.

Unit Test Notebook:

Unit test notebooks are created based on the migration from Azure Datafactory to Databricks automation. A test notebook can do basic unit testing like record count, data validation. It compares output generated by Azure Datafactory pipelines and Databricks automated notebooks. It will provide below information:

Total records
Actual Inserts
Expected Inserts
Actual updates
Expected updates
Expected records not found in actual
extra records not found in expected
