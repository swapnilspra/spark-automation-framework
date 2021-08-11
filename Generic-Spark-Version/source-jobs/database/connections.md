# OVERVIEW

#### The configuration settings below should all be considered preliminary as they are what was used in development. There is a significant chance that they are either non-optimal (e.g., may well include redundant or superfluous conditions).

There are essentially four types of processes:

* External: DST
	* PowerSelect: 
		* RDBMS: IBM DB2
		* Not crawlable
		* No Native Support by AWS Glue: manual handling of connection and jars
		* Connection: CFNConnectionDB2-4RTMEM6o15nb
	* DG_PWHARBO AWD Schema:
		* RDBMS: Oracle (External, non-RDS)
		* Crawlable
		* Discovered by the Glue Crawler dst_awd
		* Connection: AWD
	* DG_PWHARBO BI Schema:
		* RDBMS: Oracle (External, non-RDS)
		* Crawlable (according to AWS documentation)
		* Not discovered by the Glue Crawler dst_awd and required manual configuration (driver specification, etc.)
		* Connection: AWD

* Internal: Harbor Central
	* Harbor Central:
		* RDBMS: Oracle (Internal, RDS)
		* Crawlable
		* Discovered by the Glue Crawler hcqa
		* Connection: hcqa

#### These correspond to the folders under https://github.com/harborcapital/hca-datalake/tree/dev/source-jobs/database 

* awd
	* DG_PWHARBO AWD Schema: all job files except the following
	* DG_PWHARBO BI Schema: shareholder_email_outbound

* hc: all Harbor Central extracts

* powerselect: all DST PowerSelect extracts

# CONNECTIONS

* (DST PowerSelect DB2) CFNConnectionDB2-4RTMEM6o15nb

	* (Connection) Type -- JDBC

	* JDBC URL -- jdbc:db2://PSDTO.DSTCORP.NET:50000/DTO

	* VPC Id -- vpc-0432fb205c0f2e19b (qa-vpc)

	* Subnet -- subnet-00fbad7b9bd4f189b (qa-subnet-sn1-private-2a)

	* Security groups
		* sg-097584663381aabdb (dst_etl_test):
			* inbound rules: All traffic source sg-07a2c4663bd7009e1, SSH (TCP port 22) 10.202.0.0/16
			* outbound rules: 0.0.0.0/0
		* sg-0a2804d8627a1e37d (default):
			* inbound rules: All traffic source)
			* outbound rules: 0.0.0.0/0

	* Require SSL connection	false

* (DST DG_PWHARBO/AWD Oracle) AWD

	* (Connection) Type -- JDBC

	* JDBC URL-- jdbc:oracle:thin://@170.40.30.137:1528/DG_PWHARBO  
	*Note: AWD was originally a DB2 database but was migrated to a schema in DG_PWHARBO which is an Oracle database server.*

	* VPC Id -- vpc-0432fb205c0f2e19b (qa-vpc)

	* Subnet -- subnet-00fbad7b9bd4f189b (qa-subnet-sn1-private-2a)

	* Security groups	
		* sg-097584663381aabdb (dst_etl_test):
			* inbound rules: All traffic source sg-07a2c4663bd7009e1, SSH (TCP port 22) 10.202.0.0/16
			* outbound rules: 0.0.0.0/0
		* sg-0a2804d8627a1e37d (default):
			* inbound rules: All traffic source)
			* outbound rules: 0.0.0.0/0

	* Require SSL connection	false

* (Harbor Central RDS) hcqa

	* (Connection) Type -- JDBC

	* JDBC URL -- jdbc:oracle:thin://@hcqa.c3t51dkwfeeh.us-east-2.rds.amazonaws.com:1521/HCQA

	* VPC Id -- vpc-0432fb205c0f2e19b (qa-vpc)

	* Subnet -- subnet-00fbad7b9bd4f189b (qa-subnet-sn1-private-2a)

	* Security groups
		* sg-057832d72ba12de75 (qa-glue):
			* inbound rules: All traffic source sg-057832d72ba12de75
			* outbound rules: 0.0.0.0/0

	* Require SSL connection	false

# JOBS

* DST PowerSelect Jobs

	* IAM role: glue_etl_permissions
		* Role ARN -- arn:aws:iam::398870982942:role/glue_etl_permissions 
		* Role description -- Allows Glue to call AWS services on your behalf
		* Permissions policies (8 AWS managed policies applied)
			* AmazonRDSFullAccess
			* SecretsManagerReadWrite
			* AWSLambdaFullAccess
			* AmazonS3FullAccess
			* AmazonAthenaFullAccess
			* AWSGlueServiceRole
			* AmazonRDSDataFullAccess
			* AmazonVPCFullAccess

	* Glue Version 1.0: Spark 2.4, Python 3

	* Required connections: CFNConnectionDB2-4RTMEM6o15nb

	* Dependent jars path: s3://aws-glue-jdbc-drivers-398870982942-us-east-2/admin/db2jcc-db2jcc4.jar

	* Referenced files path: s3://aws-glue-jdbc-drivers-398870982942-us-east-2/admin/db2jcc_license_cu.jar

* DST DG_PWHARBO AWD Jobs  
*Note: the AWD schema was discovered by the dst_awd crawler so the Glue Data Catalog was used.*

	* IAM role: glue_etl_permissions
		* Role ARN -- arn:aws:iam::398870982942:role/glue_etl_permissions 
		* Role description -- Allows Glue to call AWS services on your behalf
		* Permissions policies (8 AWS managed policies applied)
			* AmazonRDSFullAccess
			* SecretsManagerReadWrite
			* AWSLambdaFullAccess
			* AmazonS3FullAccess
			* AmazonAthenaFullAccess
			* AWSGlueServiceRole
			* AmazonRDSDataFullAccess
			* AmazonVPCFullAccess

	* Glue Version 1.0: Spark 2.4, Python 3

	* Required connections: AWD

* DST DG_PWHARBO BI Jobs  
*Note: the BI schema was not discovered by the dst_awd crawler so the Glue Data Catalog could not be used. Additionally, the OJDBC jar had to be supplied in the job configuration.*

	* IAM role: glue_etl_permissions
		* Role ARN -- arn:aws:iam::398870982942:role/glue_etl_permissions 
		* Role description -- Allows Glue to call AWS services on your behalf
		* Permissions policies (8 AWS managed policies applied)
			* AmazonRDSFullAccess
			* SecretsManagerReadWrite
			* AWSLambdaFullAccess
			* AmazonS3FullAccess
			* AmazonAthenaFullAccess
			* AWSGlueServiceRole
			* AmazonRDSDataFullAccess
			* AmazonVPCFullAccess

	* Glue Version 1.0: Spark 2.4, Python 3

	* Required connections: AWD

	* Dependent jars path: s3://aws-glue-jdbc-drivers-398870982942-us-east-2/oracle/ojdbc7.jar

# CRAWLERS

* hcqa  
*Note: RDS Oracle database*

	* Database -- hcqa
	* Service role -- QA_DSS_Glue_Access
	* Data store -- JDBC
	* Connection -- hcqa
	* Include path -- hcqa
	* Exclude paths -- (None)

* dst_awd  
*Note: Oracle database*
	* Database -- awd_full
	* Service role -- glue_etl_permissions
	* Data store -- JDBC
	* Connection -- AWD
	* Include path -- DG_PWHARBO/%
	* Exclude paths -- (None)
