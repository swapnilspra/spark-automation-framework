# Installation using Vagrant (short)

## Install git

## Install virtualbox and vagrant
https://www.vagrantup.com/docs/installation/

## Run Vagranfile
Clone the HCA repository:

    $ git clone https://github.com/harborcapital/hca-datalake.git

In case you are running behind a proxy, use the following to set up the proxy:
For Mac/Linux (in Bash)
```
export http_proxy="http://user:password@host:port"
export https_proxy="http://user:password@host:port"
vagrant plugin install vagrant-proxyconf
```
then
```
export VAGRANT_HTTP_PROXY=${http_proxy}
export VAGRANT_NO_PROXY="127.0.0.1"
```

For Windows use set instead of export.
```
set http_proxy=http://user:password@host:port
set https_proxy=https://user:password@host:port
vagrant plugin install vagrant-proxyconf
```
then

```
set VAGRANT_HTTP_PROXY=%http_proxy%
set VAGRANT_HTTPS_PROXY=%https_proxy%
set VAGRANT_NO_PROXY="127.0.0.1"
```

Run the vagrant file

    $ cd hca-datalake
    $ vagrant up

Get a cup of coffee... or two...

To login to the Virtualbox:

Once done, login to the Virtaulbox

    $ vagrant ssh

Then continue to [Setup](#setup) below.


# Installation from scratch (long)

The following are needed as pre-requisites for a full local environment:

## Python

You need Python 3.6.

In Ubuntu, Mint and Debian you can install Python 3 like this:

    $ sudo apt-get install python3.6 python3-pip

For other Linux flavors, macOS and Windows, packages are available at

  http://www.python.org/getit/

## Download and Install OpenJDK 8
* Execute the following commands to download and install OpenJDK 8:
    ```
    sudo apt-get update
    sudo apt-get install openjdk-8-jdk
    ```
* Once installed, open your .bashrc file in the nano text editor by executing the following command:
    ```
    nano ~/.bashrc
    ```
* Scroll to the bottom using the down arrow and paste the following to define the JAVA_HOME environment variable. Press *Ctrl+x*, *Shift+y*, and *Enter* to save:
    ```
    export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
    export PATH=$PATH:$JAVA_HOME/bin
    ```

## PySpark

    $ pip3 install --user pyspark==2.4.3 --no-cache-dir 

* Once installed, open your .bashrc file in the nano text editor by executing the following command:
    ```
    nano ~/.bashrc
    ```

* Scroll to the bottom using the down arrow and paste the following to define the spark environment variables. Press *Ctrl+x*, *Shift+y*, and *Enter* to save:
    ```
    export PYSPARK_PYTHON=python3
    export SPARK_HOME=~/.local/lib/python3.6/site-packages/pyspark
    export PATH=$PATH:$SPARK_HOME/bin
    ```

Test your installation by running:

    $ pyspark

If all is well, you should see the PySpark command prompt:
```
Python 3.6.8 (default, Aug 20 2019, 17:12:48) 
[GCC 8.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
19/11/11 12:58:20 WARN Utils: Your hostname, oren-dell resolves to a loopback address: 127.0.1.1; using 10.0.0.33 instead (on interface enp0s31f6)
19/11/11 12:58:20 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
19/11/11 12:58:20 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
19/11/11 12:58:21 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.3
      /_/

Using Python version 3.6.8 (default, Aug 20 2019 17:12:48)
SparkSession available as 'spark'.
>>> 
```

## Install Postgresql


## Download HCA repository
Clone the HCA repo

    $ cd ~
    $ git clone https://github.com/harborcapital/hca-datalake.git

For convenience, create a symlink to the inner folder of the actual code

    $ ln -s hca-datalake/data-mart/etl/etl-jobs/ ~/etl_jobs

## Create a python virtualenv
    $ cd ~/etl_jobs
    $ virtualenv --python=python3.6 venv

Activate the virtual env:

    $ source ./venv/bin/activate

Install dependencies:

    $ pip install -r requirements.txt

## Download Postgres JDBC driver

Create a lib folder (at root)
Download postgresql jar file from https://jdbc.postgresql.org/download/postgresql-42.2.8.jar and place within lib folder

    $ mkdir lib
    $ cd lib
    $ wget https://jdbc.postgresql.org/download/postgresql-42.2.8.jar

## Update environment.ini

Create scripts/environment.ini (or copy sample file scripts/environment.local.ini)

Update credentials as needed in scripts/environment.ini


# Setup

# Using the Job Framework

## Running a job locally
```
$ cd ~/etl_jobs
$ source ./venv/bin/activate
$ cd scripts
(venv) vagrant@ubuntu-bionic:~/etl_jobs/scripts$ ./local_job_runner --help
Usage: local_job_runner [OPTIONS] JOB_NAME

Options:
  --schema TEXT    DB schema of the target table  [required]
  --datalake TEXT  'local' to use job data folder; 'global' to use project
                   data folder
  --loglevel TEXT  DEBUG/INFO/ERROR/WARNING. default INFO
  --help           Show this message and exit.

```

Example: Running the fund_composite job under src/jobs/fund_composite:

* Place a fund_composite.csv file in ~/hca/data/raw

```
$ cd ~/etl_jobs
$ source ./venv/bin/activate
$ cd scripts
./local_job_runner fund_composite --schema hdm
```

Example: Running the fund_composite job under src/jobs/fund/fund_composite:
```
$ cd ~/etl_jobs
$ source ./venv/bin/activate
$ cd scripts
./local_job_runner fund.fund_composite --schema hdm
```

If all goes well, the data should be loaded into the DB and a new parquet file should be created at `~/hca-data/mart`

## Running an integration test
The integration test tool uses a 'before' and 'after' folder that contains a point-in-time dump of the database.

The structure of the folder should be as follows:

`before` - sub folder containing csv files of the dependent tables. Each csv file should contain one table and be named as <table_name>.csv

`after` - sub folder containing a csv dump of the target table after with the expected state after the ETL has completed

`raw` - sub folder containing input files needed by the job

To run the test, use the hca_cli within the `scripts` folder:

```
(venv) vagrant@ubuntu-bionic:~/etl-jobs/scripts$ ./hca_cli test --help
Usage: hca_cli test [OPTIONS]

Options:
  --jobname TEXT      job name to test. If blank and test.ini file exists,
                      will be taken from ini file
  --datafolder TEXT   location of 'before' 'after' folders  [required]
  --ignore-cols TEXT  comma delimited list of columns to ignore for comparison
                      purposes. If blank and test.ini file exists, will be
                      taken from ini file
  --help              Show this message and exit.
```

### test.ini file
For automation purposes, a special test.ini file can be placed in the root of the test folder. It contains the following fields:

jobname - job to activate (similar to --jobname command line argument)

ignore-cols - columns to ignore for comparison (similar to --ignore-cols command line arguments)

Example test.ini for `fund_valuation`:

```
[default]
jobname=fund_valuation
ignore_cols=asof_wlth_idx
```

Running the test tool with a single --datafolder parameter will look for the test.ini file and use it's contents. In the future, this will enable to run all folders as part of a test automation script for regression testing purposes.


## Creating a new job
Jobs are located under `src/jobs/[Job Name]` folder. Similar jobs can be ground and placed under a folder. For example, let's assume you want to put jobs for ODS schema into a specific folder. Then, we can create a folder `src/jobs/ods/[Job Name]` and place all ODS related jobs there. 

For a job reading a csv file, there is a script, gen_job, to generate a job.py template. It requires csv file location, target table name, and file path for creating job.py.

This generator does following things:
 * add required imports
 * add sources with the given table name
 * add sample commented joins
 * add target mappings based on columns in csv header
 * add comented function definitions; extract and transform

This generator does not do following things:
 * do not update catalog files
 * do not create a folder for job.py
 * do not populate target fields for each source field

Example: Generate a job.py under `src/jobs/ods/hsg_project_document` for target table `hsg_project_document`, using `/data/raw/Project_Documents20200225152236.csv` 

```
$ cd ~/etl_jobs
$ source ./venv/bin/activate
$ cd scripts
$ mkdir -p ../src/jobs/ods/hsg_project_document
$ ./gen_job --csv /data/raw/Project_Documents20200225152236.csv --tbl hsg_project_document --path ../src/jobs/ods/hsg_project_document
```

## Defining a new raw file source in the catalog

## Testing

## Building

# Job Framework CLI

## Redact tool
`./hca_cli redact`

To use, first create the entry in the catalog.json. The tool references the entry and parses the COBOL generated file based on the copybook mappings. Currently supports two operations:

- Redact - Change values to XXX
- Encode - use a hashing function to generate a numeric fixed-width hash

