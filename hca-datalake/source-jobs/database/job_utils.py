import os
import boto3
from datetime import datetime
from urllib.parse import urlparse
import base64



def get_s3_tmp(bucket:str, tmp_path:str):
    return f"s3://{os.path.join(bucket, tmp_path, datetime.now().strftime('%Y%m%d%H%M%S'))}"


def move_to_incoming(tmp_uri:str, s3_dest_path:str, tagging:str):
    source_uri = urlparse(tmp_uri)
    bucket = source_uri.netloc
    source_prefix = source_uri.path[1:]

    s3 = boto3.resource('s3')

    print(f"SOURCEPREFIX ===> {source_prefix}")
    source_keys = s3.Bucket(bucket).objects.filter(Prefix=source_prefix)
    source_key = next(iter(source_keys))

    print(f"copying from [{source_key.key}] => [{s3_dest_path}]")
    s3.Object(bucket, s3_dest_path).copy_from(
        CopySource={
            'Bucket': bucket,
            'Key': source_key.key
        },
        TaggingDirective='REPLACE',
        Tagging=tagging)


def get_db_connection_from_glue_job(job_name:str, aws_region:str):

    # get jdbc connection from the glue catalog
    client = boto3.client('glue', region_name=aws_region)
    glue_job = client.get_job(JobName=job_name)['Job']
    db_name = next(iter(glue_job['Connections']['Connections'])) if 'Connections' in glue_job else None

    # for now lets assume every job has a db connection
    db_connection = client.get_connection(Name=db_name)
    cn_props = db_connection['Connection']['ConnectionProperties']

    # parse db props
    username = cn_props['USERNAME']
    jdbc_url = cn_props['JDBC_CONNECTION_URL']
    jdbc_driver_class = cn_props['JDBC_DRIVER_CLASS_NAME']
    ecrypted_db_password = cn_props['ENCRYPTED_PASSWORD']

    # decrypt password with kms
    kms = boto3.client('kms', region_name=aws_region)
    db_password = kms.decrypt(CiphertextBlob=bytes(base64.b64decode(ecrypted_db_password)))['Plaintext'].decode('utf-8')

    return jdbc_url, username, db_password, jdbc_driver_class

##########################################
#
# DATA SCRUBBING UTILITY FUNCTIONS
#
##########################################

# Fields in AWD (which was migrated from DB2 to Oracle) and PowerSelect (DB2) have many fields declared as CHARs.
# These fields cannot be NULL so if nothing is present, the field is returned as a string of spaces of length equal to the CHAR size.
# Thus, if a field is declared as CHAR(8) and a cell is "empty", a query will receive "        " (without the quotes.
# The issue is that PySpark/Glue will then write this as \"\" in the output, which is problematic.
# The solution is to capture these empty (whitespace) strings and return a Python None value which results in PySpark/Glue writing "" to the CSV.

def is_empty_string(s):
    if s.isspace():
        return None
    else:
        return s

# PySpark returns fields in scientific notation that are decimal and that have the value zero (0.0).
# For instance, if a field is Data Type Number with Precision 38 and Scale 15 in Oracle which Glue will call Decimal(38,15) 
# in the Glue Data Catalog, then a 0 (zero) in this field will be written as 0E-15 in the CSV output.
# A goal is to make the CSV output as basic and agnostic as possible so that any scripting or other processing language
# can read the files. As such, the following functions identify if a value is numeric and, if equal to zero, will write it out
# as a literal value. Python recognizes expressions like 0E-6 as being equal to 0.000000 so that the script writes that explicit value
# to the CSV file. The decision was made to use a scale of 12 overall since nothing is lost (i.e., the values are already zero) and
# the CSV output represents a serialized and type-free form of the data.
#
# The function is_number is used to test a value and is called by cast_string; the function cast_string is used in the definition of the UDF.

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def cast_string(x):
# check if x is None or empty string: return whichever it is
    if not x and x != 0:
        return x
# check if x comes in like 0E-10
    elif x == 0:
        return f'{0:.12f}'
# convert x to a "decimal string"
    elif is_number(x) and x != 0:
        return f'{x:.12f}'
    else:
        return x
