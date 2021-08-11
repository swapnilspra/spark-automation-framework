import sys
sys.path.insert(0, "src/")

import pkgutil
import common.parsers
from common.copybook import CopyBook
# cbook = "CLO.TIP42.CUST_POSITION.CBL"
cbook = "SDCM.POSITION.CBL"
copybook_data:str = pkgutil.get_data("metadata.copybooks",cbook).decode("utf-8") # type: ignore
copybook_lines = [line.replace("\n","").strip()+"." for line in copybook_data.split(".")]
copybook_spec = CopyBook.parse(copybook_lines) 

from pyspark.sql import SparkSession
import common.utils
spark = SparkSession \
    .builder \
    .appName("etl") \
    .getOrCreate()

file_location = "/home/oren/Downloads/DTO.HCA.SDCM.POSITION.20200224.083056.zip.pgp.txt.redacted"
common.parsers.read_cobol_file(
    spark,
    file_location=file_location,
    copybook_location=cbook,
    header_lines=1,
    footer_lines=1,
    trim=False)

