import sys
sys.path.insert(0, ".")

from pyspark.sql import SparkSession
import utils
spark = SparkSession \
    .builder \
    .appName("etl") \
    .getOrCreate()
catalog = utils.get_catalog()
# source = catalog["DTO.HCA.BSK.TIP88"]
# source = catalog["DTO.HCA.CLO.TIP42"]
# source = catalog["DTO.HCA.SDCM.POSITION"]
# # file_location = "/home/oren/Downloads/DTO.HCA.CLO.TIP42.20191028.233259.zip.pgp.txt"
# file_location = "/home/oren/Downloads/DTO.HCA.SDCM.POSITION.20200305.085940.zip.pgp.txt.redacted"
# # file_location = "/home/oren/Downloads/DTO.HCA.BSK.TIP88.20190719.224936.zip.pgp.txt"
source = catalog["R00857"]
file_location = "/home/oren/Downloads/DTO.HCA.MTF.R00857.20200212.102023.zip.pgp.txt.redacted"


import parsers
import copybook

df =parsers.read_cobol_file(
    spark,
    file_location=file_location,
    copybook_location=source.get("copybook",None),
    row_prefix=source.get("row_prefix",None), # if multiline, specify start of new row
    multiline=source.get("multiline",False), # if has a row prefix this is multiline file
    record_selector_field=source.get("record_selector_field",None),
    record_types=source.get("record_types",None),
    header_lines=source.get("skip_header_rows",0),
    footer_lines=source.get("skip_footer_rows",0)
)

df.show()


