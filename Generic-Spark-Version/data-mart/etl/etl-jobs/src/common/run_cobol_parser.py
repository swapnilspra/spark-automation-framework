import sys
sys.path.insert(0, "../")

from pyspark.sql import SparkSession
import common.utils
spark = SparkSession \
    .builder \
    .appName("etl") \
    .getOrCreate()
catalog = common.utils.get_catalog()
# source = catalog["DTO.HCA.BSK.TIP88"]
source = catalog["DTO.HCA.CLO.TIP42"]

file_location = "/home/oren/Downloads/DTO.HCA.CLO.TIP42.20191028.233259.zip.pgp.txt"

# file_location = "/home/oren/Downloads/DTO.HCA.BSK.TIP88.20190719.224936.zip.pgp.txt"

import parsers
import copybook

df =parsers.read_cobol_file(
    spark,
    file_location=file_location,
#     copybook_location="BSK.TIP88.CBL",
#     header_lines=2,
#     record_types=record_types,
#     record_selector_field=record_type_field,
#     multiline=True,
#     row_prefix="1",
#     footer_lines=1)
    copybook_location=source.get("copybook",None),
    row_prefix=source.get("row_prefix",None), # if multiline, specify start of new row
    multiline=source.get("multiline",False), # if has a row prefix this is multiline file
    record_selector_field=source.get("record_selector_field",None),
    record_types=source.get("record_types",None),
    header_lines=source.get("skip_header_rows",0),
    footer_lines=source.get("skip_footer_rows",0)
)

df.show()

