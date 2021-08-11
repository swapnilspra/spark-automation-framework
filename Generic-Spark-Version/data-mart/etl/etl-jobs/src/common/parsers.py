from common.copybook import CopyBook, Field
import pyspark.sql
import pyspark.sql.types as T
import pkgutil
from pyspark.sql.types import ArrayType, StringType, MapType, StructField, StructType, FloatType
from typing import Dict, List, Any, Tuple, Pattern, Match, Optional
from pyspark.sql import functions as F
import re
import logging

def read_cobol_file(
    spark,
    file_location:str,
    multiline=False,
    row_prefix=None,
    copybook_location:str=None,
    record_types:Dict[str,str] = {},
    record_selector_field:str=None,
    header_lines:int=0,
    footer_lines:int=0,
    trim:bool=False,
    use_header:bool=False):

    if multiline:
        # we parse the line based on row prefix. will then get reparsed by newline
        # this effects the reading of textFile into rdd below
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        conf.set("textinputformat.record.delimiter", "\n"+row_prefix )

    rdd = spark.sparkContext.textFile(file_location)\
            .zipWithIndex().filter(lambda line_index: line_index[1]>=header_lines)
    # if we need to filter footer, we also need to figure out line count. then filter again
    if (footer_lines>0):
        line_count = rdd.count()
        rdd = rdd.filter(lambda line_index: line_index[1]<=line_count-footer_lines)

    if multiline:
        # add the row prefix back
        rdd = rdd.map(lambda x: (row_prefix+x[0],x[1]))
    # take only the content. the other part of the tuple is the index
    rdd = rdd.map(lambda x: x[0])
    return read_cobol_file_rdd(
        spark,
        rdd,
        multiline,
        copybook_location,
        record_types,
        record_selector_field,
        use_header,
        trim
    )

def read_cobol_file_rdd(
    spark,
    rdd,
    multiline=False,
    copybook_location:str=None,
    record_types:Dict[str,str] = {},
    record_selector_field:str=None,
    use_header:bool=False,
    trim:bool=False):

    if (copybook_location is not None):
        # load the definition from the copybook
        copybook_contents:str = pkgutil.get_data("metadata.copybooks",copybook_location).decode("utf-8") # type: ignore
        copybook_spec = CopyBook.parse(copybook_contents)

    else:
        raise ValueError("can't parse cobol file. no copybook specified and no column specs in catalog")

    # load the file
    field_list:List[Field] = CopyBook.get_tree(copybook_spec.root)

    return_type = StructType(
        [StructField(field.name,StringType()) for field in field_list]
    )
    if record_selector_field:
        def parse_cobol_multiline(fixed_fields,record_types,record_type_fields,record_type_field_attr,rawline):
            record = {}
            lines = rawline.split("\n")
            # first parse any fields not in a group
            for field in fixed_fields:
                record[field["name"]] = lines[0][field["offset"]:field["offset"]+field["length"]]

            # read each line and parse according to record type mapping
            for line in lines:
                if len(line)==0:
                    continue
                record_type = line[record_type_field_attr["offset"]:record_type_field_attr["offset"]+record_type_field_attr["length"]]
                # extract the fields based on positions
                if record_type not in record_types:
                    # unknown record type. skip it
                    continue

                for field in record_type_fields[record_types[record_type]]:
                    record[field["name"]] = line[field["offset"]:field["offset"]+field["length"]]
            return record

        # pyspark does not handle objects well. create a list of the fixed field offsets
        fixed_field_offsets = [{
            "name":field.name,
            "offset":field.offset,
            "length":field.length
        } for field in copybook_spec.root._children if field.datatype!='group']

        # pyspark does not handle objects well. create a dictionary of the sections (redefines) with the field tree for each
        record_type_fields = {}
        for key,fieldname in record_types.items():
            field = copybook_spec.fields[fieldname]
            offsets = [{
                "name":field.name,
                "offset":field.offset,
                "length":field.length
            } for field in CopyBook.get_tree(field)]
            record_type_fields[field.name] = offsets

        record_type_field_attr = {
            "name":copybook_spec.fields[record_selector_field].name,
            "offset":copybook_spec.fields[record_selector_field].offset,
            "length":copybook_spec.fields[record_selector_field].length
        }
        # parse the header if it needs to be duplicated
        header = parse_cobol_multiline(fixed_field_offsets,record_types,record_type_fields,record_type_field_attr,rdd.first())
        rdd = rdd.zipWithIndex().filter(lambda line: line[1]>=1).map(lambda x: x[0])
        # parse and create df
        # we parse each line and if requested, append the header info to each row using dict merging with **
        df = spark.createDataFrame(
            rdd.map(lambda line: {**header,**parse_cobol_multiline(fixed_field_offsets,record_types,record_type_fields,record_type_field_attr,line)}),
            return_type)
    else:
        def parse_cobol_line(offsets,line):
            record = {}
            # extract the fields based on positions
            for field in offsets:
                record[field["name"]] = line[field["offset"]:field["offset"]+field["length"]]
            return record

        # pyspark does not handle objects well. create a list of the field offsets
        offsets = [{
            "name":field.name,
            "offset":field.offset,
            "length":field.length
        } for field in field_list]

        # parse and create df
        df = spark.createDataFrame(
            rdd.map(lambda x: parse_cobol_line(offsets,x)),
            return_type)

    @F.udf(T.DoubleType())
    def fixedstring_to_float(val,length,precision,signed):
        if val is None or val.strip()=='':
            return None
        else:
            numeric_length = length
 
            if signed=='leading':
                numeric_val = val[1:]
                sign = val[0]
                # remove one spot for sign
                numeric_length -= 1
            elif signed=='trailing':
                numeric_val = val[0:-1]
                sign = val[-1]
                # remove one spot for sign
                numeric_length -= 1
            else:
                numeric_val = val
                sign = '+'

            try:
                return float(sign+numeric_val[0:numeric_length-precision]+("." if "." not in numeric_val else "")+numeric_val[numeric_length-precision:])
            except ValueError as ve:
                print(ve)
                return None

    # convert all the numeric items
    for field in field_list:
        # unpack 
        if field.datatype=="int":
            df = df.withColumn(field.name,F.col(field.name).cast(T.LongType())) # need Long to cater for 64bit int
        elif field.datatype=="float":
            df = df.withColumn(field.name,fixedstring_to_float(F.col(field.name),F.lit(field.length),F.lit(field.precision),F.lit(field.signed)))
        else:
            # character field. check if we need to trim
            if trim:
                df = df.withColumn(field.name,F.trim(F.col(field.name)))
                df = df.withColumn(field.name, F.when(F.col(field.name)=='',F.lit(None).cast("string")).otherwise(F.col(field.name)))

    return df

def read_csv_remove_header_footer(spark,file_location,dataframe_reader,header_lines,footer_lines=0) -> pyspark.sql.DataFrame:
    # returns dataframe
    # usage example:
    #  file_location = '../src/jobs/fund_composite/tests/data/fund/fund_composite.csv'
    # reader = spark.read.option("inferSchema", "true").option("header", "true")
    # read_csv_skip_lines(file_location,2,reader)
    #

    # read as text
    # then filter out the n lines to skip
    rdd = spark.read.text(file_location).rdd\
        .zipWithIndex().filter(lambda line_index: line_index[1]>=header_lines)
    # if we need to filter footer, we also need to figure out line count. then filter again
    if (footer_lines>0):
        line_count = rdd.count()
        rdd = rdd.filter(lambda line_index: line_index[1]<=line_count-footer_lines)
    rdd = rdd.map(lambda row: row[0].value)
    # parse as csv
    return dataframe_reader.csv(rdd)



