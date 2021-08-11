from typing import Dict, List, Any
import pyspark.sql
from pyspark.sql import Window
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
import re
import itertools

class Job(ETLJob):
    # mapping between benchmark index id and a spliced index benchmark index (idx_id)
    SPLICED_INDEX_MAPPINGS = {
        "MXWDU": "ACWZSPL",
        "MLGVT13": "CITI_ML",
        "FR2VAL": "FR2XVAL"
    }


    target_table = "index_valuation"
    business_key = ["day_key","bmk_idx_key"]
    primary_key = {"idx_vltn_key":"int"}
    sources:Dict[str,Dict[str,Any]] = {
        "bloomberg_idx_data_ml": {
            "type": "file",
            "source": "bloomberg_idx_data_ml"
        },
        "bloomberg_idx_data_final": {
            "type": "file",
            "source": "bloomberg_idx_data_final"
        },
        "benchmark_index": {
            "type": "table",
            "source": "benchmark_index"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "previous_day_calendar": {
            "type": "table",
            "source": "calendar"
        },
        "previous_day_valuation": {
            "type": "table",
            "source": "index_valuation"
        },
    }
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("calendar.day_key"), "target": "day_key" },
        { "source": F.col("benchmark_index.bmk_idx_key"), "target": "bmk_idx_key" },
        { "source": F.col("bloomberg_idx_data_final.px_last_eod").cast("double"), "target": "idx_vltn_amt" },
        { "source": F.col("calculated_daily_ret"), "target": "daily_ret" },
        { "source": F.col("calculated_wlth_idx"), "target": "wlth_idx" },
        { "source": F.lit("Y"), "target": "curr_row_flg" },
        { "source": F.lit(4), "target": "src_sys_id" },
        { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key" },        
    ]
    joins:List[Dict[str,Any]] = [
        {
            "source": "bloomberg_idx_data_final"
        },
        {
            "source": "calendar",
            "conditions": [
                F.col("calendar.cal_day") == F.col("bloomberg_idx_data_final.rundate")
            ]
        },
        {
            "source": "benchmark_index",
            "conditions": [
                F.col("benchmark_index.idx_id") == F.col("bloomberg_idx_data_final.index_name") # we translate the index name to the idx_id in the overriden join method below to add the spliced indexes
            ]
        },
        # load previous day valuation. this is in two parts - join with calendar to lookup the day key then join by day key
        {
            "source": "previous_day_calendar",
            "conditions": [
                F.col("previous_day_calendar.cal_day")==F.to_date(F.date_sub(F.col("bloomberg_idx_data_final.rundate"),1))
            ]
        },
        {
            "source": "previous_day_valuation",
            "conditions": [
                F.col("previous_day_calendar.day_key")==F.col("previous_day_valuation.day_key"),
                F.col("benchmark_index.bmk_idx_key")==F.col("previous_day_valuation.bmk_idx_key")
            ],
            "type":"left_outer"
        },
    ]

    #
    # override extract to read the special bloomberg files
    #
    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)
        # get the entry from the catalog
        source:Dict[Any,Any] = catalog["bloomberg_idx_data_ml"]
        file_location:str = common.utils.get_file_location(self._env["file_prefix"],source["path"])

        df_bloomberg_idx_data_ml = self._spark.read.text(file_location).rdd.filter(
                lambda line: not line.value.startswith("START SECURITY") and not line.value.startswith("END SECURITY")
            ).map(
                lambda line: [s for s in line.value.split("|") if not s=='']
            ).toDF(['index','px_date','px_last'])
        # the index name column has an extra ' Index' suffix. remove it.
        df_bloomberg_idx_data_ml = df_bloomberg_idx_data_ml.withColumn("index",F.substring_index(F.col("index"),' ',1))
        df_inputs["bloomberg_idx_data_ml"] = df_bloomberg_idx_data_ml.alias("bloomberg_idx_data_ml")

        # get the entry from the catalog
        source = catalog["bloomberg_idx_data_final"]
        file_location = common.utils.get_file_location(self._env["file_prefix"],source["path"])

        df_inputs["bloomberg_idx_data_final"] = self.read_bloomberg(file_location).select("rundate","index_name","px_last_eod").alias("bloomberg_idx_data_final")

        return df_inputs

    #
    # override to add forward filling
    #
    def join(self,inputs:Dict[str,pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:
        # append the 'data-ml' file to 'data-final' and fix the dates
        inputs["bloomberg_idx_data_final"] = inputs["bloomberg_idx_data_final"].select(
                F.date_sub(F.to_date("rundate","yyyyMMdd"),1).alias("rundate"),"index_name","px_last_eod"
            ).union(
                inputs["bloomberg_idx_data_ml"].select(F.to_date(F.col("px_date"),"MM/dd/yyyy"),"index","px_last")
            )

        # take only last day value
        w = Window.partitionBy('index_name')
        inputs["bloomberg_idx_data_final"] = inputs["bloomberg_idx_data_final"].withColumn('max_date', F.max('rundate').over(w))\
            .where(F.col('rundate') == F.col('max_date'))\
            .drop('max_date')
        inputs["bloomberg_idx_data_final"] = inputs["bloomberg_idx_data_final"].alias("bloomberg_idx_data_final")

        # we translate the bloomberg symbols to those of benchmark index id and duplicate the 3 spliced index funds.
        # we do this by creating a map and add the entries that have a result of the map
        translate_index_name_map = F.create_map([F.lit(index_name) for index_name in itertools.chain(*self.SPLICED_INDEX_MAPPINGS.items())])
        inputs["bloomberg_idx_data_final"] = inputs["bloomberg_idx_data_final"]\
            .join(inputs["benchmark_index"],[F.col("index_name")==F.col("bloomberg_sym")])\
            .select("bloomberg_idx_data_final.*","benchmark_index.idx_id",translate_index_name_map.getItem(F.col("benchmark_index.idx_id")).alias("spliced_index") )
        
        # add the records that have a spliced_index as new records and use the spliced index as the index_name
        inputs["bloomberg_idx_data_final"] = inputs["bloomberg_idx_data_final"].withColumn("index_name",F.col("idx_id")).union(
            inputs["bloomberg_idx_data_final"].where(~F.isnull(F.col("spliced_index"))).withColumn("index_name",F.col("spliced_index"))
        )

        inputs["bloomberg_idx_data_final"] = inputs["bloomberg_idx_data_final"].alias("bloomberg_idx_data_final")
        return super().join(inputs)

    # override transform to calculate wealth index
    def transform(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_transformed = self.calculate_wealth_index(df_joined)
        
        df_transformed = super().transform(df_transformed)

        # forward fill
        df_calendar = common.utils.read_table_snapshot(
                    table_name="calendar",
                    env=self._env,
                    spark=self._spark)
        df_ffill = common.utils.forward_fill(df_calendar,df_transformed,"day_key",
            override_values={"daily_ret":F.lit(0)})

        return df_ffill

    def calculate_wealth_index(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        #
        # calculate daily_pr_diff
        #
        # if no previous day exists, should return 0
        df_transformed = df_joined
        
        # calculate daily return. previously given with this SQL:
        # idx_val_amt / (select idx_val_amt from index_valuation where curr_row_flg='Y' and  idx_key = b.idx_key and vltn_dt = b.vltn_dt - 1) - 1  DAILY_RET
        # FROM benchmark_index a, index_valuation b
        # WHERE a.idx_key = b.idx_key AND A.CURR_ROW_FLG='Y' AND B.CURR_ROW_FLG='Y'

        df_transformed = df_transformed.withColumn(
            "calculated_daily_ret", 
            (F.col("bloomberg_idx_data_final.px_last_eod").cast("double") / F.col("previous_day_valuation.idx_vltn_amt").cast("double")) - 1
        )

        # calculate wealth index. previously given by this SQL:
        # IIF(ISNULL(PREV_WLTH_IDX),1,PREV_WLTH_IDX)
        # 1*PREV_WLTH_IDX*(DAILY_RET + 1)

        df_transformed = df_transformed.withColumn(
            "calculated_wlth_idx",
            F.when(F.isnull(F.col("previous_day_valuation.wlth_idx")),F.lit(1)).otherwise(
                F.col("previous_day_valuation.wlth_idx").cast("double") * 
                (F.col("calculated_daily_ret") + 1)
            )
        )

        return df_transformed

    def read_bloomberg(self,file_location:str) -> pyspark.sql.DataFrame:
        # returns dataframe
        #
        # the bloomberg file has a header indication of:
        #  START-OF-FILE
        #  field=value
        #  START-OF-FIELDS
        #  START-OF-DATA
        #  END-OF-DATA

        # read as text
        # then filter out the n lines to skip
        rdd = self._spark.read.text(file_location).rdd.zipWithIndex()

        # find index of START-OF-FILE,END-OF-FILE etc
        line_idx_so_file = rdd.filter(lambda line: line[0][0]=="START-OF-FILE").take(1)[0][1]
        line_idx_so_fields = rdd.filter(lambda line: line[0][0]=="START-OF-FIELDS").take(1)[0][1]
        line_idx_so_data = rdd.filter(lambda line: line[0][0]=="START-OF-DATA").take(1)[0][1]
        line_idx_eo_data = rdd.filter(lambda line: line[0][0]=="END-OF-DATA").take(1)[0][1]

        # read the headers as header,value dataframe
        # we filter by line number and also filter out comment lines and blank lines
        file_description:Dict[str,str] = rdd.filter(lambda line: line[1]>line_idx_so_file and line[1]<line_idx_so_fields and not line[0].value=='' and not line[0].value.startswith("#")).map(lambda line: line[0].value.split("=")).collectAsMap()

        # read and parse the main body by delimiter of space
        fields = [
            'index_name',
            'type',
            'unknown1',
            'unknown2',
            'px_open',
            'px_last_eod',
            'px_high',
            'px_low'
        ]
        line_idx_so_data = rdd.filter(lambda line: line[0][0]=="START-OF-DATA").take(1)[0][1]
        line_idx_eo_data = rdd.filter(lambda line: line[0][0]=="END-OF-DATA").take(1)[0][1]
        NA_STR = "N.A."
        # we pad N.A. with a space since in certain cases there is no space delimiter between number and N.A. string
        rdd_data = rdd.filter(lambda line: line[1]>line_idx_so_data and line[1]<line_idx_eo_data).map(
            lambda line: [x for x in re.split(' +',line[0].value.replace(NA_STR,' '+NA_STR+' ')) if not x=='']
        )
        # add the descriptors as columns
        df_data:pyspark.sql.DataFrame = rdd_data.toDF(fields)
        for key,value in file_description.items():
            df_data = df_data.withColumn(key,F.lit(value))

        return df_data.persist() # we use persist to avoid timing issue since we collectAsMap earlier
