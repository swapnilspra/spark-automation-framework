from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up

class Job(ETLJob):
    target_table = "security"
    business_key = ["asset_id"]
    primary_key = {"secr_key":"int"}
    load_strategy = ETLJob.LoadStrategy.TYPE2

    sources:Dict[str,Dict[str,Any]] = {
        "pta_ssb_position": {
            "type": "file",
            "source": "pta_ssb_position"
        },
        "harbcust": {
            "type": "file",
            "source": "harbcust"
        },
        "fund_composite": {
            "type": "table",
            "source": "fund_composite"
        },
        "bb_country_threshold": {
            "type": "file",
            "source": "bb_country_threshold"
        },
        "investment_type": {
            "type": "dimension",
            "source": "investment_type"
        },
        "state": {
            "type": "dimension",
            "source": "state"
        },
        "existing": {
            "type": "table",
            "source": "security"
        }
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "pta_ssb_position"
        },
        {
            "source": "investment_type",
            "conditions": [
                F.col("pta_ssb_position.invest_type_cd") == F.col("investment_type.invmt_type_cd")
            ],
            "type":"leftouter"
        },
        {
            "source": "state",
            "conditions": [
                F.col("pta_ssb_position.state_cd") == F.col("state.st_cd")
            ],
            "type":"leftouter"
        },

        {
            "source": "bb_country_threshold",
            "conditions": [
                F.col("bb_country_threshold.asset_id") == F.col("alt_asset_id1")
            ],
            "type":"leftouter"
        },
        {
            "source": "existing",
            "conditions": [
                F.col("existing.asset_id") == F.col("pta_ssb_position.asset_id"),
                F.col("existing.curr_row_flg") == F.lit("Y")
            ],
            "type":"leftouter"
        },
    ]
    # override extract to get distinct records
    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = super().transform(df_joined).distinct()
        # convert empty strings to null
        def blank_as_null(x):
            return F.when(F.col(x) != "", F.col(x)).otherwise(None)
        new_columns = [
            blank_as_null(x).alias(x) for x in df_transformed.columns]

        return df_transformed.select(*new_columns)

    target_mappings:List[Dict[str,Any]] = [
        {
            "target": "sedol_id",
            "source": F.coalesce(
                    F.when(F.col("alt_asset_id_type_cd1")==F.lit("SDL"),F.col("alt_asset_id1")
                    ).when(F.col("alt_asset_id_type_cd2")==F.lit("SDL"),F.col("alt_asset_id2")
                    ).otherwise(F.lit(None)),
                    F.col("existing.sedol_id")
            )
        },
        {
            "target": "isin_id",
            "source": F.when(F.col("alt_asset_id_type_cd1")==F.lit("ISN"),F.col("alt_asset_id1")
                        ).when(F.col("alt_asset_id_type_cd2")==F.lit("ISN"),F.col("alt_asset_id2")
                        ).otherwise(F.lit(None))
        },
        { "source": F.trim(F.coalesce(F.col("pta_ssb_position.ticker_symb"),F.col("existing.tckr_sym"))), "target": "tckr_sym" },
        # { "source": F.coalesce(
        #     F.trim(F.upper(F.col("bb_country_threshold.id_bb_global_company_name"))),
        #     F.coalesce(F.col("existing.secr_nm"),F.trim(F.col("pta_ssb_position.issue_long_nm")))
        # ), "target": "secr_nm" },
        { "source": F.trim(F.upper(F.coalesce(F.col("bb_country_threshold.issuer"),F.col("existing.issuer_nm")))), "target": "issuer_nm" },
        { "source": F.trim(F.upper(F.coalesce(F.col("bb_country_threshold.id_bb_global_company_name"),F.col("existing.glbl_issuer_nm")))), "target": "glbl_issuer_nm" },
        { "source": F.col("pta_ssb_position.invest_type_cd"), "target": "invmt_type_cd" },
        { "source": F.col("investment_type.invmt_type_desc"), "target": "invmt_type_desc" },
        { "source": F.col("pta_ssb_position.dtc_cusip"), "target": "dtc_cusip" },
        { "source": F.col("pta_ssb_position.asset_id"), "target": "asset_id" },
        { "source": F.col("pta_ssb_position.state_cd"), "target": "muncpl_secr_st_cd" },
        { "source": F.upper(F.col("state.st_nm")), "target": "muncpl_secr_st" },
        # { "source": F.lit(None).cast("int"), "target": "etl_load_cyc_key" },        
        # { "source": F.lit(4), "target": "src_sys_id" },

    ]

    def extract(self,catalog:Dict[str,Any]) -> Dict[str,pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)

        # get the entry from the catalog
        source:Dict[Any,Any] = catalog["bb_country_threshold"]
        file_location:str = common.utils.get_file_location(self._env["file_prefix"],source["path"])

        # read as text
        # then filter out the n lines to skip
        rdd = self._spark.read.text(file_location).rdd.zipWithIndex()

        # find index of START-OF-FILE,END-OF-FILE etc
        line_idx_so_file = rdd.filter(lambda line: line[0][0]=="START-OF-FILE").take(1)[0][1]
        line_idx_so_fields = rdd.filter(lambda line: line[0][0]=="START-OF-FIELDS").take(1)[0][1]
        line_idx_so_data = rdd.filter(lambda line: line[0][0]=="START-OF-DATA").take(1)[0][1]
        line_idx_eo_data = rdd.filter(lambda line: line[0][0]=="END-OF-DATA").take(1)[0][1]

        rdd_fieldnames = rdd.filter(lambda line: line[1]==line_idx_so_data+1).take(1)
        columns = rdd_fieldnames[0][0].value.split("|")
        # first row has the field names, skip that to get to the data
        rdd_data = rdd.filter(lambda line: line[1]>line_idx_so_data+1 and line[1]<line_idx_eo_data)

        bb_country_threshold = rdd_data.filter(lambda line: not line[0].value.startswith("#")).map(lambda line: line[0].value.split("|")).toDF(columns)
        bb_country_threshold = bb_country_threshold.withColumn("asset_id",F.split(F.col("securities")," ")[0])
        bb_country_threshold = bb_country_threshold.alias("bb_country_threshold")
        df_inputs["bb_country_threshold"] = bb_country_threshold

        #
        # deal with harbcust
        #

        # select only distinct harbcust records
        df_inputs["harbcust"] = df_inputs["harbcust"].select("asset_id", "alt_asset_id_type_cd1", "alt_asset_id1","invest_type_cd").distinct()
        df_inputs["harbcust"] = df_inputs["harbcust"].withColumn("invest_type_cd", F.lpad(F.col("invest_type_cd"),2,"0"))

        # take only records that don't currently exist and are not in the pta file
        df_inputs["harbcust"] = df_inputs["harbcust"].join(df_inputs["existing"],"asset_id","leftanti").join(df_inputs["pta_ssb_position"],"asset_id","leftanti")

        # filter only GB funds except GB35
        df_inputs["pta_ssb_position"] = df_inputs["pta_ssb_position"].filter(F.col("fund_id")!=F.lit("GB35"))
        df_inputs["pta_ssb_position"] = df_inputs["pta_ssb_position"].join(df_inputs["fund_composite"],[
                F.col("pta_ssb_position.fund_id")==F.col("fund_composite.st_str_fund_nbr")
        ]).select("pta_ssb_position.*")
        df_inputs["pta_ssb_position"] = df_inputs["pta_ssb_position"].withColumn("src_sys_id",F.lit(None))

        # add missing columns to harbcust so we can union
        missing_columns = set(map(lambda x: x.lower(),df_inputs["pta_ssb_position"].columns))-set(map(lambda x: x.lower(),df_inputs["harbcust"].columns))
        df_inputs["harbcust"] = df_inputs["harbcust"].select("*",*[F.lit(None).alias(colname) for colname in missing_columns])
        df_inputs["harbcust"] = df_inputs["harbcust"].withColumn("src_sys_id",F.lit(11))

        # union into the dataframe
        df_inputs["pta_ssb_position"] = common.utils.align_and_union(df_inputs["pta_ssb_position"],df_inputs["harbcust"])
        return df_inputs