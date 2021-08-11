from pyspark.sql.types import ArrayType, StringType, MapType, StructField, StructType, FloatType
from typing import Dict, List, Any
import math
import logging
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
from datetime import datetime
class Job(ETLJob):
    target_table = "broker_position"
    business_key = ["brkr_key", "ser_nbr"]
    primary_key = {"brkr_posn_key": "int"}
    sources:Dict[str, Dict[str, Any]] = {
        "broker_position": {
            "type": "file",
            "source": "broker_position"
        },
        "broker": {
            "type": "table",
            "source": "broker"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "position_type": {
            "type": "dimension",
            "source": "position_type"
        }
    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "broker_position"
        },
        {
            "source": "calendar",
            "conditions": [
                F.to_timestamp("broker_position.Date","MM/dd/yyyy") == F.col("calendar.cal_day")
    ]
        },
        {
            "source": "position_type",
            "conditions": [
                F.col("broker_position.Position Types") == F.col("position_type.posn_cd")
    ]
        },
        {
            "source": "broker",
            "conditions": [
                F.col("broker_position.Fund ID") == F.col("broker.st_str_nbr"),
                F.col("broker_position.Broker Name") == F.col("broker.brkr_nm"),
                F.col("position_type.posn_type_key") == F.col("broker.posn_type_key")

    ]
        }


    ]
    target_mappings: List[Dict[str, Any]] = [
        {"source": F.col("broker.brkr_key"), "target": "brkr_key"},
        {"source": F.col("calendar.day_key"), "target": "day_key"},
        {"source": F.col("broker_position.Serial Number"), "target": "ser_nbr"},
        {"source": F.col("broker_position.Subadvisor Fund ID"), "target": "sub_advsr_fund_id"},
        {"source": F.col("broker_position.Asset ID or CUSIP"), "target": "asset_id"},
        {"source": F.col("broker_position.LX ID or Bloomberg ID or LN ID"), "target": "thrd_party_id"},
        {"source": F.col("broker_position.Other Identifiers"), "target": "othr_id"},
        {"source": F.col("broker_position.Trade ID"), "target": "trde_id"},
        {"source": F.col("broker_position.Asset Description"), "target": "asset_desc"},
        {"source": F.col("broker_position.Products"), "target": "prod_nm"},
        {"source": F.col("broker_position.Quantity"), "target": "asset_qty"},
        {"source": F.to_timestamp("broker_position.Trade Date","MM/dd/yyyy"), "target": "trde_dt"},
        {"source": F.to_timestamp("broker_position.Settle Date","MM/dd/yyyy"), "target": "sttl_dt"},
        {"source": F.col("broker_position.Purchase or Sale"), "target": "txn_type"},
        {"source": F.lit(None).cast("int"), "target": "purch_pr"},
        {"source": F.lit(None).cast("int"), "target": "vend_pr"},
        {"source": F.lit(None).cast("int"), "target": "mkt_val"},
        {"source": F.col("broker_position.Coupon"), "target": "coupn"},
        {"source": F.to_timestamp("broker_position.Maturity Date","MM/dd/yyyy"), "target": "mtry_dt"},
        {"source": F.lit(None).cast("int"), "target": "trde_amt"},
        {"source": F.lit(None).cast("int"), "target": "sttl_amt"},
        {"source": F.lit(None).cast("int"), "target": "pldg_amt"},
        {"source": F.lit(None).cast("int"), "target": "un_sttld_amt"},
        {"source": F.col("broker_position.CONTACT"), "target": "contct_nm"},
        {"source": F.col("broker_position.PHONE"), "target": "phon_det"},
        {"source": F.col("broker_position.FAX"), "target": "fax_det"},
        {"source": F.col("broker_position.ADDRESS"), "target": "addr"},
        {"source": F.col("broker_position.E-mail"), "target": "email_id"},
        {"source": F.col("broker_position.Notes"), "target": "notes"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]



