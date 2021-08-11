from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob  # must be imported after spark has been set up
from itertools import chain


class Job(ETLJob):
    target_table = "shareholder_distrib"
    business_key = ["acct_key", "fund_key", "dstrbtn_cd_key", "dstrbtn_type_key", "rec_day_key", "paybl_day_key",
                    "re_invst_day_key", "bal_ind"]
    primary_key = {"shrhldr_dstrbtn_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "cobol": {
            "type": "file",
            "source": "DTO.HCA.DVRA.TIP01"
        },
        "account": {
            "type": "table",
            "source": "account"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "calendar_distribution_record_date": {
            "type": "table",
            "source": "calendar"
        },
        "calendar_distribution_payable_date": {
            "type": "table",
            "source": "calendar"
        },
        "calendar_distribution_rein_date": {
            "type": "table",
            "source": "calendar"
        },
        "distribution_type": {
            "type": "table",
            "source": "distribution_type"
        },
        "distribution_code": {
            "type": "table",
            "source": "distribution_code"
        }

    }
    joins: List[Dict[str, Any]] = [
        {
            "source": "cobol"
        },
        {
            "source": "account",
            "conditions": [
                F.col("account.fund_nbr") == F.trim(F.col("cobol.fund-code")),
                F.col("account.acct_nbr") == F.trim(F.col("cobol.customer-account-number"))
            ]
        },
        {
            "source": "fund",
            "conditions": [
                F.col("fund.fund_nbr") == F.trim(F.col("cobol.fund-code"))
            ]
        },
        {
            "source": "distribution_code",
            "conditions": [
                F.col("distribution_code.dstrbtn_cd") == F.trim(F.col("cobol.dist-type-code"))
            ],
            "type": "left"
        },
        {
            "source": "distribution_type",
            "conditions": [
                F.col("distribution_type.dstrbtn_type_cd") == F.trim(F.col("cobol.distribution-type-code"))
            ],
            "type": "left"
        },
        {
            "source": "calendar_distribution_record_date",
            "conditions": [
                F.to_date(F.col("calendar_distribution_record_date.cal_day"),"yyyyMMdd") == F.to_date(F.col("cobol.distribution-record-date"),"yyyyMMdd")
            ]
        },
        {
            "source": "calendar_distribution_payable_date",
            "conditions": [
                F.to_date(F.col("calendar_distribution_payable_date.cal_day"),"yyyyMMdd") == F.to_date(F.col("cobol.distribution-payable-date"),"yyyyMMdd")
            ]
        },
        {
            "source": "calendar_distribution_rein_date",
            "conditions": [
                F.to_date(F.col("calendar_distribution_rein_date.cal_day"),"yyyyMMdd") == F.to_date(F.col("cobol.distribution-rein-date"),"yyyyMMdd")
            ]
        }
    ]

    target_mappings: List[Dict[str, Any]] = [

        {"source": F.col("account.acct_key"), "target": "acct_key"},
        {"source": F.col("fund.fund_key"), "target": "fund_key"},
        {"source": F.col("distribution_code.dstrbtn_cd_key"), "target": "dstrbtn_cd_key"},
        {"source": F.col("distribution_type.dstrbtn_type_key"), "target": "dstrbtn_type_key"},
        {"source": F.col("calendar_distribution_record_date.day_key"), "target": "rec_day_key"},
        {"source": F.col("calendar_distribution_payable_date.day_key"), "target": "paybl_day_key"},
        {"source": F.col("calendar_distribution_rein_date.day_key"), "target": "re_invst_day_key"},
        {"source": F.col("cobol.distribution-per-share-rate"), "target": "dstrbtn_rt_per_shr"},
        {"source": F.col("cobol.fund-expense-rate"), "target": "fund_expns_ratio"},
        {"source": F.col("cobol.reinvested-shares-price"), "target": "re_invmt_shr_pr"},
        {"source": F.col("cobol.us-withholding-rate"), "target": "pnlty_w_holding_rt"},
        {"source": F.when(F.length(F.trim(F.col("cobol.mf-sp-div-type-cde"))) == 0, F.lit(None).cast("string")).\
            otherwise(F.col("cobol.mf-sp-div-type-cde")),"target": "divd_type"},
        {"source": F.col("cobol.dtr-ltcg-type-cde"), "target": "ltcg_flg"},
        {"source": F.col("cobol.fund-expenses-amount"), "target": "fund_expns_amt"},
        {"source": F.col("cobol.fund-expense-allocation-code"), "target": "fund_expns_allocn_cd"},
        {"source": F.col("cobol.reinvested-shares-count"), "target": "shr_re_invstd"},
        {"source": F.col("cobol.account-shrs-after-dist-count"), "target": "shr_bal_aft_dstrbtn"},
        {"source": F.col("cobol.issued-shares-count"), "target": "shrs_issd"},
        {"source": F.col("cobol.distribution-amount"), "target": "dstrbtn_amt"},
        {"source": F.col("cobol.distribution-subacct-fee-amt"), "target": "sub_acctg_fee"},
        {"source": F.col("cobol.distribution-other-fee-amount"), "target": "othr_fees"},
        {"source": F.col("cobol.nra-withholding-rate"), "target": "forgn_tax_w_holding_rt"},
        {"source": F.col("cobol.nra-withholding-amount"), "target": "forgn_tax_w_holding_amt"},
        {"source": F.col("cobol.us-withholding-amount"), "target": "us_w_holding_amt"},
        {"source": F.when(F.trim(F.col("cobol.nra-withholding-amount")) == 'null', F.lit(0.0)),
         "target": "nra_whg_amt"},
        {"source": F.col("cobol.cumulative-discount-number"), "target": "cum_discnt_nbr"},
        {"source": F.when(F.length(F.trim(F.col("cobol.external-plan-id"))) == 0,F.lit(None).cast("string")).\
            otherwise(F.trim(F.col("cobol.external-plan-id"))),"target": "ext_pln_id"},
        {"source": F.col("cobol.wire-trans-grp-nbr"), "target": "wire_txn_grp"},
        {"source": F.col("cobol.balance-indicator"), "target": "bal_ind"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit(4).cast("int"), "target": "src_sys_id"}
    ]

    #
    # override extract to read the special TIP01 file
    #
    def extract(self, catalog: Dict[str, Any]) -> Dict[str, pyspark.sql.DataFrame]:
        df_inputs = super().extract(catalog)

        source: Dict[Any, Any] = catalog["DTO.HCA.DVRA.TIP01"]
        file_location: str = common.utils.get_file_location(self._env["file_prefix"], source["path"])
        rdd = self._spark.read.text(file_location).rdd

        # iterate over lines
        i = 0
        curr_header = ''
        curr_record: List[str] = []
        records = []
        for l in rdd.toLocalIterator():
            i += 1
            if i <= source["skip_header_rows"]:
                continue
            if l.value[2] == 'H':
                curr_header = l.value
                continue
            elif l.value[3:6] == '001':
                # flush previous record
                if len(curr_record) > 0:
                    records.append("\n".join(curr_record))
                # start of new record without header. add header
                curr_record = [curr_header, l.value]
            else:
                curr_record.append(l.value)

        # flush last record
        records.append("\n".join(curr_record))

        df_inputs["cobol"] = common.parsers.read_cobol_file_rdd(
            self._spark,
            self._spark.sparkContext.parallelize(records,len(records)//100), # parallelize at most 100 records in each partition. it is about 0.5K each so 100 would be 50K at most, below 100K recommendation
            copybook_location=source.get("copybook", None),
            multiline=source.get("multiline", False),  # if has a row prefix this is multiline file
            record_selector_field=source.get("record_selector_field", None),
            record_types=source.get("record_types", None)
        )
        df_inputs["cobol"] = df_inputs["cobol"].distinct().alias("cobol")
        return df_inputs

