from typing import Dict, List, Any
import pyspark.sql
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up


class Job(ETLJob):
    target_table = "account"
    business_key = ["fund_nbr", "acct_nbr"]
    primary_key = {"acct_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "cobol": {
            "type": "file",
            "source": "DTO.HCA.AMP.TIP09"
        },
        "account_position":{
            "type": "table",
            "source": "account_position"
        },
        "dealer": {
            "type": "table",
            "source": "dealer"
        },
        "dealer_level_control": {
            "type": "table",
            "source": "dealer_level_control"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "country": {
            "type": "dimension",
            "source": "dst_st_country"
        },
        "tin_code": {
            "type": "dimension",
            "source": "tin_code"
        },
        "cust_acct_stat_type": {
            "type": "dimension",
            "source": "cust_acct_stat_type"
        },
        "social_codes": {
            "type": "dimension",
            "source": "social_codes"
        },
        "acct_master": {
            "type": "dimension",
            "source": "account_master"
        },
        "acct_master_ext": {
            "type": "dimension",
            "source": "account_master_extension"
        },
        "marketing_classification": {
            "type": "dimension",
            "source": "marketing_classification"
        },
        "market_classification": {
            "type": "dimension",
            "source": "market_classification"
        }
    }

    joins: List[Dict[str, Any]] = [
        {
            "source": "cobol"
        },
        {
            "source": "social_codes",
            "conditions": [
                F.col("social_codes.acct_type_cd") == F.col("cobol.social-code")
            ]
        },
        {
            "source": "cust_acct_stat_type",
            "conditions": [
                F.col("cust_acct_stat_type.cust_acct_stat_cd") == F.col("cobol.plan-status-code")
            ]
        },
        {
            "source": "dealer_level_control",
            "conditions": [
                F.col("dealer_level_control.dlr_lvl_cntl_cd") == F.col("cobol.dealer-control-level-code")
            ]
        },
        {
            "source": "tin_code",
            "conditions": [
                F.col("tin_code.tin_cd") == F.col("cobol.ss-code")
            ]
        },
        {
            "source": "dealer",
            "conditions": [
                F.col("dealer.dlr_id") == F.col("cobol.trustee-nscc-firm-nbr")
            ],
            "type":"left_outer"
        },
        {
            "source": "acct_master",
            "conditions": [
                F.col("acct_master.fund") == F.trim(F.col("cobol.fund-code")),
                F.col("acct_master.account") == F.trim(F.col("cobol.customer-account-number"))
            ],
            "type": "left"
        },
        {
            "source": "acct_master_ext",
            "conditions": [
                F.col("acct_master_ext.fund") == F.trim(F.col("cobol.fund-code")),
                F.col("acct_master_ext.account") == F.trim(F.col("cobol.customer-account-number"))
            ],
            "type": "left"
        },
        {
            "source": "marketing_classification",
            "conditions": [
                F.col("marketing_classification.acct_nbr") == F.trim(F.col("cobol.customer-account-number"))
            ],
            "type": "left"
        },
        {
            "source": "market_classification",
            "conditions": [
                F.upper(F.trim(F.col("market_classification.mkt_clsfcn_desc"))) == F.upper(F.trim(F.col("marketing_classification.mkt_clsfcn_desc")))
            ],
            "type": "left"
        }
    ]

    target_mappings:List[Dict[str,Any]] = [
        {"source": F.lit("All"), "target": "acct_top_lvl_cd"},
        {"source": F.lit("All Accounts"), "target": "acct_top_lvl_desc"},
        {"source": F.col("social_codes.acct_type_cd"), "target": "acct_type_cd"},
        {"source": F.trim(F.upper(F.col("social_codes.cust_acct_type_nm"))), "target": "acct_type"},
        {"source": F.trim(F.col("cobol.customer-account-number")), "target": "acct_nbr"},
        {"source": F.col("cobol.fund-code"), "target": "fund_nbr"},
        {"source": F.col("cobol.tax-identification-number"), "target": "tax_id"},
        {"source": F.col("cobol.plan-status-code"), "target": "acct_stat_cd"},
        {"source": F.col("cust_acct_stat_type.cust_acct_stat_nm"), "target": "acct_stat"},
        {"source": F.to_timestamp(F.col("cobol.established-date"), "yyyyMMdd"), "target": "acct_strt_dt"},
        {"source": F.to_timestamp(F.col("cobol.last-maintenance-date"), "yyyyMMdd"), "target": "last_mntn_dt"},
        {"source": F.col("dealer_level_control.dlr_lvl_cntl_cd"), "target": "dlr_lvl_cntl_cd"},
        {"source": F.col("dealer_level_control.dlr_lvl_cntl_desc"), "target": "dlr_lvl_cntl_desc"},
        {"source": F.col("cobol.pre-auth-ck-acct"), "target": "pre_authzd_chk_acct_flg"},
        {"source": F.col("cobol.swp-account"), "target": "swp_acct_flg"},
        {"source": F.col("cobol.reinvest-to-fund"), "target": "re_invst_to_another_acct_flg"},
        {"source": F.col("cobol.monitor-vip-flag"), "target": "monitored_vip_acct"},
        {"source": F.col("cobol.fiduciary-account"), "target": "fiduciary_acct_flg"},
        {"source": F.col("cobol.mailing-flag"), "target": "mailg_flg"},
        {"source": F.col("cobol.interested-party-cde"), "target": "intr_party_flg"},
        {"source": F.col("cobol.shr-acct-mgmt-co-employee-cde"), "target": "mgmt_co_empl_flg"},
        {"source": F.col("cobol.trustee-nscc-firm-nbr").cast("int"), "target": "trst_co_nbr"},
        {"source": F.col("cobol.nscc-tpa-firm-nbr").cast("int"), "target": "tpa_nbr"},
        {"source": F.col("cobol.custodian-id"), "target": "custodian_id"},
        {"source": F.col("cobol.third-prty-admn-id"), "target": "tpa_id"},
        {"source": F.when(F.trim(F.col("cobol.external-plan-id")) == '', F.lit(None))\
            .otherwise(F.trim(F.col("cobol.external-plan-id"))),"target": "ext_pln_id"},
        {"source": F.col("tin_code.tin_cd"), "target": "tin_cd"},
        {"source": F.initcap(F.col("tin_code.tin_desc")), "target": "tin_desc"},
        {"source": F.col("cobol.alpha-code"), "target": "alpha_cd"},
        {"source": F.col("cobol.cap-gains"), "target": "cap_gains_dstrbtn_flg"},
        {"source": F.col("cobol.check-writing-acct"), "target": "chk_writing_acct_flg"},
        {"source": F.col("cobol.dividend-1"), "target": "divd_dstrbtn_opt_flg"},
        {"source": F.col("cobol.expedited-acct"), "target": "expedited_redmpn_acct_flg"},
        {"source": F.col("cobol.sub-accounting"), "target": "sub_acct_flg"},
        {"source": F.col("cobol.foreign-tax-rate"), "target": "forgn_tax_rt"},
        {"source": F.col("cobol.cumulative-discount-no"), "target": "cum_discnt_nbr"},
        {"source": F.col("cobol.loi-number"), "target": "ltr_of_intend_nbr"},
        {"source": F.col("cobol.timer-flag"), "target": "timer_flg"},
        {"source": F.col("cobol.listb-flag"), "target": "gpurch_acct_flg"},
        {"source": F.col("cobol.expedited-exchange"), "target": "expdt_xchg_acct_flg"},
        {"source": F.col("cobol.penalty-withholding"), "target": "pnlty_whg_acct_flg"},
        {"source": F.col("cobol.issue-code"), "target": "certfc_issuance_flg"},
        {"source": F.col("cobol.stop-transfer-code"), "target": "stop_xfer_flg"},
        {"source": F.col("cobol.blue-sky-exempt"), "target": "bluesky_xmptn_flg"},
        {"source": F.col("cobol.debit-card"), "target": "bnk_card_issd_flg"},
        {"source": F.col("cobol.nav-cde"), "target": "nav_acct_flg"},
        {"source": F.col("cobol.shr-acct-phone-chk-redempt-cde"), "target": "phon_chk_redmpn_flg"},
        {"source": F.col("cobol.shr-acct-house-account-cde"), "target": "house_acct_flg"},
        {"source": F.col("cobol.dividend-mail"), "target": "divd_mail_acct_flg"},
        {"source": F.col("cobol.stop-purchase"), "target": "stop_purch_acct_flg"},
        {"source": F.col("cobol.stop-mail"), "target": "stop_mail_acct_flg"},
        {"source": F.col("cobol.fractional-check"), "target": "fractional_chk_flg"},
        {"source": F.col("cobol.shr-acct-price-sched-cde"), "target": "acct_pr_sched_flg"},
        {"source": F.lit(None).cast("string"), "target": "cap_gains_dstrbtn_cd"},
        {"source": F.lit(None).cast("string"), "target": "cap_gains_dstrbtn_type"},
        {"source": F.lit(None).cast("string"), "target": "divd_dstrbtn_type_cd"},
        {"source": F.lit(None).cast("string"), "target": "divd_dstrbtn_type"},
        {"source": F.when(F.trim(F.col("cobol.fins-account-id")) == '', F.lit(None))\
            .otherwise(F.trim(F.col("cobol.fins-account-id"))), "target": "cust_bin"},
        {"source": F.col("cobol.account-origination-id"), "target": "acct_origination_id"},
        {"source": F.when(F.trim(F.col("cobol.registration-line-1-text"))== '', F.lit(None)) \
            .otherwise(F.trim(F.col("cobol.registration-line-1-text"))), "target": "regstn_line_1"},
        {"source": F.when(F.trim(F.col("cobol.registration-line-2-text"))== '', F.lit(None)) \
            .otherwise(F.trim(F.col("cobol.registration-line-2-text"))), "target": "regstn_line_2"},
        {"source": F.when(F.trim(F.col("cobol.registration-line-3-text"))== '', F.lit(None)) \
            .otherwise(F.trim(F.col("cobol.registration-line-3-text"))), "target": "regstn_line_3"},
        {"source": F.when(F.trim(F.col("cobol.registration-line-4-text"))== '', F.lit(None)) \
            .otherwise(F.trim(F.col("cobol.registration-line-4-text"))), "target": "regstn_line_4"},
        {"source": F.when(F.trim(F.col("cobol.registration-line-5-text"))== '', F.lit(None)) \
            .otherwise(F.trim(F.col("cobol.registration-line-5-text"))), "target": "regstn_line_5"},
        {"source": F.when(F.trim(F.col("cobol.registration-line-6-text"))== '', F.lit(None)) \
            .otherwise(F.trim(F.col("cobol.registration-line-6-text"))), "target": "regstn_line_6"},
        {"source": F.when(F.trim(F.col("cobol.registration-line-7-text")) == '', F.lit(None)) \
            .otherwise(F.trim(F.col("cobol.registration-line-7-text"))), "target": "regstn_line_7"},
        {"source": F.col("country.st_nm"), "target": "st_nm"},
        {"source": F.col("country.crty_full_nm"), "target": "crty_nm"},
        {"source": F.col("country.st_cd"), "target": "st_cd"},
        {"source": F.col("country.crty_iso_cd"), "target": "crty_cd"},
        {"source": F.lit(None).cast("string"), "target": "legacy_type_desc"},
        {"source": F.to_timestamp(F.lit("1900-01-01 00:00:00")), "target": "fund_strt_dt"},
        {"source": F.coalesce(F.to_timestamp(F.col("acct_master.stopmaildate"), 'MM/dd/yyyy'), F.to_timestamp(F.lit("0001-01-01 00:00:00"))), "target": "stop_mail_dt"},
        {"source": F.to_date(F.col("acct_master.acct_setup_dte"), 'MM/dd/yyyy'), "target": "acct_setup_dt"},
        {"source": F.col("cobol.shr-acct-client-defined-1-cde"), "target": "internet_acct_flg"},
        {"source": F.when(F.length(F.trim(F.col("cobol.shr-acct-client-defined-2-cde"))) == 0, F.lit(None).cast("string")).otherwise(F.trim(F.col("cobol.shr-acct-client-defined-2-cde"))), "target": "grnd_father_acct_flg"},
        {"source": F.lit(None).cast("string"), "target": "pln_clsfcn_flg"},
        {"source": F.col("market_classification.mkt_clsfcn_cd"), "target": "mktg_clsfcn_flg"},
        {"source": F.col("marketing_classification.mkt_clsfcn_desc"), "target": "mktg_clsfcn_desc"},
        {"source": F.when(F.trim(F.col("cobol.representative-number"))== '', F.lit(None))\
            .otherwise(F.trim(F.col("cobol.representative-number"))), "target": "rep_cd"},
        {"source": F.when(F.trim(F.col("cobol.representative-name"))== '', F.lit(None))\
            .otherwise(F.trim(F.col("cobol.representative-name"))), "target": "rep_nm"},
        {"source": F.col("cobol.fins-id"), "target": "dlr_id"},
        {"source": F.col("dlr_type_calc"), "target": "dlr_type"},
        {"source": F.col("hbr_dlr_id_calc").cast("int"), "target": "hbr_dlr_id"},
        {"source": F.col("hbr_dlr_nm"), "target": "hbr_dlr_nm"},
        {"source": F.when(F.trim(F.col("cobol.shr-acct-client-defined-3-cde"))== '', F.lit(None)) \
            .otherwise(F.trim(F.col("cobol.shr-acct-client-defined-3-cde"))),"target": "redmpn_fee_xmpt_flg"},
        {"source": F.col("acct_master_ext.pay_to_play"), "target": "pay_to_play_flg"},
        {"source": F.col("cobol.shr-acct-citizen-st-cntry-cde"), "target": "citzn_st_crty_cd"},
        {"source": F.lit(None).cast("string"), "target": "purg_prcsd_flg"},
        {"source": F.col("cobol.ach-flag"),"target": "ach_acct_flg"},
        {"source": F.col("dealer.dlr_nm").cast("string"),"target": "trst_nm"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.col("accnt_close_date"), "target": "acct_clsd_dt"},
        {"source": F.to_timestamp(F.lit(None), "yyyyMMdd"), "target": "purg_prcsd_dt"},
        {"source": F.lit(None).cast("int"), "target": "src_sys_id"}
    ]
    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:
        from datetime import date
        today = date.today()
        #today =F.to_timestamp(F.lit("20200218"), "yyyyMMdd")

        df_joined = super().join(inputs)
        # Create dataframes from tables
        df_country= inputs["country"]
        df_joined = df_joined\
            .join(df_country, F.col("country.dst_st_cd") == F.col("cobol.shr-acct-citizen-st-cntry-cde"), how="inner")

        df_joined = df_joined \
            .withColumn("dlr_type_calc",
                        F.when((F.col("cobol.fins-id").isin(5471, 5700)) &
                               (F.length(F.trim(F.col("cobol.trustee-nscc-firm-nbr"))) == 0), "INDIVIDUAL")
                        .otherwise(F.when((F.col("cobol.fins-id").isin(5471, 5700)) &
                                          (F.length(F.trim(F.col("cobol.trustee-nscc-firm-nbr"))) != 0), "TRUST")
                                   .otherwise(
                            F.when(((F.col("cobol.fins-id") != 5471) | (F.col("cobol.fins-id") != 5700)), "DEALER")
                            .otherwise("UNDEFINED"))))

        df_joined = df_joined \
            .withColumn("hbr_dlr_id_calc",
                        F.when(((F.col("cobol.fins-id") == 5471) | (F.col("cobol.fins-id") == 5700)) &
                               (F.length(F.trim(F.col("cobol.trustee-nscc-firm-nbr"))) != 0),
                               F.col("cobol.trustee-nscc-firm-nbr"))
                        .otherwise(F.col("cobol.fins-id")))

        df_hbr_dlr = inputs["dealer"].select(F.col("dealer.dlr_nm").alias("hbr_dlr_nm"),
                                             F.col("dealer.dlr_id").alias("hbr_dlr_id"))

        df_joined = df_joined.join(df_hbr_dlr, [F.col("hbr_dlr_id_calc") == F.col("hbr_dlr_id")], "left")

        #this is to calculate previous bussiness day from today

        prev_buss_calc = inputs["calendar"].where((F.col("calendar.cal_day")<today) & (F.col("calendar.bus_day_flg")=='Y')).agg(F.max(F.col("day_key")),F.max(F.col("cal_day"))).collect()[0]
        prev_buss_day=prev_buss_calc.asDict().get('max(day_key)')
        prev_buss_date = prev_buss_calc.asDict().get('max(cal_day)')

        print("previous bussiness day",prev_buss_day)

        #we will get account position for prev day
        inputs["account_position"]=inputs["account_position"].where(F.col("account_position.day_key")==prev_buss_day).\
            select(F.col("account_position.acct_nbr"),F.col("account_position.fund_nbr"),F.col("account_position.tot_shrs"),F.col("account_position.nav"))

        df_joined=df_joined.join(inputs["account_position"],\
                [(F.col("account_position.acct_nbr")==F.trim(F.col("cobol.customer-account-number"))) & \
                 (F.col("account_position.fund_nbr")==F.col("cobol.fund-code"))],how="left")

        #if previous bussiness day position is positive and today position is zero then today will be accnt close date
        df_joined=df_joined.withColumn("accnt_close_date",F.when(((F.col("account_position.tot_shrs")*F.col("account_position.nav"))>0) &
                  ((F.col("cobol.total-share-ct")*F.col("cobol.nav-cde"))==0),prev_buss_date).otherwise(F.to_timestamp(F.lit(None), "yyyyMMdd")))

        return df_joined
