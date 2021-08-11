# SHAREHOLDER_DISTRIB

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[divd_type](#divd_type)|character varying|1|YES||NO
|[ltcg_flg](#ltcg_flg)|character varying|1|YES||NO
|[fund_expns_allocn_cd](#fund_expns_allocn_cd)|character varying|2|YES||NO
|[ext_pln_id](#ext_pln_id)|character varying|9|YES||NO
|[bal_ind](#bal_ind)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[shrhldr_dstrbtn_key](#shrhldr_dstrbtn_key)|integer|(32,0)|NO||YES
|[acct_key](#acct_key)|integer|(32,0)|YES||NO
|[shrhldr_key](#shrhldr_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[dstrbtn_cd_key](#dstrbtn_cd_key)|integer|(32,0)|YES||NO
|[dstrbtn_type_key](#dstrbtn_type_key)|integer|(32,0)|YES||NO
|[rec_day_key](#rec_day_key)|integer|(32,0)|YES||NO
|[paybl_day_key](#paybl_day_key)|integer|(32,0)|YES||NO
|[re_invst_day_key](#re_invst_day_key)|integer|(32,0)|YES||NO
|[dstrbtn_rt_per_shr](#dstrbtn_rt_per_shr)|numeric|(38,15)|YES||NO
|[fund_expns_ratio](#fund_expns_ratio)|numeric|(38,15)|YES||NO
|[re_invmt_shr_pr](#re_invmt_shr_pr)|numeric|(38,15)|YES||NO
|[pnlty_w_holding_rt](#pnlty_w_holding_rt)|numeric|(38,15)|YES||NO
|[fund_expns_amt](#fund_expns_amt)|numeric|(38,15)|YES||NO
|[shr_re_invstd](#shr_re_invstd)|numeric|(38,15)|YES||NO
|[shr_bal_aft_dstrbtn](#shr_bal_aft_dstrbtn)|numeric|(38,15)|YES||NO
|[shrs_issd](#shrs_issd)|numeric|(38,15)|YES||NO
|[dstrbtn_amt](#dstrbtn_amt)|numeric|(38,15)|YES||NO
|[sub_acctg_fee](#sub_acctg_fee)|numeric|(38,15)|YES||NO
|[othr_fees](#othr_fees)|numeric|(38,15)|YES||NO
|[forgn_tax_w_holding_rt](#forgn_tax_w_holding_rt)|numeric|(38,15)|YES||NO
|[forgn_tax_w_holding_amt](#forgn_tax_w_holding_amt)|numeric|(38,15)|YES||NO
|[us_w_holding_amt](#us_w_holding_amt)|numeric|(38,15)|YES||NO
|[nra_whg_amt](#nra_whg_amt)|numeric|(38,15)|YES||NO
|[cum_discnt_nbr](#cum_discnt_nbr)|numeric|(38,15)|YES||NO
|[wire_txn_grp](#wire_txn_grp)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### shrhldr_dstrbtn_key
#### Description



#### Value Range

NVARCHAR

#### Logic

Auto Increment sequence Generator



### acct_key
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.ACCT_KEY

Join on ACCOUNT

ACCOUNT.FUND_NBR = SHAREHOLDER_DISTRIB.FUND-CODE
AND ACCOUNT.ACCT_NBR= SHAREHOLDER_DISTRIB.ACCOUNT-NUMBER



### shrhldr_key
#### Description



#### Value Range

NVARCHAR

#### Logic


SHAREHOLDER.SHRHLDR_KEY

Join on SHAREHOLDER Table

SHAREHOLDER.TAX_IDNFCN_NBR = SHAREHOLDER_DISTRIB.TAX_IDNFCN_NBR


### fund_key
#### Description



#### Value Range

NVARCHAR

#### Logic

FUND.FUND_KEY

Join on FUND Table

FUND.FUND_NBR = SHAREHOLDER_DISTRIB.FUND-CODE



### dstrbtn_cd_key
#### Description



#### Value Range

NVARCHAR

#### Logic

DISTRIBUTION_CODE.DSTRBTN_CD_KEY

Join on DISTRIBUTION_CODE table

DISTRIBUTION_CODE.DSTRBTN_CD=SHAREHOLDER_DISTRIB.DIST-TYPE-CODE




### dstrbtn_type_key
#### Description



#### Value Range

NVARCHAR

#### Logic

DISTRIBUTION_TYPE.DSTRBTN_TYPE_KEY

Join on DISTRIBUTION_TYPE Table

DISTRIBUTION_TYPE.DSTRBTN_TYPE_CD=SHAREHOLDER_DISTRIB.DISTRIBUTION-TYPE-CODE



### rec_day_key
#### Description



#### Value Range
Alpha Numeric

#### Logic

CALENDAR.CAL_DAY_KEY

Join on CALENDAR Table


CALENDAR.CAL_DAY = SHAREHOLDER_DISTRIB.DISTRIBUTION-RECORD-DATE


### paybl_day_key
#### Description



#### Value Range

Alpha Numeric

#### Logic

CALENDAR.CAL_DAY_KEY

Join on CALENDAR Table


CALENDAR.CAL_DAY = SHAREHOLDER_DISTRIB.DISTRIBUTION-PAYABLE-DATE



### re_invst_day_key
#### Description



#### Value Range

Alpha Numeric

#### Logic

CALENDAR.CAL_DAY_KEY

Join on CALENDAR Table


CALENDAR.CAL_DAY = SHAREHOLDER_DISTRIB.DISTRIBUTION-REIN-DATE


### dstrbtn_rt_per_shr
#### Description



#### Value Range

NUMERIC

#### Logic

SHAREHOLDER_DISTRIB.DISTRIBUTION-PER-SHARE-RATE




### fund_expns_ratio
#### Description



#### Value Range

NUMERIC

#### Logic


SHAREHOLDER_DISTRIB.FUND-EXPENSE-RATE



### re_invmt_shr_pr
#### Description



#### Value Range

NUMERIC
#### Logic


SHAREHOLDER_DISTRIB.REINVESTED-SHARES-PRICE



### pnlty_w_holding_rt
#### Description



#### Value Range

NUMERIC

#### Logic



SHAREHOLDER_DISTRIB.US-WITHHOLDING-RATE




### divd_type
#### Description



#### Value Range

NVARCHAR

#### Logic


SHAREHOLDER_DISTRIB.MF-SP-DIV-TYPE-CDE





### ltcg_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


SHAREHOLDER_DISTRIB.DTR-LTCG-TYPE-CDE



### fund_expns_amt
#### Description



#### Value Range

NUMERIC

#### Logic


SHAREHOLDER_DISTRIB.FUND-EXPENSES-AMOUNT



### fund_expns_allocn_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

SHAREHOLDER_DISTRIB.FUND-EXPENSE-ALLOCATION-CODE





### shr_re_invstd
#### Description



#### Value Range

NUMERIC

#### Logic


SHAREHOLDER_DISTRIB.REINVESTED-SHARES-COUNT



### shr_bal_aft_dstrbtn
#### Description



#### Value Range

NUMERIC

#### Logic


SHAREHOLDER_DISTRIB.ACCOUNT-SHRS-AFTER-DIST-COUNT



### shrs_issd
#### Description



#### Value Range

NUMERIC

#### Logic



SHAREHOLDER_DISTRIB.ISSUED-SHARES-COUNT


### dstrbtn_amt
#### Description



#### Value Range

NUMERIC

#### Logic


SHAREHOLDER_DISTRIB.DISTRIBUTION-AMOUNT



### sub_acctg_fee
#### Description



#### Value Range

NUMERIC

#### Logic


SHAREHOLDER_DISTRIB.DISTRIBUTION-SUBACCT-FEE-AMT



### othr_fees
#### Description



#### Value Range

NUMERIC

#### Logic

SHAREHOLDER_DISTRIB.DISTRIBUTION-OTHER-FEE-AMOUNT




### forgn_tax_w_holding_rt
#### Description



#### Value Range

NVARCHAR

#### Logic


SHAREHOLDER_DISTRIB.NRA-WITHHOLDING-RATE



### forgn_tax_w_holding_amt
#### Description



#### Value Range

NUMERIC

#### Logic

SHAREHOLDER_DISTRIB.NRA-WITHHOLDING-AMOUNT




### us_w_holding_amt
#### Description



#### Value Range
NUMERIC

#### Logic


SHAREHOLDER_DISTRIB.US-WITHHOLDING-AMOUNT




### nra_whg_amt
#### Description



#### Value Range

NVARCHAR

#### Logic

SHAREHOLDER_DISTRIB.NRA-WITHHOLDING-AMOUNT



### cum_discnt_nbr
#### Description



#### Value Range

NUMERIC

#### Logic

SHAREHOLDER_DISTRIB.CUMULATIVE-DISCOUNT-NUMBER



### ext_pln_id
#### Description



#### Value Range

NVARCHAR

#### Logic

SHAREHOLDER_DISTRIB.EXTERNAL-PLAN-ID




### wire_txn_grp
#### Description



#### Value Range

NVARCHAR

#### Logic


SHAREHOLDER_DISTRIB.WIRE-TRANS-GRP-NBR



### bal_ind
#### Description



#### Value Range

NVARCHAR

#### Logic


SHAREHOLDER_DISTRIB.BALANCE-INDICATOR



### curr_row_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


'Y'


### row_strt_dttm
#### Description



#### Value Range

NVARCHAR

#### Logic

SYSDATE



### row_stop_dttm
#### Description



#### Value Range

NVARCHAR

#### Logic





### etl_load_cyc_key
#### Description



#### Value Range

NVARCHAR

#### Logic





### src_sys_id
#### Description



#### Value Range

NVARCHAR

#### Logic





#Files used


1. SHAREHOLDER_DISTRIB (DTO.HCA.DVRA.TIP01.YYMMDDHHMMSS)



## File Location


dstprod/DTO.HCA.DVRA.TIP01.20190801.225741.zip.pgp.txt


## Tables Used


2. ACCOUNT
3. SHAREHOLDER
4. FUND
5. CALENDAR
6. DISTRIBUTION_TYPE
7. DISTRIBUTION_CODE


## Business Keys

acct_key
fund_key
dstrbtn_d_key
dstrbtn_type_key
rec_day_key
paybl_day_key
re_invst_day_key
bal_ind



##### COBOL File



