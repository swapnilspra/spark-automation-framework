# FUND_DIVIDEND

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[est_divd_flg](#est_divd_flg)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[fund_divd_key](#fund_divd_key)|integer|(32,0)|NO||YES
|[fund_key](#fund_key)|integer|(32,0)|NO||YES
|[rec_day_key](#rec_day_key)|integer|(32,0)|YES||YES
|[reinvest_day_key](#reinvest_day_key)|integer|(32,0)|YES||YES
|[paybl_day_key](#paybl_day_key)|integer|(32,0)|YES||YES
|[tot_divd](#tot_divd)|numeric|(38,15)|YES||NO
|[net_invmt_incm](#net_invmt_incm)|numeric|(38,15)|YES||NO
|[shrs_of_rec](#shrs_of_rec)|numeric|(38,15)|YES||NO
|[re_invst_pr](#re_invst_pr)|numeric|(38,15)|YES||NO
|[ordn_incm](#ordn_incm)|numeric|(38,15)|YES||NO
|[tot_shrt_term_gains](#tot_shrt_term_gains)|numeric|(38,15)|YES||NO
|[tot_mid_term_gains](#tot_mid_term_gains)|numeric|(38,15)|YES||NO
|[tot_long_term_gains](#tot_long_term_gains)|numeric|(38,15)|YES||NO
|[shrt_term_cap_gains](#shrt_term_cap_gains)|numeric|(38,15)|YES||NO
|[mid_term_cap_gains](#mid_term_cap_gains)|numeric|(38,15)|YES||NO
|[long_term_cap_gains](#long_term_cap_gains)|numeric|(38,15)|YES||NO
|[non_txbl_discnt](#non_txbl_discnt)|numeric|(38,15)|YES||NO
|[forgn_tax_cr](#forgn_tax_cr)|numeric|(38,15)|YES||NO
|[re_invst_idx](#re_invst_idx)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### fund_divd_key
#### Description

Generated key 

#### Value Range

ALL NUMBERS

#### Logic

sequence generator  Autoincrement +1 for new inserts 




### fund_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE: FUND_DIVIDEND.CSV , FUND.CSV

Condition : FUND_NBR (FROM FUND_DIVIDEND.CSV) = FUND_NBR (FROM FUND.CSV)

Output Column: FUND_KEY (FROM FUND.CSV)





### rec_day_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

RECORD_DATE

FILE: FUND_DIVIDEND.CSV , CALENDAR.CSV

Condition : RECORD_DATE (FROM FUND_DIVIDEND.CSV) =  CAL_DAY (FROM CALENDAR.CSV)

Output Column: DAY_KEY (FROM CALENDAR.CSV)



### reinvest_day_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

REINVEST_DATE

FILE: FUND_DIVIDEND.CSV , CALENDAR.CSV

Condition : REINVEST_DATE (FROM FUND_DIVIDEND.CSV) =  CAL_DAY (FROM CALENDAR.CSV)

Output Column: DAY_KEY (FROM CALENDAR.CSV)



### paybl_day_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

PAYABLE_DATE

FILE: FUND_DIVIDEND.CSV , CALENDAR.CSV

Condition : PAYABLE_DATE (FROM FUND_DIVIDEND.CSV) =  CAL_DAY (FROM CALENDAR.CSV)

Output Column: DAY_KEY (FROM CALENDAR.CSV)



### tot_divd
#### Description

#### Value Range

ALL NUMBERS

#### Logic

All the columns are from FUND_DIVIDEND.CSV

CAP_GAINS_SHORT+CAP_GAINS_MID+CAP_GAINS_LONG+ORD_INCOME+NON_TAXABLE_DIS AS TOT_DIVIDEND



### net_invmt_incm
#### Description

#### Value Range

ALL NUMBERS

#### Logic

NET_INVEST_INCOME




### shrs_of_rec
#### Description

#### Value Range

ALL NUMBERS

#### Logic

SHRS_OF_RECORD



### re_invst_pr
#### Description

#### Value Range

ALL NUMBERS

#### Logic

REINVEST_PRICE




### ordn_incm
#### Description

#### Value Range

ALL NUMBERS

#### Logic

ORD_INCOME



### tot_shrt_term_gains
#### Description

#### Value Range

ALL NUMBERS

#### Logic

TOTAL_GAINS_SHORT




### tot_mid_term_gains
#### Description

#### Value Range

ALL NUMBERS

#### Logic

TOTAL_GAINS_MID




### tot_long_term_gains
#### Description

#### Value Range

ALL NUMBERS

#### Logic

TOTAL_GAINS_LONG




### shrt_term_cap_gains
#### Description

#### Value Range

ALL NUMBERS

#### Logic

CAP_GAINS_SHORT




### mid_term_cap_gains
#### Description

#### Value Range

ALL NUMBERS

#### Logic

CAP_GAINS_MID




### long_term_cap_gains
#### Description

#### Value Range

ALL NUMBERS

#### Logic

CAP_GAINS_LONG 




### non_txbl_discnt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

NON_TAXABLE_DIS




### forgn_tax_cr
#### Description

#### Value Range

ALL NUMBERS

#### Logic

FOREIGN_TAX_CREDIT 




### re_invst_idx
#### Description

#### Value Range

ALL NUMBERS

#### Logic

REINVEST_INDEX




### est_divd_flg
#### Description

#### Value Range

ALL STRING Values

#### Logic

ESTIMATED_DIVIDEND




### curr_row_flg
#### Description

#### Value Range

Always value 'Y'

#### Logic




Always value 'Y'



### row_strt_dttm
#### Description

#### Value Range

CURRENT DATE

#### Logic

CALCULTED COLUMN - SYSDATE FUNCTION



### row_stop_dttm
#### Description

#### Value Range

ALL DATE VALUES

#### Logic





### etl_load_cyc_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

Column From Informatica - ETL KEY 



### src_sys_id
#### Description

#### Value Range

Always value 4 

#### Logic

Column From Informatica - SOURCE ID

Always value 4 



## Source files

###files:

Add path - https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund_dividend.csv
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund.csv
File type  - Comma Delimited FILE


FUND_DIVIDEND.csv
FUND.CSV
CALENDAR.CSV


## BUSINESS KEYS
1. FUND_NBR
2. REINVEST_DATE

