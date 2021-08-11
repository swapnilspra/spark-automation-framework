# FUND_EXPENSE_RATIO

This table holds the
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[manual_entry_flg](#manual_entry_flg)|character|1|YES|'N'::bpchar|NO
|[per_flg](#per_flg)|character varying|1|YES||NO
|[fund_expns_ratio_key](#fund_expns_ratio_key)|integer|(32,0)|NO||YES
|[expns_ratio_wavr_type_key](#expns_ratio_wavr_type_key)|integer|(32,0)|YES||YES
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[gr_expns_ratio](#gr_expns_ratio)|numeric|(38,15)|YES||NO
|[net_expns_ratio](#net_expns_ratio)|numeric|(38,15)|YES||NO
|[expns_wavr_ratio](#expns_wavr_ratio)|numeric|(38,15)|YES||NO
|[cayman_ratio](#cayman_ratio)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[pass_thru_mgmt_fee_wavr](#pass_thru_mgmt_fee_wavr)|numeric|(38,15)|YES||NO
|[semi_annl_rpt_dt](#semi_annl_rpt_dt)|timestamp without time zone|6|YES||NO
|[efftv_strt_dt](#efftv_strt_dt)|timestamp without time zone|6|YES||NO
|[efftv_end_dt](#efftv_end_dt)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### fund_expns_ratio_key
#### Description

Generated key 

#### Value Range

ALL NUMBERS

#### Logic
 
sequence generator  Autoincrement +1 for new inserts



### expns_ratio_wavr_type_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

Pass NULL
## This is updated by Suresh





### fund_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE: FUND_EXPENSE_RATIO.CSV , FUND.CSV

Condition : FUND_NBR (FROM FUND_EXPENSE_RATIO.CSV) = FUND_NBR (FROM FUND.CSV)

Output Column: FUND_KEY (FROM FUND.CSV)




### semi_annl_rpt_dt
#### Description

#### Value Range

ALL DATE VALUES

#### Logic

Not connected in Informatica and we dont have column in file




### efftv_strt_dt
#### Description

#### Value Range

ALL DATE VALUES

#### Logic

EFFTV_STRT_DT




### efftv_end_dt
#### Description

#### Value Range

ALL DATE VALUES

#### Logic

EFFTV_END_DT




### gr_expns_ratio
#### Description

#### Value Range

ALL NUMBERS

#### Logic

GR_EXPNS_RATIO




### net_expns_ratio
#### Description

#### Value Range

ALL NUMBERS

#### Logic

EXP_RATIO_NET



### expns_wavr_ratio
#### Description

#### Value Range

ALL NUMBERS

#### Logic

EXPNS_WAVR_RATIO





### cayman_ratio
#### Description

#### Value Range

ALL NUMBERS

#### Logic

CAYMAN_RATIO




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



### manual_entry_flg
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

MANUAL_ENTRY_FLG



### per_flg
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

MANUAL_ENTRY_FLG



### pass_thru_mgmt_fee_wavr
#### Description

#### Value Range

ALL NUMBERS
#### Logic

PASS_THRU_MGMT_FEE_WAVR




## Sources


###files:

FUND_EXPENSE_RATIO.CSV
FUND.CSV

File type : Comma Delimited
File path : https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund_expense_ratio.csv
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund.csv




## Business key
fund_key
efftv_strt_dt
efftv_end_dt
per_flg






