# DIVIDEND_TAX_RATE

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[divd_tax_rt_key](#divd_tax_rt_key)|integer|(32,0)|NO||YES
|[tax_yr](#tax_yr)|numeric|(38,15)|YES||NO
|[ordn_incm_rt](#ordn_incm_rt)|numeric|(38,15)|YES||NO
|[ltcg_rt](#ltcg_rt)|numeric|(38,15)|YES||NO
|[stcg_rt](#stcg_rt)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### divd_tax_rt_key
#### Description



#### Value Range

Number		

#### Logic


Auto Increment sequence number





### tax_yr
#### Description



#### Value Range

Number

#### Logic
TAX_YR






### ordn_incm_rt
#### Description



#### Value Range

Decimal

#### Logic


ORDN_INCM_RT




### ltcg_rt
#### Description



#### Value Range

Decimal

#### Logic

LTCG_RT





### stcg_rt
#### Description



#### Value Range

Decimal

#### Logic


STCG_RT




### curr_row_flg
#### Description



#### Value Range

N/A

#### Logic







### row_strt_dttm
#### Description



#### Value Range

N/A

#### Logic







### row_stop_dttm
#### Description



#### Value Range

N/A

#### Logic







### etl_load_cyc_key
#### Description



#### Value Range

N/A

#### Logic







### src_sys_id
#### Description



#### Value Range

N/A

#### Logic



## File
File Name: dividend_tax_rate.csv
File Type: Comma Delimited
File Location : https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/internal/dividend_tax_rate.csv



