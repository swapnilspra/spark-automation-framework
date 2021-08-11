# RETURN_PERIODS

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[per_nm](#per_nm)|character varying|10|YES||NO
|[per_desc](#per_desc)|character varying|25|YES||NO
|[per_ut](#per_ut)|character varying|20|YES||NO
|[daily_msr_flg](#daily_msr_flg)|character varying|1|YES||NO
|[mo_end_msr_flg](#mo_end_msr_flg)|character varying|1|YES||NO
|[full_desc](#full_desc)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[per_key](#per_key)|integer|(32,0)|NO||YES
|[per_msr](#per_msr)|numeric|(38,15)|YES||NO
|[annl_fctr](#annl_fctr)|numeric|(38,15)|YES||NO
|[per_sort_ord](#per_sort_ord)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### per_key
#### Description



#### Value Range

Number

#### Logic

Auto Increment sequence Number





### per_nm
#### Description



#### Value Range

Varchar

#### Logic

PER_NM





### per_desc
#### Description



#### Value Range

Varchar

#### Logic


PER_DESC




### per_ut
#### Description



#### Value Range

Varchar

#### Logic


PER_UT




### per_msr
#### Description



#### Value Range

Number

#### Logic

PER_MSR





### annl_fctr
#### Description



#### Value Range

Number

#### Logic



ANNL_FCTR



### daily_msr_flg
#### Description



#### Value Range

Char

#### Logic


DAILY_MSR_FLG




### mo_end_msr_flg
#### Description



#### Value Range

Char

#### Logic

MO_END_MSR_FLG





### per_sort_ord
#### Description



#### Value Range

Number

#### Logic


PER_SORT_ORD




### full_desc
#### Description



#### Value Range

N/A

#### Logic


FULL_DESC




### curr_row_flg
#### Description



#### Value Range

Varchar

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




## FILE

File Name:
File Type : Comma Delimited
File Location: https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/internal/return_periods.csv





