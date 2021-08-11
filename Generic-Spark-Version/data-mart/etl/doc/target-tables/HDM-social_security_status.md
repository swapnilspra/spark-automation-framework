# SOCIAL_SECURITY_STATUS

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[ssn_stat_desc](#ssn_stat_desc)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[ssn_stat_key](#ssn_stat_key)|integer|(32,0)|NO||YES
|[ssn_stat_cd](#ssn_stat_cd)|numeric|(38,15)|NO||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|NO||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### ssn_stat_key
#### Description



#### Value Range

NVARCHAR

#### Logic

Auto Increment Key


### ssn_stat_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

SSN_STAT_CD


### ssn_stat_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

SSN_STAT_DESC


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





### curr_row_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

'Y'



### src_sys_id
#### Description



#### Value Range

NVARCHAR

#### Logic





### etl_load_cyc_key
#### Description



#### Value Range

NVARCHAR

#### Logic


------------------------------------------------------

#FIles Used
1. social_security_status.csv

File Type -- Comma Delimited
File Location -- /internal/social_security_status.csv

## Business Key
SSN_STAT_CD
