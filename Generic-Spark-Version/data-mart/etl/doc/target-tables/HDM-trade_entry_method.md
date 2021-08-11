# TRADE_ENTRY_METHOD

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[trde_entry_mthd_cd](#trde_entry_mthd_cd)|character varying|3|NO||NO
|[trde_entry_mthd_desc](#trde_entry_mthd_desc)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[trde_entry_mthd_key](#trde_entry_mthd_key)|integer|(32,0)|NO||YES
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|NO||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### trde_entry_mthd_key
#### Description



#### Value Range

NVARCHAR

#### Logic


Auto Increment Key


### trde_entry_mthd_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

TRDE_ENTRY_MTHD_CD



### trde_entry_mthd_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

TRDE_ENTRY_MTHD_DESC




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

-------------------------------------------------


#Files used
1. trade_entry_method.csv

File Type -- Comma Delimited
File Location -- /internal/trade_entry_method.csv


## Business Key
TRDE_ENTRY_MTHD_CD