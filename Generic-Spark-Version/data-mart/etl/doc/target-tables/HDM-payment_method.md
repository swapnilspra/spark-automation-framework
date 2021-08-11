# PAYMENT_METHOD

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[pmt_mthd_top_lvl_cd](#pmt_mthd_top_lvl_cd)|character varying|3|YES||NO
|[pmt_mthd_top_lvl_desc](#pmt_mthd_top_lvl_desc)|character varying|60|YES||NO
|[pmt_mthd_cd](#pmt_mthd_cd)|character varying|1|YES||NO
|[pmt_mthd_desc](#pmt_mthd_desc)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[pmt_mthd_key](#pmt_mthd_key)|integer|(32,0)|NO||YES
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### pmt_mthd_key
#### Description

#### Value Range

NVARCHAR

#### Logic

Auto Increment Key


### pmt_mthd_top_lvl_cd
#### Description



#### Value Range

NVARCHAR

#### Logic





### pmt_mthd_top_lvl_desc
#### Description



#### Value Range

NVARCHAR

#### Logic





### pmt_mthd_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

PMT_MTHD_CD


### pmt_mthd_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

PMT_MTHD_DESC


### row_strt_dttm
#### Description



#### Value Range

NVARCHAR

#### Logic





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

#Files Used
1. payment_method.csv


File Type -- Comma Delimited
File Location -- /internal/payment_method.csv


## Business Key
PMT_MTHD_CD
