# REASON_CODE

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[entity_desc](#entity_desc)|character varying|255|YES||NO
|[rsn_shrt_desc](#rsn_shrt_desc)|character varying|25|YES||NO
|[rsn_long_desc](#rsn_long_desc)|character varying|255|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[rsn_key](#rsn_key)|integer|(32,0)|NO||YES
|[entity_rsn_cd](#entity_rsn_cd)|numeric|(38,15)|YES||NO
|[rsn_cd](#rsn_cd)|numeric|(38,15)|NO||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### rsn_key
#### Description



#### Value Range

NVARCHAR

#### Logic

Auto Increment Key

### entity_rsn_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

ENTITY_RSN_CD




### entity_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

ENTITY_DESC




### rsn_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

RSN_CD


### rsn_shrt_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

RSN_SHRT_DESC


### rsn_long_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

RSN_LONG_DESC

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





### curr_row_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

'Y'


--------------------------------------------------------

#FIles Used
1. reason_code.csv


File Type -- Comma Delimited
File Location -- /internal/reason_code.csv


## Business Key

RSN_CD