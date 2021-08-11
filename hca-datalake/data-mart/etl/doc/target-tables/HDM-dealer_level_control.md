# DEALER_LEVEL_CONTROL

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[dlr_lvl_cntl_cd](#dlr_lvl_cntl_cd)|character varying|3|NO||NO
|[dlr_lvl_cntl_desc](#dlr_lvl_cntl_desc)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[dlr_lvl_cntl_key](#dlr_lvl_cntl_key)|integer|(32,0)|NO||YES
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|NO||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### dlr_lvl_cntl_key
#### Description



#### Value Range

NVARCHAR

#### Logic

Auto Increment Key



### dlr_lvl_cntl_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

DLR_LVL_CNTL_CD




### dlr_lvl_cntl_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

DLR_LVL_CNTL_DESC




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

----------------------------------------------------------------------


#Files Used
1. dealer_level_control.csv

File Type -- Comma Delimited
File Location -- /internal/dealer_level_control.csv

## Business Key
DLR_LVL_CNTL_CD