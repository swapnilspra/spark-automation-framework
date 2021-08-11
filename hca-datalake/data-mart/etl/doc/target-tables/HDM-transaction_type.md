# TRANSACTION_TYPE

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[txn_cd_desc](#txn_cd_desc)|character varying|60|YES||NO
|[txn_type_desc](#txn_type_desc)|character varying|60|YES||NO
|[top_lvl_txn_cd](#top_lvl_txn_cd)|character varying|3|NO||NO
|[top_lvl_txn_desc](#top_lvl_txn_desc)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[shr_bal_effct_flg](#shr_bal_effct_flg)|character varying|1|YES||NO
|[txn_type_key](#txn_type_key)|integer|(32,0)|NO||YES
|[txn_sfx_cd](#txn_sfx_cd)|numeric|(38,15)|YES||NO
|[txn_cd](#txn_cd)|numeric|(38,15)|YES||NO
|[txn_type_cd](#txn_type_cd)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|NO||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### txn_type_key
#### Description

#### Value Range

NVARCHAR

#### Logic

Auto Increment Key

### txn_sfx_cd
#### Description

Transaction Suffix Code

#### Value Range

NVARCHAR

#### Logic

TXN_SFX_CD - transcation_type.TRANS_SUFFIX

### txn_cd
#### Description

Transaction Code

#### Value Range

NVARCHAR

#### Logic

TXN_CD - transcation_type.TRANS_CODE

### txn_cd_desc
#### Description

Transaction Code Description

#### Value Range

NVARCHAR

#### Logic

TXN_CD_DESC - transaction_type.VOL_TRAN_DESC

### txn_type_cd
#### Description

TRansaction Type Code

#### Value Range

NVARCHAR

#### Logic

TXN_TYPE_CD - transaction_group.TXN_TYPE_CD

### txn_type_desc
#### Description

Transaction Type Description
#### Value Range

NVARCHAR

#### Logic
TXN_TYPE_DESC - transaction_group.TXN_TYPE_DESC

### top_lvl_txn_cd
#### Description

Top Level Transaction Code

#### Value Range

NVARCHAR

#### Logic

Fixed Value - 'All'

### top_lvl_txn_desc
#### Description

Top Level Transaction Description

#### Value Range

NVARCHAR

#### Logic

Fixed - 'All Transactions'


### curr_row_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


Fixed value 'Y'


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





### shr_bal_effct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

SHR_BAL_EFFCT_FLG - transaction_type.BALANCE_INDICATOR


----------------------------------------------------------------

## Files Used
1. transaction_group.csv
2. transaction_type.csv


File Type -- Comma Delimited
File Location -- /powerselect/transaction_type.csv
Dimensiom     -- transaction_group.csv


## Business Key

txn_sfx_cd
txn_cd

Match the TRANS_CODE & TRANS_SUFFIX from transaction_type.csv with TRANS_CODE & TRANS_SUFFIX from transaction_group.csv. The join is would be a left join.
