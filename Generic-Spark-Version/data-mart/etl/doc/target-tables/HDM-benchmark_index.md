# BENCHMARK_INDEX

## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[idx_id](#idx_id)|character varying|25|YES||NO
|[idx_nm](#idx_nm)|character varying|150|YES||NO
|[idx_shrt_nm](#idx_shrt_nm)|character varying|12|YES||NO
|[bloomberg_sym](#bloomberg_sym)|character varying|25|YES||NO
|[idx_type_desc](#idx_type_desc)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[bmk_idx_key](#bmk_idx_key)|integer|(32,0)|NO||YES
|[robeco_idx_id](#robeco_idx_id)|numeric|(38,15)|YES||NO
|[idx_type](#idx_type)|numeric|(38,15)|YES||NO
|[bmk_legacy_id](#bmk_legacy_id)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### bmk_idx_key
#### Description

Generated key 

#### Value Range

ALL NUMBERS

#### Logic
 
sequence generator  Autoincrement +1 for new inserts 



### idx_id
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

IDX_ID


### idx_nm
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

IDX_NM



### idx_shrt_nm
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

IDX_SHRT_NM


### robeco_idx_id
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

ROBECO_IDX_ID

### bloomberg_sym
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

BLOOMBERG_SYM


### idx_type
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

IDX_TYPE


### idx_type_desc
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

IDX_TYPE_DESC


### bmk_legacy_id
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

BMK_LEGACY_ID



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

benchmark.csv

File Format: Comma Delimited
File Path : https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/benchmark.csv

## Business Key
IDX_ID

