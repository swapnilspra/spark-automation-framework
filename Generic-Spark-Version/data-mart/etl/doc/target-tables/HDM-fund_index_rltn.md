# FUND_INDEX_RLTN

This table holds the lorem ipsum dolor sit amet.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[prim_bmk_flg](#prim_bmk_flg)|character varying|1|YES||NO
|[gr_ret_inlsn_flg](#gr_ret_inlsn_flg)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[fund_idx_key](#fund_idx_key)|integer|(32,0)|NO||YES
|[fund_compst_key](#fund_compst_key)|integer|(32,0)|YES||NO
|[bmk_idx_key](#bmk_idx_key)|integer|(32,0)|YES||NO
|[perf_run_type_key](#perf_run_type_key)|integer|(32,0)|YES||NO
|[link_prio](#link_prio)|numeric|(38,15)|YES||NO
|[rpt_sort_ord](#rpt_sort_ord)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### fund_idx_key
#### Description

Generated key 

#### Value Range

ALL NUMBERS

#### Logic

sequence generator  Autoincrement +1 for new inserts 




### fund_compst_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE: fund_index_rltn.csv , FUND_COMPOSITE.csv

Condition : COMPST_FUND_ID (FROM fund_index_rltn.csv) =  COMPST_FUND_ID (FUND_COMPOSITE.csv)

Output Column: FUND_COMPST_KEY(FUND_COMPOSITE.CSV)




### bmk_idx_key
#### Description


#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE: fund_index_rltn.csv , BENCHMARK_INDEX.CSV

Condition : IDX_ID (FROM fund_index_rltn.csv) =  IDX_ID (FROM BENCHMARK_INDEX.CSV)

Output Column: BMK_IDX_KEY (FROM BENCHMARK_INDEX.CSV)




### perf_run_type_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE: fund_index_rltn.csv , PERF_RUN_TYPE.CSV

Condition : PERF_APP_CD (FROM fund_index_rltn.csv) =  RUN_TYPE_CD (FROM PERF_RUN_TYPE.CSV)

Output Column: PERF_RUN_TYPE_KEY (FROM PERF_RUN_TYPE.CSV)




### link_prio
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LINK_PRIO



### rpt_sort_ord
#### Description


#### Value Range

ALL NUMBERS

#### Logic

Not Avaiable in source file




### prim_bmk_flg
#### Description



#### Value Range

ALL STRING VALUES

#### Logic

PRIM_BMK_FLG



### gr_ret_inlsn_flg
#### Description



#### Value Range

ALL STRING VALUES

#### Logic

GR_RET_INLSN_FLG




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

###files:

fund_index_rltn.csv
BENCHMARK_INDEX.CSV
PERF_RUN_TYPE.CSV
FUND_COMPOSITE.csv



File type : Comma Delimited
File Path: https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund_index_rltn.csv
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/benchmark.csv
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/perf_run_type.csv
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund_composite.csv


## Business key

fund_compst_key
bmk_idx_key
perf_run_type_key


