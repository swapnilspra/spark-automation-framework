# FUND_COMPOSITE

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[all_compst_cd](#all_compst_cd)|character varying|10|YES||NO
|[all_compst_desc](#all_compst_desc)|character varying|20|YES||NO
|[asset_cls_nm](#asset_cls_nm)|character varying|40|YES||NO
|[asset_cls_desc](#asset_cls_desc)|character varying|255|YES||NO
|[sub_cls_nm](#sub_cls_nm)|character varying|40|YES||NO
|[sub_cls_desc](#sub_cls_desc)|character varying|255|YES||NO
|[styl_nm](#styl_nm)|character varying|40|YES||NO
|[styl_desc](#styl_desc)|character varying|255|YES||NO
|[compst_nm](#compst_nm)|character varying|255|YES||NO
|[perf_cd](#perf_cd)|character varying|20|YES||NO
|[perf_nm](#perf_nm)|character varying|255|YES||NO
|[compst_stat_cd](#compst_stat_cd)|character varying|1|YES||NO
|[compst_stat_desc](#compst_stat_desc)|character varying|50|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[fund_styl](#fund_styl)|character varying|100|YES||NO
|[sales_cat](#sales_cat)|character varying|60|YES||NO
|[st_str_fund_nbr](#st_str_fund_nbr)|character varying|25|YES||NO
|[fund_compst_key](#fund_compst_key)|integer|(32,0)|NO||YES
|[compst_cd](#compst_cd)|integer|(32,0)|NO||NO
|[compst_ord](#compst_ord)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[compst_src_id](#compst_src_id)|integer|(32,0)|YES||NO
|[risk_ret_ord](#risk_ret_ord)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
|[compst_incpn_dt](#compst_incpn_dt)|timestamp without time zone|6|YES||NO

## TABLE INSERT OR UPDATE LOGIC

If the record compst_cd exist then update the existing record
if not exist then insert new recored from file source 


### fund_compst_key
#### Description

Generated key 

#### Value Range

ALL NUMBERS

#### Logic
 
sequence generator  Autoincrement +1 for new inserts 



### all_compst_cd
#### Description

#### Value Range

Always value  'All'

#### Logic

Always value  'All'

### all_compst_desc
#### Description

#### Value Range

Always value 'All Composites'

#### Logic

Always value 'All Composites'

### asset_cls_nm
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

ASSET_CLS_NM




### asset_cls_desc
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

ASSET_CLS_DESC



### sub_cls_nm
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

SUB_CLS_NM




### sub_cls_desc
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

SUB_CLS_DESC




### styl_nm
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

STYL_NM



### styl_desc
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

STYL_DESC




### compst_cd
#### Description

Business key for insert or update rows


#### Value Range

all numbers

#### Logic

COMPST_FUND_ID




### compst_nm
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

COMPST_NM




### perf_cd
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

PERF_CD 




### perf_nm
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

PERF_NM




### compst_ord
#### Description

#### Value Range

ALL NUMBERS

#### Logic

COMPST_ORD




### compst_stat_cd
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

COMPST_STAT_CD




### compst_stat_desc
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

COMPST_STAT_DESC

INITCAP function - sets the first letter of each word in uppercase


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





### curr_row_flg
#### Description

#### Value Range

Always value 'Y'

#### Logic


Always value 'Y'



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


### compst_src_id
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source fund_composite.csv 



### fund_styl
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

FUND_STYL



### risk_ret_ord
#### Description

#### Value Range

ALL NUMBERS

#### Logic

RISK_RET_ORD




### compst_incpn_dt
#### Description

#### Value Range

ALL DATE VALUES

#### Logic

COMPST_INCPN_DT




### sales_cat
#### Description

#### Value Range

AL STRING VALUES

#### Logic

SALES_CAT 




### st_str_fund_nbr
#### Description

#### Value Range

ALL STRING VALUES

#### Logic

ST_STR_FUND_NBR



## Source files

FUND_COMPOSITE.CSV

 File Type : Comma delimited File
 File Location : https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund_composite.csv
 
 ## Business KEY
 
 COMPST_FUND_ID
 