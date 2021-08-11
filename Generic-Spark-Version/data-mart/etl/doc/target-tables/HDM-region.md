# REGION

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[st_cd](#st_cd)|character varying|3|NO||NO
|[st_nm](#st_nm)|character varying|60|YES||NO
|[mktg_reg_nm](#mktg_reg_nm)|character varying|60|YES||NO
|[crty_cd](#crty_cd)|character varying|3|NO||NO
|[crty_nm](#crty_nm)|character varying|20|YES||NO
|[glbl_reg_cd](#glbl_reg_cd)|character varying|3|NO||NO
|[glbl_reg_nm](#glbl_reg_nm)|character varying|60|YES||NO
|[reg_type_cd](#reg_type_cd)|character varying|3|NO||NO
|[reg_type_nm](#reg_type_nm)|character varying|60|YES||NO
|[top_lvl_reg_cd](#top_lvl_reg_cd)|character varying|3|NO||NO
|[top_lvl_reg_nm](#top_lvl_reg_nm)|character varying|20|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[reg_key](#reg_key)|integer|(32,0)|NO||YES
|[mktg_reg_id](#mktg_reg_id)|numeric|(38,15)|NO||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### reg_key
#### Description


#### Value Range

NVARCHAR

#### Logic

Auto Increment Key


### st_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

ST_CD




### st_nm
#### Description



#### Value Range

NVARCHAR

#### Logic

ST_NM




### mktg_reg_id
#### Description



#### Value Range

NVARCHAR

#### Logic

MKTG_REG_ID




### mktg_reg_nm
#### Description



#### Value Range

NVARCHAR

#### Logic

MKTG_REG_NM




### crty_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

CRTY_CD




### crty_nm
#### Description



#### Value Range

NVARCHAR

#### Logic

CRTY_NM




### glbl_reg_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

GLBL_REG_CD




### glbl_reg_nm
#### Description



#### Value Range

NVARCHAR

#### Logic

GLBL_REG_NM




### reg_type_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

REG_TYPE_CD




### reg_type_nm
#### Description



#### Value Range

NVARCHAR

#### Logic





### top_lvl_reg_cd
#### Description



#### Value Range

NVARCHAR

#### Logic





### top_lvl_reg_nm
#### Description



#### Value Range

NVARCHAR

#### Logic





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


----------------------------------------------------------------

#Files Used
1. region.csv


File Type -- Comma Delimited
File Location -- /internal/region.csv


## Business Key

MKTG_REG_ID

REG_TYPE_CD

ST_CD

GLBL_REG_CD

MKTG_REG_ID

CRTY_CD




