# FUND

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[all_fund_cd](#all_fund_cd)|character varying|10|YES||NO
|[all_fund_desc](#all_fund_desc)|character varying|25|YES||NO
|[shr_cls_desc](#shr_cls_desc)|character varying|100|YES||NO
|[fund_nm](#fund_nm)|character varying|50|YES||NO
|[fund_long_desc](#fund_long_desc)|character varying|255|YES||NO
|[fund_abrv](#fund_abrv)|character varying|50|YES||NO
|[fund_stat_cd](#fund_stat_cd)|character varying|1|YES||NO
|[fund_stat_desc](#fund_stat_desc)|character varying|50|YES||NO
|[cusip_id](#cusip_id)|character varying|9|YES||NO
|[quot_sym](#quot_sym)|character varying|8|YES||NO
|[st_str_fund_nbr](#st_str_fund_nbr)|character varying|4|YES||NO
|[tax_id](#tax_id)|character varying|9|YES||NO
|[divd_freq_cd](#divd_freq_cd)|character varying|1|YES||NO
|[divd_freq_desc](#divd_freq_desc)|character varying|100|YES||NO
|[cap_gain_freq_cd](#cap_gain_freq_cd)|character varying|1|YES||NO
|[cap_gain_freq_desc](#cap_gain_freq_desc)|character varying|100|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|NO||NO
|[fund_compst_key](#fund_compst_key)|integer|(32,0)|YES||NO
|[shr_cls_cd](#shr_cls_cd)|numeric|(38,15)|YES||NO
|[fund_nbr](#fund_nbr)|numeric|(38,15)|YES||NO
|[fund_sort_ord](#fund_sort_ord)|numeric|(38,15)|YES||NO
|[addr_id](#addr_id)|numeric|(38,15)|YES||NO
|[risk_lvl](#risk_lvl)|numeric|(38,15)|YES||NO
|[reg_acct_init_invmt](#reg_acct_init_invmt)|numeric|(38,15)|YES||NO
|[ira_acct_init_invmt](#ira_acct_init_invmt)|numeric|(38,15)|YES||NO
|[ugma_acct_init_invmt](#ugma_acct_init_invmt)|numeric|(38,15)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[fund_incpn_dt](#fund_incpn_dt)|timestamp without time zone|0|YES||NO
|[fund_clos_dt](#fund_clos_dt)|timestamp without time zone|0|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|0|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|0|YES||NO
|[perf_incpn_dt](#perf_incpn_dt)|timestamp without time zone|0|YES||NO
### fund_key
#### Description



#### Value Range

NVARCHAR

#### Logic

Auto Increment sequence Generator 



### all_fund_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.COMPST_FUND_ID



### all_fund_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

# not available




### fund_compst_key
#### Description



#### Value Range

NVARCHAR

#### Logic



We need to perform join on file "fund_composite.csv"

COMPST_FUND_KEY - Get details from file "fund_composite.csv" 
where  COMPST_FUND_ID =fund.COMPST_FUND_ID


### shr_cls_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

fund.SHR_CLS_CD




### shr_cls_desc
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.SHR_CLS_DESC



### fund_nm
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.FUND_NM



### fund_long_desc
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.FUND_LONG_DESC



### fund_abrv
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.FUND_ABRV



### fund_stat_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

fund.FUND_STAT_CD




### fund_stat_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

fund.FUND_STAT_DESC




### fund_incpn_dt
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.FUND_INCPN_DT



### fund_clos_dt
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.FUND_CLOS_DT



### fund_nbr
#### Description



#### Value Range

NVARCHAR

#### Logic

fund.FUND_NBR




### fund_sort_ord
#### Description



#### Value Range

NVARCHAR

#### Logic

fund.FUND_SORT_ORD




### addr_id
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.ADDR_ID



### cusip_id
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.CUSIP_ID



### quot_sym
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.QUOT_SYM



### st_str_fund_nbr
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.ST_STR_FUND_NBR



### tax_id
#### Description



#### Value Range

NVARCHAR

#### Logic

fund.TAX_ID




### risk_lvl
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.RSK_LVL



### divd_freq_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

fund.DIVD_FREQ_CD




### divd_freq_desc
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.DIVD_FREQ_DESC



### cap_gain_freq_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.CAP_GAIN_FREQ_CD



### cap_gain_freq_desc
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.CAP_GAIN_FREQ_DESC



### reg_acct_init_invmt
#### Description



#### Value Range

NVARCHAR

#### Logic

fund.REG_ACCT_INIT_INVMT




### ira_acct_init_invmt
#### Description



#### Value Range

NVARCHAR

#### Logic


fund.IRA_ACCT_INIT_INVMT



### ugma_acct_init_invmt
#### Description



#### Value Range

NVARCHAR

#### Logic

fund.UGMA_ACCT_INIT_INVMT




### row_strt_dttm
#### Description



#### Value Range

DATE

#### Logic





### row_stop_dttm
#### Description



#### Value Range

DATE

#### Logic





### curr_row_flg
#### Description



#### Value Range

NVARCHAR

#### Logic





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





### perf_incpn_dt
#### Description



#### Value Range

DATE

#### Logic



fund.PERF_INCPN_DT


#FIles Used 

1.fund.csv
2.fund_composite.csv


File Type : Comma delimited File
 File Location : https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund_composite.csv
 https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund.csv
 
 
 ## Business Key
 FUND_NBR