# AFTER_TAX_RETURN

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[atax_ret_key](#atax_ret_key)|integer|(32,0)|NO||YES
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[per_key](#per_key)|integer|(32,0)|YES||NO
|[sold_amt](#sold_amt)|numeric|(38,15)|YES||NO
|[cb_amt](#cb_amt)|numeric|(38,15)|YES||NO
|[gls_amt](#gls_amt)|numeric|(38,15)|YES||NO
|[st_tax_gls](#st_tax_gls)|numeric|(38,15)|YES||NO
|[lt_tax_gls](#lt_tax_gls)|numeric|(38,15)|YES||NO
|[tax_amt](#tax_amt)|numeric|(38,15)|YES||NO
|[net_prcd_atax](#net_prcd_atax)|numeric|(38,15)|YES||NO
|[annlzn_fctr](#annlzn_fctr)|numeric|(38,15)|YES||NO
|[atax_ret_wo_sale](#atax_ret_wo_sale)|numeric|(38,15)|YES||NO
|[atax_ret](#atax_ret)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[strt_per](#strt_per)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### atax_ret_key
#### Description



#### Value Range

N/A

#### Logic







### day_key
#### Description



#### Value Range

N/A

#### Logic







### fund_key
#### Description



#### Value Range

N/A

#### Logic







### per_key
#### Description



#### Value Range

N/A

#### Logic







### sold_amt
#### Description



#### Value Range

N/A

#### Logic







### cb_amt
#### Description



#### Value Range

N/A

#### Logic







### gls_amt
#### Description



#### Value Range

N/A

#### Logic







### st_tax_gls
#### Description



#### Value Range

N/A

#### Logic







### lt_tax_gls
#### Description



#### Value Range

N/A

#### Logic







### tax_amt
#### Description



#### Value Range

N/A

#### Logic







### net_prcd_atax
#### Description



#### Value Range

N/A

#### Logic







### strt_per
#### Description



#### Value Range

N/A

#### Logic







### annlzn_fctr
#### Description



#### Value Range

N/A

#### Logic







### atax_ret_wo_sale
#### Description



#### Value Range

N/A

#### Logic







### atax_ret
#### Description



#### Value Range

N/A

#### Logic







### curr_row_flg
#### Description



#### Value Range

N/A

#### Logic







### row_strt_dttm
#### Description



#### Value Range

N/A

#### Logic







### row_stop_dttm
#### Description



#### Value Range

N/A

#### Logic







### etl_load_cyc_key
#### Description



#### Value Range

N/A

#### Logic







### src_sys_id
#### Description



#### Value Range

N/A

#### Logic



##

Procedure location: https://github.com/harborcapital/hca-datalake/pull/435
Procedure Name: pkg_after_tax


LOAD_AFTER_TAX_RETURN -- P_DATE = last day of the previous month

LOAD_AFTER_TAX_CALC must load before LOAD_AFTER_TAX_RETURN 

runs once a year
We need to run twice 
once for the p_target_flg = y and once for p_target_flg ='N'

if we running in jan its dec31 will be run date


Query used to load the table
SELECT ROWNUM + t_last_key atax_ret_key,DAY_KEY,FUND_KEY,PER_KEY,SOLD_AMT,CB_AMT,GLS_AMT,ST_TAX_GLS,LT_TAX_GLS,
         TAX_AMT,NET_PRCD_ATAX,STRT_PER,ANNLZN_FCTR,ATAX_RET_WO_SALE,ATAX_RET
         FROM TABLE(PKG_AFTER_TAX.GET_AFTER_TAX_RETURN(P_END_DATE))



