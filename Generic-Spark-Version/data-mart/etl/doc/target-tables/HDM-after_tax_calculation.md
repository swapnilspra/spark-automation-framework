# AFTER_TAX_CALCULATION

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[atax_calc_key](#atax_calc_key)|integer|(32,0)|NO||YES
|[reinv_day_key](#reinv_day_key)|integer|(32,0)|YES||NO
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[per_key](#per_key)|integer|(32,0)|YES||NO
|[tax_yr](#tax_yr)|numeric|(38,15)|YES||NO
|[cb_amt](#cb_amt)|numeric|(38,15)|YES||NO
|[shrs_calc](#shrs_calc)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### atax_calc_key
#### Description



#### Value Range

N/A

#### Logic


Auto Increment Sequence Generator



### reinv_day_key
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






### tax_yr
#### Description



#### Value Range

N/A

#### Logic






### cb_amt
#### Description



#### Value Range

N/A

#### Logic






### shrs_calc
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

Loading Information

We must load this table after loading table with LOAD_AFTER_TAX_WI procedure

P_DATE - Last day of the Month, For Example if we are running in January. The P_DATE will be 31/Dec
LOAD_AFTER_TAX_CALC - same as above but runs once a year
We need to run twice 
once for the p_target_flg = 'Y' and once for p_target_flg ='N'


Query Used to load the table

 SELECT ROWNUM + t_last_key ATAX_CALC_KEY,TAX_YR,REINV_DAY_KEY,DAY_KEY,FUND_KEY,PER_KEY,CB_AMT,SHRS_CALC
            FROM TABLE(pkg_after_tax.get_after_tax_calc(p_end_date, p_target_flg))
			
			
