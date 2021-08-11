# SDCM_FUND_BALANCE

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[fund_bal_key](#fund_bal_key)|integer|(32,0)|NO||YES
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[tot_fund_shrs](#tot_fund_shrs)|numeric|(38,15)|YES||NO
|[tot_fund_asset](#tot_fund_asset)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### fund_bal_key
#### Description



#### Value Range

N/A

#### Logic

Auto Increment Sequence Generator



### fund_key
#### Description



#### Value Range

N/A

#### Logic


FUND.FUND_KEY

Join with FUND Table 

BALANCE.FUND-CODE = FUND.FUND_NBR
 

### day_key
#### Description



#### Value Range

N/A

#### Logic


CALENDAR.CAL_DAY_KEY

Join on CALENDAR Table

BALANCE.SUPER-SHEET-DTE = CALENDAR.CAL_DAY



### tot_fund_shrs
#### Description



#### Value Range

N/A

#### Logic


BALANCE.SHARES-OUTSTANDING-AMT


### tot_fund_asset
#### Description



#### Value Range

N/A

#### Logic


Join with FUND Table 

BALANCE.FUND-CODE = FUND.FUND_NBR

Get FUND.FUND_KEY 

FUND.FUND_KEY= FUND_VALUATION.FUND_KEY
 

(BALANCE.SHARES-OUTSTANDING-AMT * FUND_VALUATION.nav_amt)




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




### Files Used
1. SDCM.BALANCE.CBL (DTO.HCA.SDCM.BALANCE.YYYYMMDD.HH24MISS.ZIP.PGP.TXT)


## File Location


dstprod/DTO.HCA.SDCM.BALANCE.YYYYMMDD.HH24MISS.ZIP.PGP.TXT


### Tables Used

2. CALENDAR
3. FUND
4. FUND_VALUATION


### Business Keys


