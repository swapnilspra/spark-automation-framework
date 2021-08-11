# BLUESKY_TRANSACTIONS

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[blue_sky_txn_key](#blue_sky_txn_key)|character|18|NO||YES
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[reg_key](#reg_key)|integer|(32,0)|YES||NO
|[shr_qty](#shr_qty)|numeric|(38,15)|YES||NO
|[txn_amt](#txn_amt)|numeric|(38,15)|YES||NO
|[mtd_rept_amt](#mtd_rept_amt)|numeric|(38,15)|YES||NO
|[mtd_re_open_amt](#mtd_re_open_amt)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### blue_sky_txn_key
#### Description



#### Value Range

N/A

#### Logic

Auto Increment sequence Number



### day_key
#### Description



#### Value Range

N/A

#### Logic

CALENDAR.CAL_KEY

BLUESKY_TRANSACTIONS.DOLLAR-CODE = CALENDAR.CAL_DAY


IIF(ASCII(DOLLAR-CODE)=0,'11111111',DOLLAR-CODE)




### fund_key
#### Description



#### Value Range

N/A

#### Logic

FUND.FUND_KEY

BLUESKY_TRANSACTIONS.FUND-CODE = FUND.FUND_NBR



### reg_key
#### Description



#### Value Range

N/A

#### Logic

REGION.REG_KEY



BLUESKY_TRANSACTIONS.STATE-CODE = REGION.ST_CD

NVL(ST_CD,'XX') AS ST_CD





### shr_qty
#### Description



#### Value Range

N/A

#### Logic

BLUESKY_TRANSACTIONS.SHARE-DATA



### txn_amt
#### Description



#### Value Range

N/A

#### Logic

BLUESKY_TRANSACTIONS.AMOUNT-DATA



### mtd_rept_amt
#### Description



#### Value Range

N/A

#### Logic

BLUESKY_TRANSACTIONS.MTD-REPEAT-AMT 



### mtd_re_open_amt
#### Description



#### Value Range

N/A

#### Logic

BLUESKY_TRANSACTIONS.MTD-REOPEN-AMT



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





### curr_row_flg
#### Description



#### Value Range

N/A

#### Logic





## Files Used

1. BLUESKY_TRANSACTIONS (DTO.HCA.BSK.TIP88.YYYYMMDD.HHMISS.zip.pgp.txt)



### File Location

dstprod/DTO.HCA.BSK.TIP88.20190719.224936.zip.pgp.txt



###Tables Used

2. CALENDAR
3. REGION
4. FUND



### Business Keys

day_key (date)
fund_Key (fund_nbr)
reg_key (st_crty_cd)



###COBOL Copybooks

1. BSK.TIP88.cbl


