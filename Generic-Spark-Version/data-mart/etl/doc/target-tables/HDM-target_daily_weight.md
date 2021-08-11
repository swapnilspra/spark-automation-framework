# TARGET_DAILY_WEIGHT

This table holds the lorem ipsum dolor sit amet.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[trgt_daily_wgt_key](#trgt_daily_wgt_key)|integer|(32,0)|NO||YES
|[trgt_fund_key](#trgt_fund_key)|integer|(32,0)|YES||NO
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[shr_qty](#shr_qty)|numeric|(38,15)|YES||NO
|[mkt_val](#mkt_val)|numeric|(38,15)|YES||NO
|[daily_holdg_wgt](#daily_holdg_wgt)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### trgt_daily_wgt_key
#### Description

Target Daily Weights Key

#### Value Range

N/A

#### Logic

```
Autoincrement +1 for new inserts
```

### trgt_fund_key
#### Description

Target Fund Key

#### Logic

```
file.FUND_ID = FUND.STATE_STR_FUND_NBR where FUND.CLS_ID=1
```

### day_key
#### Description

Daily Weights Day Key

#### Logic

```
file.CALEN_DT = CALENDAR.CAL_DAY
```

### fund_key
#### Description

FUND KEY

#### Logic

```
File.TICKER_SYMBOL = FUND.QUOT_SYM where FUND.CLS_ID = 1
```

### shr_qty
#### Description

Share Quantity

#### Logic

```
File.SHRPAR_QTY
```

### mkt_val
#### Description

Market Value

#### Logic

```
file.MKTVAL_BTL
```

### daily_holdg_wgt
#### Description

Daily Holdings Weight

#### Logic

Aggregate the product of shrpar_qty and mktprc_bam   (shrpar_qty * mktprc_bam ) over each fund and holding date.

```
      (file.SHRPAR_QTY * fund.valuation.rt_per_shr_amt) / SUM (file.SHRPAR_QTY * fund.valuation.rt_per_shr_amt)
            OVER (PARTITION BY file.CALEN_DT, file.FUND_ID)
-- Important to note that the file.CALEN_DT and file.FUND_ID should be matched to fund_valuation.vltn_dt and fund_valuation.fund_key

```

### curr_row_flg
#### Description

Current Row FLag

#### Logic

```
'Y'
```

### row_strt_dttm
#### Description

Row Start Date time

#### Logic

```
Row insert date time
```

-------
Files Used - StateStreet Bank's ssb_hlds.txt
Frequency - Daily (Business Days)

Joins

FUND, FUND_VALUATION, CALENDAR

Business Keys - (TRGT_FUND_KEY, DAY_KEY, FUND_KEY)

