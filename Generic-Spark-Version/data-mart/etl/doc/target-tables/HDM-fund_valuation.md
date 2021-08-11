# FUND_VALUATION

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[fund_vltn_key](#fund_vltn_key)|integer|(32,0)|NO||YES
|[fund_key](#fund_key)|integer|(32,0)|NO||NO
|[day_key](#day_key)|integer|(32,0)|NO||YES
|[nav_amt](#nav_amt)|numeric|(38,15)|YES||NO
|[tot_shrs](#tot_shrs)|numeric|(38,15)|YES||NO
|[pr_per_shr](#pr_per_shr)|numeric|(38,15)|YES||NO
|[wlth_idx](#wlth_idx)|integer|(32,0)|YES||NO
|[asof_pr](#asof_pr)|numeric|(38,15)|YES||NO
|[daily_pr_diff](#daily_pr_diff)|numeric|(38,15)|YES||NO
|[asof_wlth_idx](#asof_wlth_idx)|integer|(32,0)|YES||NO
|[hsg_pr_revs](#hsg_pr_revs)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### fund_vltn_key
#### Description

Generated key 

#### Value Range

ALL NUMBERS

#### Logic
 
sequence generator  Autoincrement +1 for new inserts 


### fund_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

FUND

FILE: FUND.ssbnav<dt>.txt , Fund.csv

Condition : FUND (FROM ssbnav<dt>.txt) = FUND_NBR (FROM Fund.csv)

Output Column: FUND_KEY (FROM Fund.csv )


### day_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE: PRICE_EFF_DATE.ssbnav<dt>.txt ,CALENDAR.CSV

Condition : PRICE_EFF_DATE (FROM ssbnav<dt>.txt) =  CAL_DAY (FROM CALENDAR.CSV)

Output Column: DAY_KEY (FROM CALENDAR.CSV)


### nav_amt
#### Description

#### Value Range

ALL NUMBERS

#### Logic
PRICE                    PIC'(9)9V(6)9'


### tot_shrs
#### Description

#### Value Range

ALL NUMBERS

#### Logic

SHARES_OUTSTNDG          PIC'(12)9V(3)9'



We are joining with FUND.csv
Condition : FUND (FROM ssbnav<dt>.txt) = FUND_NBR (FROM Fund.csv)
to get the SHARES_OUTSTNDG based on the match


### pr_per_shr
#### Description

#### Value Range

ALL NUMBERS

#### Logic

PRICE                    PIC'(9)9V(6)9'

Join

Condition : FUND (FROM ssbnav<dt>.txt) = FUND_NBR (FROM Fund.csv)
if the condition is match populate PRICE columns


### wlth_idx
#### Description

#### Value Range

ALL NUMBERS

#### Logic

Wealth index needs to be calculated in the code

FURMULA : 
CALCULATION OF DAILY WEALTH INDEX
r = Daily Return
c = Current Day NAV
p =Previous Day NAV
d = Dividend for the day
w = Wealth Index for current day
wp = wealth Index for previous day
r = (c + d) / p - 1
w = 1 * wp * (1 + r)

Current Day NAV = fund_valuation.pr_per_shr for TODAY
Previous Day NACV = fund_valuation.pr_per_shr for YESTERDAY 
Dividend for the day = fund_dividend.tot_divd for TODAY (this can be null)


### asof_pr
#### Description

#### Value Range

ALL NUMBERS

#### Logic
PRICE                    PIC'(9)9V(6)9'

same as the value pr_per_shr

### daily_pr_diff
#### Description

#### Value Range

ALL NUMBERS

#### Logic

PRICE_EFF_DATE  PIC'(8)9'
join with fund_divedend.csv 

FUND (FROM ssbnav<dt>.txt) = FUND_NBR (FROM fund_divedend.csv) and
substr(PRICE_EFF_DATE) (FROM ssbnav<dt>.txt) = REINVEST_DATE (FROM fund_divedend.csv)

 ORD_INCOME
 CAP_GAINS_SHORT
 CAP_GAINS_LONG 
 NON_TAXABLE_DIS

(PRICE - PREVIOUS_PRICE+ORD_INCOME+CAP_GAINS_SHORT+CAP_GAINS_LONG + NON_TAXABLE_DIS) / PREVIOUS_PRICE


Previous price is the price of the previous day(Yesterday). 
the calcualtion is as follows
iif(isnull(PREVIOUS_PRICE),PRICE,PREVIOUS_PRICE)

In Case of Holidays - The price of the previous day is populted into this column

### asof_wlth_idx
#### Description

#### Value Range

ALL NUMBERS

#### Logic

Just copy the same value as wlth_idx


### hsg_pr_revs
#### Description

#### Value Range

ALL NUMBERS

#### Logic

Not loading this columns


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



## Sources

###files:

ssbnav<dt>.txt
FUND.csv
CALENDAR.csv
fund_divedend.csv


## Note
We need to populate the a row for holidays which is same as previous day


