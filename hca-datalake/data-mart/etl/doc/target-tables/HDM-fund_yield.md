# FUND_YIELD

This table holds the lorem ipsum dolor sit amet.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[fund_yld_key](#fund_yld_key)|integer|(32,0)|NO||YES
|[fund_key](#fund_key)|integer|(32,0)|NO||YES
|[day_key](#day_key)|integer|(32,0)|NO||YES
|[yld_type_key](#yld_type_key)|integer|(32,0)|NO||NO
|[fund_yld](#fund_yld)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### fund_yld_key
#### Description
 
#### Value Range
ALL NUMBERS
#### Logic

sequence generator  Autoincrement +1 for new inserts 


### fund_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE: Fund.csv

Condition : Alternate Fund ID (FROM SEC_YLD<dlt>.CSV) = FUND_NBR (FROM Fund.csv)

Output Column: FUND_KEY (FROM Fund.csv )


### day_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE:  CALENDAR.csv

Condition : DATE (FROM SEC_YLD<dlt>.CSV) =  CAL_DAY (FROM CALENDAR.csv)

Output Column: DAY_KEY (FROM CALENDAR.csv)



### yld_type_key
#### Description


#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE: SEC_YLD_071919.csv 

Condition :  YLD_CODE (FROM) =  YLD_CD (FROM SEC_YLD_071919.csv)

Output Column: YLD_TYPE_KEY (FROM SEC_YLD_071919.csv)


### fund_yld
#### Description

#### Value Range

ALL NUMBERS

#### Logic

SEC_YLD_in,30NOCU    30-Day Current (-G/L)
SEC_YLD_WO_in, U30NC    Unsubsidized 30-Day Current (-G/L)
power((SEC_YLD_in*30/DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1, 30NOEF    30-Day Effective (-G/L)
power((SEC_YLD_WO_in*30/DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1, U30NE    Unsubsidized 30-Day Effective (-G/L)
DAY_30_TIP_v,30TIPNOCU    30-Day TIPS Current (-G/L), 30TIPNOCU    30-Day TIPS Current (-G/L)
DAY_30_TIP_WO_v,U30TIPNOCU    Unsubsidized 30-Day TIPS Current (-G/L)
power((DAY_30_TIP_v*30/DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1,30TIPNOEF    30-Day TIPS Effective (-G/L)
power((DAY_30_TIP_WO_v*30/DAYS_IN_YEAR_v+1),(DAYS_IN_YEAR_v/30))-1), U30TIPNE    Unsubsidized 30-Day TIPS Effective (-G/L)


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

SEC_YLD<dlt>.CSV
Fund.csv
CALENDAR.csv
yield_types.csv

File Type : Comma Delimited
File Location: 
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/ssb/SEC_YLD_071919.CSV
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund.csv
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/yield_types.csv








