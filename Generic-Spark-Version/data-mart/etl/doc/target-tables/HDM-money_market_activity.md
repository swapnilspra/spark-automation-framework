# MONEY_MARKET_ACTIVITY

## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[mony_mkt_key](#mony_mkt_key)|integer|(32,0)|NO||YES
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[gain_loss_amt](#gain_loss_amt)|numeric|(38,15)|YES||NO
|[dstrbtn_amt](#dstrbtn_amt)|numeric|(38,15)|YES||NO
|[divd_paybl_amt](#divd_paybl_amt)|numeric|(38,15)|YES||NO
|[basis_points](#basis_points)|numeric|(38,15)|YES||NO
|[days_to_mtry](#days_to_mtry)|numeric|(38,15)|YES||NO
|[intr_w_gain_loss](#intr_w_gain_loss)|numeric|(38,15)|YES||NO
|[intr_wo_gain_loss](#intr_wo_gain_loss)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[weighted_avg_life](#weighted_avg_life)|numeric|(38,15)|YES||NO
|[net_asset_at_cost](#net_asset_at_cost)|numeric|(38,15)|YES||NO
|[net_asset_at_mkt](#net_asset_at_mkt)|numeric|(38,15)|YES||NO
|[invmt_at_cost](#invmt_at_cost)|numeric|(38,15)|YES||NO
|[invmt_at_val](#invmt_at_val)|numeric|(38,15)|YES||NO
|[mkt_based_pr](#mkt_based_pr)|numeric|(38,15)|YES||NO
|[sub_w_drv_in_amt](#sub_w_drv_in_amt)|numeric|(38,15)|YES||NO
|[redmpn_amt](#redmpn_amt)|numeric|(38,15)|YES||NO
|[net_shrhldr_cash_flow_amt](#net_shrhldr_cash_flow_amt)|numeric|(38,15)|YES||NO
|[daily_lqd_asset_amt](#daily_lqd_asset_amt)|numeric|(38,15)|YES||NO
|[daily_lqd_asset_rt](#daily_lqd_asset_rt)|numeric|(38,15)|YES||NO
|[wkl_lqd_asset_amt](#wkl_lqd_asset_amt)|numeric|(38,15)|YES||NO
|[wkl_lqd_asset_rt](#wkl_lqd_asset_rt)|numeric|(38,15)|YES||NO
|[shdw_nav](#shdw_nav)|numeric|(38,15)|YES||NO
|[cnst_nav](#cnst_nav)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### mony_mkt_key
#### Description

Generated key

#### Value Range

ALL NUMBERS

#### Logic

sequence generator  Autoincrement +1 for new inserts 



### fund_key
#### Description

#### Value Range

#### Logic

LOOKUP

FILE: HDM_FUND,

FUND_NBR = FUND_NBR (FROM HDM_FUND)

OUTPUT COLUMN: FUND_KEY




### day_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE: HDM_CALENDER, MMKT_info

Condition :  DATE (FROM MMKT_info) =  CAL_DAY (FROM HDM_CALENDER)

Output Column: DAY_KEY (FROM HDM_CALENDAR)




### gain_loss_amt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source MMKT_info.CSV 

column name= GAIN_LOSS



### dstrbtn_amt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source MMKT_info.CSV 

column name= DISTRIBUTION




### divd_paybl_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source MMKT_info.CSV 

column name= DIVIDEND_PAYABLE



### basis_points
#### Description

Lorem ipsum dolor sit amet

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source MMKT_info.CSV 

column name= BASIS_POINTS


### days_to_mtry
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source MMKT_info.CSV 

column name= DAYS_TO_MATURITY



### intr_w_gain_loss
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source RATES.csv

column name= Exchange Rate

condition:  WITH GL = Y



### intr_wo_gain_loss
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source RATES.csv

column name= Exchange Rate

condition:  WITH GL = N



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

#### Logic
the row needs to be empty 
when we update the row we insert sysdate formula



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
 


### weighted_avg_life
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source MMKT_info_mmddyy.CSV 

column name= WEIGHTED_AVG_LIFE



### net_asset_at_cost
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source GB05_Comparison_NAV_Fund_Summary.csv

column name= Amortized Cost Net Assets



### net_asset_at_mkt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source GB05_Comparison_NAV_Fund_Summary.csv

column name= Net Assets Including Market Value




### invmt_at_cost
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source MM_INVEST.csv

column name= amortized cost



### invmt_at_val
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source MM_INVEST.csv

column name= MARKET VALUE



### mkt_based_pr
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file Source GB05_Comparison_NAV_Fund_Summary.csv

column name= Current_Comparison_NAV



### sub_w_drv_in_amt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file harbor_mm_web_disclosure.txt

column name= SUBSCRIPTIONS_INCLUDING_DIVREIN



### redmpn_amt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file harbor_mm_web_disclosure.txt

column name= REDEMPTIONS



### net_shrhldr_cash_flow_amt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file harbor_mm_web_disclosure.txt

column name= NET_SHAREHOLDER_FLOWS



### daily_lqd_asset_amt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file harbor_mm_web_disclosure.txt

column name= DAILY_LIQUID_ASSETS_DOLLAR_AMOUNT



### daily_lqd_asset_rt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file harbor_mm_web_disclosure.txt

column name= DAILY_LIQUID_ASSETS_PERCENT



### wkl_lqd_asset_amt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file harbor_mm_web_disclosure.txt

column name= WEEKLY_LIQUID_ASSETS_DOLLAR_AMOUNT



### wkl_lqd_asset_rt
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file harbor_mm_web_disclosure.txt

column name= WEEKLY_LIQUID_ASSETS_PERCENT





### shdw_nav
#### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file harbor_mm_web_disclosure.txt

column name= SHADOWNAV




### cnst_nav
### Description

#### Value Range

ALL NUMBERS

#### Logic

As is from file harbor_mm_web_disclosure.txt

column name= CONSTANTNAV


## Sources

###files:

harbor_mm_web_disclosure.txt
GB05_Comparison_NAV_Fund_Summary.csv
MMKT_info.csv
RATES.csv
hdm_fund
MM_INVEST.csv

how to marge the files?


harbor_mm_web_disclosure.FUND_CODE = GB05_Comparison_NAV_Fund_Summary.Fund
and harbor_mm_web_disclosure.BASIS_IND = GB05_Comparison_NAV_Fund_Summary.Dual Pricing Basis
---- need to take only the rows where BASIS_IND and Dual Pricing Basis only a number not a letter

MM_INVEST.MCH FUND NUMBER = harbor_mm_web_disclosure.FUND_CODE 
and MM_INVEST.BASIS = harbor_mm_web_disclosure.BASIS_IND
--- need to take only the rows where BASIS_IND and BASIS only a number not a letter


harbor_mm_web_disclosure.FUND_CODE  = hdm_fund.ST_STR_FUND_NBR (bring FUND_NBR COLUMN ONLY)

hdm_fund.FUND_NBR = MMKT_info.Alternate Fund ID

MMKT_info.Alternate Fund ID = RATES.ssb_fund





## EXCEL MAPPING FILE
m_DIM_ADW_HDM_MONEY_MARKET_ACTIVITY


