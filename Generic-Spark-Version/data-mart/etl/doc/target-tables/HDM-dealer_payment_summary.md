# DEALER_PAYMENT_SUMMARY

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[wk_ord_nbr](#wk_ord_nbr)|character varying|25|YES||NO
|[compnsn_pyee_type_cd](#compnsn_pyee_type_cd)|character varying|25|YES||NO
|[pre_agreed_sched_ovrd_type](#pre_agreed_sched_ovrd_type)|character varying|25|YES||NO
|[eligbl_compnsn_ovrd_type](#eligbl_compnsn_ovrd_type)|character varying|25|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[trl_mode_flg](#trl_mode_flg)|character varying|1|YES||NO
|[bus_line](#bus_line)|character varying|25|YES||NO
|[dlr_pmt_sum_key](#dlr_pmt_sum_key)|integer|(32,0)|NO||YES
|[dlr_fee_type_key](#dlr_fee_type_key)|integer|(32,0)|YES||NO
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[dlr_key](#dlr_key)|integer|(32,0)|YES||NO
|[dlr_pmt_mthd_key](#dlr_pmt_mthd_key)|integer|(32,0)|YES||NO
|[pre_agree_shr_rt](#pre_agree_shr_rt)|numeric|(38,15)|YES||NO
|[pre_agree_avg_asset](#pre_agree_avg_asset)|numeric|(38,15)|YES||NO
|[pre_agree_compnsn](#pre_agree_compnsn)|numeric|(38,15)|YES||NO
|[eligbl_shr_rt](#eligbl_shr_rt)|numeric|(38,15)|YES||NO
|[eligbl_avg_asset](#eligbl_avg_asset)|numeric|(38,15)|YES||NO
|[eligbl_compnsn](#eligbl_compnsn)|numeric|(38,15)|YES||NO
|[tot_compnsn](#tot_compnsn)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[compnsn_diff](#compnsn_diff)|numeric|(38,15)|YES||NO
|[prev_per_compnsn](#prev_per_compnsn)|numeric|(38,15)|YES||NO
|[avg_asset_diff](#avg_asset_diff)|numeric|(38,15)|YES||NO
|[prev_per_asset](#prev_per_asset)|numeric|(38,15)|YES||NO
|[pmt_day_key](#pmt_day_key)|integer|(32,0)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### dlr_pmt_sum_key
#### Description



#### Value Range

N/A

#### Logic

Auto Increment sequence Generator 


### dlr_fee_type_key
#### Description



#### Value Range

N/A

#### Logic


DEALER_FEE_TYPE.DLR_FEE_TYPE_KEY


Join on DEALER_FEE_TYPE Table


DEALER_PAYMENT_SUMMARY.TLR-CMPN-FEE-CD = DEALER_FEE_TYPE.DLR_FEE_TYPE_CD



### day_key
#### Description



#### Value Range

N/A

#### Logic


CAL_DAY_KEY

Join on CALENDAR Table

DEALER_PAYMENT_SUMMARY.TLR-PYO-PER-END-DT = CALENDAR.CAL_DAY


### fund_key
#### Description



#### Value Range

N/A

#### Logic


FUND.FUND_KEY

Join on FUND Table

DEALER_PAYMENT_SUMMARY.FUND-CODE  = FUND.FUND_NBR  



### dlr_key
#### Description



#### Value Range

N/A

#### Logic


DLR_KEY

Join on DEALER Table


DEALER_PAYMENT_SUMMARY.FINANCIAL-INST-ID = DEALER.DLR_ID



### dlr_pmt_mthd_key
#### Description



#### Value Range

N/A

#### Logic


DLR_PMT_MTHD_KEY

Join on DEALER_PAYMENT_SUMMARY Table
by columns
TLR_PYO_WKO_NBR_ID, 
TLR_PYO_PER_DATE, 
FINANCIAL_INST_ID, 
FUND_CODE, 
TLR_CMPN_FEE_CD
Group by these columns and take min(payment_method_cd)
Left outer join. If not exists, use payment_method_cd = 'NA'

Join with DEALER_PAYMENT_METHOD.PMT_MTHD_CD

Join on DEALER_PAYMENT_TRIAL Table and Get PAYMENT-METHOD-CD

DEALER_PAYMENT_SUMMARY.TLR_PYO_WKO_NBR_ID = DEALER_PAYMENT_DETAIL.TLR_PYO_WKO_NBR_ID(+)
         AND DEALER_PAYMENT_SUMMARY.TLR_PYO_PER_END_DATE = DEALER_PAYMENT_DETAIL.TLR_PYO_PER_DATE(+)
         AND DEALER_PAYMENT_SUMMARY.FUND_CODE = DEALER_PAYMENT_DETAIL.FUND_CODE(+)
         AND DEALER_PAYMENT_SUMMARY.FINANCIAL_INST_ID = DEALER_PAYMENT_DETAIL.FINANCIAL_INST_ID(+)
		 
		 
### wk_ord_nbr
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.TLR-PYO-WKO-NBR-ID


### compnsn_pyee_type_cd
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.CMPN-PAYEE-TYPE-CD


### pre_agreed_sched_ovrd_type
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.PREAGREE-CMPN-SCH-ORR-TYP




### pre_agree_shr_rt
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.PREAGREE-SHARE-RT



### pre_agree_avg_asset
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.PREAGREE-AVERAGE-ASSETS-AT




### pre_agree_compnsn
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.PREAGREE-COMPENSATION-AT



### eligbl_compnsn_ovrd_type
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.ELIGIBLE-CMPN-SCH-ORR-TYP



### eligbl_shr_rt
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.ELIGIBLE-SHARE-RT



### eligbl_avg_asset
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.ELIGIBLE-AVERAGE-ASSETS-AT 



### eligbl_compnsn
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.ELIGIBLE-COMPENSATION-AT



### tot_compnsn
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_SUMMARY.TOTAL-COMPENSATION-AT


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




### compnsn_diff
#### Description



#### Value Range

N/A

#### Logic
DEALER_PAYMENT_SUMMARY.TOT_COMPENSATION-PREV_DEALER_PAYMENT_SUMMARY.TOT_COMPENSATION


Join with DEALER_PAYMENT_SUMMARY alias as PREV_DEALER_PAYMENT_SUMMARY
PREV_DEALER_PAYMENT_SUMMARY.PMT_PER_BEG_DT = ADD_TO_DATE(PMT_PER_BEG_DT,'MM',-1),
PREV_DEALER_PAYMENT_SUMMARY.PMT_PER_END_DT LAST_DAY(ADD_TO_DATE(PMT_PER_END_DT,'MM',-1))))

** note - add_to_date needs to subtract month "Oracle style", meaning that Oct 31st minus 1 month becomes september 30th


### prev_per_compnsn
#### Description


#### Value Range

N/A

#### Logic

PREV_DEALER_PAYMENT_SUMMARY.TOT_COMPENSATION


### avg_asset_diff
#### Description



#### Value Range

N/A

#### Logic

PREV_DEALER_PAYMENT_SUMMARY.AVG_ASSET-DEALER_PAYMENT_SUMMARY.AVG_ASSET

### prev_per_asset
#### Description



#### Value Range

N/A

#### Logic


PREV_DEALER_PAYMENT_SUMMARY.AVG_ASSET

** note: in current implementation this is Null. Ignore in testing


### trl_mode_flg
#### Description



#### Value Range

N/A

#### Logic


'N'



### pmt_day_key
#### Description



#### Value Range

N/A

#### Logic



CAL_DAY_KEY

Join on CALENDAR Table
add one month to the TLR-PYO-PER-END-DT. Should become last day of the month = CALENDAR.CAL_DAY



### bus_line
#### Description



#### Value Range

N/A

#### Logic


'MTF'



## Files Used
1. DEALER_PAYMENT_SUMMARY (DTO.HCA.MTF.R00858.YYYYMMDD.HHMISS.zip.pgp.txt)
2. DEALER_FEE_TYPE
3. CALENDAR
4. FUND
5. DEALER_PAYMENT_METHOD

## Business keys

dlr_pmt_sum_key

dlr_fee_type_key](#dlr_fee_type_key)|integer|(32,0)|YES||NO

day_key

fund_key

dlr_key

dlr_pmt_mthd_key

wk_ord_nbr