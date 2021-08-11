# DEALER_PAYMENT_TRIAL

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[wk_ord_id](#wk_ord_id)|character varying|25|YES||NO
|[nscc_flg](#nscc_flg)|character varying|1|YES||NO
|[nscc_nm_use_flg](#nscc_nm_use_flg)|character varying|1|YES||NO
|[pmt_ofst_cd](#pmt_ofst_cd)|character varying|25|YES||NO
|[payee_type](#payee_type)|character varying|3|YES||NO
|[splt_compnsn_flg](#splt_compnsn_flg)|character varying|1|YES||NO
|[prev_compnsn_ovrd_type](#prev_compnsn_ovrd_type)|character varying|4|YES||NO
|[eligbl_compnsn_ovrd_type](#eligbl_compnsn_ovrd_type)|character varying|4|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[dlr_pmt_trl_key](#dlr_pmt_trl_key)|integer|(32,0)|NO||YES
|[dlr_key](#dlr_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[dlr_branch_key](#dlr_branch_key)|integer|(32,0)|YES||NO
|[dlr_fee_type_key](#dlr_fee_type_key)|integer|(32,0)|YES||NO
|[dlr_pmt_mthd_key](#dlr_pmt_mthd_key)|integer|(32,0)|YES||NO
|[acct_key](#acct_key)|integer|(32,0)|YES||NO
|[payout_type_key](#payout_type_key)|integer|(32,0)|YES||NO
|[pmt_day_key](#pmt_day_key)|integer|(32,0)|YES||NO
|[invc_day_key](#invc_day_key)|integer|(32,0)|YES||NO
|[pre_agrd_shr_rt](#pre_agrd_shr_rt)|numeric|(38,15)|YES||NO
|[pre_agrd_asset](#pre_agrd_asset)|numeric|(38,15)|YES||NO
|[pre_agrd_fee](#pre_agrd_fee)|numeric|(38,15)|YES||NO
|[prev_splt_fee](#prev_splt_fee)|numeric|(38,15)|YES||NO
|[tot_eligbl_rt](#tot_eligbl_rt)|numeric|(38,15)|YES||NO
|[tot_eligbl_asset](#tot_eligbl_asset)|numeric|(38,15)|YES||NO
|[tot_eligbl_fee](#tot_eligbl_fee)|numeric|(38,15)|YES||NO
|[tot_compnsn](#tot_compnsn)|numeric|(38,15)|YES||NO
|[avg_eligbl_bp](#avg_eligbl_bp)|numeric|(38,15)|YES||NO
|[avg_eligbl_asset](#avg_eligbl_asset)|numeric|(38,15)|YES||NO
|[avg_eligbl_fees](#avg_eligbl_fees)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|NO||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### dlr_pmt_trl_key
#### Description



#### Value Range

N/A

#### Logic

Auto Increment Sequence Generator



### dlr_key
#### Description



#### Value Range

N/A

#### Logic

DEALER.DLR_KEY

Join on DEALER Table


DEALER_PAYMENT_TRIAL.FINANCIAL-INST-ID = DEALER.DLR_ID



### fund_key
#### Description



#### Value Range

N/A

#### Logic

FUND.FUND_KEY

Join on FUND Table

DEALER_PAYMENT_TRIAL.FUND-CODE  = FUND.FUND_NBR                    
DEALER_PAYMENT_TRIAL.ACCOUNT-NUMBER  = (Join FUND with ACCOUNT GET ACCT_NBR FUND.FUND_NBR=ACCOUNT.FUND_NBR)




### dlr_branch_key
#### Description



#### Value Range

N/A

#### Logic

DEALER_BRANCH.DLR_BRANCH_KEY

Join on DEALER_BRANCH Table

DEALER_PAYMENT_TRIAL.FINCL-INST-BRCH-ID = DEALER_BRANCH.BRANCH_ID




### dlr_fee_type_key
#### Description


#### Value Range

N/A

#### Logic

DEALER_FEE_TYPE.DLR_FEE_TYPE_KEY


Join on DEALER_FEE_TYPE Table


DEALER_PAYMENT_TRIAL.TLR-CMPN-FEE-CD = DEALER_FEE_TYPE.DLR_FEE_TYPE_CD



### dlr_pmt_mthd_key
#### Description



#### Value Range

N/A

#### Logic


DLR_PMT_MTHD_KEY

Join on DEALER_PAYMENT_METHOD Table

DEALER_PAYMENT_TRIAL.PAYMENT-METHOD-CD = DEALER_PAYMENT_METHOD.PMT_MTHD_CD





### acct_key
#### Description



#### Value Range

N/A

#### Logic

ACCT_KEY

Join on ACCOUNT Table

DEALER_PAYMENT_TRIAL.FUND-CODE  = ACCOUNT.FUND_NBR                    
DEALER_PAYMENT_TRIAL.ACCOUNT-NUMBER  = =ACCOUNT.ACCT_NBR




### payout_type_key
#### Description



#### Value Range

N/A

#### Logic

PAYOUT_TYPE_KEY

Join on DEALER_PAYOUT_TYPE Table

DEALER_PAYMENT_TRIAL.TLR-PAYOUT-TYP-CD = DEALER_PAYOUT_TYPE.PAYOUT_TYPE_CD


### pmt_day_key
#### Description



#### Value Range

N/A

#### Logic


CAL_DAY_KEY

Join on CALENDAR Table

DEALER_PAYMENT_TRIAL.TLR-PYO-PER-END-DT = CALENDAR.CAL_DAY

This logic is used on Informatica
LAST_DAY(ADD_TO_DATE(TLR-PYO-PER-END-DT,'MM',1))



### invc_day_key
#### Description



#### Value Range

N/A

#### Logic



CAL_DAY_KEY

Join on CALENDAR Table

DEALER_PAYMENT_TRIAL.TLR-PYO-PER-END-DT = CALENDAR.CAL_DAY


### wk_ord_id
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.TLR-PYO-WKO-NBR-ID



### nscc_flg
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.NSCC-INDICATOR-CD 



### nscc_nm_use_flg
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.NSCC-NAME-USE-CD


### pmt_ofst_cd
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.TLR-PAYOUT-TYP-CD



### payee_type
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.CMPN-PAYEE-TYPE-CD


### splt_compnsn_flg
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.SPLIT-COMPENSATION-CD


### prev_compnsn_ovrd_type
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.PRV-CMPN-SCH-ORR-TYP


### eligbl_compnsn_ovrd_type
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.ELG-CMPN-SCH-ORR-TYP


### pre_agrd_shr_rt
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.PRE-AGREE-BASIS-POINT-RT



### pre_agrd_asset
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.PRE-AGREE-ASSET-AT


### pre_agrd_fee
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.PRE-AGREE-COMPENSATION-AT



### prev_splt_fee
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.PRV-SPLIT-COMPENSATION-AT




### tot_eligbl_rt
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.TOTAL-ELIGIBLE-BASIS-POINT-RT



### tot_eligbl_asset
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.TOTAL-ELIGIBLE-ASSET-AT



### tot_eligbl_fee
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.TOTAL-ELIGIBLE-COMPENSATION-AT



### tot_compnsn
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.TOTAL-COMPENSATION-AT


### avg_eligbl_bp
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.ELIGIBLE-BASIS-POINT-RT  



### avg_eligbl_asset
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.ELIGIBLE-ASSET-AT




### avg_eligbl_fees
#### Description



#### Value Range

N/A

#### Logic


DEALER_PAYMENT_TRIAL.ELIGIBLE-COMPENSATION-AT




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

1. DEALER_PAYMENT_TRIAL (DTO.HCA.MTF.R00857.TRIAL.YYYYMMDD.HHMISS.zip.pgp.txt)
2. DEALER
3. DEALER_BRANCH
4. ACCOUNT
5. FUND
6. DEALER_FEE_TYPE
7. DEALER_PAYMENT_METHOD
8. DEALER_PAYOUT_TYPE
9. CALENDAR

