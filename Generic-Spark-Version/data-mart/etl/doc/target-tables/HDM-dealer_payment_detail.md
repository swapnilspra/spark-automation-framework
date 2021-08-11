# DEALER_PAYMENT_DETAIL

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
|[dlr_pmt_det_key](#dlr_pmt_det_key)|integer|(32,0)|NO||YES
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[dlr_key](#dlr_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[dlr_branch_key](#dlr_branch_key)|integer|(32,0)|YES||NO
|[dlr_fee_type_key](#dlr_fee_type_key)|integer|(32,0)|YES||NO
|[dlr_pmt_mthd_key](#dlr_pmt_mthd_key)|integer|(32,0)|YES||NO
|[acct_key](#acct_key)|integer|(32,0)|YES||NO
|[payout_type_key](#payout_type_key)|integer|(32,0)|YES||NO
|[pre_agreed_shr_rt](#pre_agreed_shr_rt)|numeric|(38,15)|YES||NO
|[pre_agreed_asset](#pre_agreed_asset)|numeric|(38,15)|YES||NO
|[pre_agreed_fee](#pre_agreed_fee)|numeric|(38,15)|YES||NO
|[prev_splt_fee](#prev_splt_fee)|numeric|(38,15)|YES||NO
|[tot_eligbl_rt](#tot_eligbl_rt)|numeric|(38,15)|YES||NO
|[tot_eligbl_asset](#tot_eligbl_asset)|numeric|(38,15)|YES||NO
|[tot_eligbl_fee](#tot_eligbl_fee)|numeric|(38,15)|YES||NO
|[tot_compnsn](#tot_compnsn)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[avg_eligbl_bp](#avg_eligbl_bp)|numeric|(38,15)|YES||NO
|[avg_eligbl_asset](#avg_eligbl_asset)|numeric|(38,15)|YES||NO
|[avg_eligbl_fees](#avg_eligbl_fees)|numeric|(38,15)|YES||NO
|[pmt_day_key](#pmt_day_key)|integer|(32,0)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### dlr_pmt_det_key
#### Description



#### Value Range

NVARCHAR

#### Logic


Auto Increment sequence Generator 


### day_key
#### Description



#### Value Range

NVARCHAR

#### Logic

CALENDAR.CAL_DAY_KEY


CALENDAR.CAL_DAY = DEALER_PAYMENT_DETAIL.TLR-PYO-PER-END-DT
 


### dlr_key
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER.DLR_KEY


DEALER.DLR_ID = DEALER_PAYMENT_DETAIL.FINANCIAL-INST-ID



 


### fund_key
#### Description



#### Value Range

NVARCHAR

#### Logic


FUND.FUND_KEY

DEALER_PAYMENT_DETAIL.FUND-CODE = FUND.FUND_NBR 




### dlr_branch_key
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_BRANCH.DLR_BRANCH_KEY


DEALER_PAYMENT_DETAIL.FINCL-INST-BRCH-ID = DEALER_BRANCH.branch_id
DEALER_PAYMENT_DETAIL.FINANCIAL-INST-ID  = DEALER_BRANCH.DLR_ID



### dlr_fee_type_key
#### Description


#### Value Range

NVARCHAR

#### Logic


DEALER_FEE_TYPE.DLR_FEE_TYPE_KEY


DEALER_PAYMENT_DETAIL.TLR-CMPN-FEE-CD = DEALER_FEE_TYPE.DLR_FEE_TYPE_CD




### dlr_pmt_mthd_key
#### Description

#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_METHOD.DLR_PMT_MTHD_KEY

DEALER_PAYMENT_DETAIL.PAYMENT-METHOD-CD = DEALER_PAYMENT_METHOD.pmt_mthd_cd



### acct_key
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.ACCT_KEY

DEALER_PAYMENT_DETAIL.ACCOUNT-NUMBER = ACCOUNT.ACCOUNT-NUMBER
AND DEALER_PAYMENT_DETAIL.FUND-CODE = ACCOUNT.FUND-CODE 




### wk_ord_id
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.TLR-PYO-WKO-NBR-ID  



### nscc_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.NSCC_IND_FLG




### nscc_nm_use_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.NSCC_NM_USE_FLG



### pmt_ofst_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.CMPN-PAYEE-TYPE-CD   



### payee_type
#### Description



#### Value Range

NVARCHAR

#### Logic



DEALER_PAYMENT_DETAIL.TLR-CMPN-FEE-CD  


### splt_compnsn_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.SPLIT-COMPENSATION-CD                          




### prev_compnsn_ovrd_type
#### Description



#### Value Range

NVARCHAR

#### Logic



DEALER_PAYMENT_DETAIL.PRV-CMPN-SCH-ORR-TYP 
    


### eligbl_compnsn_ovrd_type
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.ELG-CMPN-SCH-ORR-TYP 



### payout_type_key
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYOUT_TYPE.PAYOUT_TYPE_KEY


DEALER_PAYMENT_DETAIL.TLR-PAYOUT-TYP-CD = DEALER_PAYOUT_TYPE.PAYOUT_TYPE_CD


### pre_agreed_shr_rt
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.PRE-AGREE-BASIS-POINT-RT  




### pre_agreed_asset
#### Description



#### Value Range

NVARCHAR

#### Logic



DEALER_PAYMENT_DETAIL.PRE-AGREE-ASSET-AT  


### pre_agreed_fee
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.PRE-AGREE-COMPENSATION-AT  




### prev_splt_fee
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.PRV-SPLIT-COMPENSATION-AT  




### tot_eligbl_rt
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.TOTAL-ELIGIBLE-BASIS-POINT-RT 



### tot_eligbl_asset
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.TOTAL-ELIGIBLE-ASSET-AT



### tot_eligbl_fee
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.TOTAL-ELIGIBLE-COMPENSATION-AT              
 


### tot_compnsn
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.TOTAL-COMPENSATION-AT



### curr_row_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


'Y'


### row_strt_dttm
#### Description



#### Value Range

NVARCHAR

#### Logic


SYSDATE


### row_stop_dttm
#### Description



#### Value Range

NVARCHAR

#### Logic





### etl_load_cyc_key
#### Description



#### Value Range

NVARCHAR

#### Logic





### src_sys_id
#### Description



#### Value Range

NVARCHAR

#### Logic





### avg_eligbl_bp
#### Description



#### Value Range

NVARCHAR

#### Logic

DEALER_PAYMENT_DETAIL.AVG_ELIGBL_BP




### avg_eligbl_asset
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.AVG_ELIGBL_ASSET



### avg_eligbl_fees
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_PAYMENT_DETAIL.AVG_ELIGBL_FEES




### pmt_day_key
#### Description



#### Value Range

NVARCHAR

#### Logic



CALENDAR.CAL_DAY_KEY


DEALER_PAYMENT_DETAIL.TLR-PYO-PER-END-DT = CALENDAR.CAL_DAY



 

###FIles Used 

1. DEALER_PAYMENT_DETAIL (DTO.HCA.MTF.R00857.YYYYMMDD.HHMMSS.zip.pgp.txt)



### File Location

dstprod/DTO.HCA.MTF.R00857.20190814.111215.zip.pgp.txt



### Business Keys

day_key
pmt_day_key
dlr_key
fund_key
dlr_branch_key
dlr_fee_type_key
acct_key
wk_ord_id




### Tables

1. CALENDAR 
2. DEALER_BRANCH  
3. DEALER 
4. ACCOUNT 
5. FUND
6. DEALER_PAYOUT_TYPE 
7. DEALER_FEE_TYPE 
8. DEALER_PAYMENT_METHOD



### COBOL File

MTF.R00857.CBL