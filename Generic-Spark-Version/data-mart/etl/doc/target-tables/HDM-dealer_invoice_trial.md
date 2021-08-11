# DEALER_INVOICE_TRIAL

This table holds the 
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[invc_nbr](#invc_nbr)|character varying|25|YES||NO
|[bus_line](#bus_line)|character varying|255|YES||NO
|[invc_freq_flg](#invc_freq_flg)|character varying|1|YES||NO
|[rec_splt_flg](#rec_splt_flg)|character varying|1|YES||NO
|[dlr_cat](#dlr_cat)|character varying|255|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[dlr_invc_trl_key](#dlr_invc_trl_key)|integer|(32,0)|NO||YES
|[dlr_key](#dlr_key)|integer|(32,0)|YES||NO
|[acct_key](#acct_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[invc_day_key](#invc_day_key)|integer|(32,0)|YES||NO
|[dlr_fee_type_key](#dlr_fee_type_key)|integer|(32,0)|YES||NO
|[pmt_day_key](#pmt_day_key)|integer|(32,0)|YES||NO
|[fee_rt](#fee_rt)|numeric|(38,15)|YES||NO
|[tot_fee_amt](#tot_fee_amt)|numeric|(38,15)|YES||NO
|[invc_avg_asset](#invc_avg_asset)|numeric|(38,15)|YES||NO
|[posn_cnt](#posn_cnt)|numeric|(38,15)|YES||NO
|[per_acct_fee](#per_acct_fee)|numeric|(38,15)|YES||NO
|[dlr_portal_asset](#dlr_portal_asset)|numeric|(38,15)|YES||NO
|[portal_posn_cnt](#portal_posn_cnt)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[invc_dt](#invc_dt)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### dlr_invc_trl_key
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


DLR_KEY

Join on DEALER Table

DEALER_INVOICE_TRIAL.DEALER_NUMBER = DEALER.DLR_ID

IIF(ISNULL(((LTRIM(RTRIM(DEALER_NUMBER))))),
((LTRIM(RTRIM(SUBSTR(DEALER_NUMBER,INSTR(DEALER_NUMBER,'and', -1) +3,25))))),
((LTRIM(RTRIM(DEALER_NUMBER)))))


### acct_key
#### Description



#### Value Range

N/A

#### Logic


ACCT_KEY

Join on ACCOUNT Table

DEALER_INVOICE_TRIAL.Harbor_Account_Number = ACCOUNT.ACCOUNT-NUMBER
AND DEALER_INVOICE_TRIAL.Fund_Number = ACCOUNT.FUND-CODE 


### fund_key
#### Description



#### Value Range

N/A

#### Logic





### invc_day_key
#### Description



#### Value Range

N/A

#### Logic





### dlr_fee_type_key
#### Description



#### Value Range

N/A

#### Logic





### pmt_day_key
#### Description



#### Value Range

N/A

#### Logic


CAL_DAY_KEY

Join on CALENDAR Table

DEALER_INVOICE.PAYMENT_DATE = CALENDAR.CAL_DAY


### fee_rt
#### Description



#### Value Range

N/A

#### Logic


DEALER_INVOICE_TRIAL.Basis_Points



### tot_fee_amt
#### Description



#### Value Range

N/A

#### Logic


DEALER_INVOICE_TRIAL.Invoice_Amount


### invc_nbr
#### Description



#### Value Range

N/A

#### Logic





### invc_dt
#### Description



#### Value Range

N/A

#### Logic





### bus_line
#### Description



#### Value Range

N/A

#### Logic


DEALER_INVOICE_TRIAL.business_line


### invc_avg_asset
#### Description



#### Value Range

N/A

#### Logic


DEALER_INVOICE_TRIAL.Average_Assets__Invoice


### posn_cnt
#### Description



#### Value Range

N/A

#### Logic


DEALER_INVOICE_TRIAL.Invoice_Amount


### per_acct_fee
#### Description



#### Value Range

N/A

#### Logic



DEALER_INVOICE_TRIAL.Dollars_Per_Account


### dlr_portal_asset
#### Description



#### Value Range

N/A

#### Logic


DEALER_INVOICE_TRIAL.Assets_from_Portal


### portal_posn_cnt
#### Description



#### Value Range

N/A

#### Logic


DEALER_INVOICE_TRIAL.Positions_From_Portal


### invc_freq_flg
#### Description



#### Value Range

N/A

#### Logic





### rec_splt_flg
#### Description



#### Value Range

N/A

#### Logic





### dlr_cat
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






## Files Used
1. DEALER_INVOICE_TRIAL.csv
2. DEALER
3. ACCOUNT
