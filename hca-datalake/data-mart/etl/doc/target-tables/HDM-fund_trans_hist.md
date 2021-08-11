# FUND_TRANS_HIST

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[nav_cd](#nav_cd)|character varying|1|YES||NO
|[undr_wrtr_commsn_cd](#undr_wrtr_commsn_cd)|character varying|1|YES||NO
|[cdsc_wavr_rsn_cd](#cdsc_wavr_rsn_cd)|character varying|4|YES||NO
|[nscc_shrt_term_trader_wavr_rsn](#nscc_shrt_term_trader_wavr_rsn)|character varying|1|YES||NO
|[bluesky_txn_flg](#bluesky_txn_flg)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[txn_desc](#txn_desc)|character varying|255|YES||NO
|[shr_bal_effct_flg](#shr_bal_effct_flg)|character varying|1|YES||NO
|[bluesky_reopen_acct_flg](#bluesky_reopen_acct_flg)|character varying|1|YES||NO
|[opr_id](#opr_id)|character varying|25|YES||NO
|[nscc_cntl_nbr](#nscc_cntl_nbr)|character varying|15|YES||NO
|[txn_hist_key](#txn_hist_key)|integer|(32,0)|NO||YES
|[txn_type_key](#txn_type_key)|integer|(32,0)|YES||NO
|[reg_key](#reg_key)|integer|(32,0)|YES||NO
|[dlr_key](#dlr_key)|integer|(32,0)|YES||NO
|[shrhldr_key](#shrhldr_key)|integer|(32,0)|YES||NO
|[acct_key](#acct_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[efftv_day_key](#efftv_day_key)|integer|(32,0)|YES||NO
|[sttl_day_key](#sttl_day_key)|integer|(32,0)|YES||NO
|[pmt_day_key](#pmt_day_key)|integer|(32,0)|YES||NO
|[batch_day_key](#batch_day_key)|integer|(32,0)|YES||NO
|[dlr_branch_key](#dlr_branch_key)|integer|(32,0)|YES||NO
|[trde_orig_key](#trde_orig_key)|integer|(32,0)|YES||NO
|[rsn_key](#rsn_key)|integer|(32,0)|YES||NO
|[ssn_stat_key](#ssn_stat_key)|integer|(32,0)|YES||NO
|[trde_entry_mthd_key](#trde_entry_mthd_key)|integer|(32,0)|YES||NO
|[dlr_lvl_cntl_key](#dlr_lvl_cntl_key)|integer|(32,0)|YES||NO
|[pmt_mthd_key](#pmt_mthd_key)|integer|(32,0)|YES||NO
|[shrhldr_role_key](#shrhldr_role_key)|integer|(32,0)|NO||NO
|[acct_nbr](#acct_nbr)|numeric|(38,15)|YES||NO
|[fund_txn_id](#fund_txn_id)|numeric|(38,15)|YES||NO
|[fund_txn_id_2](#fund_txn_id_2)|numeric|(38,15)|YES||NO
|[ltr_of_intend_nbr](#ltr_of_intend_nbr)|numeric|(38,15)|YES||NO
|[batch_nbr](#batch_nbr)|numeric|(38,15)|YES||NO
|[discnt_cat](#discnt_cat)|numeric|(38,15)|YES||NO
|[fund_ord_nbr](#fund_ord_nbr)|numeric|(38,15)|YES||NO
|[acct_pr_sched](#acct_pr_sched)|numeric|(38,15)|YES||NO
|[contrbn_tax_yr](#contrbn_tax_yr)|integer|(32,0)|YES||NO
|[shrs](#shrs)|numeric|(38,15)|YES||NO
|[shr_pr](#shr_pr)|numeric|(38,15)|YES||NO
|[gr_amt](#gr_amt)|numeric|(38,15)|YES||NO
|[sales_pct](#sales_pct)|numeric|(38,15)|YES||NO
|[dlr_commsn_amt](#dlr_commsn_amt)|numeric|(38,15)|YES||NO
|[undr_wrtr_commsn_amt](#undr_wrtr_commsn_amt)|numeric|(38,15)|YES||NO
|[net_amt](#net_amt)|numeric|(38,15)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|NO||NO
|[frto_fund_key](#frto_fund_key)|integer|(32,0)|YES||NO
|[spr_sheet_day_key](#spr_sheet_day_key)|numeric|(38,15)|YES||NO
|[reg_of_sale_key](#reg_of_sale_key)|integer|(32,0)|YES||NO
|[tpa_dlr_key](#tpa_dlr_key)|integer|(32,0)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### txn_hist_key
#### Description



#### Value Range

N/A

#### Logic

Auto Increment sequence Generator 


### txn_type_key
#### Description



#### Value Range

N/A

#### Logic

TRANSACTION_TYPE.TXN_TYPE_KEY

Join on table TRANSACTION_TYPE

TRANSACTION_TYPE.TXN_SFX_CD = FUND_TRANS_HIST.TRANSACTION-SUFFIX AND 
TRANSACTION_TYPE.TXN_CD = FUND_TRANS_HIST.TRANSACTION-CODE


### reg_key
#### Description


#### Value Range

N/A

#### Logic



REGION.REG_KEY

Join on REGION Using below output

DST_ST_COUNTRY.ST_CD = REGION.ST_CD


Join on DST_ST_COUNTRY Table

Get DST_ST_COUNTRY.ST_CD 

DST_ST_COUNTRY.DST_ST_CD = FUND_TRANS_HIST.RESIDENT-STATE-CODE



### dlr_key
#### Description



#### Value Range

N/A

#### Logic

DEALER.DLR_KEY


Join on DEALER Table

DEALER.DLR_ID = FUND_TRANS_HIST.FINS-ID


### shrhldr_key
#### Description



#### Value Range

N/A

#### Logic

SHAREHOLDER.SHRHLDR_KEY

Join on SHAREHOLDER Table

 
SHAREHOLDER.CUST_IDNFCN_NBR_1 = FUND_TRANS_HIST.MUTUAL-FUND-TRANSACTION-ID-1 
AND SHAREHOLDER.CUST_IDNFCN_NBR_2 = FUND_TRANS_HIST.MUTUAL-FUND-TRANSACTION-ID-2



### acct_key
#### Description



#### Value Range

N/A

#### Logic


ACCOUNT.ACCT_KEY

Join on ACCOUNT

ACCOUNT.FUND_NBR = FUND_TRANS_HIST.FUND-CODE  AND
ACCOUNT.ACCT_NBR= FUND_TRANS_HIST.ACCOUNT-NUMBER


### fund_key
#### Description


#### Value Range

N/A

#### Logic

FUND.FUND_KEY

Join on FUND Table

FUND.FUND_NBR = FUND_TRANS_HIST.FUND-CODE


### efftv_day_key
#### Description



#### Value Range

N/A

#### Logic

CALENDAR.CAL_DAY_KEY

Join on CALENDAR Table


CALENDAR.CAL_DAY = FUND_TRANS_HIST.TRADE-DATE


### sttl_day_key
#### Description



#### Value Range

N/A

#### Logic

CAL_DAY_KEY

Join on CALENDAR Table


CALENDAR.CAL_DAY  = FUND_TRANS_HIST.TRADE-DATE


### pmt_day_key
#### Description



#### Value Range

N/A

#### Logic



CALENDAR.CAL_DAY_KEY

Join on CALENDAR Table


CALENDAR.CAL_DAY  = FUND_TRANS_HIST.CONFIRM-DATE


### batch_day_key
#### Description


#### Value Range

N/A

#### Logic


CALENDAR.CAL_DAY_KEY

Join on  CALENDAR Table

CALENDAR.CAL_DAY = FUND_TRANS_HIST.BATCH_DT



### dlr_branch_key
#### Description



#### Value Range

N/A

#### Logic

DEALER_BRANCH.DLR_BRANCH_KEY

Join on DEALER_BRANCH table

 
FINS-ID = DEALER_BRANCH.DLR_ID 
AND DEALER-BRANCH-ID  = DEALER_BRANCH.BRANCH_ID



### trde_orig_key
#### Description



#### Value Range

N/A

#### Logic

TRADE_ORIGIN.TRDE_ORIG_KEY

Join on TRADE_ORIGIN Table

TRADE_ORIGIN.TRDE_ORIG_ID = FUND_TRANS_HIST.TRADE-ORIGIN-ID


### rsn_key
#### Description



#### Value Range

N/A

#### Logic

REASON_CODE.RSN_KEY

Join on REASON_CODE Table


REASON_CODE.ENTITY_RSN_CD = FUND_TRANS_HIST.NSCC-NAV-REASON-CODE



### ssn_stat_key
#### Description



#### Value Range

N/A

#### Logic

SOCIAL_SECURITY_STATUS.SSN_STAT_KEY

Join on SOCIAL_SECURITY_STATUS Table

SOCIAL_SECURITY_STATUS.SSN_STAT_CD = FUND_TRANS_HIST.SOCIAL-SECURITY-NBR-STATUS-CDE


### trde_entry_mthd_key
#### Description



#### Value Range

N/A

#### Logic

TRADE_ENTRY_METHOD.TRDE_ENTRY_MTHD_KEY

Join on TRADE_ENTRY_METHOD Table


TRADE_ENTRY_METHOD.TRDE_ENTRY_MTHD_CD = FUND_TRANS_HIST.TRADE-ENTRY-METHOD-CDE



### dlr_lvl_cntl_key
#### Description



#### Value Range

N/A

#### Logic

DEALER_LEVEL_CONTROL.DLR_LVL_CNTL_KEY

Join on DEALER_LEVEL_CONTROL Table


DEALER_LEVEL_CONTROL.DLR_LVL_CNTL_CD = FUND_TRANS_HIST.DEALER-CONTROL-LEVEL-CODE


### pmt_mthd_key
#### Description



#### Value Range

N/A

#### Logic

PAYMENT_METHOD.PMT_MTHD_KEY

Join on PAYMENT_METHOD Table

PAYMENT_METHOD.PMT_MTHD_CD = FUND_TRANS_HIST.PAYMENT-METHOD-CDE



### shrhldr_role_key
#### Description



#### Value Range

N/A

#### Logic

NULL 



### acct_nbr
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.CUSTOMER-ACCOUNT-NUMBER


### fund_txn_id
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.MUTUAL-FUND-TRANSACTION-ID-1


### fund_txn_id_2
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.MUTUAL-FUND-TRANSACTION-ID-2



### ltr_of_intend_nbr
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.LOI-NUMBER


### batch_nbr
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.BATCH-NUMBER


### discnt_cat
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.DISCOUNT-CATEGORY


### fund_ord_nbr
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.ORDER-NUMBER 

### nav_cd
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.NSCC-NAV-REASON-CDE


### acct_pr_sched
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.SHR-ACCT-PRICE-SCHED-CDE


### undr_wrtr_commsn_cd
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.UNDERWRITER-COMM-EFFECT-CDE 



### cdsc_wavr_rsn_cd
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.FIN-TR-CDSC-WV-RSN 



### nscc_shrt_term_trader_wavr_rsn
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.NSCC-STTR-WV-ID



### contrbn_tax_yr
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.CONTRIBUTION-YEAR


### shrs
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.TRANSACTION-SHARES-COUNT



### shr_pr
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.PER-SHARE-PRICE-AMOUNT 


### gr_amt
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.TRANSACTION-GROSS-AMOUNT


### sales_pct
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.SALES-CHARGE-PERCENT 


### dlr_commsn_amt
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.DEALER-COMMISSION-AMOUNT


### undr_wrtr_commsn_amt
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.UNDERWRITER-COMMISSION-AMOUNT



### net_amt
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.NET-ASSET-VALUE-AT


### bluesky_txn_flg
#### Description



#### Value Range

N/A

#### Logic

HBR_TRANSMTD.BLUESKY_NEW_REPEAT

Join on HBR_TRANSMTD Table

HBR_TRANSMTD.TRANS_ID1 = FUND_TRANS_HIST.MUTUAL-FUND-TRANSACTION-ID-1 
AND HBR_TRANSMTD.TRANS_ID2 = FUND_TRANS_HIST.MUTUAL-FUND-TRANSACTION-ID-2


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




### curr_row_flg
#### Description



#### Value Range

N/A

#### Logic




### src_sys_id
#### Description



#### Value Range

N/A

#### Logic




### etl_load_cyc_key
#### Description



#### Value Range

N/A

#### Logic




### txn_desc
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.VOL-TRAN-DESC




### shr_bal_effct_flg
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.SHARE-BALANCE-EFFECT-CODE



### bluesky_reopen_acct_flg
#### Description



#### Value Range

N/A

#### Logic


HBR_TRANSMTD.BLUESKY_REOPEN_ACCOUNT_CDE

Join on HBR_TRANSMTD Table

HBR_TRANSMTD.TRANS_ID1 = FUND_TRANS_HIST.MUTUAL-FUND-TRANSACTION-ID-1 
AND HBR_TRANSMTD.TRANS_ID2 = FUND_TRANS_HIST.MUTUAL-FUND-TRANSACTION-ID-2


### frto_fund_key
#### Description



#### Value Range

N/A

#### Logic

FUND.FUND_KEY

Join on FUND Table

FUND.FUND_NBR = FUND_TRANS_HIST.FUND-CODE


### spr_sheet_day_key
#### Description



#### Value Range

N/A

#### Logic

CALENDAR.CAL_DAY_KEY

Join on CALENDAR table 

CALENDAR.CAL_DAY = FUND_TRANS_HIST.SPR_SHEET_DT

Super Sheet Date (SPT_SHEET_DT) is extracted from the first line of the  DFA.TIP02 file.
$ head /inboundfiles/dstprod/daily_adw_incremental/*TIP02*. 
RHR001FINANCIALDIRECT201907252019072522471686DODMU1170150000001

vari=$(awk -F" " 'NR==1{print $3}' $file)




### opr_id
#### Description



#### Value Range

N/A

#### Logic


FUND_TRANS_HIST.OPERATOR-ID


### reg_of_sale_key
#### Description



#### Value Range

N/A

#### Logic


REGION.REG_KEY


Join on REGION Using below output

DST_ST_COUNTRY.ST_CD = REGION.ST_CD
AND DST_ST_COUNTRY.CRTY_ISO_CD = REGION.CRTY_CD


Join on DST_ST_COUNTRY Table

Get ST_CD and CRTY_ISO_CD


DST_ST_COUNTRY.DST_ST_CD = FUND_TRANS_HIST.RESIDENT-STATE-CODE



### nscc_cntl_nbr
#### Description



#### Value Range

N/A

#### Logic

FUND_TRANS_HIST.NSCC-CONTROL-NUMBER


### tpa_dlr_key
#### Description



#### Value Range

N/A

#### Logic

NSCC-CONTROL-NUMBER

IF NSCC-CONTROL-NUMBER NULL then take the Filler value from the file from DETAIL-RECORD6
ELSE NSCC-CONTROL-NUMBER


### Files used 

1. FUND_TRANS_HIST (DTO.HCA.DFA.TIP02.YYMMDD.HHMMSS.txt)



### File Location

dstprod/DTO.HCA.DFA.TIP02.20190719.225113.zip.pgp.txt



#### Tables Used


2. TRANSACTION_TYPE
3. REGION
4. DEALER
5. SHAREHOLDER
6. DEALER_BRANCH
7. FUND
8. CALENDAR
9. DST_ST_COUNTRY
10. TRADE_ORIGIN
11. REASON_CODE
12. SOCIAL_SECURITY_STATUS
13. TRADE_ENTRY_METHOD
14. DEALER_LEVEL_CONTROL
15.	PAYMENT_METHOD
16. ACCOUNT
17. HBR_TRANSMTD
18. NSCC_NAV_REASON


## Business Keys

fund_txn_id
fund_txn_id_2


###COBOL Files

DFA.TIP02.CBL
