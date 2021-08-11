# PENDING_TRADE

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[txn_ref_id](#txn_ref_id)|character varying|100|YES||NO
|[grp_txn_ref](#grp_txn_ref)|character varying|100|YES||NO
|[entry_type](#entry_type)|character varying|60|YES||NO
|[nca_flg](#nca_flg)|character varying|1|YES||NO
|[rvrs_flg](#rvrs_flg)|character varying|1|YES||NO
|[trde_delvy_flg](#trde_delvy_flg)|character varying|1|YES||NO
|[rep_cd](#rep_cd)|character varying|60|YES||NO
|[aft_hrs_flg](#aft_hrs_flg)|character varying|1|YES||NO
|[nscc_confrm_id](#nscc_confrm_id)|character varying|100|YES||NO
|[trde_src](#trde_src)|character varying|60|YES||NO
|[dln_levy](#dln_levy)|character varying|255|YES||NO
|[pln_id](#pln_id)|character varying|25|YES||NO
|[ltst_batch_flg](#ltst_batch_flg)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[pend_trde_key](#pend_trde_key)|integer|(32,0)|NO||YES
|[dlr_key](#dlr_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[acct_key](#acct_key)|integer|(32,0)|YES||NO
|[txn_type_key](#txn_type_key)|integer|(32,0)|YES||NO
|[trde_day_key](#trde_day_key)|integer|(32,0)|YES||NO
|[sttl_day_key](#sttl_day_key)|integer|(32,0)|YES||NO
|[log_day_key](#log_day_key)|integer|(32,0)|YES||NO
|[txn_pr](#txn_pr)|numeric|(38,15)|YES||NO
|[gr_cash_val](#gr_cash_val)|numeric|(38,15)|YES||NO
|[cb_val](#cb_val)|numeric|(38,15)|YES||NO
|[txn_tax_amt](#txn_tax_amt)|numeric|(38,15)|YES||NO
|[shr_val](#shr_val)|numeric|(38,15)|YES||NO
|[net_cash_val](#net_cash_val)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[load_seq_nbr](#load_seq_nbr)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### pend_trde_key
#### Description


#### Value Range

NVARCHAR

#### Logic

Auto increment Key


### dlr_key
#### Description



#### Value Range

NVARCHAR

#### Logic

DLR_KEY

Join on DEALER Table

DEALER.DLR_ID = PENDING_TRADE.DEALER_NUM 


### fund_key
#### Description



#### Value Range

NVARCHAR

#### Logic

FUND_KEY

Join on FUND Table

FUND.FUND_NBR = PENDING_TRADE.PORT_ID


### acct_key
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCT_KEY

Join on file ACCOUNT Table


ACCOUNT.ACCT_NBR = PENDING_TRADE.TA_ACCOUNT_NUM   
AND ACCOUNT.CURR_ROW_FLG = 'Y'


### txn_type_key
#### Description



#### Value Range

NVARCHAR

#### Logic

TXN_TYPE_KEY

Join on TRANSACTION_TYPE Table

TRANSACTION_TYPE.TXN_CD = PENDING_TRADE.TRANS_TYPE 
AND TRANSACTION_TYPE.TXN_SFX_CD = PENDING_TRADE.TO_DECIMAL(SUBSTR(LTRIM(RTRIM(FUND_TRANS_TYPE_in)),4,6),0)


### trde_day_key
#### Description



#### Value Range

NVARCHAR

#### Logic

CAL_DAY_KEY

Join on CALENDAR Table

CALENDAR.CAL_DAY = PENDING_TRADE.TRADE_DATE


### sttl_day_key
#### Description



#### Value Range

NVARCHAR

#### Logic

CAL_DAY_KEY

Join on CALENDAR Table

CALENDAR.CAL_DAY = PENDING_TRADE.SETTLEMENT_DATE


### log_day_key
#### Description



#### Value Range

NVARCHAR

#### Logic

CAL_DAY_KEY

Join on CALENDAR Table

CALENDAR.CAL_DAY = PENDING_TRADE.LOG_DATE

### txn_ref_id
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.TRANS_REF_ID



### grp_txn_ref
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.GROUP_TRANS_REF


### entry_type
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.ENTRY_TYPE



### nca_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.NCA_FLAG



### rvrs_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


PENDING_TRADE.REVERSE_FLAG


### trde_delvy_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.TRADE_DELIVERY



### rep_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.REP_CODE


### aft_hrs_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


PENDING_TRADE.AFTER_HOURS


### nscc_confrm_id
#### Description



#### Value Range

NVARCHAR

#### Logic


PENDING_TRADE.NSCC_CONFIRM


### trde_src
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.TRADE_SOURCE


### dln_levy
#### Description



#### Value Range

NVARCHAR

#### Logic


PENDING_TRADE.DILUTION_LEVY


### pln_id
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.PLAN_ID



### txn_pr
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.TRANS_PRICE



### gr_cash_val
#### Description



#### Value Range

NVARCHAR

#### Logic


PENDING_TRADE.GROSS_CASH_VAL


### cb_val
#### Description



#### Value Range

NVARCHAR

#### Logic


PENDING_TRADE.COST_BASIS


### txn_tax_amt
#### Description



#### Value Range

NVARCHAR

#### Logic


PENDING_TRADE.TRANS_TAX_AMOUNT


### shr_val
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.SHARE_VAL



### net_cash_val
#### Description



#### Value Range

NVARCHAR

#### Logic

PENDING_TRADE.NET_CASH_VAL



### ltst_batch_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


'Y'


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




### load_seq_nbr
#### Description



#### Value Range

NVARCHAR

#### Logic



#FIles Used 

1. PENDING_TRADE (DTO.HCA.SPOT.TXN.YYMMDD.HHMMSS.txt)
2. DEALER
3. FUND
4. ACCOUNT
5. CALENDAR
6. TRANSACTION_TYPE

