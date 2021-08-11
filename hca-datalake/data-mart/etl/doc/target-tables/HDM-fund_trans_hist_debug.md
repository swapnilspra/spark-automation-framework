# FUND_TRANS_HIST_DEBUG

This table holds the lorem ipsum dolor sit amet.
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
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### txn_hist_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### txn_type_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### reg_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### dlr_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### shrhldr_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### acct_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fund_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### efftv_day_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### sttl_day_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### pmt_day_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### batch_day_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### dlr_branch_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### trde_orig_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rsn_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### ssn_stat_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### trde_entry_mthd_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### dlr_lvl_cntl_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### pmt_mthd_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### shrhldr_role_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### acct_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fund_txn_id
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fund_txn_id_2
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### ltr_of_intend_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### batch_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### discnt_cat
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fund_ord_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### nav_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### acct_pr_sched
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### undr_wrtr_commsn_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cdsc_wavr_rsn_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### nscc_shrt_term_trader_wavr_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### contrbn_tax_yr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### shrs
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### shr_pr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### gr_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### sales_pct
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### dlr_commsn_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### undr_wrtr_commsn_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### net_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### bluesky_txn_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### row_strt_dttm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### row_stop_dttm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_row_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### src_sys_id
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### etl_load_cyc_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### txn_desc
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### shr_bal_effct_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### bluesky_reopen_acct_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### frto_fund_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### spr_sheet_day_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### opr_id
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



