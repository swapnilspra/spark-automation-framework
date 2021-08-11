# ACCT_POSN

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[ACCT_POSN_key](#ACCT_POSN_key)|integer|(32,0)|NO||YES
|[acct_key](#acct_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[shrhldr_key](#shrhldr_key)|integer|(32,0)|YES||NO
|[dlr_key](#dlr_key)|integer|(32,0)|YES||NO
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[acct_nbr](#acct_nbr)|numeric|(38,15)|YES||NO
|[shrhldr_role_key](#shrhldr_role_key)|integer|(32,0)|NO||NO
|[tot_shrs](#tot_shrs)|numeric|(38,15)|YES||NO
|[un_issd_shrs](#un_issd_shrs)|numeric|(38,15)|YES||NO
|[issd_shrs](#issd_shrs)|numeric|(38,15)|YES||NO
|[nav](#nav)|numeric|(38,15)|YES||NO
|[rsrvd_shrs](#rsrvd_shrs)|numeric|(38,15)|YES||NO
|[accrd_divd_amt](#accrd_divd_amt)|numeric|(38,15)|YES||NO
|[escrw_shrs](#escrw_shrs)|numeric|(38,15)|YES||NO
|[collctd_shrs](#collctd_shrs)|numeric|(38,15)|YES||NO
|[collctd_bal_amt](#collctd_bal_amt)|numeric|(38,15)|YES||NO
|[curr_incm_divd](#curr_incm_divd)|numeric|(38,15)|YES||NO
|[curr_long_term_cap_gain](#curr_long_term_cap_gain)|numeric|(38,15)|YES||NO
|[curr_shrt_term_cap_gain](#curr_shrt_term_cap_gain)|numeric|(38,15)|YES||NO
|[curr_forgn_w_holdg](#curr_forgn_w_holdg)|numeric|(38,15)|YES||NO
|[curr_divd_w_holdg](#curr_divd_w_holdg)|numeric|(38,15)|YES||NO
|[curr_forgn_tax_paid_amt](#curr_forgn_tax_paid_amt)|numeric|(38,15)|YES||NO
|[curr_yr_1099b_gr_redmpn_amt](#curr_yr_1099b_gr_redmpn_amt)|numeric|(38,15)|YES||NO
|[curr_yr_1099b_gr_w_holdg_amt](#curr_yr_1099b_gr_w_holdg_amt)|numeric|(38,15)|YES||NO
|[collctd_shrs_net_chg_cnt](#collctd_shrs_net_chg_cnt)|numeric|(38,15)|YES||NO
|[tot_shrs_net_chg_cnt](#tot_shrs_net_chg_cnt)|numeric|(38,15)|YES||NO
|[dsgntd_w_holdg_amt](#dsgntd_w_holdg_amt)|numeric|(38,15)|YES||NO
|[dsgntd_w_holdg_rt](#dsgntd_w_holdg_rt)|numeric|(38,15)|YES||NO
|[divd_w_holdg_amt](#divd_w_holdg_amt)|numeric|(38,15)|YES||NO
|[divd_w_holdg_rt](#divd_w_holdg_rt)|numeric|(38,15)|YES||NO
|[shrt_term_cap_gain_w_holdg_amt](#shrt_term_cap_gain_w_holdg_amt)|numeric|(38,15)|YES||NO
|[shrt_term_cap_gain_w_holdg_rt](#shrt_term_cap_gain_w_holdg_rt)|numeric|(38,15)|YES||NO
|[long_term_cap_gain_w_holdg_amt](#long_term_cap_gain_w_holdg_amt)|numeric|(38,15)|YES||NO
|[long_term_cap_gain_w_holdg_rt](#long_term_cap_gain_w_holdg_rt)|numeric|(38,15)|YES||NO
|[partition_yr](#partition_yr)|integer|(32,0)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[dlr_branch_key](#dlr_branch_key)|integer|(32,0)|YES||NO
|[tot_bal_amt](#tot_bal_amt)|numeric|(38,15)|YES||NO
|[shrhldr_copy_key](#shrhldr_copy_key)|numeric|(38,15)|YES||NO
|[hbr_dlr_key](#hbr_dlr_key)|integer|(32,0)|YES||NO
|[acct_stat_cd](#acct_stat_cd)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### ACCT_POSN_key
#### Description



#### Value Range

NUMBER

#### Logic

Auto increment Key


### acct_key
#### Description



#### Value Range

NUMBER

#### Logic

ACCOUNT.ACCT_KEY

Join on ACCOUNT Table

ACCOUNT.ACCOUNT-NUMBER = ACCT_POSN.ACCOUNT-NUMBER  
AND ACCOUNT.FUND-CODE = ACCT_POSN.FUND-CODE


### fund_key
#### Description


#### Value Range

NUMBER

#### Logic

FUND.FUND_KEY

Join on FUND Table

FUND.FUND_NBR = ACCT_POSN.FUND_CODE



### shrhldr_key
#### Description



#### Value Range

NUMBER

#### Logic


NULL


### dlr_key
#### Description



#### Value Range

NUMBER

#### Logic

DEALER.DLR_KEY

Join on DEALER Table

DEALER.DLR_ID = ACCT_POSN.FINS-ID




### day_key
#### Description



#### Value Range

NUMBER

#### Logic

CALENDAR.CAL_DAY_KEY

Super Sheet Date (SPT_SHEET_DT) is extracted from the first line of the  APR.TIP03 file.
$ head /inboundfiles/dstprod/daily_adw_incremental/*TIP03*. 
RHR001ACCT POSITION  201912112019121122505308DODMU1440140000001  

vari=$(awk -F" " 'NR==1{print $3}' $file)

Join on CALENDAR Table

CALENDAR.DAY_KEY = SPT_SHEET_DT


### acct_nbr
#### Description



#### Value Range

NUMBER

#### Logic


ACCT_POSN.ACCOUNT-NUMBER


### shrhldr_role_key
#### Description



#### Value Range

NUMBER

#### Logic


NULL



### tot_shrs
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.TOTAL-SHARES-COUNT



### un_issd_shrs
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.UNISSUED-SHARES-COUNT


### issd_shrs
#### Description



#### Value Range

NUMBER

#### Logic


ACCT_POSN.ISSUED-SHARES-COUNT


### nav
#### Description



#### Value Range

NUMBER

#### Logic



ACCT_POSN.NET-ASSET-VALUE 


### rsrvd_shrs
#### Description



#### Value Range

NUMBER

#### Logic


ACCT_POSN.RESERVED_SHARE_CNT


### accrd_divd_amt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.ACCRUED-DIVIDEND-AMOUNT


### escrw_shrs
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.ESCROW-SHARES-COUNT


### collctd_shrs
#### Description



#### Value Range

NUMBER

#### Logic


ACCT_POSN.COLLECTED-SHARES-CNT


### collctd_bal_amt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.COLLECTED-BALANCE-AMOUNT



### curr_incm_divd
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.CUR-INCOME-DIVIDENDS


### curr_long_term_cap_gain
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.CUR-LONG-TERM-CAP-GAINS



### curr_shrt_term_cap_gain
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.CUR-SHORT-TERM-CAP-GAINS



### curr_forgn_w_holdg
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.CUR-FOREIGN-WITHHOLDING


### curr_divd_w_holdg
#### Description



#### Value Range

NUMBER

#### Logic


ACCT_POSN.CUR-DIVIDEND-WITHHOLDING



### curr_forgn_tax_paid_amt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.CUR-FOREIGN-TAX-PAID-AMT



### curr_yr_1099b_gr_redmpn_amt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.1099B-GROSS-REDEMPTION-AMT



### curr_yr_1099b_gr_w_holdg_amt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.1099B-GROSS-WITHHOLD-AMT




### collctd_shrs_net_chg_cnt
#### Description



#### Value Range

NUMBER

#### Logic
ACCT_POSN.COLLECTED-SHARES-CNT




### tot_shrs_net_chg_cnt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.TOTAL-SHARES-NET-CHG-CNT




### dsgntd_w_holdg_amt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.DSG-WTHH-AMT




### dsgntd_w_holdg_rt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.DSG-WTHH-RTE



### divd_w_holdg_amt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.DIVIDEND-WTH-AT



### divd_w_holdg_rt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.DIVIDEND-WTH-RT 



### shrt_term_cap_gain_w_holdg_amt
#### Description



#### Value Range

NUMBER

#### Logic


ACCT_POSN.STCG-WTH-AT 



### shrt_term_cap_gain_w_holdg_rt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.STCG-WTH-RT



### long_term_cap_gain_w_holdg_amt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.LTCG-WTH-AT



### long_term_cap_gain_w_holdg_rt
#### Description



#### Value Range

NUMBER

#### Logic

ACCT_POSN.LTCG-WTH-RT 



### partition_yr
#### Description



#### Value Range

NUMBER

#### Logic

Get the Year from below date (SPT_SHEET_DT)

Super Sheet Date (SPT_SHEET_DT) is extracted from the first line of the  APR.TIP03 file.
$ head /inboundfiles/dstprod/daily_adw_incremental/*TIP03*. 
RHR001ACCT POSITION  201912112019121122505308DODMU1440140000001  

vari=$(awk -F" " 'NR==1{print $3}' $file)




### row_strt_dttm
#### Description



#### Value Range

DATE

#### Logic

SYSDATE


### row_stop_dttm
#### Description



#### Value Range

DATE

#### Logic




### curr_row_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

'Y'


### etl_load_cyc_key
#### Description



#### Value Range

NUMBER

#### Logic




### src_sys_id
#### Description



#### Value Range

NUMBER

#### Logic




### dlr_branch_key
#### Description



#### Value Range

NUMBER

#### Logic


DEALER_BRANCH.DLR_BRANCH_KEY

Join on DEALER_BRANCH Table

DEALER_BRANCH.DLR_ID = ACCT_POSN.FINS-ID 
AND DEALER_BRANCH.BRANCH_ID = ACCT_POSN.DEALER-BRANCH-ID



### tot_bal_amt
#### Description



#### Value Range

NUMBER

#### Logic



ACCT_POSN.TOTAL-SHARES-COUNT * ACCT_POSN.NET-ASSET-VALUE



### shrhldr_copy_key
#### Description



#### Value Range

NUMBER

#### Logic


NULL



### hbr_dlr_key
#### Description



#### Value Range

NUMBER

#### Logic

DEALER.DLR_KEY

 Join on ACCOUNT Table
 
 Get HBR_DLR_ID

ACCOUNT.ACCOUNT-NUMBER = ACCT_POSN.ACCOUNT-NUMBER  
AND ACCOUNT.FUND-CODE = ACCT_POSN.FUND-CODE

After account return HBR_DLR_ID

Join with DEALER

DLR_KEY

Join on DEALER Table

DEALER.DLR_ID = HBR_DLR_ID


### acct_stat_cd
#### Description



#### Value Range

NUMBER

#### Logic

ACCOUNT.ACCT_STAT_CD

Join on ACCOUNT Table


ACCOUNT.ACCOUNT-NUMBER = ACCT_POSN.ACCOUNT-NUMBER  
AND ACCOUNT.FUND-CODE = ACCT_POSN.FUND-CODE



###FIle Used

1. ACCT_POSN (DTO.HCA.APR.TIP03.YYMMDD.HHMMSS)



## File Location


dstprod/DTO.HCA.APR.TIP03.20190719.224520.zip.pgp.txt


## Tables Used

2. FUND
3. ACCOUNT
4. DEALER
5. DEALER_BRANCH
6. CALENDAR


###Business Keys

1. ACCOUNT-NUMBER
2. FUND-CODE
3. SPT_SHEET_DT


### COBOL File

APR.TIP03.CBL
