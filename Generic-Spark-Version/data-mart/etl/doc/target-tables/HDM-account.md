# ACCOUNT

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[acct_top_lvl_cd](#acct_top_lvl_cd)|character varying|3|YES||NO
|[acct_top_lvl_desc](#acct_top_lvl_desc)|character varying|60|YES||NO
|[acct_type](#acct_type)|character varying|60|YES||NO
|[acct_stat](#acct_stat)|character varying|60|YES||NO
|[dlr_lvl_cntl_desc](#dlr_lvl_cntl_desc)|character varying|60|YES||NO
|[intr_party_flg](#intr_party_flg)|character varying|1|YES||NO
|[mgmt_co_empl_flg](#mgmt_co_empl_flg)|character varying|1|YES||NO
|[ext_pln_id](#ext_pln_id)|character varying|25|YES||NO
|[tin_desc](#tin_desc)|character varying|60|YES||NO
|[alpha_cd](#alpha_cd)|character varying|10|YES||NO
|[nav_acct_flg](#nav_acct_flg)|character varying|1|YES||NO
|[phon_chk_redmpn_flg](#phon_chk_redmpn_flg)|character varying|1|YES||NO
|[house_acct_flg](#house_acct_flg)|character varying|1|YES||NO
|[cap_gains_dstrbtn_cd](#cap_gains_dstrbtn_cd)|character|18|YES||NO
|[cap_gains_dstrbtn_type](#cap_gains_dstrbtn_type)|character|18|YES||NO
|[divd_dstrbtn_type_cd](#divd_dstrbtn_type_cd)|character|18|YES||NO
|[divd_dstrbtn_type](#divd_dstrbtn_type)|character|18|YES||NO
|[cust_bin](#cust_bin)|character varying|20|YES||NO
|[regstn_line_1](#regstn_line_1)|character varying|50|YES||NO
|[regstn_line_2](#regstn_line_2)|character varying|50|YES||NO
|[regstn_line_3](#regstn_line_3)|character varying|50|YES||NO
|[regstn_line_4](#regstn_line_4)|character varying|50|YES||NO
|[regstn_line_5](#regstn_line_5)|character varying|50|YES||NO
|[regstn_line_6](#regstn_line_6)|character varying|50|YES||NO
|[regstn_line_7](#regstn_line_7)|character varying|50|YES||NO
|[st_nm](#st_nm)|character varying|60|YES||NO
|[crty_nm](#crty_nm)|character varying|60|YES||NO
|[st_cd](#st_cd)|character varying|3|NO||NO
|[crty_cd](#crty_cd)|character varying|3|NO||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[legacy_type_desc](#legacy_type_desc)|character varying|100|YES||NO
|[internet_acct_flg](#internet_acct_flg)|character varying|1|YES||NO
|[grnd_father_acct_flg](#grnd_father_acct_flg)|character varying|1|YES||NO
|[pln_clsfcn_flg](#pln_clsfcn_flg)|character varying|1|YES||NO
|[mktg_clsfcn_flg](#mktg_clsfcn_flg)|character varying|1|YES||NO
|[mktg_clsfcn_desc](#mktg_clsfcn_desc)|character varying|255|YES||NO
|[rep_cd](#rep_cd)|character varying|25|YES||NO
|[rep_nm](#rep_nm)|character varying|255|YES||NO
|[dlr_type](#dlr_type)|character varying|20|YES||NO
|[hbr_dlr_nm](#hbr_dlr_nm)|character varying|60|YES||NO
|[trst_nm](#trst_nm)|character varying|60|YES||NO
|[redmpn_fee_xmpt_flg](#redmpn_fee_xmpt_flg)|character varying|1|YES||NO
|[pay_to_play_flg](#pay_to_play_flg)|character varying|1|YES||NO
|[purg_prcsd_flg](#purg_prcsd_flg)|character varying|1|YES||NO
|[acct_key](#acct_key)|integer|(32,0)|NO||YES
|[acct_type_cd](#acct_type_cd)|numeric|(38,15)|YES||NO
|[acct_nbr](#acct_nbr)|numeric|(38,15)|YES||NO
|[fund_nbr](#fund_nbr)|integer|(32,0)|YES||NO
|[tax_id](#tax_id)|numeric|(38,15)|YES||NO
|[acct_stat_cd](#acct_stat_cd)|integer|(32,0)|YES||NO
|[dlr_lvl_cntl_cd](#dlr_lvl_cntl_cd)|integer|(32,0)|NO||NO
|[pre_authzd_chk_acct_flg](#pre_authzd_chk_acct_flg)|integer|(32,0)|YES||NO
|[swp_acct_flg](#swp_acct_flg)|integer|(32,0)|YES||NO
|[ach_acct_flg](#ach_acct_flg)|integer|(32,0)|YES||NO
|[re_invst_to_another_acct_flg](#re_invst_to_another_acct_flg)|integer|(32,0)|YES||NO
|[monitored_vip_acct](#monitored_vip_acct)|integer|(32,0)|YES||NO
|[fiduciary_acct_flg](#fiduciary_acct_flg)|integer|(32,0)|YES||NO
|[mailg_flg](#mailg_flg)|integer|(32,0)|YES||NO
|[trst_co_nbr](#trst_co_nbr)|numeric|(38,15)|YES||NO
|[tpa_nbr](#tpa_nbr)|numeric|(38,15)|YES||NO
|[custodian_id](#custodian_id)|numeric|(38,15)|YES||NO
|[tpa_id](#tpa_id)|numeric|(38,15)|YES||NO
|[tin_cd](#tin_cd)|integer|(32,0)|YES||NO
|[cap_gains_dstrbtn_flg](#cap_gains_dstrbtn_flg)|integer|(32,0)|NO||NO
|[chk_writing_acct_flg](#chk_writing_acct_flg)|integer|(32,0)|YES||NO
|[divd_dstrbtn_opt_flg](#divd_dstrbtn_opt_flg)|integer|(32,0)|YES||NO
|[expedited_redmpn_acct_flg](#expedited_redmpn_acct_flg)|integer|(32,0)|YES||NO
|[sub_acct_flg](#sub_acct_flg)|integer|(32,0)|YES||NO
|[forgn_tax_rt](#forgn_tax_rt)|numeric|(38,15)|YES||NO
|[cum_discnt_nbr](#cum_discnt_nbr)|numeric|(38,15)|YES||NO
|[ltr_of_intend_nbr](#ltr_of_intend_nbr)|numeric|(38,15)|YES||NO
|[timer_flg](#timer_flg)|integer|(32,0)|YES||NO
|[gpurch_acct_flg](#gpurch_acct_flg)|integer|(32,0)|YES||NO
|[expdt_xchg_acct_flg](#expdt_xchg_acct_flg)|integer|(32,0)|YES||NO
|[pnlty_whg_acct_flg](#pnlty_whg_acct_flg)|integer|(32,0)|YES||NO
|[certfc_issuance_flg](#certfc_issuance_flg)|integer|(32,0)|YES||NO
|[stop_xfer_flg](#stop_xfer_flg)|integer|(32,0)|YES||NO
|[bluesky_xmptn_flg](#bluesky_xmptn_flg)|integer|(32,0)|YES||NO
|[bnk_card_issd_flg](#bnk_card_issd_flg)|integer|(32,0)|YES||NO
|[divd_mail_acct_flg](#divd_mail_acct_flg)|integer|(32,0)|YES||NO
|[stop_purch_acct_flg](#stop_purch_acct_flg)|integer|(32,0)|YES||NO
|[stop_mail_acct_flg](#stop_mail_acct_flg)|integer|(32,0)|YES||NO
|[fractional_chk_flg](#fractional_chk_flg)|integer|(32,0)|YES||NO
|[acct_pr_sched_flg](#acct_pr_sched_flg)|integer|(32,0)|YES||NO
|[acct_origination_id](#acct_origination_id)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[dlr_id](#dlr_id)|numeric|(38,15)|YES||NO
|[hbr_dlr_id](#hbr_dlr_id)|numeric|(38,15)|YES||NO
|[citzn_st_crty_cd](#citzn_st_crty_cd)|integer|(32,0)|YES||NO
|[acct_strt_dt](#acct_strt_dt)|timestamp without time zone|6|YES||NO
|[last_mntn_dt](#last_mntn_dt)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
|[fund_strt_dt](#fund_strt_dt)|timestamp without time zone|6|YES||NO
|[stop_mail_dt](#stop_mail_dt)|timestamp without time zone|6|YES||NO
|[acct_setup_dt](#acct_setup_dt)|timestamp without time zone|6|YES||NO
|[acct_clsd_dt](#acct_clsd_dt)|timestamp without time zone|6|YES||NO
|[purg_prcsd_dt](#purg_prcsd_dt)|timestamp without time zone|6|YES||NO


### acct_key
#### Description



#### Value Range

NUMBER

#### Logic



Auto Increment Key


### acct_top_lvl_cd
#### Description



#### Value Range

Varchar

#### Logic

'All'



### acct_top_lvl_desc
#### Description



#### Value Range

Varchar

#### Logic


'All Accounts'


### acct_type_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

SOCIAL_CODE.ACCT_TYPE_CD



ACCOUNT.SOCIAL-CODE = SOCIAL_CODE.ACCT_TYPE_CD


### acct_type
#### Description



#### Value Range

NVARCHAR

#### Logic


SOCIAL_CODE.CUST_ACCT_TYPE_NM


ACCOUNT.SOCIAL-CODE = SOCIAL_CODE.ACCT_TYPE_CD



### acct_nbr
#### Description



#### Value Range

Alpha/Num

#### Logic


ACCOUNT.ACCOUNT-NUMBER



### fund_nbr
#### Description



#### Value Range

Alpha/Num

#### Logic


ACCOUNT.FUND-CODE



### tax_id
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.TAX-IDENTIFICATION-NUMBER



### acct_stat_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.PLAN-STATUS-CODE



### acct_stat
#### Description



#### Value Range

NVARCHAR

#### Logic


CUST_ACCT_STATUS_TYPE.CUST_ACCT_STAT_NM



ACCOUNT.PLAN-STATUS-CODE = CUST_ACCT_STATUS_TYPE.CUST_ACCT_STAT_CD



### acct_strt_dt
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.ESTABLISHED-DATE



### last_mntn_dt
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.LAST-MAINTENANCE-DATE



### dlr_lvl_cntl_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_LEVEL_CONTROL.DLR_LVL_CNTL_CD


ACCOUNT.DEALER-CONTROL-LEVEL-CODE = DEALER_LEVEL_CONTROL.DLR_LVL_CNTL_CD
   


### dlr_lvl_cntl_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

DEALER_LEVEL_CONTROL.DLR_LVL_CNTL_DESC


ACCOUNT.DEALER-CONTROL-LEVEL-CODE = DEALER_LEVEL_CONTROL.DLR_LVL_CNTL_CD



### pre_authzd_chk_acct_flg
#### Description



#### Value Range

Numeric

#### Logic


ACCOUNT.PRE-AUTH-CK-ACCT


### swp_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.SWP-ACCOUNT



### ach_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic



ACCOUNT.ACH-FLAG


### re_invst_to_another_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.REINVEST-TO-FUND



### monitored_vip_acct
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.MONITOR-VIP-FLAG


### fiduciary_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.FIDUCIARY-ACCOUNT


### mailg_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.MAILING-FLAG



### intr_party_flg
#### Description



#### Value Range

NVARCHAR

#### Logic



ACCOUNT.INTERESTED-PARTY-CDE


### mgmt_co_empl_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.SHR-ACCT-MGMT-CO-EMPLOYEE-CDE 


### trst_co_nbr
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.TRUSTEE-NSCC-FIRM-NBR


### tpa_nbr
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.NSCC-TPA-FIRM-NBR 



### custodian_id
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.CUSTODIAN-ID



### tpa_id
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.THIRD-PRTY-ADMN-ID



### ext_pln_id
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.EXTERNAL-PLAN-ID 



### tin_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


TIN_CODE.TIN_CD

Join on TIN_CODE table

ACCOUNT.SS-CODE = TIN_CODE.TIN_CD



### tin_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

TIN_CODE.TIN_DESC

Join on TIN_CODE table

ACCOUNT.SS-CODE = TIN_CODE.TIN_CD


### alpha_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.ALPHA-CODE




### cap_gains_dstrbtn_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.CAP-GAINS



### chk_writing_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.CHECK-WRITING-ACCT


### divd_dstrbtn_opt_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.DIVIDEND-1 



### expedited_redmpn_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.EXPEDITED-ACCT



### sub_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.SUB-ACCOUNTING




### forgn_tax_rt
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.FOREIGN-TAX-RATE


### cum_discnt_nbr
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.CUMULATIVE-DISCOUNT-NO



### ltr_of_intend_nbr
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.LOI-NUMBER 



### timer_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.TIMER-FLAG 




### gpurch_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.LISTB-FLAG  





### expdt_xchg_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.EXPEDITED-EXCHANGE


### pnlty_whg_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.PENALTY-WITHHOLDING



### certfc_issuance_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.ISSUE-CODE



### stop_xfer_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.STOP-TRANSFER-CODE




### bluesky_xmptn_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.BLUE-SKY-EXEMPT 




### bnk_card_issd_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.DEBIT-CARD 



### nav_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.NAV-CDE




### phon_chk_redmpn_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.SHR-ACCT-PHONE-CHK-REDEMPT-CDE



### house_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.SHR-ACCT-HOUSE-ACCOUNT-CDE



### divd_mail_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.DIVIDEND-MAIL



### stop_purch_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.STOP-PURCHASE 



### stop_mail_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.STOP-MAIL



### fractional_chk_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.FRACTIONAL-CHECK




### acct_pr_sched_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.SHR-ACCT-PRICE-SCHED-CDE


### cap_gains_dstrbtn_cd
#### Description


#### Value Range

NVARCHAR

#### Logic


NULL



### cap_gains_dstrbtn_type
#### Description



#### Value Range

NVARCHAR

#### Logic


NULL




### divd_dstrbtn_type_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


NULL




### divd_dstrbtn_type
#### Description



#### Value Range

NVARCHAR

#### Logic


NULL



### cust_bin
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.FINS-ACCOUNT-ID 


### acct_origination_id
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.ACCOUNT-ORIGINATION-ID



### regstn_line_1
#### Description



#### Value Range
Alpha/Num

#### Logic

ACCOUNT.REGISTRATION-LINE-1-TEXT      



### regstn_line_2
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.REGISTRATION-LINE-2-TEXT      


### regstn_line_3
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.REGISTRATION-LINE-3-TEXT 


### regstn_line_4
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.REGISTRATION-LINE-4-TEXT 




### regstn_line_5
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.REGISTRATION-LINE-5-TEXT 

### regstn_line_6
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.REGISTRATION-LINE-6-TEXT  

### regstn_line_7
#### Description



#### Value Range

NVARCHAR

#### Logic



ACCOUNT.REGISTRATION-LINE-7-TEXT 


### st_nm
#### Description



#### Value Range

NVARCHAR

#### Logic


REGION.ST_NM


ACCOUNT.SHR-ACCT-CITIZEN-ST-CNTRY-CDE = REGION.ST_CD



### crty_nm
#### Description



#### Value Range

NVARCHAR

#### Logic

REGION.CRTY_NM


ACCOUNT.SHR-ACCT-CITIZEN-ST-CNTRY-CDE = REGION.CRTY_CD



### st_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.SHR-ACCT-CITIZEN-ST-CNTRY-CDE


### crty_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.SHR-ACCT-CITIZEN-ST-CNTRY-CDE


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





### curr_row_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

'Y'



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





### legacy_type_desc
#### Description



#### Value Range

NVARCHAR

#### Logic


NULL


### fund_strt_dt
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.ESTABLISHED-DATE



### stop_mail_dt
#### Description



#### Value Range

NVARCHAR

#### Logic


HBR_MASTER.STOPMAILDATE


ACCOUNT.FUND-CODE = HBR_MASTER.FUND AND
ACCOUNT.ACCOUNT-NUMBER = HBR_MASTER.ACCOUNT


### acct_setup_dt
#### Description



#### Value Range

NVARCHAR

#### Logic


HBR_MASTER.ACCT_SETUP_DTE

ACCOUNT.FUND-CODE = HBR_MASTER.FUND AND
ACCOUNT.ACCOUNT-NUMBER = HBR_MASTER.ACCOUNT


### internet_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.SHR-ACCT-CLIENT-DEFINED-1-CDE



### grnd_father_acct_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.SHR-ACCT-CLIENT-DEFINED-6-CDE


### pln_clsfcn_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


NULL



### mktg_clsfcn_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


MARKET_CLASSIFICATION.MKT_CLSFCN_CD


ACCOUNT.CUSTOMER-ACCOUNT-NUMBER = MARKET_CLASSIFICATION.MKT_CLSFCN_CD 


### mktg_clsfcn_desc
#### Description



#### Value Range

NVARCHAR

#### Logic


MARKET_CLASSIFICATION.MKT_CLSFCN_DESC

ACCOUNT.CUSTOMER-ACCOUNT-NUMBER = MARKET_CLASSIFICATION.MKT_CLSFCN_CD 



### rep_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.REPRESENTATIVE-NUMBER



### rep_nm
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.REPRESENTATIVE-NAME


### dlr_id
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.FINS-ID 


### dlr_type
#### Description



#### Value Range

NVARCHAR

#### Logic


WHEN ACCOUNT.FINS_ID IN (5471, 5700) AND ACCOUNT.TRUSTEE_NSCC_FIRM_NBR IS NOT NULL   THEN 'TRUST'
WHEN ACCOUNT.FINS_ID IN (5471, 5700) AND ACCOUNT.TRUSTEE_NSCC_FIRM_NBR IS NULL THEN  'INDIVIDUAL'
WHEN ACCOUNT.FINS_ID NOT IN (5471, 5700) THEN 'DEALER'  ELSE 'UNDEFINED'

### hbr_dlr_id
#### Description



#### Value Range

NVARCHAR

#### Logic


When ACCOUNT.FINS_ID in (5471, 5700) AND  ACCOUNT.TRUSTEE_NSCC_FIRM_NBR is not null
 THEN ACCOUNT.TRUSTEE_NSCC_FIRM_NBR ELSE FINS_ID


### hbr_dlr_nm
#### Description



#### Value Range

NVARCHAR

#### Logic

WHEN ACCOUNT.FINS_ID IN (5471, 5700) 
AND ACCOUNT.TRUSTEE_NSCC_FIRM_NBR IS NOT NULL  
concatenate(ACCOUNT.CMR_NME_PRE_TXT, ACCOUNT.CMR_FRST_NME, ACCOUNT.CMR_MDL_NAME, ACCOUNT.CMR_CRP,LST_NME, ACCOUNT.CMR_NME_SFX_TXT)

### trst_nm
#### Description



#### Value Range

NVARCHAR

#### Logic



ACCOUNT.FUND-CODE = ACCT_POSN.FUND-CODE                     
ACCOUNT.ACCOUNT-NUMBER = ACCT_POSN.ACCOUNT-NUMBER               

AND ACCOUNT.DEALER-CONTROL-LEVEL-CODE =4 THEN  
Concatenate( ACCOUNT.CMR-NME-PRE-TXT, ACCOUNT.CMR-FRST-NME, ACCOUNT.CMR-MDL-NME, ACCOUNT.CMR-CRP-LST-NME, ACCOUNT.CMR-NME-SFX-TXT )



### acct_clsd_dt
#### Description



#### Value Range

NVARCHAR

#### Logic

If ACCOUNT.acct_clsd_dt is NULL
IF
 (ACCOUNT.TOTAL-SHARE-CT  * ACCOUNT.NAV-CDE) for the current_day = 0 
 AND (ACCOUNT.TOTAL-SHARE-CT  * ACCOUNT.NAV-CDE)  >0 for previous business day 
 THEN acct_clsd_dt is previous business day. 
 
 All other cases it is NULL




### redmpn_fee_xmpt_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCOUNT.SHR-ACCT-CLIENT-DEFINED-3-CDE


### pay_to_play_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

HBR_MASTER_EXTENTION.PAY_TO_PLAY

ACCOUNT.FUND-CODE = HBR_MASTER_EXTENTION.FUND AND
ACCOUNT.ACCOUNT-NUMBER = HBR_MASTER_EXTENTION.ACCOUNT


### citzn_st_crty_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCOUNT.SHR_ACCT_CITIZEN_ST_CNTRY_CDE



### purg_prcsd_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


NULL 



### purg_prcsd_dt
#### Description



#### Value Range

NVARCHAR

#### Logic

NULL 



#### Files Used

1. ACCOUNT (DTO.HCA.AMP.TIP09.YYMMDD.HHMMSS)


### File Location

dstprod/DTO.HCA.AMP.TIP09.20190719.224714.zip.pgp.txt


#### Tables Used

2. ACCT_POSN 
3. TIN_CODE
4. MARKET_CLASSIFICATION
5. REGION
6. CUSTOMER_ACCOUNT_TYPE
7. CUST_ACCT_STATUS_TYPE
8. DEALER_LEVEL_CONTROL
9. HBR_MASTER
10. HBR_MASTER_EXTENTION
11. SOCIAL_CODE


## Business Keys

1. FUND-CODE                    
2. ACCOUNT-NUMBER 


#### COBOL Files

AMP.TIP09.CBL