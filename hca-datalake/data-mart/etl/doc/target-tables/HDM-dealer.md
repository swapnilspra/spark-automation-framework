# DEALER

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[dlr_top_lvl](#dlr_top_lvl)|character varying|3|YES||NO
|[dlr_top_lvl_desc](#dlr_top_lvl_desc)|character varying|60|YES||NO
|[dlr_grp_id](#dlr_grp_id)|character varying|25|YES||NO
|[dlr_grp_nm](#dlr_grp_nm)|character varying|255|YES||NO
|[dlr_id](#dlr_id)|character varying|25|YES||NO
|[sellg_agrmt_rqrd_flg](#sellg_agrmt_rqrd_flg)|character varying|1|YES||NO
|[sellg_agrmt_12b1_flg](#sellg_agrmt_12b1_flg)|character varying|1|YES||NO
|[payout_freq_cd](#payout_freq_cd)|character varying|1|YES||NO
|[payout_freq_desc](#payout_freq_desc)|character varying|60|YES||NO
|[stmt_freq_cd](#stmt_freq_cd)|character varying|2|YES||NO
|[stmt_freq_desc](#stmt_freq_desc)|character varying|60|YES||NO
|[recv_12b1_tape_flg](#recv_12b1_tape_flg)|character varying|1|YES||NO
|[auto_divd_chk_flg](#auto_divd_chk_flg)|character varying|1|YES||NO
|[wire_incl_accrl_flg](#wire_incl_accrl_flg)|character varying|1|YES||NO
|[sipc_insd_flg](#sipc_insd_flg)|character varying|1|YES||NO
|[dlr_fdic_sell_flg](#dlr_fdic_sell_flg)|character varying|1|YES||NO
|[mbr_ship_type_cd](#mbr_ship_type_cd)|character varying|2|YES||NO
|[mbr_ship_type](#mbr_ship_type)|character varying|60|YES||NO
|[inval_rep_hndlg_cd](#inval_rep_hndlg_cd)|character varying|1|YES||NO
|[inval_rep_hndlg_desc](#inval_rep_hndlg_desc)|character varying|60|YES||NO
|[invmt_slip_addr_cd](#invmt_slip_addr_cd)|character varying|1|YES||NO
|[invmt_slip_addr](#invmt_slip_addr)|character varying|60|YES||NO
|[grp_sales_chnl_id](#grp_sales_chnl_id)|character varying|25|YES||NO
|[divd_confirm_sprsn_flg](#divd_confirm_sprsn_flg)|character varying|1|YES||NO
|[daily_confirm_sprsn_flg](#daily_confirm_sprsn_flg)|character varying|1|YES||NO
|[onln_inval_rep_hndlg_flg](#onln_inval_rep_hndlg_flg)|character varying|1|YES||NO
|[dlr_rep_st_lic_vldn_flg](#dlr_rep_st_lic_vldn_flg)|character varying|1|YES||NO
|[dlr_inval_rep_st_lic_actn_flg](#dlr_inval_rep_st_lic_actn_flg)|character varying|1|YES||NO
|[nasd_regstn_vldn_lic_flg](#nasd_regstn_vldn_lic_flg)|character varying|1|YES||NO
|[inval_rep_nasd_regstr_flg](#inval_rep_nasd_regstr_flg)|character varying|1|YES||NO
|[dlr_nm](#dlr_nm)|character varying|60|YES||NO
|[addr_line_1](#addr_line_1)|character varying|60|YES||NO
|[addr_line_2](#addr_line_2)|character varying|60|YES||NO
|[addr_line_3](#addr_line_3)|character varying|60|YES||NO
|[addr_line_4](#addr_line_4)|character varying|60|YES||NO
|[city_nm](#city_nm)|character varying|60|YES||NO
|[st](#st)|character varying|2|YES||NO
|[crty](#crty)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[spr_onbs_flg](#spr_onbs_flg)|character varying|1|YES||NO
|[onbs_flg](#onbs_flg)|character varying|1|YES||NO
|[actv_flg](#actv_flg)|character varying|1|YES|'Y'::character varying|NO
|[dlr_key](#dlr_key)|integer|(32,0)|NO||YES
|[sec_id](#sec_id)|numeric|(38,15)|YES||NO
|[trste_dflt_dlr_id](#trste_dflt_dlr_id)|numeric|(38,15)|YES||NO
|[grp_fincl_inst_id](#grp_fincl_inst_id)|numeric|(38,15)|YES||NO
|[postl_zip_cd](#postl_zip_cd)|integer|(32,0)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[branch_estb_dt](#branch_estb_dt)|timestamp without time zone|6|YES||NO
|[branch_closed_dt](#branch_closed_dt)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
|[intrl_cntl_doc_dt](#intrl_cntl_doc_dt)|timestamp without time zone|6|YES||NO
|[ltst_cntl_audt_recpt_dt](#ltst_cntl_audt_recpt_dt)|timestamp without time zone|6|YES||NO
|[inactv_dt](#inactv_dt)|timestamp without time zone|6|YES||NO


### dlr_key
#### Description



#### Value Range

NVARCHAR

#### Logic

Auto Increment Key



### dlr_top_lvl
#### Description



#### Value Range

NVARCHAR

#### Logic

'All'



### dlr_top_lvl_desc
#### Description



#### Value Range

NVARCHAR

#### Logic


'All Dealers'




### dlr_grp_id
#### Description



#### Value Range

Numeric

#### Logic

Join HSG_DEALER_GROUPING and HSG_DEALER_PROFILE_DATABASE on DEALER_GROUPING.ID = DEALER_PROFILE_DATABASE.DEALER_GROUP_ID
and then derive  DLR_GRP_ID by matching the DLR_NBR.


### dlr_grp_nm
#### Description



#### Value Range

NVARCHAR

#### Logic


IF HSG_DEALER_GROUPING.DLR_GRP_NM = 'NO GROUPING' THEN DEALER.FINCL_INST_NAME_1
            ELSE HSG_DEALER_GROUPING.DLR_GRP_NM matched by DLR_NBR.



### dlr_id
#### Description



#### Value Range

Numeric

#### Logic


DEALER.FINANCIAL-INST-ID


### branch_estb_dt
#### Description



#### Value Range

Alpha/Num

#### Logic

DEALER.FI-ESTABLISHED-DTE



### branch_closed_dt
#### Description



#### Value Range

Alpha/Num

#### Logic


DEALER.FI-CLOSED-DTE



### sellg_agrmt_rqrd_flg
#### Description



#### Value Range

Alpha/Num

#### Logic


DEALER.FI-SLS-AGMT-REQD



### sellg_agrmt_12b1_flg
#### Description



#### Value Range

Alpha/Num

#### Logic

DEALER.FI-12B1-AGMT-REQD




### payout_freq_cd
#### Description


#### Value Range

Alpha/Num

#### Logic


DEALER.FI-12B1-PAY-FREQ


### payout_freq_desc
#### Description



#### Value Range

Varchar

#### Logic

PAYOUT_FREQUENCY.FREQ_DESC


DEALER.FI-12B1-PAY-FREQ  = PAYOUT_FREQUENCY.FREQ_CD


### stmt_freq_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

DEALER.FI-STM-FRQ-CD


### stmt_freq_desc
#### Description



#### Value Range

NVARCHAR


#### Logic

STATEMENT_FREQUENCY.FREQ_DESC


DEALER.FI-STM-FRQ-CD = STATEMENT_FREQUENCY.FREQ_CD



### recv_12b1_tape_flg
#### Description



#### Value Range

Alpha/Num

#### Logic

DEALER.FI-12B1-TAPE-CDE


### auto_divd_chk_flg
#### Description


#### Value Range

NVARCHAR

#### Logic

DEALER.FI-AUTO-DIV-CHK-CD


### wire_incl_accrl_flg
#### Description


#### Value Range

Alpha/Num

#### Logic

DEALER.WIRE-INCL-ACRL-CDE


### sipc_insd_flg
#### Description

#### Value Range

Alpha/Num

#### Logic

DEALER.SIPC-INSURED-CDE



### dlr_fdic_sell_flg
#### Description


#### Value Range

Alpha/Num

#### Logic

DEALER.SELLS-IN-FDIC-CDE



### sec_id
#### Description


#### Value Range

Numeric

#### Logic

DEALER.SEQUENCE-NUMBER-ID


### mbr_ship_type_cd
#### Description

#### Value Range

Alpha/Num

#### Logic


DEALER.FI-MEMBER-TYPE-CDE


### mbr_ship_type
#### Description


#### Value Range

Alpha/Num

#### Logic

DEALER_MEMBERSHIP_TYPE.TYPE_DESC

DEALER.FI-MEMBER-TYPE-CDE  = DEALER_MEMBERSHIP_TYPE.MBRS_TYPE_CD





#### inval_rep_hndlg_cd
#### Description


#### Value Range

NVARCHAR

#### Logic

DEALER.ONLIN-INVLD-REP-CD


### inval_rep_hndlg_desc
#### Description


#### Value Range

Alpha/Num

#### Logic


INVALID_REP_HANDLING.HNDLG_DESC

DEALER.ONLIN-INVLD-REP-CD = INVALID_REP_HANDLING.REP_HNDLG_CD


### invmt_slip_addr_cd
#### Description



#### Value Range

Alpha/Num

#### Logic

DEALER.IVT-SLIP-ADR-CD



### invmt_slip_addr
#### Description


#### Value Range

NVARCHAR

#### Logic


INVESTMENT_SLIP_ADDRESS.SLIP_ADDR_DESC

DEALER.IVT-SLIP-ADR-CD = INVESTMENT_SLIP_ADDRESS.SLIP_ADDR_CD




### trste_dflt_dlr_id
#### Description


#### Value Range

Numeric

#### Logic

DEALER.TRUSTEE-DFL-DLR-ID




### grp_sales_chnl_id
#### Description


#### Value Range

Alpha/Num

#### Logic


DEALER.FI-FG-SLS-CHN-TYPE


### grp_fincl_inst_id
#### Description



#### Value Range

Numeric

#### Logic


DEALER.MERGED-TO-FG-FI-ID


### divd_confirm_sprsn_flg
#### Description



#### Value Range

Alpha/Num

#### Logic


DEALER.DIV-CONFIRM-SUPPRESSION-CD



### daily_confirm_sprsn_flg
#### Description



#### Value Range
Alpha/Num

#### Logic


DEALER.DAILY-CONFIRM-SUPPRESSION-CD



### onln_inval_rep_hndlg_flg
#### Description



#### Value Range

Alpha/Num

#### Logic


DEALER.ONLIN-INVLD-REP-CD

### dlr_rep_st_lic_vldn_flg
#### Description



#### Value Range

Alpha/Num

#### Logic

DEALER.FI-STATE-LIC-VLD


### dlr_inval_rep_st_lic_actn_flg
#### Description



#### Value Range

Alpha/Num

#### Logic

DEALER.FI-INVLD-ST-LIC-CD




### nasd_regstn_vldn_lic_flg
#### Description



#### Value Range

Alpha/Num

#### Logic

DEALER.FI-NASD-LIC-VLD


### inval_rep_nasd_regstr_flg
#### Description



#### Value Range

Alpha/Num

#### Logic

DEALER.FI-INVLD-NASD-LIC


### dlr_nm
#### Description



#### Value Range

Alpha/Num

#### Logic


DEALER.FINCL-INST-NAME-1



### addr_line_1
#### Description



#### Value Range

NVARCHAR

#### Logic

DEALER_BRANCH.FI-BRCH-ADDR-1-TXT

DEALER_BRANCH.DLR_ID= DEALER.FINANCIAL-INST-ID AND
DEALER_BRANCH.DLR_ID=0


### addr_line_2
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_BRANCH.FI-BRCH-ADDR-2-TXT


DEALER_BRANCH.DLR_ID= DEALER.FINANCIAL-INST-ID AND
DEALER_BRANCH.DLR_ID=0

### addr_line_3
#### Description



#### Value Range

NVARCHAR

#### Logic


DEALER_BRANCH.FI-BRCH-ADDR-3-TXT

DEALER_BRANCH.DLR_ID= DEALER.FINANCIAL-INST-ID AND
DEALER_BRANCH.DLR_ID=0


### addr_line_4
#### Description



#### Value Range

NVARCHAR

#### Logic

DEALER_BRANCH.FI-BRCH-ADDR-4-TXT

DEALER_BRANCH.DLR_ID= DEALER.FINANCIAL-INST-ID AND
DEALER_BRANCH.DLR_ID=0



### city_nm
#### Description



#### Value Range

NVARCHAR

#### Logic

DEALER_BRANCH.BRANCH_CITY

DEALER_BRANCH.DLR_ID= DEALER.FINANCIAL-INST-ID AND
DEALER_BRANCH.DLR_ID=0




### st
#### Description



#### Value Range

NVARCHAR

#### Logic

DEALER_BRANCH.BRANCH_ST



DEALER_BRANCH.DLR_ID= DEALER.FINANCIAL-INST-ID AND
DEALER_BRANCH.DLR_ID=0


### postl_zip_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

DEALER_BRANCH.BRANCH_ZIP_CD


DEALER_BRANCH.DLR_ID= DEALER.FINANCIAL-INST-ID AND
DEALER_BRANCH.DLR_ID=0


### crty
#### Description



#### Value Range

NVARCHAR

#### Logic


Default to 'US'


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





### spr_onbs_flg
#### Description



#### Value Range

NVARCHAR

#### Logic


HSG_DEALER_PROFILE_DATABASE.TradesSuperOmnibusValue

DECODE (UPPER (HSG_DEALER_PROFILE_DATABASE.TradesSuperOmnibusValue),'YES', 'Y','NO', 'N')


DEALER.FINANCIAL-INST-ID = HSG_DEALER_PROFILE_DATABASE.DEALERNUMBER




### onbs_flg
#### Description



#### Value Range

NVARCHAR

#### Logic



HSG_DEALER_PROFILE_DATABASE.TradesOmnibusValue

DECODE (UPPER (HSG_DEALER_PROFILE_DATABASE.TradesOmnibusValue),'YES', 'Y' ,'NO','N','U')

Join with HSG_DEALER_PROFILE_DATABASE

DEALER.FINANCIAL-INST-ID = HSG_DEALER_PROFILE_DATABASE.DEALERNUMBER




### intrl_cntl_doc_dt
#### Description



#### Value Range

NVARCHAR

#### Logic


HSG_DEALER_PROFILE_DATABASE.Created


DEALER.FINANCIAL-INST-ID = HSG_DEALER_PROFILE_DATABASE.DEALERNUMBER




### ltst_cntl_audt_recpt_dt
#### Description



#### Value Range

NVARCHAR

#### Logic

HSG_DEALER_PROFILE_DATABASE.DateOfReceiptOfMostRecentControlAuditReport

DEALER.FINANCIAL-INST-ID = HSG_DEALER_PROFILE_DATABASE.DEALERNUMBER





### inactv_dt
#### Description



#### Value Range

NVARCHAR

#### Logic


HSG_DEALER_PROFILE_DATABASE.InactiveDate


DEALER.FINANCIAL-INST-ID = HSG_DEALER_PROFILE_DATABASE.DEALERNUMBER




### actv_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

Default 'Y'





# Files Used

1. DEALER (DTO.HCA.FDP.TIP56.YYYYMMDD.HHMMSS.zip.pgp.txt)


### File Location


dstprod/DTO.HCA.FDP.TIP56.20190719.231402.zip.pgp.txt


### Tables Used

HSG_DEALER_PROFILE_DATABASE

STATEMENT_FREQUENCY

DEALER_BRANCH

INVALID_REP_HANDLING

INVESTMENT_SLIP_ADDRESS

DEALER_MEMBERSHIP_TYPE

PAYOUT_FREQUENCY




## Business Keys

1. FINANCIAL-INST-ID

### COBOL Files

FDP.TIP56.CBL


