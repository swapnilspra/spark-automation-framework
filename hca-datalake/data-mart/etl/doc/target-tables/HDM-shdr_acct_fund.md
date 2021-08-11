# SHDR_ACCT_FUND

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[prim_shrhldr_flg](#prim_shrhldr_flg)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[email_addr](#email_addr)|character varying|255|YES||NO
|[email_cnsnt_flg](#email_cnsnt_flg)|character varying|1|YES||NO
|[shrhldr_acct_key](#shrhldr_acct_key)|integer|(32,0)|NO||YES
|[dlr_branch_key](#dlr_branch_key)|integer|(32,0)|NO||NO
|[fund_key](#fund_key)|integer|(32,0)|NO||NO
|[acct_key](#acct_key)|integer|(32,0)|NO||NO
|[shrhldr_key](#shrhldr_key)|integer|(32,0)|NO||NO
|[dlr_key](#dlr_key)|integer|(32,0)|NO||NO
|[reg_key](#reg_key)|integer|(32,0)|NO||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### shrhldr_acct_key
#### Description



#### Value Range

NUMBERS

#### Logic


Auto Increment sequence Generator


### dlr_branch_key
#### Description



#### Value Range

NUMBERS

#### Logic

DEALER_BRANCH.DLR_BRANCH_KEY



SHDR_ACCT_FUND.FINANCIAL-INST-ID = DEALER_BRANCH.DLR_ID
AND SHDR_ACCT_FUND.FINCL-INST-BRCH-ID  = DEALER_BRANCH.BRANCH_ID


### fund_key
#### Description



#### Value Range

NUMBERS

#### Logic

FUND.FUND_KEY


SHDR_ACCT_FUND.FUND-CODE= FUND.FUND_NBR


### acct_key
#### Description



#### Value Range

NUMBERS

#### Logic

ACCOUNT.ACCT_KEY



SHDR_ACCT_FUND.FUND-CODE = ACCOUNT.FUND_NBR  AND
SHDR_ACCT_FUND.ACCOUNT-NUMBER = ACCOUNT.ACCT_NBR


### shrhldr_key
#### Description



#### Value Range

NUMBERS

#### Logic

SHAREHOLDER.SHRHLDR_KEY


SHDR_ACCT_FUND.CMR-BPR-ID1 = SHAREHOLDER.CUST_IDNFCN_NBR_1
SHDR_ACCT_FUND.CMR-BPR-ID2 = SHAREHOLDER.CUST_IDNFCN_NBR_2




### dlr_key
#### Description



#### Value Range

NUMBERS

#### Logic

DEALER.DLR_KEY



SHDR_ACCT_FUND.FINANCIAL-INST-ID = DTO.HCA_AMP_TIP09.FINCL_ID


### reg_key
#### Description



#### Value Range

NUMBERS

#### Logic

REGION.REG_KEY



Join on REGION Using below output

DST_ST_COUNTRY.ST_CD = REGION.ST_CD
AND DST_ST_COUNTRY.CRTY_ISO_CD = REGION.CRTY_CD


Join on DST_ST_COUNTRY Table

Get ST_CD and CRTY_ISO_CD


DST_ST_COUNTRY.DST_ST_CD = ACCOUNT.RESIDENT-STATE-COUNTRY
AND DST_ST_COUNTRY.DST_CRTY_CD = ACCOUNT.RESIDENT-STATE-COUNTRY





### prim_shrhldr_flg
#### Description



#### Value Range

NUMBERS

#### Logic

SHDR_ACCT_FUND.CLOR-TAX-RSP-PRTY



### row_strt_dttm
#### Description



#### Value Range

DATE

#### Logic





### row_stop_dttm
#### Description



#### Value Range

DATE

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





### curr_row_flg
#### Description



#### Value Range

NVARCHAR

#### Logic





### email_addr
#### Description



#### Value Range

NVARCHAR

#### Logic

HBR_MASTER.STM_EML_ADR_TXT



SHDR_ACCT_FUND.FUND-CODE = HBR_MASTER.FUND  AND
SHDR_ACCT_FUND.ACCOUNT-NUMBER  = HBR_MASTER.ACCOUNT





### email_cnsnt_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

HBR_MASTER.SAMGMTEMPL



SHDR_ACCT_FUND.FUND-CODE = HBR_MASTER.FUND  AND
SHDR_ACCT_FUND.ACCOUNT-NUMBER  = HBR_MASTER.ACCOUNT



### FIles used

1. SHDR_ACCT_FUND (DTO.HCA.AMP.TIP09.20190719.224714.zip.pgp.txt)

### File Location

dstprod/DTO.HCA.AMP.TIP09.20190719.224714.zip.pgp.txt


### Business Key

ACCOUNT-NUMBER
FINANCIAL-INST-ID
FUND-CODE
CUST_IDNFCN_NBR_1
CUST_IDNFCN_NBR_2 (to get the shareholder key)


### Tables Used

FUND
DEALER_BRANCH
DEALER
REGION
DST_ST_COUNTRY
HBR_MASTER
ACCOUNT
SHAREHOLDER


### COBOL File

AMP.TIP09.CBL
