# CASH_HOLDING

## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[cash_holdg_key](#cash_holdg_key)|integer|(32,0)|NO||YES
|[fund_compst_key](#fund_compst_key)|integer|(32,0)|YES||YES
|[day_key](#day_key)|integer|(32,0)|YES||YES
|[cash](#cash)|numeric|(38,15)|YES||NO
|[shrt_term_invmt](#shrt_term_invmt)|numeric|(38,15)|YES||NO
|[recbl](#recbl)|numeric|(38,15)|YES||NO
|[paybl](#paybl)|numeric|(38,15)|YES||NO
|[net_shrs](#net_shrs)|numeric|(38,15)|YES||NO
|[net_asset](#net_asset)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[cash_coltrl](#cash_coltrl)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### cash_holdg_key
#### Description

Generated key 

#### Value Range

ALL NUMBERS

#### Logic
 
sequence generator  Autoincrement +1 for new inserts 



### fund_compst_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

	file :FUND_COMPOSITE.CSV , full_prc.csv
	Fund_ID (from full_prc.csv)  = ST_STR_FUND_NBR (from FUND_COMPOSITE.CSV)
	Output Column: COMPST_FUND_KEY (FROM FUND_COMPOSITE.CSV)
	



### day_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

LOOKUP

FILE:  full_prc.csv, CALENDAR.CSV

Condition : Date (FROM full_prc.csv) = CAL_DAY (FROM CALENDAR.CSV)

Output Column: DAY_KEY (FROM CALENDAR.CSV)



### cash
#### Description

#### Value Range

ALL NUMBERS

#### Logic



column name : Fund_Cash




### shrt_term_invmt
#### Description

#### Value Range

ALL NUMBERS

#### Logic


column name : SHORT_TERM





### recbl
#### Description

#### Value Range

ALL NUMBERS

#### Logic



column name : INV. REC



### paybl
#### Description

#### Value Range

ALL NUMBERS

#### Logic


column name : INV. PAY





### net_shrs
#### Description

#### Value Range

ALL NUMBERS

#### Logic



column name : Net Pending Shares



### net_asset
#### Description

#### Value Range

ALL NUMBERS

#### Logic


column name : Total Net Assets




### curr_row_flg
#### Description

#### Value Range

Always value 'Y'

#### Logic



column name: CURR_ROW_FLG

Always value 'Y'



### row_strt_dttm
#### Description

#### Value Range

CURRENT DATE

#### Logic

CALCULTED COLUMN - SYSDATE FUNCTION



### row_stop_dttm
#### Description

#### Value Range

ALL DATE VALUES

#### Logic




### etl_load_cyc_key
#### Description

#### Value Range

ALL NUMBERS

#### Logic

Column From Informatica - ETL KEY 



### src_sys_id
#### Description

#### Value Range

Always value 4 

#### Logic

Column From Informatica - SOURCE ID

Always value 4 



### cash_coltrl
#### Description

#### Value Range

ALL NUMBERS

#### Logic

[4:31 PM] Suresh Nair
    
 FUND_ID and CALEN_DT are the two business keys in the PTA_SSB_POSITION file that we can use.  Here's the logic I woud use:


SUM (shrpar_qty) 
group by fund_id and calen_dt 
where PTA_SSB_POSITION_MMDDYY.ISSUE_CLS_CD = 26

	Lookup fund_composite.st_str_fund_nbr  = fund_id get the fund_compst_key
	Lookup  calendar.cal_day = calen_dt  get the day_key
	Use fund_compst_key and day_key to join cash_holdings table and update cash_coltrl = shrpar_qty
	 
 please note that only few funds will have cash collateral value, but that is normal.




--------------------------------------------------------------------------------------------
## Source files

full_prc_MMDDYY.csv
CALENDAR.csv
FUND_COMPOSITE.csv
PTA_SSB_POSITION_MMDDYY.txt


###files:

Files Type: Comma Delimited and Pipe Delimited
File Location: https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/fund/fund_composite.csv
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/ssb/PTA_SSB_POSITION_080619.txt
https://datalake-fileprocbucket-g5v3fkn4q5et.s3.us-east-2.amazonaws.com/ssb/full_prc_071919.CSV



Header for PTA_SSB_POSITION_Header
TYPE,
FUND_ID,
BASE_CNTRY_CD,
CRNCY_CD_BASE,
CALEN_DT,
ASSET_ID,
ALT_ASSET_ID_TYPE_CD1,
ALT_ASSET_ID1,
ALT_ASSET_ID_TYPE_CD2,
ALT_ASSET_ID2,
DTC_CUSIP,
TICKER_SYMB,
ISSUE_LONG_NM,
INVEST_IND,
ASSET_GRP_CD,
POS_TYPE_CD,
INVEST_TYPE_CD,
STATE_CD,
INCORP_CNTRY_CD,
ISSUE_CNTRY_CD,
TRD_CNTRY_CD,
NRATAX_CNTRY_CD,
ISSUE_DT,
POS_MTRTY_DT,
DAYS_TOMAT_CNT,
ORIG_CPN_RT,
POS_INT_RT,
VAR_RT_CHG_DT,
VAR_RT_FREQ_CD,
MOODY_RTG,
SNP_RTG,
MJR_INDSTY_CD,
MNR_INDSTY_CD,
POOL_TYPE_CD,
LOT_ACCT_NUM,
REPO_NUM,
TOT_LOTS,
TRD_DT,
SETTLE_DT,
ISSUE_CLS_CD,
RPT_CLS_CD,
CALL_PUT_IND,
ORG_STRIKE_PRC,
CRNT_STRIKE_PRC,
SHRPAR_QTY,
CONTRACTS_QTY,
ORG_FACE_POS_QTY,
UNTCST_BTL,
UNTCST_LTL,
CST_ORG_BTL,
CST_ORG_LTL,
CST_BTL,
CST_LTL,
AMTZD_BTL,
AMTZD_LTL,
ACCRD_BTL,
ACCRD_LTL,
MKTVAL_BTL,
MKTVAL_LTL,
UNRLZD_GNLS_BTL,
UNRLZD_GNLS_LTL,
MKTPRC_BAM,
MKTPRC_LAM,
MKTPRC_EAM,
CRNT_XCHRT,
CRNTEXCH_RT_SRC_CD,
FASPRC_SRC_CD,
MKTPRC_CRNCY_CD,
MKTPRC_CRNCYCD_EUR,
MKT_VAL_CD,
DAY_61_PRICE,
DAY_61_DAY,
DAY_61_YLD,
FILLER,
BRKR_FINS,
SW_CLOSING_FX,
SSB_TRADE_ID,
INCM_CRNCY_CD,
FVM_CD





