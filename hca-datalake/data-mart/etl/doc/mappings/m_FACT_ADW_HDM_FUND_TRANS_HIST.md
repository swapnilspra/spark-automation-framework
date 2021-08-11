|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	SHR_BAL_EFFCT_CD	|	string	|	1	|	9	|		|		|	Seq Number	|		|		|		|	Table: HDM.FUND_TRANS_HIST<br>Condition: FUND_TXN_ID = in_FUND_TXN_ID AND FUND_TXN_ID_2 = in_FUND_TXN_ID_2<br>	|	FUND_TRANS_HIST	|	TXN_HIST_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	CONTRBN_YR	|	double	|	15	|		|		|		|		|		|	Table: HDM.TRANSACTION_TYPE<br>Condition: TXN_SFX_CD = IN_TXN_SFX_CD AND TXN_CD = IN_TXN_CD<br>output Column: TXN_TYPE_KEY<br>	|		|		|		|	TXN_TYPE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	VOLN_TXN_DESC	|	string	|	26	|		|		|		|		|		|	Table: HDM.REGION<br>Condition: ST_CD = IN_ST_CD AND MKTG_REG_ID = IN_MKTG_REG_ID AND CRTY_CD = IN_CRTY_CD<br>output Column:REG_KEY<br>	|		|		|		|	REG_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SHR_PR_AMT	|	double	|	15	|		|		|		|		|		|	Table: HDM.DEALER<br>Condition: DLR_ID = IN_DLR_ID<br>output ColumnDLR_KEY<br>	|		|		|		|	DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	GR_TXN_AMT	|	double	|	15	|		|		|		|		|		|	Table:HDM.SHAREHOLDER<br>Condition: CUST_IDNFCN_NBR = IN_CUST_IDNFCN_NBR AND CUST_IDNFCN_NBR_2 = IN_CUST_IDNFCN_NBR_2<br>output Column:SHRHLDR_KEY<br>	|		|		|		|	SHRHLDR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SHRS_TRANSACTED	|	double	|	15	|		|		|		|		|		|	Table:HDM.ACCOUNT<br>Condition: ACCT_NBR = IN_ACCT_NBR AND FUND_NBR = IN_FUND_NBR<br>output Column:ACCT_KEY<br>	|		|		|		|	ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	DISCNT_CAT	|	double	|	15	|		|		|		|		|		|	Table:HDM.FUND<br>Condition: FUND_NBR = IN_FUND_NBR<br>output Column:FUND_KEY<br>	|		|		|		|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ORD_NBR	|	double	|	15	|		|		|		|		|		|	//:LKP.LKP_DM_DIM_CALENDAR(TXN_EFFTV_DT)<br><br>Table:LKP.LKP_DM_DIM_CALENDAR<br>Condition: CAL_DAY = TXN_EFFTV_DT<br>output Column:DAY_KEY<br>	|		|		|		|	EFFTV_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	NSCC_SHRT_TERM_WAVR_RSN	|	string	|	1	|		|		|		|		|		|	//::LKP.LKP_DM_DIM_CALENDAR(TXN_STTL_DT)<br><br>Table:LKP.LKP_DM_DIM_CALENDAR<br>Condition: CAL_DAY = TXN_STTL_DT<br>output Column:DAY_KEY<br>	|		|		|		|	STTL_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	PCT_SALES_CHARGES	|	double	|	15	|		|		|		|		|		|	:LKP.LKP_DM_DIM_CALENDAR(PMT_DT)<br><br>Table:LKP.LKP_DM_DIM_CALENDAR<br>Condition: CAL_DAY = PMT_DT<br>output Column:DAY_KEY<br>	|		|		|		|	PMT_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	DLR_COMMSN_AMT	|	double	|	15	|		|		|		|		|		|	//:LKP.LKP_DM_DIM_CALENDAR(BATCH_DT)<br><br>Table:LKP.LKP_DM_DIM_CALENDAR<br>Condition: CAL_DAY = BATCH_DT<br>output Column:DAY_KEY<br>	|		|		|		|	BATCH_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	UNDERWRITER_COMMSN_AMT	|	double	|	15	|		|		|		|		|		|	<br>Table:HDM.DEALER_BRANCH<br>Condition: DLR_ID = IN_DLR_ID AND BRANCH_ID = IN_BRANCH_ID<br>output Column:DLR_BRANCH_KEY<br>	|		|		|		|	DLR_BRANCH_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SPR_SHEET_DT	|	date/time	|	29	|		|		|		|		|		|	<br>Table:HDM.TRADE_ORIGIN<br>Condition: TRDE_ORIG_ID = in_TRDE_ORIG_ID<br>output Column:TRDE_ORIG_KEY<br>	|		|		|		|	TRDE_ORIG_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ACCT_PR_SCHED	|	decimal	|	11	|		|		|		|		|		|	<br>Table:HDM.REASON_CODE<br>Condition: RSN_CD = in_RSN_CD<br>output Column:RSN_KEY<br>	|		|		|		|	RSN_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	UNDR_WRTR_COMMSN	|	string	|	1	|		|		|		|		|		|	<br>Table:HDM.SOCIAL_SECURITY_STATUS<br>Condition: SSN_STAT_CD = in_SSN_STAT_CD<br>output Column:SSN_STAT_KEY<br>	|		|		|		|	SSN_STAT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	CDSC_WAVR_RSN	|	string	|	4	|		|		|		|		|		|	<br>Table:HDM.TRADE_ENTRY_METHOD<br>Condition: TRDE_ENTRY_MTHD_CD = in_TRDE_ENTRY_MTHD_CD<br>output Column:TRDE_ENTRY_MTHD_KEY<br>	|		|		|		|	TRDE_ENTRY_MTHD_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	BLUESKY_TXN_FLG	|	string	|	1	|		|		|		|		|		|	<br>Table:HDM.DEALER_LEVEL_CONTROL<br>Condition: DLR_LVL_CNTL_CD = in_DLR_LVL_CNTL_CD<br>output Column:DLR_LVL_CNTL_KEY<br>	|		|		|		|	DLR_LVL_CNTL_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	BLUESKY_REOPEN_ACCT_FLG	|	string	|	1	|		|		|		|		|		|	<br>Table:HDM.PAYMENT_METHOD<br>Condition: PMT_MTHD_CD = in_PMT_MTHD_CD<br>output Column:PMT_MTHD_KEY<br>	|		|		|		|	PMT_MTHD_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	FUND_TXN_ID	|	double	|	15	|		|		|		|		|	"IIF(ISNULL(SHRHLDR_ROLE_KEY),-1,SHRHLDR_ROLE_KEY)"	|		|		|		|		|	SHRHLDR_ROLE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	FUND_TXN_ID_2	|	double	|	15	|		|		|		|	ACCT_NBR	|		|		|		|		|		|	ACCT_NBR	|	number	|	15	|		|		|
|		|	TXN_EFFTV_DT	|	date/time	|	29	|		|		|		|	FUND_TXN_ID	|		|		|		|		|		|	FUND_TXN_ID	|	number	|	15	|		|		|
|		|	TXN_STTL_DT	|	date/time	|	29	|		|		|		|	FUND_TXN_ID_2	|		|		|		|		|		|	FUND_TXN_ID_2	|	number	|	15	|		|		|
|		|	BATCH_NBR	|	double	|	15	|		|		|		|	LTR_OF_INTEND_NBR	|		|		|		|		|		|	LTR_OF_INTEND_NBR	|	number	|	15	|		|		|
|		|	BATCH_DT	|	date/time	|	29	|		|		|		|	BATCH_NBR	|		|		|		|		|		|	BATCH_NBR	|	number	|	15	|		|		|
|		|	PMT_DT	|	date/time	|	29	|		|		|		|	DISCNT_CAT	|		|		|		|		|		|	DISCNT_CAT	|	number	|	15	|		|		|
|		|	LTR_OF_INTEND_NBR	|	double	|	15	|		|		|		|	FUND_ORD_NBR	|		|		|		|		|		|	FUND_ORD_NBR	|	number	|	15	|		|		|
|		|	FUND_TXN_CD	|	decimal	|	3	|		|		|		|	NAV_CD	|		|		|		|		|		|	NAV_CD	|	varchar2	|	1	|		|		|
|		|	TXN_SFX	|	decimal	|	3	|		|		|		|	ACCT_PR_SCHED	|		|		|		|		|		|	ACCT_PR_SCHED	|	number	|	15	|		|		|
|		|	NAV_RSN_CD	|	decimal	|	11	|		|		|		|	UNDR_WRTR_COMMSN_CD	|		|		|		|		|		|	UNDR_WRTR_COMMSN_CD	|	varchar2	|	1	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|	CDSC_WAVR_RSN_CD	|		|		|		|		|		|	CDSC_WAVR_RSN_CD	|	varchar2	|	4	|		|		|
|		|	FUND_FROM_FUND_CD	|	decimal	|	11	|		|		|		|	NSCC_SHRT_TERM_TRADER_WAVR_RSN	|		|		|		|		|		|	NSCC_SHRT_TERM_TRADER_WAVR_RSN	|	varchar2	|	1	|		|		|
|		|	ACCT_NBR	|	decimal	|	20	|		|		|		|	CONTRBN_TAX_YR	|		|		|		|		|		|	CONTRBN_TAX_YR	|	"number(p,s)"	|	4	|		|		|
|		|	DLR_ID	|	string	|	25	|		|		|		|	SHRS	|		|		|		|		|		|	SHRS	|	number	|	15	|		|		|
|		|	BRANCH_ID	|	string	|	25	|		|		|		|	SHR_PR	|		|		|		|		|		|	SHR_PR	|	number	|	15	|		|		|
|		|	SHRHLDR_ROLE_KEY	|	decimal	|	10	|		|		|		|	GR_AMT	|		|		|		|		|		|	GR_AMT	|	number	|	15	|		|		|
|		|	CUST_ID	|	decimal	|	11	|		|		|		|	SALES_PCT	|		|		|		|		|		|	SALES_PCT	|	number	|	15	|		|		|
|		|	CUST_ID_2	|	decimal	|	11	|		|		|		|	DLR_COMMSN_AMT	|		|		|		|		|		|	DLR_COMMSN_AMT	|	number	|	15	|		|		|
|		|	ST_CD	|	string	|	3	|		|		|		|	UNDR_WRTR_COMMSN_AMT	|		|		|		|		|		|	UNDR_WRTR_COMMSN_AMT	|	number	|	15	|		|		|
|		|	MKT_REG_ID	|	decimal	|	10	|		|		|		|	NET_AMT	|		|		|		|		|		|	NET_AMT	|	number	|	15	|		|		|
|		|	STATE_OF_SALE_CD	|	string	|	2	|		|		|		|	BLUESKY_TXN_FLG	|		|		|		|		|		|	BLUESKY_TXN_FLG	|	varchar2	|	1	|		|		|
|		|	STATE_OF_SALE_MKT_REG_ID	|	decimal	|	10	|		|		|		|		|	SYSDATE	|		|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	CRTY_ISO_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	TRDE_ORIG_ID	|	decimal	|	11	|		|		|		|		|	Y'	|		|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	ASOF_RSN_CD	|	decimal	|	3	|		|		|		|		|	4	|		|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	SSN_STAT_CD	|	decimal	|	11	|		|		|		|		|	$$etlcyckey	|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	TRDE_ENTRY_MTHD_CD	|	string	|	1	|		|		|		|	TXN_DESC	|		|		|		|		|		|	TXN_DESC	|	varchar2	|	255	|		|		|
|		|	DLR_LVL_CNTL_CD	|	string	|	3	|		|		|		|	SHR_BAL_EFFCT_FLG	|		|		|		|		|		|	SHR_BAL_EFFCT_FLG	|	varchar2	|	1	|		|		|
|		|	PMT_MTHD_CD	|	string	|	1	|		|		|		|	BLUESKY_REOPEN_ACCT_FLG	|		|		|		|		|		|	BLUESKY_REOPEN_ACCT_FLG	|	varchar2	|	1	|		|		|
|		|	OPR_ID	|	string	|	8	|		|		|		|		|		|	Table: HDM.FUND<br>Condition: FUND_NBR = IN_FUND_NBR<br>output ColumnFUND_KEY<br>	|		|		|		|	FRTO_FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	NSCC_CNTL_NBR	|	string	|	15	|		|		|		|		|		|	//:: :LKP.LKP_DM_DIM_CALENDAR(SPR_SHEET_DT)<br><br>Table:LKP.LKP_DM_DIM_CALENDAR<br>Condition: CAL_DAY = SPR_SHEET_DT<br>output Column:DAY_KEY<br>	|		|		|		|	SPR_SHEET_DAY_KEY	|	number	|	15	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
