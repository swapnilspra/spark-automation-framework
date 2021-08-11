|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	BLUESKY_TXN_FLG	|	string	|	1	|	9	|		|		|	BLUESKY_TXN_FLG	|		|		|	Update the existing records	|		|	FUND_TRANS_HIST	|	TXN_HIST_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	BLUESKY_REOPEN_ACCT_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	TXN_TYPE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	FUND_TXN_ID	|	double	|	15	|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	REG_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	FUND_TXN_ID_2	|	double	|	15	|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	TXN_HIST_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	SHRHLDR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	EFFTV_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	STTL_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	PMT_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	BATCH_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	DLR_BRANCH_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	TRDE_ORIG_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	RSN_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	SSN_STAT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	TRDE_ENTRY_MTHD_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	DLR_LVL_CNTL_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	PMT_MTHD_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	SHRHLDR_ROLE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	ACCT_NBR	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	FUND_TXN_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	FUND_TXN_ID_2	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	LTR_OF_INTEND_NBR	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	BATCH_NBR	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	DISCNT_CAT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	FUND_ORD_NBR	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	NAV_CD	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	ACCT_PR_SCHED	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	UNDR_WRTR_COMMSN_CD	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	CDSC_WAVR_RSN_CD	|	varchar2	|	4	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	NSCC_SHRT_TERM_TRADER_WAVR_RSN	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	CONTRBN_TAX_YR	|	"number(p,s)"	|	4	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	SHRS	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	SHR_PR	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	GR_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	SALES_PCT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	DLR_COMMSN_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	UNDR_WRTR_COMMSN_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	NET_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|	BLUESKY_REOPEN_ACCT_FLG	|		|		|		|		|	FUND_TRANS_HIST	|	BLUESKY_TXN_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	TXN_DESC	|	varchar2	|	255	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	SHR_BAL_EFFCT_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|	BLUESKY_REOPEN_ACCT_FLG	|		|		|		|		|	FUND_TRANS_HIST	|	BLUESKY_REOPEN_ACCT_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	FRTO_FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	FUND_TRANS_HIST	|	SPR_SHEET_DAY_KEY	|	number	|	15	|		|		|
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
