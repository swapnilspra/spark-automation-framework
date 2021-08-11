|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	HBR_MASTER	|	FUND	|	Integer	|		|		|		|		|	ACCT_KEY (from Lookup)	|	ACCT_KEY (from Lookup)	|	"Lookup the table 'HDM.ACCOUNT' with 'FUND' and 'ACCOUNT'<br><br>//Lookup Condition<br>ACCT_NBR = IN_ACCT_NBR AND FUND_NBR = IN_FUND_NBR<br><br>//Lookup Output Columns<br>ACCT_KEY, STOP_MAIL_DT, ACCT_SETUP_DT"	|	Update the table when the non-key fields are not matched	|	ACCOUNT	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	ACCOUNT	|	String	|	11	|		|		|		|		|		|		|		|	ACCOUNT	|	ACCT_TOP_LVL_CD	|	varchar2	|	3	|		|		|		|
|		|	ACCT_SETUP_DTE	|	Date/Time	|		|		|		|		|		|		|		|		|	ACCOUNT	|	ACCT_TOP_LVL_DESC	|	varchar2	|	60	|		|		|		|
|		|	STOPMAILDATE	|	Date/Time	|		|		|		|		|		|		|		|		|	ACCOUNT	|	ACCT_TYPE_CD	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	ACCT_TYPE	|	varchar2	|	60	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	ACCT_NBR	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	FUND_NBR	|	"number(p,s)"	|	7	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	TAX_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	ACCT_STAT_CD	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	ACCT_STAT	|	varchar2	|	60	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	ACCT_STRT_DT	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	LAST_MNTN_DT	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	DLR_LVL_CNTL_CD	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	DLR_LVL_CNTL_DESC	|	varchar2	|	60	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	PRE_AUTHZD_CHK_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	SWP_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	ACH_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	RE_INVST_TO_ANOTHER_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	MONITORED_VIP_ACCT	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	FIDUCIARY_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	MAILG_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	INTR_PARTY_FLG	|	varchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	MGMT_CO_EMPL_FLG	|	varchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	TRST_CO_NBR	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	TPA_NBR	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	CUSTODIAN_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	TPA_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	EXT_PLN_ID	|	varchar2	|	25	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	TIN_CD	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	TIN_DESC	|	varchar2	|	60	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	ALPHA_CD	|	varchar2	|	10	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	CAP_GAINS_DSTRBTN_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	CHK_WRITING_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	DIVD_DSTRBTN_OPT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	EXPEDITED_REDMPN_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	SUB_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	FORGN_TAX_RT	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	CUM_DISCNT_NBR	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	LTR_OF_INTEND_NBR	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	TIMER_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	GPURCH_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	EXPDT_XCHG_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	PNLTY_WHG_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	CERTFC_ISSUANCE_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	STOP_XFER_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	BLUESKY_XMPTN_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	BNK_CARD_ISSD_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	NAV_ACCT_FLG	|	varchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	PHON_CHK_REDMPN_FLG	|	varchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT	|	HOUSE_ACCT_FLG	|	varchar2	|	1	|		|		|		|
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
