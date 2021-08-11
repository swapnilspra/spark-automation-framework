|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	ACCOUNT_POSITION	|	ACCT_POSN_KEY	|	decimal	|	10	|		|		|		|	Direct Move	|	ACCT_POSN_KEY	|		|	Update the row start time	|		|	ACCOUNT_POSITION	|	ACCT_POSN_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|	ACCOUNT_POSITION	|	ACCT_KEY	|	decimal	|	10	|		|		|		|	Direct Move	|	ACCT_KEY	|		|		|		|	ACCOUNT_POSITION	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|	ACCOUNT_POSITION	|	FUND_KEY	|	decimal	|	10	|		|		|		|	Direct Move	|	FUND_KEY	|		|		|		|	ACCOUNT_POSITION	|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|	ACCOUNT_POSITION	|	SHRHLDR_KEY	|	decimal	|	10	|		|		|		|	Direct Move	|	SHRHLDR_KEY	|		|		|		|	ACCOUNT_POSITION	|	SHRHLDR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|	ACCOUNT_POSITION	|	DAY_KEY	|	decimal	|	10	|		|		|		|	Lookup the table 'HDM.DEALER' with 'DLR_ID' and get the 'DLR_KEY'	|	:LKP.LKP_DLR_KEY(TO_CHAR(DLR_ID))	|	Lookup the table 'HDM.DEALER' with 'DLR_ID' and get the 'DLR_KEY'	|		|		|	ACCOUNT_POSITION	|	DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|	ACCOUNT_POSITION	|	ACCT_NBR	|	double	|	15	|		|		|		|	Direct Move	|	DAY_KEY	|		|		|		|	ACCOUNT_POSITION	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|	ACCOUNT_POSITION	|	DLR_ID	|	double	|	15	|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	ACCT_NBR	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	SHRHLDR_ROLE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	TOT_SHRS	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	UN_ISSD_SHRS	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	ISSD_SHRS	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	NAV	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	RSRVD_SHRS	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	ACCRD_DIVD_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	ESCRW_SHRS	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	COLLCTD_SHRS	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	COLLCTD_BAL_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	CURR_INCM_DIVD	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	CURR_LONG_TERM_CAP_GAIN	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	CURR_SHRT_TERM_CAP_GAIN	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	CURR_FORGN_W_HOLDG	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	CURR_DIVD_W_HOLDG	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	CURR_FORGN_TAX_PAID_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	CURR_YR_1099B_GR_REDMPN_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	CURR_YR_1099B_GR_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	COLLCTD_SHRS_NET_CHG_CNT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	TOT_SHRS_NET_CHG_CNT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	DSGNTD_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	DSGNTD_W_HOLDG_RT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	DIVD_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	DIVD_W_HOLDG_RT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	SHRT_TERM_CAP_GAIN_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	SHRT_TERM_CAP_GAIN_W_HOLDG_RT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	LONG_TERM_CAP_GAIN_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	LONG_TERM_CAP_GAIN_W_HOLDG_RT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	PARTITION_YR	|	"number(p,s)"	|	4	|		|		|
|		|		|		|		|		|		|		|	job start time	|	SESSSTARTTIME	|		|		|		|	ACCOUNT_POSITION	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	DLR_BRANCH_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	TOT_BAL_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	HBR_DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_POSITION	|	ACCT_STAT_CD	|	number	|	15	|		|		|
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
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
