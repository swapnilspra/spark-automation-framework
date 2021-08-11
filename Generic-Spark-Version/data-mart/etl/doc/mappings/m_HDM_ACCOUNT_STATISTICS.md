|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	VW_ACCT_SUMM	|	FUND_NBR	|	decimal	|	11	|		|		|		|	Seq Number	|		|		|		|		|	ACCOUNT_STATISTICS	|	ACCT_STATS_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_ACCT_SUMM	|	ACCT_BAL_DT	|	date/time	|	29	|		|		|		|	GEND_KEY	|		|		|		|		|	ACCOUNT_STATISTICS	|	GEND_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_ACCT_SUMM	|	FUND_KEY	|	decimal	|	10	|		|		|		|	ACCT_CAT_KEY	|		|		|		|		|	ACCOUNT_STATISTICS	|	ACCT_CAT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_ACCT_SUMM	|	DAY_KEY	|	decimal	|	10	|		|		|		|	BAL_CAT_KEY	|		|		|		|		|	ACCOUNT_STATISTICS	|	BAL_CAT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_ACCT_SUMM	|	SETUP_YR	|	string	|	4	|		|		|		|	FUND_KEY	|		|		|		|		|	ACCOUNT_STATISTICS	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_ACCT_SUMM	|	ACCT_SETUP_YR_KEY	|	decimal	|	10	|		|		|		|	DAY_KEY	|		|		|		|		|	ACCOUNT_STATISTICS	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_ACCT_SUMM	|	GEND_KEY	|	double	|	15	|		|		|		|	ACCT_SETUP_YR_KEY	|		|		|		|		|	ACCOUNT_STATISTICS	|	ACCT_SETUP_YR_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_ACCT_SUMM	|	ACCT_CAT_KEY	|	double	|	15	|		|		|		|	ACCT_BAL_AMT	|		|		|		|		|	ACCOUNT_STATISTICS	|	ACCT_BAL_AMT	|	number	|	15	|		|		|
|	VW_ACCT_SUMM	|	BAL_CAT_KEY	|	double	|	15	|		|		|		|	TOT_SHRS	|		|		|		|		|	ACCOUNT_STATISTICS	|	TOT_SHRS	|	char	|	18	|		|		|
|	VW_ACCT_SUMM	|	ACCT_BAL_AMT	|	double	|	15	|		|		|		|	ACCT_CNT	|		|		|		|		|	ACCOUNT_STATISTICS	|	ACCT_CNT	|	number	|	15	|		|		|
|	VW_ACCT_SUMM	|	TOT_SHRS	|	double	|	15	|		|		|		|	SHRHLDR_CNT	|		|		|		|		|	ACCOUNT_STATISTICS	|	SHRHLDR_CNT	|	number	|	15	|		|		|
|	VW_ACCT_SUMM	|	ACCT_CNT	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|	ACCOUNT_STATISTICS	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|	VW_ACCT_SUMM	|	SHRHLDR_CNT	|	double	|	15	|		|		|		|		|	SYSDATE	|		|		|		|	ACCOUNT_STATISTICS	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ACCOUNT_STATISTICS	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|		|	ACCOUNT_STATISTICS	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	4	|		|		|		|	ACCOUNT_STATISTICS	|	SRC_SYS_ID	|	number	|	15	|		|		|
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
