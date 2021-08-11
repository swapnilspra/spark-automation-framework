|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_ID	|	string	|	100	|		|		|		|	Auto Increment the key value by '1' for each records	|	Use sequence generator transformation to generate the key for each row	|		|		|	BUD_ACCT_ACCRUAL	|	ACCT_ACCRL_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_SOURCE_BUDGET_ID	|	string	|	60	|		|		|		|	Remove the empty space on 'ACCRUAL_ID'	|	ltrim(rtrim(ACCRUAL_ID))	|		|		|	BUD_ACCT_ACCRUAL	|	ACCRL_ID	|	nvarchar2	|	100	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_SOURCE_ASSUMPTION_ID	|	string	|	100	|		|		|		|	Direct move from Source 'ACCRUAL_SOURCE'	|	ACCRUAL_SOURCE	|		|		|	BUD_ACCT_ACCRUAL	|	ACCRL_SRC	|	nvarchar2	|	60	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_SOURCE	|	string	|	30	|		|		|		|	Direct move from Source 'FISCAL_YEAR'	|	FISCAL_YEAR	|		|		|	BUD_ACCT_ACCRUAL	|	FISCL_YR	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FUND_CMP	|	string	|	20	|		|		|		|	Direct move from Source 'ACCRUAL_STATUS'	|	ACCRUAL_STATUS	|		|		|	BUD_ACCT_ACCRUAL	|	ACCRL_STAT	|	nvarchar2	|	60	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FUND_NAME	|	string	|	80	|		|		|		|	Direct move from Source 'NOTES'	|	NOTES	|		|		|	BUD_ACCT_ACCRUAL	|	NTES	|	nvarchar2	|	4000	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FUND_ORDER	|	double	|	15	|		|		|		|	Direct move from Source 'INSERT_USER'	|	INSERT_USER	|		|		|	BUD_ACCT_ACCRUAL	|	INSR_USR	|	nvarchar2	|	25	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|	BUD_ACCT_ACCRUAL	|	INSR_TS	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|	"iif(isnull(VERSION),0,VERSION)"	|		|		|	BUD_ACCT_ACCRUAL	|	VRSN	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FISCAL_YEAR	|	double	|	15	|		|		|		|	Default to 'Y'	|	'Y'	|		|		|	BUD_ACCT_ACCRUAL	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_STATUS	|	string	|	30	|		|		|		|	Current Date/Time	|	sysdate	|		|		|	BUD_ACCT_ACCRUAL	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	NOTES	|	string	|	1000	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	DAILY_ACCRUAL	|	double	|	15	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ORIGINAL_BUDGET	|	double	|	15	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	REVISED_BUDGET	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	REMAINING_TO_ACCRUE	|	double	|	15	|		|		|		|		|		|	Table Name : HDM.BUD_ACCT_ACCRUAL<br>Condition: ACCRL_ID = ACCRUAL_ID_out (Source Data)<br>Output Column: ACCT_ACCRL_KEY	|	Update the Existing records	|	BUD_ACCT_ACCRUAL	|	ACCT_ACCRL_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FYTD_EXPENSE	|	double	|	15	|		|		|		|	Remove the empty space on 'ACCRUAL_ID'	|	ltrim(rtrim(ACCRUAL_ID))	|		|		|	BUD_ACCT_ACCRUAL	|	ACCRL_ID	|	nvarchar2	|	100	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	EFFECTIVE_DATE	|	string	|	30	|		|		|		|	Direct move from Source 'ACCRUAL_SOURCE'	|	ACCRUAL_SOURCE	|		|		|	BUD_ACCT_ACCRUAL	|	ACCRL_SRC	|	nvarchar2	|	60	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_START_DATE	|	string	|	30	|		|		|		|	Direct move from Source 'FISCAL_YEAR'	|	FISCAL_YEAR	|		|		|	BUD_ACCT_ACCRUAL	|	FISCL_YR	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUE_BY_DATE	|	string	|	30	|		|		|		|	Direct move from Source 'ACCRUAL_STATUS'	|	ACCRUAL_STATUS	|		|		|	BUD_ACCT_ACCRUAL	|	ACCRL_STAT	|	nvarchar2	|	60	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	SSB_EFFECTIVE_DATE	|	string	|	30	|		|		|		|	Direct move from Source 'NOTES'	|	NOTES	|		|		|	BUD_ACCT_ACCRUAL	|	NTES	|	nvarchar2	|	4000	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	NBR_ACCRUAL_DAYS	|	double	|	15	|		|		|		|	Direct move from Source 'INSERT_USER'	|	INSERT_USER	|		|		|	BUD_ACCT_ACCRUAL	|	INSR_USR	|	nvarchar2	|	25	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	INSERT_USER	|	string	|	30	|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|	BUD_ACCT_ACCRUAL	|	INSR_TS	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|	"iif(isnull(VERSION),0,VERSION)"	|		|		|	BUD_ACCT_ACCRUAL	|	VRSN	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	CURRENT_DAILY_ACCRUAL_ID	|	string	|	100	|		|		|		|	Default to 'Y'	|	'Y'	|		|		|	BUD_ACCT_ACCRUAL	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	CURRENT_DAILY_ACCRUAL_RATE	|	double	|	15	|		|		|		|	Current Date/Time	|	sysdate	|		|		|	BUD_ACCT_ACCRUAL	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	PAYABLE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	EXPENSE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
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
