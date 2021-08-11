|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	VW_BDGT_BUDGET	|	ACCOUNT_ID	|	string	|	30	|		|		|		|	Increment the key value by '1' for each record insert	|	Use sequence generator to generate the key	|		|		|	BUDGET	|	BUD_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|	VW_BDGT_BUDGET	|	ACCOUNT_DESCRIPTION	|	string	|	200	|		|		|		|		|	ltrim(rtrim(BUDGET_ID))	|		|		|	BUDGET	|	BUD_ID	|	nvarchar2	|	60	|		|		|		|
|	VW_BDGT_BUDGET	|	ACCOUNT_TYPE	|	string	|	30	|		|		|		|		|	BUTGET_TYPE	|		|		|	BUDGET	|	BUD_TYPE	|	nvarchar2	|	25	|		|		|		|
|	VW_BDGT_BUDGET	|	ACTIVE	|	string	|	5	|		|		|		|		|	"to_date(END_DATE,'YYYY-MM-DD')"	|		|		|	BUDGET	|	END_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_BUDGET	|	CATEGORY_TYPE	|	string	|	30	|		|		|		|		|	FISCAL_YEAR	|		|		|	BUDGET	|	FISCL_YR	|	number	|	15	|		|		|		|
|	VW_BDGT_BUDGET	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|		|	INIT_BUDGET_WITH	|		|		|	BUDGET	|	INIT_BUD_W	|	nvarchar2	|	60	|		|		|		|
|	VW_BDGT_BUDGET	|	PREPAID_EXPENSE	|	string	|	5	|		|		|		|		|	"to_date(START_DATE,'YYYY-MM-DD')"	|		|		|	BUDGET	|	STRT_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_BUDGET	|	ACCOUNT_SUBTYPE	|	string	|	30	|		|		|		|		|	DESCRIPTION	|		|		|	BUDGET	|	BUD_DESC	|	nvarchar2	|	255	|		|		|		|
|	VW_BDGT_BUDGET	|	PAYEE	|	string	|	100	|		|		|		|		|	BUDGET_SOURCE	|		|		|	BUDGET	|	BUD_SRC	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_BUDGET	|	VERSION	|	double	|	15	|		|		|		|		|	"iif(lower(IS_FISCAL_YEAR_END)='true',1,0)"	|		|		|	BUDGET	|	FISCL_YE_FLG	|	"number(p,s)"	|	38	|		|		|		|
|	VW_BDGT_BUDGET	|	INSERT_USER	|	string	|	30	|		|		|		|		|	INSERT_USER	|		|		|	BUDGET	|	INSR_USR	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_BUDGET	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|	BUDGET	|	INSR_TS	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|	VERSION	|		|		|	BUDGET	|	VRSN	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|	BUDGET	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|	BUDGET	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUDGET	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUDGET	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUDGET	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	Update	|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|	"Lookup the table : HDM.HDM.BUDGET<br><br>Condition : ACCT_ID = ACCOUNT_ID_out (From Source)<br><br>Ouput Column: BUD_KEY, VRSN, "	|		|	BUDGET	|	BUD_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|		|		|		|		|		|		|		|	ltrim(rtrim(BUDGET_ID))	|		|		|	BUDGET	|	BUD_ID	|	nvarchar2	|	60	|		|		|		|
|		|		|		|		|		|		|		|		|	Account_Description	|		|		|	BUDGET	|	BUD_TYPE	|	nvarchar2	|	25	|		|		|		|
|		|		|		|		|		|		|		|		|	Account_TYPE	|		|		|	BUDGET	|	END_DT	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|	"iif(isnull(ACTIVE),'N',iif(ACTIVE='true','Y','N'))"	|		|		|	BUDGET	|	FISCL_YR	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|	Category Type	|		|		|	BUDGET	|	INIT_BUD_W	|	nvarchar2	|	60	|		|		|		|
|		|		|		|		|		|		|		|		|	Expense_Category	|		|		|	BUDGET	|	STRT_DT	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|	"iif(isnull(PREPAID_EXPENSE),'N',iif(PREPAID_EXPENSE='true','Y','N'))"	|		|		|	BUDGET	|	BUD_DESC	|	nvarchar2	|	255	|		|		|		|
|		|		|		|		|		|		|		|		|	ACCOUNT_SUBTYPE	|		|		|	BUDGET	|	BUD_SRC	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|		|	PAYEE	|		|		|	BUDGET	|	FISCL_YE_FLG	|	"number(p,s)"	|	38	|		|		|		|
|		|		|		|		|		|		|		|		|	INSERT_USR	|		|		|	BUDGET	|	INSR_USR	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|	BUDGET	|	INSR_TS	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|	BUDGET	|	VRSN	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|	BUDGET	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|	BUDGET	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUDGET	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUDGET	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUDGET	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
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
