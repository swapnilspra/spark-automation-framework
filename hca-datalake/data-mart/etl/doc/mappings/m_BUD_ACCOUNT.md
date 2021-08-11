|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|	Increment the key value by '1' for each record insert	|	Use sequence generator to generate the key	|		|		|	BUD_ACCOUNT	|	BUD_ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	ACCOUNT_ID	|	string	|	30	|		|		|		|	remove empty space on 'ACCOUNT_ID'	|	ltrim(rtrim(ACCOUNT_ID))	|		|		|	BUD_ACCOUNT	|	ACCT_ID	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ACCOUNT	|	ACCOUNT_DESCRIPTION	|	string	|	200	|		|		|		|	direct move	|	Account_Description	|		|		|	BUD_ACCOUNT	|	ACCT_DESC	|	nvarchar2	|	200	|		|		|		|
|	VW_BDGT_ACCOUNT	|	ACCOUNT_TYPE	|	string	|	30	|		|		|		|	direct move	|	Account_TYPE	|		|		|	BUD_ACCOUNT	|	ACCT_TYPE	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ACCOUNT	|	ACTIVE	|	string	|	5	|		|		|		|	If Active is 'true' then'Y' else 'N'	|	"iif(isnull(ACTIVE),'N',iif(ACTIVE='true','Y','N'))"	|		|		|	BUD_ACCOUNT	|	ACTV_FLG	|	nvarchar2	|	1	|		|		|		|
|	VW_BDGT_ACCOUNT	|	CATEGORY_TYPE	|	string	|	30	|		|		|		|	direct move	|	Category Type	|		|		|	BUD_ACCOUNT	|	CAT_TYPE	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ACCOUNT	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|	direct move	|	Expense_Category	|		|		|	BUD_ACCOUNT	|	EXPNS_CAT	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ACCOUNT	|	PREPAID_EXPENSE	|	string	|	5	|		|		|		|	If PREPAID_EXPENSE is 'true' then'Y' else 'N'	|	"iif(isnull(PREPAID_EXPENSE),'N',iif(PREPAID_EXPENSE='true','Y','N'))"	|		|		|	BUD_ACCOUNT	|	PRPD_EXPNS_FLG	|	nvarchar2	|	1	|		|		|		|
|	VW_BDGT_ACCOUNT	|	ACCOUNT_SUBTYPE	|	string	|	30	|		|		|		|	direct move	|	ACCOUNT_SUBTYPE	|		|		|	BUD_ACCOUNT	|	ACCT_SUBT	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ACCOUNT	|	PAYEE	|	string	|	100	|		|		|		|	direct move	|	PAYEE	|		|		|	BUD_ACCOUNT	|	PAYEE	|	nvarchar2	|	100	|		|		|		|
|	VW_BDGT_ACCOUNT	|	VERSION	|	double	|	15	|		|		|		|	INSERT_USR	|	INSERT_USR	|		|		|	BUD_ACCOUNT	|	INSR_USR	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ACCOUNT	|	INSERT_USER	|	string	|	30	|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|	BUD_ACCOUNT	|	INSR_TS	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCOUNT	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|	"iif(isnull(VERSION),0 ElseVERSION)"	|	"iif(isnull(VERSION),0,VERSION)"	|		|		|	BUD_ACCOUNT	|	VRSN	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	Default to 'Y'	|	Y'	|		|		|	BUD_ACCOUNT	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|	Current Date	|	SYSDATE	|		|		|	BUD_ACCOUNT	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCOUNT	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCOUNT	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCOUNT	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	Update	|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|	"Lookup the table : HDM.BUD_ACCOUNT<br><br>Condition : ACCT_ID = ACCOUNT_ID_out (From Source)<br><br>Ouput Column: BUD_ACCT_KEY, VRSN, "	|		|	BUD_ACCOUNT	|	BUD_ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|		|		|		|		|		|		|	remove empty space on 'ACCOUNT_ID'	|	ltrim(rtrim(ACCOUNT_ID))	|		|		|	BUD_ACCOUNT	|	ACCT_ID	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	direct move	|	Account_Description	|		|		|	BUD_ACCOUNT	|	ACCT_DESC	|	nvarchar2	|	200	|		|		|		|
|		|		|		|		|		|		|		|	direct move	|	Account_TYPE	|		|		|	BUD_ACCOUNT	|	ACCT_TYPE	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	If Active is 'true' then'Y' else 'N'	|	"iif(isnull(ACTIVE),'N',iif(ACTIVE='true','Y','N'))"	|		|		|	BUD_ACCOUNT	|	ACTV_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|	direct move	|	Category Type	|		|		|	BUD_ACCOUNT	|	CAT_TYPE	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	direct move	|	Expense_Category	|		|		|	BUD_ACCOUNT	|	EXPNS_CAT	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	If PREPAID_EXPENSE is 'true' then'Y' else 'N'	|	"iif(isnull(PREPAID_EXPENSE),'N',iif(PREPAID_EXPENSE='true','Y','N'))"	|		|		|	BUD_ACCOUNT	|	PRPD_EXPNS_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|	direct move	|	ACCOUNT_SUBTYPE	|		|		|	BUD_ACCOUNT	|	ACCT_SUBT	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	direct move	|	PAYEE	|		|		|	BUD_ACCOUNT	|	PAYEE	|	nvarchar2	|	100	|		|		|		|
|		|		|		|		|		|		|		|	INSERT_USR	|	INSERT_USR	|		|		|	BUD_ACCOUNT	|	INSR_USR	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|	BUD_ACCOUNT	|	INSR_TS	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|	"iif(isnull(VERSION),0 ElseVERSION)"	|	"iif(isnull(VERSION),0,VERSION)"	|		|		|	BUD_ACCOUNT	|	VRSN	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	Default to 'Y'	|	Y'	|		|		|	BUD_ACCOUNT	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|	Current Date	|	SYSDATE	|		|		|	BUD_ACCOUNT	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCOUNT	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCOUNT	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCOUNT	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
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
