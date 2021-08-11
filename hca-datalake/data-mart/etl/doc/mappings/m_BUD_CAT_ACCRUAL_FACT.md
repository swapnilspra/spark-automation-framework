|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update, if record alredy present else insert it"	|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_CATEGORY_ACCRUAL_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table Name: HDM.BUD_CAT_ACCRUAL_FACT<br>Condition: CAT_ACCRL_ID = FUND_CATEGORY_ACCRUAL_ID_out<br>Output Column: CAT_ACCRL_FACT_KEY	|		|		|	BUD_CAT_ACCRUAL_FACT	|	CAT_ACCRL_FACT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_CMP	|	string	|	20	|		|		|		|		|		|	Table Name: HDM.BUD_FUND_COMPOSITE <br>Condition: FUND_COMPST_ID = FUND_CMP_out <br>Output Column: BUD_FUND_COMPST_KEY	|		|		|	BUD_CAT_ACCRUAL_FACT	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_NAME	|	string	|	80	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = SSB_EXPENSE_ACCOUNT<br>Output Column: BUD_ACCT_KEY	|		|		|	BUD_CAT_ACCRUAL_FACT	|	EXPNS_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_ORDER	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = SSB_PAYABLE_ACCOUNT <br>Output Column: BUD_ACCT_KEY	|		|		|	BUD_CAT_ACCRUAL_FACT	|	PAYBL_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	DAILY_ACCRUAL	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = DEBIT_ACCOUNT_ID<br>Output Column: BUD_ACCT_KEY	|		|		|	BUD_CAT_ACCRUAL_FACT	|	DBT_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ADJUSTMENT_VALUE	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = CREDIT_ACCOUNT_ID<br>Output Column: BUD_ACCT_KEY	|		|		|	BUD_CAT_ACCRUAL_FACT	|	CR_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ACCRUAL_TYPE	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_CAT_ACCRUAL_DIM<br>Condition: CAT_ACCRL_ID = FUND_CATEGORY_ACCRUAL_ID_out<br>Output Column: CAT_ACCRL_DIM_KEY	|		|		|	BUD_CAT_ACCRUAL_FACT	|	CAT_ACCRL_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|	ltrim(rtrim(FUND_CATEGORY_ACCRUAL_ID))	|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	CAT_ACCRL_ID	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ACCRUAL_STATUS	|	string	|	30	|		|		|		|	DAILY_ACCRUAL	|		|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	DAILY_ACCRL	|	number	|	15	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|	ADJUSTMENT_VALUE	|		|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	ADJMT_VAL	|	number	|	15	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	SSB_PAYABLE_ACCOUNT	|	string	|	30	|		|		|		|	FISCAL_YEAR	|		|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	FISCL_YR	|	number	|	15	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	SSB_EXPENSE_ACCOUNT	|	string	|	30	|		|		|		|	ACCRUAL_STATUS	|		|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	ACCRL_STAT	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	DEBIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	EFFTV_DT	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	CREDIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|	"to_date(SSB_EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	SSB_EFFTV_DT	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|	"to_date(EXPIRATION_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	EXPN_DT	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	SSB_EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	INSERT_TS	|	string	|	30	|		|		|		|		|	Y'	|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	INSERT_USER	|	string	|	30	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	NOTES	|	string	|	1000	|		|		|		|		|		|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EXPIRATION_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_CAT_ACCRUAL_FACT	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ROUNDED_UP_TO_ONE_PENNY	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	LINE_ITEM_ACCRUAL_ID	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	LINE_ITEM_DAILY_ACCRUAL	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	LINE_ITEM_ADJUSTMENT_VALUE	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
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
