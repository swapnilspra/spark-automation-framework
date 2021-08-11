|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	Only for Insert	|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	CATEGORY_ACCRUAL_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|		|		|		|	BUD_CAT_ACCT_ACCRUAL	|	CAT_ACCT_ACCRL_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_CMP	|	string	|	20	|		|		|		|		|		|	Table Name: HDM.BUD_CAT_ACCT_ACCRUAL<br>Condition: CAT_ACCRL_DIM_KEY = CAT_ACCRL_DIM_KEY1 AND ACCT_ACCRL_KEY = ACCT_ACCRL_KEY1<br>Output Column: CAT_ACCT_ACCRL_KEY 	|		|		|	BUD_CAT_ACCT_ACCRUAL	|	CAT_ACCRL_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_NAME	|	string	|	80	|		|		|		|		|		|	Table Name: HDM.BUD_ACCT_ACCRUAL<br>Condition: ACCRL_ID = LINE_ITEM_ACCRUAL_ID1<br>Output Column: ACCT_ACCRL_KEY	|		|		|	BUD_CAT_ACCT_ACCRUAL	|	ACCT_ACCRL_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_ORDER	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|	BUD_CAT_ACCT_ACCRUAL	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	DAILY_ACCRUAL	|	double	|	15	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_CAT_ACCT_ACCRUAL	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ADJUSTMENT_VALUE	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_CAT_ACCT_ACCRUAL	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ACCRUAL_TYPE	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_CAT_ACCT_ACCRUAL	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_CAT_ACCT_ACCRUAL	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ACCRUAL_STATUS	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	PAYABLE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EXPENSE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	DEBIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	CREDIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	SSB_EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	INSERT_USER	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	NOTES	|	string	|	1000	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EXPIRATION_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ROUNDED_UP_TO_ONE_PENNY	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	LINE_ITEM_ACCRUAL_ID	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	LINE_ITEM_ID_IN_ACCRUAL	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	LINE_ITEM_DAILY_ACCRUAL	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	LINE_ITEM_ACTIVITY_ID	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	LINE_ITEM_ID_IN_ADJ	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
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
