|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update, if record alredy present else insert it"	|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_CATEGORY_ACCRUAL_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table Name: HDM.BUD_CAT_ACCRUAL_DIM<br>Condition: CAT_ACCRL_ID = FUND_CATEGORY_ACCRUAL_ID_out<br>Output Column: CAT_ACCRL_DIM_KEY	|		|		|	BUD_CAT_ACCRUAL_DIM	|	CAT_ACCRL_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_CMP	|	string	|	20	|		|		|		|		|	ltrim(rtrim(FUND_CATEGORY_ACCRUAL_ID))	|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	CAT_ACCRL_ID	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_NAME	|	string	|	80	|		|		|		|	NOTES	|		|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	NTES	|	nvarchar2	|	4000	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FUND_ORDER	|	double	|	15	|		|		|		|	ACCRUAL_TYPE	|		|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	ACCRL_TYPE	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	DAILY_ACCRUAL	|	double	|	15	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TS)/1000)"	|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	INSR_DT	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ADJUSTMENT_VALUE	|	double	|	15	|		|		|		|	INSERT_USER	|		|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	INSR_USR	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ACCRUAL_TYPE	|	string	|	30	|		|		|		|		|	"iif(isnull(ROUNDED_UP_TO_ONE_PENNY),0,iif(ROUNDED_UP_TO_ONE_PENNY='false',0,1))"	|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	RNDEDUP_TO_1PENNY_FLG	|	"number(p,s)"	|	38	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	ACCRUAL_STATUS	|	string	|	30	|		|		|		|		|	Y'	|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	SSB_PAYABLE_ACCOUNT	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	SSB_EXPENSE_ACCOUNT	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	DEBIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_CAT_ACCRUAL_DIM	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	CREDIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	SSB_EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	INSERT_TS	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	INSERT_USER	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	NOTES	|	string	|	1000	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	EXPIRATION_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_CATEGORY_ACCRUAL	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
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
