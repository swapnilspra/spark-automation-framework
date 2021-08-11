|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_LIABILITY_FUND	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table Name:  HDM.BUD_LIABILITY_FUND<br>Condition: LIAB_KEY = LIAB_KEY1 AND BUD_FUND_COMPST_KEY = BUD_FUND_COMPST_KEY1<br>Output Column: LIAB_FUND_KEY	|		|		|	BUD_LIABILITY_FUND	|	LIAB_FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	BUDGET_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUDGET<br>Condition: BUD_ID = BUDGET_ID_out <br>Output Column: BUD_KEY	|		|		|	BUD_LIABILITY_FUND	|	BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	PAYABLE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_EXPENSE_LINE_ITEM<br>Condition: EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out <br>Output Column: EXPNS_LINE_ITEM_KEY	|		|		|	BUD_LIABILITY_FUND	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = PAYABLE_ACCOUNT_ID_out<br>Output Column: BUD_ACCT_KEY 	|		|		|	BUD_LIABILITY_FUND	|	BUD_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	FUND_CMP	|	string	|	20	|		|		|		|		|		|	Table Name: HDM.BUD_DATA_SOURCE_DIM<br>Condition: DATA_SRC_ID = DATA_SOURCE_ID_out<br>Output Column: DATA_SRC_DIM_KEY	|		|		|	BUD_LIABILITY_FUND	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	FUND_NAME	|	string	|	80	|		|		|		|		|		|	Table Name: HDM.BUD_FUND_COMPOSITE<br>Condition: FUND_COMPST_ID = FUND_CMP_out<br>Output Column: BUD_FUND_COMPST_KEY	|		|		|	BUD_LIABILITY_FUND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	FUND_ORDER	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUD_LIABILITY<br>Condition: LINE_ITEM_ID = LINE_ITEM_ID_out AND BUD_ID = BUDGET_ID_out <br>Output Column: LIAB_KEY 	|		|		|	BUD_LIABILITY_FUND	|	LIAB_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	ALLOCATION	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUDGET<br>Condition: BUD_ID = PRIOR_YEAR_PAY_DIFF_BUDGET_ID_out<br>Output Column: BUD_KEY	|		|		|	BUD_LIABILITY_FUND	|	PRIR_YR_PMT_DIFF_BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	DATA_SOURCE_VALUE	|	double	|	15	|		|		|		|	ALLOCATION	|		|		|		|		|	BUD_LIABILITY_FUND	|	ALLOCN	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	FYTD_LIABILITY	|	double	|	15	|		|		|		|	DATA_SOURCE_VALUE	|		|		|		|		|	BUD_LIABILITY_FUND	|	DATA_SRC_VAL	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	FYTD_EXPENSE	|	double	|	15	|		|		|		|	FYTD_LIABILITY	|		|		|		|		|	BUD_LIABILITY_FUND	|	FYTD_LIAB	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	PRIOR_YEAR_FYE_LIABILITY	|	double	|	15	|		|		|		|	FYTD_EXPENSE	|		|		|		|		|	BUD_LIABILITY_FUND	|	FYTD_EXPNS	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	PRIOR_YEAR_PAYMENT_DIFF	|	double	|	15	|		|		|		|	PRIOR_YEAR_FYE_LIABILITY	|		|		|		|		|	BUD_LIABILITY_FUND	|	PRIR_YR_FYE_LIAB	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	PRIOR_YEAR_PAY_DIFF_BUDGET_ID	|	string	|	100	|		|		|		|	PRIOR_YEAR_PAYMENT_DIFF	|		|		|		|		|	BUD_LIABILITY_FUND	|	PRIR_YR_PMT_DIFF	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	PENDING_INVOICE_TOTAL	|	double	|	15	|		|		|		|	PENDING_INVOICE_TOTAL	|		|		|		|		|	BUD_LIABILITY_FUND	|	PEND_INVC_TOT	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	PROCESSED_INVOICE_TOTAL	|	double	|	15	|		|		|		|	PROCESSED_INVOICE_TOTAL	|		|		|		|		|	BUD_LIABILITY_FUND	|	PRCSD_INVC_TOT	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	REMAINING_TO_ACCRUE	|	double	|	15	|		|		|		|	REMAINING_TO_ACCRUE	|		|		|		|		|	BUD_LIABILITY_FUND	|	REMNG_TO_ACCRUE	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	FYE_LIABILITY	|	double	|	15	|		|		|		|	FYE_LIABILITY	|		|		|		|		|	BUD_LIABILITY_FUND	|	FYE_LIAB	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	NEEDED_FYE_LIABILITY	|	double	|	15	|		|		|		|	NEEDED_FYE_LIABILITY	|		|		|		|		|	BUD_LIABILITY_FUND	|	NEEDED_FYE_LIAB	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	SUGGESTED_LIABILITY_ADJUSTMENT	|	double	|	15	|		|		|		|	SUGGESTED_LIABILITY_ADJUSTMENT	|		|		|		|		|	BUD_LIABILITY_FUND	|	SUGGESTED_LIAB_ADJMT	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	ACTUAL_LIABILITY_ADJUSTMENT	|	double	|	15	|		|		|		|	ACTUAL_LIABILITY_ADJUSTMENT	|		|		|		|		|	BUD_LIABILITY_FUND	|	ACTL_LIAB_ADJMT	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	ONE_TIME_ADJUSTMENTS	|	double	|	15	|		|		|		|	ONE_TIME_ADJUSTMENTS	|		|		|		|		|	BUD_LIABILITY_FUND	|	ONE_TM_ADJMTS	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	LINE_ITEM_BUDGET	|	double	|	15	|		|		|		|	LINE_ITEM_BUDGET	|		|		|		|		|	BUD_LIABILITY_FUND	|	LINE_ITEM_BUD	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	INSERT_USER	|	string	|	30	|		|		|		|	VERSION	|		|		|		|		|	BUD_LIABILITY_FUND	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|		|	Y'	|		|		|		|	BUD_LIABILITY_FUND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_LIABILITY_FUND	|	VERSION	|	double	|	15	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_LIABILITY_FUND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_LIABILITY_FUND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_LIABILITY_FUND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_LIABILITY_FUND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
