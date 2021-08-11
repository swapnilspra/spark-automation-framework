|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table  Name : HDM.BUD_PMT_DIFF_FUND <br>Condition: BUD_KEY = BUD_KEY1 AND EXPNS_LINE_ITEM_KEY = EXPNS_LINE_ITEM_KEY1 AND FISCL_YR = FISCAL_YEAR AND BUD_FUND_COMPST_KEY = BUD_FUND_COMPST_KEY1 <br>Output Column: PMT_DIFF_FUND_KEY	|		|		|	BUD_PMT_DIFF_FUND	|	PMT_DIFF_FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	BUDGET_ID	|	string	|	30	|		|		|		|		|		|	Table  Name : HDM.BUDGET <br>Condition: BUD_ID = BUDGET_ID_out<br>Output Column: BUD_KEY 	|		|		|	BUD_PMT_DIFF_FUND	|	BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|		|	Table  Name : HDM.BUD_EXPENSE_LINE_ITEM <br>Condition: EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out<br>Output Column: EXPNS_LINE_ITEM_KEY 	|		|		|	BUD_PMT_DIFF_FUND	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|		|	Table  Name : HDM.BUD_FUND_COMPOSITE<br>Condition: FUND_COMPST_ID = FUND_CMP_out <br>Output Column: BUD_FUND_COMPST_KEY	|		|		|	BUD_PMT_DIFF_FUND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	FUND_CMP	|	string	|	20	|		|		|		|		|		|	Table  Name : BUD_KEY<br>Condition:  BUD_ID = BUDGET_ID_out<br>Output Column: BUD_KEY 	|		|		|	BUD_PMT_DIFF_FUND	|	FUND_LVL_BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	FUND_ORDER	|	double	|	15	|		|		|		|	FISCAL_YEAR	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	FISCL_YR	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	FUND_NAME	|	string	|	80	|		|		|		|	ORIGINAL_ALLOCATION	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	ORIG_ALLOCN	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	FUND_LEVEL_BUDGET_ID	|	string	|	100	|		|		|		|	TOTAL_INVOICES_PAID	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	TOT_INVCS_PAID	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	ORIGINAL_ALLOCATION	|	double	|	15	|		|		|		|	TOTAL_INVOICES_PENDING	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	TOT_INVCS_PEND	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	PAYMENT_DIFFERENTIAL	|	double	|	15	|		|		|		|	ACCOUNT_TRANSFERS	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	ACCT_XFERS	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	REMAINING_LIABILITY	|	double	|	15	|		|		|		|	PAYMENT_DIFFERENTIAL	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	PMT_DIFFTL	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	ASSUMPTION_CHANGE_ALLOCATION	|	double	|	15	|		|		|		|	REMAINING_LIABILITY	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	REMNG_LIAB	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	TOTAL_INVOICES_PAID	|	double	|	15	|		|		|		|	ASSUMPTION_CHANGE_ALLOCATION	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	ASSMPN_CHG_ALLOCN	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	TOTAL_INVOICES_PENDING	|	double	|	15	|		|		|		|	PRIOR_YEAR_FLG	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	PRIR_YR_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	ACCOUNT_TRANSFERS	|	double	|	15	|		|		|		|	VERSION	|		|		|		|		|	BUD_PMT_DIFF_FUND	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	INVOICE_ID	|	string	|	100	|		|		|		|		|	Y'	|		|		|		|	BUD_PMT_DIFF_FUND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	ALLOCATION_ID	|	string	|	100	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_PMT_DIFF_FUND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	ALLOCATION_VALUE	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_PMT_DIFF_FUND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	STATUS	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_PMT_DIFF_FUND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_PMT_DIFF_FUND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF_FUND	|	PRIOR_YEAR_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
