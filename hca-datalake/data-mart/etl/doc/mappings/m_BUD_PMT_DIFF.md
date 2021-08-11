|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_PMT_DIFF <br>Condition: BUD_KEY = BUD_KEY1 AND EXPNS_LINE_ITEM_KEY = EXPNS_LINE_ITEM_KEY1 AND FISCL_YR = FISCAL_YEAR <br>Output Column: PMT_DIFF_KEY	|		|		|	HDM.BUD_PMT_DIFF	|	PMT_DIFF_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	BUDGET_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUDGET <br>Condition: BUD_ID = BUDGET_ID_out <br>Output Column: BUD_KEY 	|		|		|	HDM.BUD_PMT_DIFF	|	BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUD_EXPENSE_LINE_ITEM <br>Condition: EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out <br>Output Column: EXPNS_LINE_ITEM_KEY 	|		|		|	HDM.BUD_PMT_DIFF	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	INVOICE_EXPECTATION	|	string	|	30	|		|		|		|	FISCAL_YEAR	|		|		|		|		|	HDM.BUD_PMT_DIFF	|	FISCL_YR	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|	INVOICE_EXPECTATION	|		|		|		|		|	HDM.BUD_PMT_DIFF	|	INVC_EXPCTATION	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	EXPENSE_ITEM	|	string	|	100	|		|		|		|	PAYMENT_DIFFERENTIAL	|		|		|		|		|	HDM.BUD_PMT_DIFF	|	PMT_DIFFTL	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	EXPENSE_LINE_ITEM	|	string	|	100	|		|		|		|	REMAINING_LIABILITY	|		|		|		|		|	HDM.BUD_PMT_DIFF	|	REMNG_LIAB	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	PAYMENT_DIFFERENTIAL	|	double	|	15	|		|		|		|	ADJUSTMENT_TYPE	|		|		|		|		|	HDM.BUD_PMT_DIFF	|	ADJMT_TYPE	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	REMAINING_LIABILITY	|	double	|	15	|		|		|		|		|	"iif(isnull(RESHUFFLE_PAYMENT_DIFF),'N',iif(RESHUFFLE_PAYMENT_DIFF='true','Y','N'))"	|		|		|		|	HDM.BUD_PMT_DIFF	|	RESHUFFLE_PMT_DIFF_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	ADJUSTMENT_TYPE	|	string	|	30	|		|		|		|		|	"iif(isnull(EDITABLE),'N',iif(EDITABLE='true','Y','N'))"	|		|		|		|	HDM.BUD_PMT_DIFF	|	EDITBL_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	LINE_ITEM_ORIGINAL_BUDGET	|	double	|	15	|		|		|		|	PRIOR_YEAR_FLG	|		|		|		|		|	HDM.BUD_PMT_DIFF	|	PRIR_YR_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	LINE_ITEM_INVOICES_PAID	|	double	|	15	|		|		|		|	NOTES	|		|		|		|		|	HDM.BUD_PMT_DIFF	|	NTES	|	nvarchar2	|	4000	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	LINE_ITEM_PENDING_INVOICE_TOT	|	double	|	15	|		|		|		|	INSERT_USER	|		|		|		|		|	HDM.BUD_PMT_DIFF	|	INSR_USR	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	LINE_ITEM_ACCT_TRANSFER_TOT	|	double	|	15	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	HDM.BUD_PMT_DIFF	|	INSR_TS	|	date	|	19	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	LINE_ITEM_ASSUMPTION_CHNG_TOT	|	double	|	15	|		|		|		|	VERSION	|		|		|		|		|	HDM.BUD_PMT_DIFF	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	RESHUFFLE_PAYMENT_DIFF	|	string	|	5	|		|		|		|		|	Y'	|		|		|		|	HDM.BUD_PMT_DIFF	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	EDITABLE	|	string	|	5	|		|		|		|		|	SYSDATE	|		|		|		|	HDM.BUD_PMT_DIFF	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	NOTES	|	string	|	4000	|		|		|		|		|		|		|		|		|	HDM.BUD_PMT_DIFF	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	INSERT_USER	|	string	|	30	|		|		|		|		|		|		|		|		|	HDM.BUD_PMT_DIFF	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|		|		|		|		|		|	HDM.BUD_PMT_DIFF	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_PAYMENT_DIFF	|	PRIOR_YEAR_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
