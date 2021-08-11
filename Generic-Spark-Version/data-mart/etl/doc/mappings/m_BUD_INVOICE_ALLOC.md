|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table name: HDM.BUD_INVOICE_ALLOC<br>Condition: BUD_INVC_KEY = BUD_INVC_KEY1<br>Output Column: BUD_INVC_ALLOC_KEY	|		|		|	BUD_INVOICE_ALLOC	|	BUD_INVC_ALLOC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC	|	ALLOCATION_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_INVOICE<br>Condition: INVC_ID = INVOICE_ID_out AND ALLOCN_ID = ALLOCATION_ID_out AND INVC_LINE_ID = INVOICE_LINE_ID_out <br>Output Column: BUD_INVC_KEY	|		|		|	BUD_INVOICE_ALLOC	|	BUD_INVC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_LINE_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_DATA_SOURCE_DIM<br>Condition: DATA_SRC_ID = DATA_SOURCE_ID_out<br>Output Column: DATA_SRC_DIM_KEY	|		|		|	BUD_INVOICE_ALLOC	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC	|	ALLOCATION_AMOUNT	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUD_EXPENSE_LINE_ITEM<br>Condition: EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out<br>Output Column: EXPNS_LINE_ITEM_KEY	|		|		|	BUD_INVOICE_ALLOC	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC	|	ALLOCATION_TYPE	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_INVOICE_ALLOC	|	BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|	TAX_AMOUNT	|		|		|		|		|	BUD_INVOICE_ALLOC	|	TAX_AMT	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC	|	FISCAL_YEAR	|	double	|	15	|		|		|		|	INVOICE_AMOUNT	|		|		|		|		|	BUD_INVOICE_ALLOC	|	INVC_AMT	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_DATE	|	string	|	20	|		|		|		|	INVOICE_LINE_AMOUNT	|		|		|		|		|	BUD_INVOICE_ALLOC	|	INVC_LINE_AMT	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_NUMBER	|	string	|	100	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_INVOICE_ALLOC	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_VENDOR	|	string	|	100	|		|		|		|	ALLOCATION_AMOUNT	|		|		|		|		|	BUD_INVOICE_ALLOC	|	ALLOCN_AMT	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|		|	Y'	|		|		|		|	BUD_INVOICE_ALLOC	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_INVC_ALLOC	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_INVOICE_ALLOC	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_INVC_ALLOC	|	TAX_AMOUNT	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_INVOICE_ALLOC	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_AMOUNT	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_INVOICE_ALLOC	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_LINE_AMOUNT	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_INVOICE_ALLOC	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC	|	EXPENSE_ITEM	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	EXPENSE_LINE_ITEM	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	NOTES	|	string	|	2000	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	PENDING	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_SOURCE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_DESCRIPTION	|	string	|	4000	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	ACCOUNTING_DATE	|	string	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	ACCOUNTING_PERIOD	|	string	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	DELETED	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
