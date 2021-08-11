|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	INVOICE_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table Name : HDM.BUD_INVOICE_ALLOC_FUND<br>Condition : BUD_INVC_KEY = BUD_INVC_KEY1 AND BUD_FUND_COMPST_KEY = BUD_FUND_COMPST_KEY1<br>Output Column: BUD_INVC_ALLOC_FUND_KEY	|		|		|	BUD_INVOICE_ALLOC_FUND	|	BUD_INVC_ALLOC_FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	ALLOCATION_ID	|	string	|	100	|		|		|		|		|		|	Table Name : HDM.BUD_INVOICE<br>Condition : INVC_ID = INVOICE_ID_out AND ALLOCN_ID = ALLOCATION_ID_out AND INVC_LINE_ID = INVOICE_LINE_ID_out<br>Output Column: BUD_INVC_KEY	|		|		|	BUD_INVOICE_ALLOC_FUND	|	BUD_INVC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	INVOICE_LINE_ID	|	string	|	100	|		|		|		|		|		|	Table Name : HDM.BUD_DATA_SOURCE_DIM<br>Condition : DATA_SRC_ID = DATA_SOURCE_ID_out<br>Output Column: DATA_SRC_DIM_KEY	|		|		|	BUD_INVOICE_ALLOC_FUND	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|		|		|	Table Name : HDM.BUD_FUND_COMPOSITE<br>Condition : FUND_COMPST_ID = FUND_CMP_out<br>Output Column: BUD_FUND_COMPST_KEY	|		|		|	BUD_INVOICE_ALLOC_FUND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|		|		|	Table Name : HDM.BUD_EXPENSE_LINE_ITEM<br>Condition : EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out<br>Output Column: EXPNS_LINE_ITEM_KEY	|		|		|	BUD_INVOICE_ALLOC_FUND	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	NOTES	|	string	|	300	|		|		|		|	ADJUSTMENT	|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	FUND_ADJMT	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	ADJUSTMENT	|	double	|	15	|		|		|		|	RESIDUAL_VALUE	|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	FUND_RSDL_VAL	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	RESIDUAL_VALUE	|	double	|	15	|		|		|		|	RATE	|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	FUND_RT	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	FUND_CMP	|	string	|	20	|		|		|		|	TAX_RATE	|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	TAX_RT	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	FUND_NAME	|	string	|	80	|		|		|		|	DATA_SOURCE_VALUE	|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	FUND_DATA_SRC_VAL	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	FUND_ORDER	|	double	|	15	|		|		|		|	ALLOCATION	|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	FUND_ALLOCN	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	RATE	|	double	|	15	|		|		|		|	TAX_ALLOCATION	|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	FUND_TAX_ALLOCN	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	TAX_RATE	|	double	|	15	|		|		|		|	INVOICE_ALLOCATION	|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	FUND_INVC_ALLOCN	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	DATA_SOURCE_VALUE	|	double	|	15	|		|		|		|	NOTES	|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	NTES	|	nvarchar2	|	300	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	ALLOCATION	|	double	|	15	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	TAX_ALLOCATION	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	INVOICE_ALLOCATION	|	double	|	15	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_INVC_ALLOC_FUND	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_INVOICE_ALLOC_FUND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
