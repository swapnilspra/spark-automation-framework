|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_INVOICE<br>Condition: INVC_ID = INVOICE_ID_out AND ALLOCN_ID = ALLOCATION_ID_out AND INVC_LINE_ID = INVOICE_LINE_ID_out<br>Output Column: BUD_INVC_KEY	|		|		|	BUD_INVOICE	|	BUD_INVC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC	|	ALLOCATION_ID	|	string	|	100	|		|		|		|		|	ltrim(rtrim(INVOICE_ID))	|		|		|		|	BUD_INVOICE	|	INVC_ID	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_LINE_ID	|	string	|	100	|		|		|		|		|	ltrim(rtrim(ALLOCATION_ID))	|		|		|		|	BUD_INVOICE	|	ALLOCN_ID	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_INVC_ALLOC	|	ALLOCATION_AMOUNT	|	double	|	15	|		|		|		|		|	ltrim(rtrim(INVOICE_LINE_ID))	|		|		|		|	BUD_INVOICE	|	INVC_LINE_ID	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_INVC_ALLOC	|	ALLOCATION_TYPE	|	string	|	30	|		|		|		|	FISCAL_YEAR	|		|		|		|		|	BUD_INVOICE	|	FISCL_YR	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|	INVOICE_DATE	|		|		|		|		|	BUD_INVOICE	|	INVC_ALLOCN_TYPE	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_INVC_ALLOC	|	FISCAL_YEAR	|	double	|	15	|		|		|		|	INVOICE_NUMBER	|		|		|		|		|	BUD_INVOICE	|	INVC_NBR	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_DATE	|	string	|	20	|		|		|		|	INVOICE_VENDOR	|		|		|		|		|	BUD_INVOICE	|	INVC_VEND	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_NUMBER	|	string	|	100	|		|		|		|		|	"to_date(SSB_EFFECTIVE_DATE,'YYYY-MM_DD')"	|		|		|		|	BUD_INVOICE	|	SSB_EFFTV_DT	|	date	|	19	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_VENDOR	|	string	|	100	|		|		|		|		|	"iif(isnull(PENDING),'N',iif(lower(PENDING)='x','Y','N'))"	|		|		|		|	BUD_INVOICE	|	PEND_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INSERT_USER	|	string	|	30	|		|		|		|	INVOICE_SOURCE	|		|		|		|		|	BUD_INVOICE	|	INVC_SRC	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INSERT_TS	|	string	|	30	|		|		|		|	INVOICE_DESCRIPTION	|		|		|		|		|	BUD_INVOICE	|	INVC_DESC	|	nvarchar2	|	4000	|		|		|
|	VW_BDGT_INVC_ALLOC	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|		|	"to_date(ACCOUNTING_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_INVOICE	|	ACCTG_DT	|	date	|	19	|		|		|
|	VW_BDGT_INVC_ALLOC	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|		|	"iif(isnull(DELETED),'N',iif(DELETED='true','Y','N'))"	|		|		|		|	BUD_INVOICE	|	DEL_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_INVC_ALLOC	|	TAX_AMOUNT	|	double	|	15	|		|		|		|		|	"iif(isnull(BUDGETED),'N',iif(BUDGETED='true','Y','N'))"	|		|		|		|	BUD_INVOICE	|	BUDED_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_AMOUNT	|	double	|	15	|		|		|		|		|	"iif(isnull(REPORTABLE1099),'N',iif(REPORTABLE1099='true','Y','N'))"	|		|		|		|	BUD_INVOICE	|	RPTB1099_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_LINE_AMOUNT	|	double	|	15	|		|		|		|	ACCOUNTING_PERIOD	|		|		|		|		|	BUD_INVOICE	|	ACCTG_PER	|	nvarchar2	|	20	|		|		|
|	VW_BDGT_INVC_ALLOC	|	EXPENSE_ITEM	|	string	|	100	|		|		|		|	NOTES	|		|		|		|		|	BUD_INVOICE	|	NTES	|	nvarchar2	|	2000	|		|		|
|	VW_BDGT_INVC_ALLOC	|	EXPENSE_LINE_ITEM	|	string	|	100	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	BUD_INVOICE	|	INSR_TS	|	date	|	19	|		|		|
|	VW_BDGT_INVC_ALLOC	|	VERSION	|	double	|	15	|		|		|		|	INSERT_USER	|		|		|		|		|	BUD_INVOICE	|	INSR_USR	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_INVC_ALLOC	|	NOTES	|	string	|	2000	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_INVOICE	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC	|	SSB_EFFECTIVE_DATE	|	string	|	20	|		|		|		|		|	Y'	|		|		|		|	BUD_INVOICE	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_INVC_ALLOC	|	PENDING	|	string	|	5	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_INVOICE	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_SOURCE	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_INVOICE	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_DESCRIPTION	|	string	|	4000	|		|		|		|		|		|		|		|		|	BUD_INVOICE	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_INVC_ALLOC	|	ACCOUNTING_DATE	|	string	|	20	|		|		|		|		|		|		|		|		|	BUD_INVOICE	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_INVC_ALLOC	|	ACCOUNTING_PERIOD	|	string	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	DELETED	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_PDF_NODE_ID	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_PDF_NODE_NAME	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_PDF_NODE_URL	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	INVOICE_PDF_PARENT_NODE_ID	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	BUDGETED	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_INVC_ALLOC	|	REPORTABLE1099	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
