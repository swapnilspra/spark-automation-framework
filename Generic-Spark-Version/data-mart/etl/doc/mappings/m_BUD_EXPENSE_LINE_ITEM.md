|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_LINE_ITEM	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table Name: HDM.BUD_EXPENSE_LINE_ITEM <br>Condition: EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out <br>Output Column: EXPNS_LINE_ITEM_KEY	|		|		|	BUD_EXPENSE_LINE_ITEM	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM	|	ACTIVE	|	string	|	5	|		|		|		|		|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	EXPNS_LINE_ITEM_NM	|	nvarchar2	|	255	|		|		|
|	VW_BDGT_LINE_ITEM	|	ALLOCATION_TYPE	|	string	|	30	|		|		|		|		|	ltrim(rtrim(LINE_ITEM_ID))	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	EXPNS_LINE_ITEM_ID	|	nvarchar2	|	255	|		|		|
|	VW_BDGT_LINE_ITEM	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|	NOTES	|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	LINE_ITEM_NTES	|	nvarchar2	|	1000	|		|		|
|	VW_BDGT_LINE_ITEM	|	EXPENSE_ITEM	|	string	|	100	|		|		|		|		|	"iif(isnull(ACTIVE),0,iif(lower(ACTIVE)='false',0,1))"	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	ACTV_FLG	|	"number(p,s)"	|	38	|		|		|
|	VW_BDGT_LINE_ITEM	|	EXPENSE_LINE_ITEM	|	string	|	100	|		|		|		|	ALLOCATION_TYPE	|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	ALLOCN_TYPE	|	nvarchar2	|	60	|		|		|
|	VW_BDGT_LINE_ITEM	|	NOTES	|	string	|	1000	|		|		|		|	EXPENSE_ITEM	|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	EXPNS_ITEM_NM	|	nvarchar2	|	255	|		|		|
|	VW_BDGT_LINE_ITEM	|	INVOICE_VENDORS	|	string	|	30	|		|		|		|	EXPENSE_CATEGORY	|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	EXPNS_CAT_NM	|	nvarchar2	|	255	|		|		|
|	VW_BDGT_LINE_ITEM	|	SSB_EXPENSE_ACCOUNT	|	string	|	20	|		|		|		|	INVOICE_VENDORS	|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	INVC_VENDS	|	nvarchar2	|	255	|		|		|
|	VW_BDGT_LINE_ITEM	|	SSB_PAYABLE_ACCOUNT	|	string	|	20	|		|		|		|		|	ltrim(rtrim(SSB_EXPENSE_ACCOUNT))	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	EXPNS_ACCT_ID	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_LINE_ITEM	|	PREPAID_EXPENSE	|	string	|	5	|		|		|		|		|	ltrim(rtrim(SSB_PAYABLE_ACCOUNT))	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	PAYBL_ACCT_ID	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_LINE_ITEM	|	HIDDEN	|	string	|	5	|		|		|		|		|	"iif(isnull(PREPAID_EXPENSE),0,iif(lower(PREPAID_EXPENSE)='false',0,1))"	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	PRAPAID_EXPNS_FLG	|	"number(p,s)"	|	38	|		|		|
|	VW_BDGT_LINE_ITEM	|	HIDDEN_REASON	|	string	|	1000	|		|		|		|		|	"iif(isnull(HIDDEN),0,iif(lower(HIDDEN)='false',0,1))"	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	HDDN_FLG	|	"number(p,s)"	|	38	|		|		|
|	VW_BDGT_LINE_ITEM	|	VERSION	|	double	|	15	|		|		|		|	HIDDEN_REASON	|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	HDDN_RSN	|	nvarchar2	|	1000	|		|		|
|	VW_BDGT_LINE_ITEM	|	INSERT_USER	|	string	|	30	|		|		|		|	INSERT_USER	|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	INSR_USR	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_LINE_ITEM	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	INSR_TS	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	VRSN	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_EXPENSE_LINE_ITEM	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
