|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|	SEQ NUmber	|		|	Table Name: HDM.BUD_LINEITEM_ALLOC<br>Condition: BUD_KEY = BUD_KEY1 AND EXPNS_LINE_ITEM_KEY = EXPNS_LINE_ITEM_KEY1<br>Output Column: LINEITEM_ALLOCN_KEY	|		|		|	BUD_LINEITEM_ALLOC	|	LINEITEM_ALLOCN_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	BUDGET_ID	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUDGET<br>Condition: BUD_ID = BUDGET_ID_out <br>Output Column: BUD_KEY 	|		|		|	BUD_LINEITEM_ALLOC	|	BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	EXPENSE_ITEM	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_EXPENSE_LINE_ITEM <br>Condition: EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out <br>Output Column: EXPNS_LINE_ITEM_KEY	|		|		|	BUD_LINEITEM_ALLOC	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	EXPENSE_LINE_ITEM	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_DATA_SOURCE_DIM<br>Condition: DATA_SRC_ID = DATA_SOURCE_ID_out <br>Output Column: DATA_SRC_DIM_KEY  	|		|		|	BUD_LINEITEM_ALLOC	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	ALLOCATION_TYPE	|	string	|	30	|		|		|		|	ALLOCATION_AMOUNT	|		|		|		|		|	BUD_LINEITEM_ALLOC	|	ALLOCN_AMT	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	ALLOCATION_AMOUNT	|	double	|	15	|		|		|		|	LINE_ITEM_TO_BE_REALLOCATED	|		|		|		|		|	BUD_LINEITEM_ALLOC	|	LINE_ITEM_TO_BE_REALLOCD	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|	NOTES	|		|		|		|		|	BUD_LINEITEM_ALLOC	|	NTES	|	nvarchar2	|	4000	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|	INSERT_USER	|		|		|		|		|	BUD_LINEITEM_ALLOC	|	INSR_USR	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	LINE_ITEM_TO_BE_REALLOCATED	|	string	|	100	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	BUD_LINEITEM_ALLOC	|	INSR_TS	|	date	|	19	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	NOTES	|	string	|	4000	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_LINEITEM_ALLOC	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	VERSION	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|	BUD_LINEITEM_ALLOC	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	INSERT_USER	|	string	|	30	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_LINEITEM_ALLOC	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_LINEITEM_ALLOC	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_LINEITEM_ALLOC	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_LINEITEM_ALLOC	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
