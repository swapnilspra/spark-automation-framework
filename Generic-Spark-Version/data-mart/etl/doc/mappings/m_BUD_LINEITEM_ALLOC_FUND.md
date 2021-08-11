|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table Name: HDM.BUD_LINEITEM_ALLOC_FUND <br>Condition : EXPNS_LINE_ITEM_KEY = EXPNS_LINE_ITEM_KEY1 AND BUD_KEY = BUD_KEY1 AND BUD_FUND_COMPST_KEY = BUD_FUND_COMPST_KEY1 <br>Output Column: LINE_ITM_ALLC_FUND_KEY 	|		|		|	BUD_LINEITEM_ALLOC_FUND	|	LINE_ITM_ALLC_FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	BUDGET_ID	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.CALENDAR <br>Condition : CAL_DAY = STRT_DT <br>Output Column: DAY_KEY 	|		|		|	BUD_LINEITEM_ALLOC_FUND	|	BUD_STRT_DT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_FUND_COMPOSITE <br>Condition : FUND_COMPST_ID = FUND_CMP_out <br>Output Column: BUD_FUND_COMPST_KEY 	|		|		|	BUD_LINEITEM_ALLOC_FUND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	ALLOCATION_NOTES	|	string	|	1000	|		|		|		|		|		|	Table Name: HDM.BUDGET<br>Condition :  BUD_ID = BUDGET_ID_out  <br>Output Column:  BUD_KEY 	|		|		|	BUD_LINEITEM_ALLOC_FUND	|	BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	FUND_NAME	|	string	|	80	|		|		|		|		|		|	Table Name: HDM.BUD_EXPENSE_LINE_ITEM<br>Condition : EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out <br>Output Column: EXPNS_LINE_ITEM_KEY 	|		|		|	BUD_LINEITEM_ALLOC_FUND	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	FUND_ORDER	|	double	|	15	|		|		|		|		|		|	Table Name: HDM .BUD_DATA_SOURCE_DIM<br>Condition : DATA_SRC_ID = DATA_SOURCE_ID_out <br>Output Column: DATA_SRC_DIM_KEY 	|		|		|	BUD_LINEITEM_ALLOC_FUND	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	RATE	|	double	|	15	|		|		|		|	RATE	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	RT	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	DATA_SOURCE_VALUE	|	double	|	15	|		|		|		|	DATA_SOURCE_VALUE	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	DATA_SRC_VAL	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	ALLOCATION	|	double	|	15	|		|		|		|	ALLOCATION	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	ALLOCN_AMT	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	FUND_CMP	|	string	|	20	|		|		|		|	NOTES	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	ALLOCN_PER_FUND_NTES	|	nvarchar2	|	1000	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	PER_FUND_NOTES	|	string	|	1000	|		|		|		|	ADJUSTMENT	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	ADJMT	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	ADJUSTMENT	|	double	|	15	|		|		|		|	RESIDUAL_VALUE	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	RSDL_VAL	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	RESIDUAL_VALUE	|	double	|	15	|		|		|		|	ORIGINAL_ALLOCATION	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	ORIG_ALLOCN	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	ORIGINAL_ALLOCATION	|	double	|	15	|		|		|		|	REALLOCATED_REMAINING	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	REALLOCD_REMNG	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	REALLOCATED_REMAINING	|	double	|	15	|		|		|		|	FYTD_EXPENSE	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	FYTD_EXPNS	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	FYTD_EXPENSE	|	double	|	15	|		|		|		|	ASSUMPTION_CHANGE	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	ASSMPN_CHG	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	ASSUMPTION_CHANGE	|	double	|	15	|		|		|		|	TO_BE_REALLOCATED	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	TO_BE_REALLOCD	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	TO_BE_REALLOCATED	|	double	|	15	|		|		|		|		|	ltrim(rtrim(SOURCE_BUDGET_ID))	|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	SRC_BUD_KEY	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	SOURCE_BUDGET_ID	|	string	|	30	|		|		|		|	PAYMENT_DIFFERENTIAL	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	PMT_DIFFTL	|	number	|	15	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	PAYMENT_DIFFERENTIAL	|	double	|	15	|		|		|		|	ALLOCATION_NOTES	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	ALLOCN_NTES	|	nvarchar2	|	1000	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	VERSION	|	double	|	15	|		|		|		|	INSERT_USER	|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	INSR_USR	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	INSERT_USER	|	string	|	30	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	INSR_TS	|	date	|	19	|		|		|
|	VW_BDGT_LINE_ITEM_ALLOC_FUND	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	VRSN	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_LINEITEM_ALLOC_FUND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
