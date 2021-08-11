|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	Updaye if record already present	|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	TRANSFER_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_ACCT_TRANSFER_FND <br>Condition: ACCT_XFER_KEY = ACCT_XFER_KEY1 AND BUD_FUND_COMPST_KEY = BUD_FUND_COMPST_KEY1 <br>Output Column: ACCT_XFER_FUND_KEY 	|		|		|	BUD_ACCT_TRANSFER_FND	|	ACCT_XFER_FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	TRANSFER_GROUP_NAME	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_ACCT_TRANSFER<br>Condition: XFER_ID = TRANSFER_ID<br>Output Column: ACCT_XFER_KEY	|		|		|	BUD_ACCT_TRANSFER_FND	|	ACCT_XFER_KEY	|	number	|	15	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	ENTRY_NAME	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = SOURCE_ACCOUNT_ID_out<br>Output Column: BUD_ACCT_KEY	|		|		|	BUD_ACCT_TRANSFER_FND	|	SRC_BUD_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|		|	Table Name:  HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = DESTINATION_ACCOUNT_ID_out<br>Output Column: BUD_ACCT_KEY	|		|		|	BUD_ACCT_TRANSFER_FND	|	DEST_BUD_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SSB_EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_EXPENSE_LINE_ITEM<br>Condition: EXPNS_LINE_ITEM_ID = SOURCE_LINE_ITEM_ID_out<br>Output Column: EXPNS_LINE_ITEM_KEY	|		|		|	BUD_ACCT_TRANSFER_FND	|	SRC_EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SOURCE_LINE_ITEM_ACCOUNT	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_EXPENSE_LINE_ITEM<br>Condition: EXPNS_LINE_ITEM_ID = DESTINATION_LINE_ITEM_ID_out<br>Output Column: EXPNS_LINE_ITEM_KEY	|		|		|	BUD_ACCT_TRANSFER_FND	|	DEST_EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SOURCE_LINE_ITEM_NAME	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_FUND_COMPOSITE<br>Condition: FUND_COMPST_ID = FUND_CMP<br>Output Column: BUD_FUND_COMPST_KEY	|		|		|	BUD_ACCT_TRANSFER_FND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SOURCE_LINE_ITEM_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT <br>Condition: ACCT_ID = DEBIT_ACCOUNT_ID<br>Output Column: BUD_ACCT_KEY	|		|		|	BUD_ACCT_TRANSFER_FND	|	DBT_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SOURCE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT <br>Condition: ACCT_ID = CREDIT_ACCOUNT_ID_out<br>Output Column: BUD_ACCT_KEY	|		|		|	BUD_ACCT_TRANSFER_FND	|	CR_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DESTINATION_LINE_ITEM_ACCOUNT	|	string	|	100	|		|		|		|		|	ltrim(rtrim(TRANSFER_ID))	|		|		|		|	BUD_ACCT_TRANSFER_FND	|	XFER_ID	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DESTINATION_LINE_ITEM_NAME	|	string	|	100	|		|		|		|	TRANSFER_VALUE	|		|		|		|		|	BUD_ACCT_TRANSFER_FND	|	XFER_VAL	|	number	|	15	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DESTINATION_LINE_ITEM_ID	|	string	|	100	|		|		|		|	VERSION	|		|		|		|		|	BUD_ACCT_TRANSFER_FND	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DESTINATION_ACCOUNT_ID	|	string	|	30	|		|		|		|		|	Y'	|		|		|		|	BUD_ACCT_TRANSFER_FND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	FUND_CMP	|	string	|	20	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_ACCT_TRANSFER_FND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	FUND_NAME	|	string	|	80	|		|		|		|		|		|		|		|		|	BUD_ACCT_TRANSFER_FND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	FUND_ORDER	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_ACCT_TRANSFER_FND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DEBIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_ACCT_TRANSFER_FND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	CREDIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	TRANSFER_VALUE	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	INSERT_USER	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	INSERT_TS	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	EDITABLE	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	TRANSFER_STATUS	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	NOTES	|	string	|	4000	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
