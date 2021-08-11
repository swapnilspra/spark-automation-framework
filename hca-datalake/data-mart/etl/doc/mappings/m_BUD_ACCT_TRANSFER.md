|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	VW_BDGT_ACCT_TRANSFER	|	TRANSFER_ID	|	string	|	100	|		|		|		|	Auto increment the key value by '1' for each new record	|	Use sequence generator to generate the key	|		|		|	BUD_ACCT_TRANSFER	|	ACCT_XFER_KEY	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	TRANSFER_GROUP_NAME	|	string	|	100	|		|		|		|	Remove empty spaces on 'TRANSFER_ID'	|	ltrim(rtrim(TRANSFER_ID))	|		|		|	BUD_ACCT_TRANSFER	|	XFER_ID	|	nvarchar2	|	100	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	ENTRY_NAME	|	string	|	100	|		|		|		|	TRANSFER_GROUP_NAME' from Source	|	TRANSFER_GROUP_NAME	|		|		|	BUD_ACCT_TRANSFER	|	XFER_GRP_NM	|	nvarchar2	|	100	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	EFFECTIVE_DATE	|	string	|	30	|		|		|		|	ENTRY_NAME' from Source	|	ENTRY_NAME	|		|		|	BUD_ACCT_TRANSFER	|	ENTRY_NM	|	nvarchar2	|	100	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SSB_EFFECTIVE_DATE	|	string	|	30	|		|		|		|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|	BUD_ACCT_TRANSFER	|	EFFTV_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SOURCE_LINE_ITEM_ACCOUNT	|	string	|	100	|		|		|		|	"to_date(SSB_EFFECTIVE_DATE,'YYYY-MM-DD')"	|	"to_date(SSB_EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|	BUD_ACCT_TRANSFER	|	SSB_EFFTV_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SOURCE_LINE_ITEM_NAME	|	string	|	100	|		|		|		|	INSERT_USER' from Source'	|	INSERT_USER	|		|		|	BUD_ACCT_TRANSFER	|	INSR_USR	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SOURCE_LINE_ITEM_ID	|	string	|	100	|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|	BUD_ACCT_TRANSFER	|	INSR_TS	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	SOURCE_ACCOUNT_ID	|	string	|	30	|		|		|		|	"iif(isnull(EDITABLE),'N',iif(EDITABLE='true','Y','N'))"	|	"iif(isnull(EDITABLE),'N',iif(EDITABLE='true','Y','N'))"	|		|		|	BUD_ACCT_TRANSFER	|	EDITBL_FLG	|	nvarchar2	|	1	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DESTINATION_LINE_ITEM_ACCOUNT	|	string	|	100	|		|		|		|	TRANSFER_STATUS' from Source	|	TRANSFER_STATUS	|		|		|	BUD_ACCT_TRANSFER	|	XFER_STAT	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DESTINATION_LINE_ITEM_NAME	|	string	|	100	|		|		|		|	NOTES' from Source	|	NOTES	|		|		|	BUD_ACCT_TRANSFER	|	NTES	|	nvarchar2	|	4000	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DESTINATION_LINE_ITEM_ID	|	string	|	100	|		|		|		|	VERSION from Source	|	VERSION	|		|		|	BUD_ACCT_TRANSFER	|	VRSN	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DESTINATION_ACCOUNT_ID	|	string	|	30	|		|		|		|	Default to 'Y'	|	'Y'	|		|		|	BUD_ACCT_TRANSFER	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	FUND_CMP	|	string	|	20	|		|		|		|	current date/time	|	sysdate	|		|		|	BUD_ACCT_TRANSFER	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	FUND_NAME	|	string	|	80	|		|		|		|		|		|		|		|	BUD_ACCT_TRANSFER	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	FUND_ORDER	|	double	|	15	|		|		|		|		|		|		|		|	BUD_ACCT_TRANSFER	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	DEBIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|	BUD_ACCT_TRANSFER	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	CREDIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	TRANSFER_VALUE	|	double	|	15	|		|		|		|		|		|		|		|	Update records	|		|		|		|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	INSERT_USER	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_ACCT_TRANSFER<br>Condition : XFER_ID = TRANSFER_ID_out<br>Output Column : ACCT_XFER_KEY	|	Update the existing records if its existing	|	BUD_ACCT_TRANSFER	|	ACCT_XFER_KEY	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	INSERT_DATE	|	string	|	30	|		|		|		|	Remove empty spaces on 'TRANSFER_ID'	|	ltrim(rtrim(TRANSFER_ID))	|		|		|	BUD_ACCT_TRANSFER	|	XFER_ID	|	nvarchar2	|	100	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	NOTES	|	string	|	5	|		|		|		|	TRANSFER_GROUP_NAME' from Source	|	TRANSFER_GROUP_NAME	|		|		|	BUD_ACCT_TRANSFER	|	XFER_GRP_NM	|	nvarchar2	|	100	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	EDITABLE	|	string	|	5	|		|		|		|	ENTRY_NAME' from Source	|	ENTRY_NAME	|		|		|	BUD_ACCT_TRANSFER	|	ENTRY_NM	|	nvarchar2	|	100	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	TRANSFER_STATUS	|	string	|	4000	|		|		|		|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|	BUD_ACCT_TRANSFER	|	EFFTV_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_TRANSFER	|	VERSION	|	double	|	15	|		|		|		|	"to_date(SSB_EFFECTIVE_DATE,'YYYY-MM-DD')"	|	"to_date(SSB_EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|	BUD_ACCT_TRANSFER	|	SSB_EFFTV_DT	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|	INSERT_USER' from Source'	|	INSERT_USER	|		|		|	BUD_ACCT_TRANSFER	|	INSR_USR	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|	BUD_ACCT_TRANSFER	|	INSR_TS	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|	"iif(isnull(EDITABLE),'N',iif(EDITABLE='true','Y','N'))"	|	"iif(isnull(EDITABLE),'N',iif(EDITABLE='true','Y','N'))"	|		|		|	BUD_ACCT_TRANSFER	|	EDITBL_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|	TRANSFER_STATUS' from Source	|	TRANSFER_STATUS	|		|		|	BUD_ACCT_TRANSFER	|	XFER_STAT	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	NOTES' from Source	|	NOTES	|		|		|	BUD_ACCT_TRANSFER	|	NTES	|	nvarchar2	|	4000	|		|		|		|
|		|		|		|		|		|		|		|	VERSION from Source	|	VERSION	|		|		|	BUD_ACCT_TRANSFER	|	VRSN	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	Default to 'Y'	|	'Y'	|		|		|	BUD_ACCT_TRANSFER	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|	current date/time	|	sysdate	|		|		|	BUD_ACCT_TRANSFER	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCT_TRANSFER	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCT_TRANSFER	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCT_TRANSFER	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
