|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	TRUSTEE_ELECTION_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table Name: HDM.BUD_TRUSTEE_ELECTION <br>Condition: TRSTE_ELCTN_ID = TRUSTEE_ELECTION_ID_out AND BUD_FUND_COMPST_KEY = BUD_FUND_COMPST_KEY1 <br>Output Column: TRSTE_ELCTN_KEY	|		|		|	BUD_TRUSTEE_ELECTION	|	TRSTE_ELCTN_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	TRUSTEE_NAME	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_FUND_COMPOSITE <br>Condition: FUND_COMPST_ID = FUND_CMP_out <br>Output Column: BUD_FUND_COMPST_KEY 	|		|		|	BUD_TRUSTEE_ELECTION	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|	ltrim(rtrim(TRUSTEE_ELECTION_ID))	|		|		|		|	BUD_TRUSTEE_ELECTION	|	TRSTE_ELCTN_ID	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	FUND_CMP	|	string	|	20	|		|		|		|		|	ltrim(rtrim(TRUSTEE_NAME))	|		|		|		|	BUD_TRUSTEE_ELECTION	|	TRSTE_NM	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	FUND_NAME	|	string	|	80	|		|		|		|	FISCAL_YEAR	|		|		|		|		|	BUD_TRUSTEE_ELECTION	|	FISCL_YR	|	number	|	15	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	FUND_ORDER	|	double	|	15	|		|		|		|	DATA_SOURCE_VALUE	|		|		|		|		|	BUD_TRUSTEE_ELECTION	|	DATA_SRC_VAL	|	number	|	15	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	DATA_SOURCE_VALUE	|	double	|	15	|		|		|		|	NOTES	|		|		|		|		|	BUD_TRUSTEE_ELECTION	|	NTES	|	nvarchar2	|	4000	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	NOTES	|	string	|	4000	|		|		|		|	INSERT_USER	|		|		|		|		|	BUD_TRUSTEE_ELECTION	|	INSR_USR	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	INSERT_USER	|	string	|	30	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	BUD_TRUSTEE_ELECTION	|	INSR_TS	|	date	|	19	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|	VERSION	|		|		|		|		|	BUD_TRUSTEE_ELECTION	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_TRUSTEE_ELECTION	|	VERSION	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|	BUD_TRUSTEE_ELECTION	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	BUD_TRUSTEE_ELECTION	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_TRUSTEE_ELECTION	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_TRUSTEE_ELECTION	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_TRUSTEE_ELECTION	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
