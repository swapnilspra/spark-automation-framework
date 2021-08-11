|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table Name: HDM.BUD_DATA_SOURCE_FACT<br>Condition: DATA_SRC_DIM_KEY = DATA_SRC_DIM_KEY1 <br>Output Column: DATA_SRC_FACT_KEY	|		|		|	BUD_DATA_SOURCE_FACT	|	DATA_SRC_FACT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_DATA_SOURCE	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUD_DATA_SOURCE_DIM <br>Condition: DATA_SRC_ID = DATA_SOURCE_ID_out <br>Output Column: DATA_SRC_DIM_KEY	|		|		|	BUD_DATA_SOURCE_FACT	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_DATA_SOURCE	|	SOURCE_TYPE	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_TRUSTEE_ELECTION <br>Condition: TRSTE_ELCTN_ID = TRUSTEE_ELECTION_ID_out <br>Output Column: TRSTE_ELCTN_KEY	|		|		|	BUD_DATA_SOURCE_FACT	|	TRSTE_ELCTN_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_DATA_SOURCE	|	SHORT_NAME	|	string	|	30	|		|		|		|	QUARTERLY_SALARY	|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	QTRLY_SAL	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	SRC_NAME	|	string	|	100	|		|		|		|	CARRYOVER_YEAR	|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	NEG_CARROVR_BAL_YR	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TNA_DATE	|	string	|	30	|		|		|		|	CARRYOVER_AMOUNT	|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	NEG_CARROVR_BAL_AMT	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TNA_END_DATE	|	string	|	30	|		|		|		|	FEE_PER_MILLION	|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	FEE_PER_MIL	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TNA_START_DATE	|	string	|	30	|		|		|		|	FEE_REQUIRED_MINIMUM	|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	FEE_RQRD_MIN	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	NBR_ACCOUNTS_DATE	|	string	|	10	|		|		|		|	RIC_PERCENT	|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	RIC_PCT	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	FEE_PER_MILLION	|	double	|	15	|		|		|		|	TOTAL_FEE	|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	TOT_FEE	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	REG24F2_DATE	|	string	|	30	|		|		|		|	TOTAL_ISSUER_MARKET_CAP	|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	TOT_ISSR_MKT_CAP	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	FEE_REQUIRED_MINIMUM	|	double	|	15	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_DATA_SOURCE_FACT	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	RIC_PERCENT	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|	BUD_DATA_SOURCE_FACT	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TOTAL_FEE	|	double	|	15	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_DATA_SOURCE_FACT	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TOTAL_ISSUER_MARKET_CAP	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	NOTES	|	string	|	2000	|		|		|		|		|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_DATA_SOURCE	|	INVOICE_START_DATE	|	string	|	10	|		|		|		|		|		|		|		|		|	BUD_DATA_SOURCE_FACT	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	INVOICE_END_DATE	|	string	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	INVOICE_FISCAL_YEAR	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	INVOICE_FEE_TYPE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	QUARTERLY_SALARY	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	TRUSTEE_ELECTION_ID	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	CARRYOVER_YEAR	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	CARRYOVER_AMOUNT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	REG24F2_START_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	REG24F2_END_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	CALCULATION_METHOD	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	INSERT_USER	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
