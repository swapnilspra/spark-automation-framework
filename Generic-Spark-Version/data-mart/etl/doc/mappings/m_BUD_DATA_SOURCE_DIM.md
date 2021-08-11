|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_DATA_SOURCE_DIM <br>Condition: DATA_SRC_ID = DA TA_SOURCE_ID_out<br>Output Column: DATA_SRC_DIM_KEY	|		|		|	BUD_DATA_SOURCE_DIM	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_DATA_SOURCE	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|	ltrim(rtrim(DATA_SOURCE_ID))	|		|		|		|	BUD_DATA_SOURCE_DIM	|	DATA_SRC_ID	|	nvarchar2	|	60	|		|		|
|	VW_BDGT_DATA_SOURCE	|	SOURCE_TYPE	|	string	|	30	|		|		|		|	FISCAL_YEAR	|		|		|		|		|	BUD_DATA_SOURCE_DIM	|	FISCL_YR	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	SHORT_NAME	|	string	|	30	|		|		|		|		|	ltrim(rtrim(SOURCE_TYPE))	|		|		|		|	BUD_DATA_SOURCE_DIM	|	SRC_TYPE	|	nvarchar2	|	60	|		|		|
|	VW_BDGT_DATA_SOURCE	|	SRC_NAME	|	string	|	100	|		|		|		|		|	ltrim(rtrim(SHORT_NAME))	|		|		|		|	BUD_DATA_SOURCE_DIM	|	SHRT_NM	|	nvarchar2	|	60	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TNA_DATE	|	string	|	30	|		|		|		|		|	ltrim(rtrim(SRC_NAME))	|		|		|		|	BUD_DATA_SOURCE_DIM	|	SRC_NM	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TNA_END_DATE	|	string	|	30	|		|		|		|		|	"to_date(TNA_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	TNA_DT	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TNA_START_DATE	|	string	|	30	|		|		|		|		|	"to_date(TNA_END_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	TNA_END_DT	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	NBR_ACCOUNTS_DATE	|	string	|	10	|		|		|		|		|	"to_date(TNA_START_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	TNA_STRT_DT	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	FEE_PER_MILLION	|	double	|	15	|		|		|		|		|	"to_date(NBR_ACCOUNTS_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	NBR_ACCTS_DT	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	REG24F2_DATE	|	string	|	30	|		|		|		|		|	"to_date(REG24F2_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	REG24F2_DT	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	FEE_REQUIRED_MINIMUM	|	double	|	15	|		|		|		|	NOTES	|		|		|		|		|	BUD_DATA_SOURCE_DIM	|	DATA_SRC_NTES	|	nvarchar2	|	2000	|		|		|
|	VW_BDGT_DATA_SOURCE	|	RIC_PERCENT	|	double	|	15	|		|		|		|		|	"to_date(INVOICE_START_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	INVC_STRT_DT	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TOTAL_FEE	|	double	|	15	|		|		|		|		|	"to_date(INVOICE_END_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	INVC_END_DT	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TOTAL_ISSUER_MARKET_CAP	|	double	|	15	|		|		|		|	INVOICE_FISCAL_YEAR	|		|		|		|		|	BUD_DATA_SOURCE_DIM	|	INVC_FISCL_YR	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	NOTES	|	string	|	2000	|		|		|		|	INVOICE_FEE_TYPE	|		|		|		|		|	BUD_DATA_SOURCE_DIM	|	INVC_FEE_TYPE	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_DATA_SOURCE	|	INVOICE_START_DATE	|	string	|	10	|		|		|		|		|	"to_date(REG24F2_START_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	REG24F2_STRT_DT	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_DATA_SOURCE	|	INVOICE_END_DATE	|	string	|	10	|		|		|		|		|	"to_date(REG24F2_END_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	REG24F2_END_DT	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_DATA_SOURCE	|	INVOICE_FISCAL_YEAR	|	double	|	15	|		|		|		|	CALCULATION_METHOD	|		|		|		|		|	BUD_DATA_SOURCE_DIM	|	CALC_MTHD	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_DATA_SOURCE	|	INVOICE_FEE_TYPE	|	string	|	30	|		|		|		|	INSERT_USER	|		|		|		|		|	BUD_DATA_SOURCE_DIM	|	INSR_USR	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_DATA_SOURCE	|	QUARTERLY_SALARY	|	double	|	15	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	INSR_TS	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	TRUSTEE_ELECTION_ID	|	string	|	100	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_DATA_SOURCE_DIM	|	VRSN	|	"number(p,s)"	|	8	|		|		|
|	VW_BDGT_DATA_SOURCE	|	CARRYOVER_YEAR	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|	BUD_DATA_SOURCE_DIM	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_DATA_SOURCE	|	CARRYOVER_AMOUNT	|	double	|	15	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_DATA_SOURCE_DIM	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	REG24F2_START_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_DATA_SOURCE_DIM	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE	|	REG24F2_END_DATE	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_DATA_SOURCE_DIM	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_DATA_SOURCE	|	CALCULATION_METHOD	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_DATA_SOURCE_DIM	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE	|	INSERT_USER	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
