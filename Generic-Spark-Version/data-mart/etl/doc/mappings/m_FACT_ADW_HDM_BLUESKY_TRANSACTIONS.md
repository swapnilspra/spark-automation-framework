|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	SPR_SHEET_DT	|	date/time	|	29	|	9	|		|		|	Seq Number	|		|		|		|	//If record already exisit update the record<br>Table : BLUESKY_TRANSACTIONS<br>Condition: DAY_KEY = in_DAY_KEY AND FUND_KEY = in_FUND_KEY AND REG_KEY = in_REG_KEY	|	BLUESKY_TRANSACTIONS	|	BLUE_SKY_TXN_KEY	|	varchar2	|	18	|		|	Y	|
|		|	MTD_REPT_AMT	|	double	|	15	|		|		|		|		|		|	//Day_KEY<br>Table : HDM.CALENDAR<br>Condition: CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	BLUESKY_TRANSACTIONS	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SHR_QTY	|	double	|	15	|		|		|		|		|		|	//FUND_KEY<br>Table : HDM.FUND<br>Condition: FUND_NBR = IN_FUND_NBR<br>Output Column: FUND_KEY	|		|		|	BLUESKY_TRANSACTIONS	|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	TXN_AMT	|	double	|	15	|		|		|		|		|	"IIF(ISNULL(REG_KEY),300,REG_KEY)"	|	//REG_KEY<br>Table :HDM.REGION<br>Condition: ST_CD = IN_ST_CD AND MKTG_REG_ID = IN_MKTG_REG_ID AND CRTY_CD = IN_CRTY_CD<br>Output Column: REG_KEY	|		|		|	BLUESKY_TRANSACTIONS	|	REG_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	MTD_REOPEN_AMT	|	double	|	15	|		|		|		|		|	"SUM(SHR_QTY) for the combination 'DAY_KEY', 'FUND_KEY','REG_KEY'"	|		|		|		|	BLUESKY_TRANSACTIONS	|	SHR_QTY	|	number	|	15	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|	"SUM(TXN_AMT) for the combination 'DAY_KEY', 'FUND_KEY','REG_KEY'"	|		|		|		|	BLUESKY_TRANSACTIONS	|	TXN_AMT	|	number	|	15	|		|		|
|		|	ST_CD	|	string	|	3	|		|		|		|		|	"SUM(MTD_REPT_AMT) for the combination 'DAY_KEY', 'FUND_KEY','REG_KEY'"	|		|		|		|	BLUESKY_TRANSACTIONS	|	MTD_REPT_AMT	|	number	|	15	|		|		|
|		|	REG_KEY	|	decimal	|	10	|		|		|		|		|	"SUM(MTD_REOPEN_AMT) for the combination 'DAY_KEY', 'FUND_KEY','REG_KEY'"	|		|		|		|	BLUESKY_TRANSACTIONS	|	MTD_RE_OPEN_AMT	|	number	|	15	|		|		|
|		|	CRTY_ISO_CD	|	string	|	2	|		|		|		|		|		|	SYSDATE	|		|		|	BLUESKY_TRANSACTIONS	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BLUESKY_TRANSACTIONS	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|	BLUESKY_TRANSACTIONS	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|	4	|		|		|	BLUESKY_TRANSACTIONS	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|	Y'	|		|		|	BLUESKY_TRANSACTIONS	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|	Seq Number	|		|		|		|	"IIF(NOT ISNULL(v_ERR_MSG),'Y','N')"	|	ETL_LOAD_ERROR	|	ERR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|	ETL_LOAD_ERROR	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|	"IIF(<br>      (NOT ISNULL(v_DAY_KEY_ERR_MSG)<br>                                                         OR<br>       NOT ISNULL(v_FUND_KEY_ERR_MSG)),<br>(v_DAY_KEY_ERR_MSG&vert;&vert;v_FUND_KEY_ERR_MSG&vert;&vert;' FOR FUND NUMBER:'&vert;&vert;TO_CHAR(FUND_NBR)&vert;&vert;' AND SPR_SHEET_DT:'&vert;&vert;TO_CHAR(SPR_SHEET_DT)&vert;&vert;' AND ST_CD:'&vert;&vert;TO_CHAR(ST_CD)&vert;&vert;' AND REG_KEY:'&vert;&vert;TO_CHAR(in_REG_KEY)&vert;&vert;' AND CRTY_ISO_CD:'&vert;&vert;TO_CHAR(CRTY_ISO_CD)),NULL)"	|		|		|	ETL_LOAD_ERROR	|	ERR_MSG	|	varchar2	|	4000	|		|		|
|		|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|	ETL_LOAD_ERROR	|	RUN_DT_TS	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
