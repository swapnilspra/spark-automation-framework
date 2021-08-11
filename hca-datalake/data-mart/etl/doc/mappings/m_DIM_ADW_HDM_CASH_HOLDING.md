|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	FUND_CASH_HOLDG_KEY	|	decimal	|	10	|		|		|		|		|	\\\\for new record<br>used sequence generator to get the autoincrement key	|	//For the existing record<br>Table : HDM.CASH_HOLDING<br>Condition : FUND_COMPST_KEY = in_FUND_COMPST_KEY AND DAY_KEY = in_DAY_KEY<br>output column : CASH_HOLDG_KEY<br>	|		|	CASH_HOLDING	|	CASH_HOLDG_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	COMPST_FUND_KEY	|	decimal	|	10	|		|		|		|		|		|	//FUND COMPST KEY<br>Table : HDM.FUND_COMPOSITE<br>Condition : COMPST_CD = IN_COMPST_CD<br>output column : FUND_COMPST_KEY<br>	|		|	CASH_HOLDING	|	FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	//DAY KEY<br>Table : HDM.CALENDAR<br>Condition : CAL_DAY = IN_CAL_DAY<br>output column : DAY_KEY<br>	|		|	CASH_HOLDING	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	CASH_HOLDG_DT	|	date/time	|	29	|		|		|		|	CASH' From Source	|	CASH	|		|		|	CASH_HOLDING	|	CASH	|	number	|	15	|		|		|		|
|		|	CASH	|	double	|	15	|		|		|		|	SHRT_TERM_INVMT' From Source	|	SHRT_TERM_INVMT	|		|		|	CASH_HOLDING	|	SHRT_TERM_INVMT	|	number	|	15	|		|		|		|
|		|	SHRT_TERM_INVMT	|	double	|	15	|		|		|		|	RECVB' From Source	|	RECVB	|		|		|	CASH_HOLDING	|	RECBL	|	number	|	15	|		|		|		|
|		|	RECVB	|	double	|	15	|		|		|		|	PAYBL' From Source	|	PAYBL	|		|		|	CASH_HOLDING	|	PAYBL	|	number	|	15	|		|		|		|
|		|	PAYBL	|	double	|	15	|		|		|		|	NET_SHRS' From Source	|	NET_SHRS	|		|		|	CASH_HOLDING	|	NET_SHRS	|	number	|	15	|		|		|		|
|		|	NET_SHRS	|	double	|	15	|		|		|		|	NET_ASSET' From Source	|	NET_ASSET	|		|		|	CASH_HOLDING	|	NET_ASSET	|	number	|	15	|		|		|		|
|		|	NET_ASSET	|	double	|	15	|		|		|		|	Default to 'Y'	|	Y'	|		|		|	CASH_HOLDING	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	SYSDATE ' From Source	|	SYSDATE	|		|		|	CASH_HOLDING	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|	CASH_HOLDING	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|	CASH_HOLDING	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|	4	|	4	|		|		|	CASH_HOLDING	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|	CASH_COLTRL' From Source	|	CASH_COLTRL	|		|		|	CASH_HOLDING	|	CASH_COLTRL	|	number	|	15	|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	COMPST_FUND_ID	|	decimal	|	11	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	CASH_COLTRL	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
