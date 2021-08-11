|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	refer SQ	|	TRGT_MDL_WGT_HDR_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	"Insert if new record<br><br>Update the record if already exists<br><br>REC_DELN_FLG='D' nd record exits, Delete the existing record"	|	Table Name: HDM.TARGET_DAILY_WEIGHT<br>Condition:DAY_KEY = DAY_KEY1 AND FUND_KEY = FUND_KEY1 AND TRGT_FUND_KEY = TRGT_FUND_KEY1	|	TARGET_DAILY_WEIGHT	|	TRGT_WGT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	Table Name: HDM.FUND<br>Condition:FUND_NBR = IN_FUND_NBR<br>Output Column : FUND_KEY	|		|		|		|	TRGT_FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	FUND_NBR	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|	EFFTV_STRT_DT	|	date	|	19	|		|		|
|		|	EFFTV_STRT_DT	|	date/time	|	29	|		|		|		|	EFFTV_END_DT	|		|		|		|		|		|	EFFTV_END_DT	|	date	|	19	|		|		|
|		|	EFFTV_END_DT	|	date/time	|	29	|		|		|		|		|		|	Table Name: HDM.FUND<br>Condition:FUND_NBR = IN_FUND_NBR<br>Output Column : FUND_KEY	|		|		|		|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	WGT_TYPE	|	string	|	25	|		|		|		|	TRGT_FUND_WGT	|		|		|		|		|		|	TRGT_FUND_WGT	|	number	|	15	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	WGT_TYPE	|		|		|		|		|		|	WGT_TYPE	|	varchar2	|	25	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|	Y'	|		|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|	SYSDATE	|		|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|	$$etlcyckey	|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	TGT_FUND_NBR	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ROW_STRT_DTTM1	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	FUND_WGT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	REC_DELN_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
