|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	refer SQ	|	USR_Q_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	Insert if new record<br><br>Update the record if already exists<br><br>	|	Table Name: HDM.AWD_USER_QUEUE<br>Condition:USR_KEY = IN_USR_KEY AND Q_KEY = IN_Q_KEY AND DAY_KEY = IN_DAY_KEY	|	HDM_AWD_USER_QUEUE	|	USR_Q_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	Table Name : HDM.AWD_QUEUE<br>Condition: Q_CD = IN_Q_CD<br>Output Column: Q_KEY	|		|		|	HDM_AWD_USER_QUEUE	|	Q_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	Q_KEY	|	decimal	|	10	|		|		|		|		|		|	Table Name : HDM.AWD_USER<br>Condition:USR_ID = IN_USR_ID<br>Output Column: USR_KEY	|		|		|	HDM_AWD_USER_QUEUE	|	USR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	USR_KEY	|	decimal	|	10	|		|		|		|		|		|	Table Name : HDM.CALENDAR<br>Condition:CAL_DAY = IN_PERMSN_DT<br>Output Column: DAY_KEY	|		|		|	HDM_AWD_USER_QUEUE	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PERMSN_DT	|	date/time	|	29	|		|		|		|		|		|		|		|		|	HDM_AWD_USER_QUEUE	|	UT_CD	|	varchar2	|	25	|		|		|
|		|	EXIST_FLG	|	string	|	1	|		|		|		|	EXIST_FLG	|		|		|		|		|	HDM_AWD_USER_QUEUE	|	EXIST_FLG	|	varchar2	|	1	|		|		|
|		|	VIEW_FLG	|	string	|	1	|		|		|		|	VIEW_FLG	|		|		|		|		|	HDM_AWD_USER_QUEUE	|	VIEW_FLG	|	varchar2	|	1	|		|		|
|		|	UPDT_FLG	|	string	|	1	|		|		|		|	UPDT_FLG	|		|		|		|		|	HDM_AWD_USER_QUEUE	|	UPDT_FLG	|	varchar2	|	1	|		|		|
|		|	WK_SLCTN_FLG	|	string	|	1	|		|		|		|	WK_SLCTN_FLG	|		|		|		|		|	HDM_AWD_USER_QUEUE	|	WK_SLCTN_FLG	|	varchar2	|	1	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|	Y'	|		|		|		|	HDM_AWD_USER_QUEUE	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|	SYSDATE	|		|		|		|	HDM_AWD_USER_QUEUE	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|	ROW_STOP_DTTM	|		|		|		|		|	HDM_AWD_USER_QUEUE	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|	$$etlcyckey	|		|		|		|	HDM_AWD_USER_QUEUE	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|	HDM_AWD_USER_QUEUE	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	Q_CD	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	USR_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
