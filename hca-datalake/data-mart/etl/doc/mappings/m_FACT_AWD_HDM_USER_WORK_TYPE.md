|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	refer SQ	|	USR_WK_TYPE_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	Insert if new record<br><br>Update the record if already exists<br><br>	|	Table Name: HDM.AWD_USER_WORK_TYPE<br>Condition:USR_KEY = IN_USR_KEY AND WK_TYPE_KEY = IN_WK_TYPE_KEY AND DAY_KEY = IN_DAY_KEY AND REC_TYPE_KEY = IN_REC_TYPE_KEY	|	HDM_AWD_USER_WORK_TYPE	|	USR_WK_TYPE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	Table Name: HDM.AWD_RECORD_TYPE<br>Condition:  REC_CD = IN_REC_CD<br>Output Column: REC_TYPE_KEY	|		|		|	HDM_AWD_USER_WORK_TYPE	|	REC_TYPE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	REC_TYPE_KEY	|	decimal	|	10	|		|		|		|		|		|	Table Name: HDM.AWD_WORK_TYPE<br>Condition:  WK_TYPE_NM = IN_WK_TYPE_NM<br>Output Column: WK_TYPE_KEY	|		|		|	HDM_AWD_USER_WORK_TYPE	|	WK_TYPE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	WK_TYPE_KEY	|	decimal	|	10	|		|		|		|		|		|	Table Name: HDM.AWD_USER<br>Condition:  USR_ID = IN_USR_ID<br>Output Column: USR_KEY	|		|		|	HDM_AWD_USER_WORK_TYPE	|	USR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	USR_KEY	|	decimal	|	10	|		|		|		|		|		|	Table Name: HDM.CALENDAR<br>Condition:  CAL_DAY = IN_PERMSN_DT<br>Output Column: DAY_KEY	|		|		|	HDM_AWD_USER_WORK_TYPE	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	UT_CD	|	string	|	25	|		|		|		|	UT_CD	|		|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	UT_CD	|	varchar2	|	25	|		|	Y	|
|		|	PERMSN_DT	|	date/time	|	29	|		|		|		|	EXIST_FLG	|		|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	EXIST_FLG	|	varchar2	|	1	|		|		|
|		|	EXIST_FLG	|	string	|	1	|		|		|		|	VIEW_FLG	|		|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	VIEW_FLG	|	varchar2	|	1	|		|		|
|		|	VIEW_FLG	|	string	|	1	|		|		|		|	UPDT_FLG	|		|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	UPDT_FLG	|	varchar2	|	1	|		|		|
|		|	UPDT_FLG	|	string	|	1	|		|		|		|	WK_SLCTN_FLG	|		|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	WK_SLCTN_FLG	|	varchar2	|	1	|		|		|
|		|	WK_SLCTN_FLG	|	string	|	1	|		|		|		|	LAST_MAINT_DT	|		|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	LAST_MAINT_DT	|	date	|	19	|		|		|
|		|	LAST_MAINT_DT	|	date/time	|	29	|		|		|		|		|	Y'	|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|	SYSDATE	|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|	$$etlcyckey	|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|		|	HDM_AWD_USER_WORK_TYPE	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	USR_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	WK_TYPE_NM	|	string	|	60	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	REC_CD	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
