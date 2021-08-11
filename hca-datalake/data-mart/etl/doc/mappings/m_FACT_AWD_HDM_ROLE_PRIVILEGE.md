|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	refer SQ	|	ROLE_PRVL_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	Insert if new record<br><br>Update the record if already exists<br><br>	|	Table Name: HDM.AWD_ROLE_PRIVILEGE<br>Condition:ROLE_KEY = IN_ROLE_KEY AND PRVL_TYPE_KEY = IN_PRVL_TYPE_KEY AND REC_TYPE_KEY = IN_REC_TYPE_KEY AND DAY_KEY = IN_DAY_KEY	|	HDM_AWD_ROLE_PRIVILEGE	|	ROLE_PRVL_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	REC_TYPE_KEY	|	decimal	|	10	|		|		|		|		|		|	Table Name: HDM.AWD_RECORD_TYPE<br>Condition: REC_CD = IN_REC_CD<br>Output Column: REC_TYPE_KEY	|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	REC_TYPE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ROLE_KEY	|	decimal	|	10	|		|		|		|		|		|	Table Name: HDM.AWD_ROLE<br>Condition: ROLE_NM = IN_ROLE_NM<br>Output Column: ROLE_KEY	|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	ROLE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	Table Name: HDM.AWD_PRIVILEGE_TYPE<br>Condition:PRVL_TYPE = IN_PRVL_TYPE<br>Output Column: PRVL_TYPE_KEY	|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	PRVL_TYPE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	PRVL_TYPE_KEY	|	decimal	|	10	|		|		|		|		|		|	Table Name: HDM.CALENDAR<br>Condition:CAL_DAY = IN_PERMSN_DT<br>Output Column: DAY_KEY	|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	UT_CD	|	string	|	25	|		|		|		|	UT_CD	|		|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	UT_CD	|	varchar2	|	25	|		|		|
|		|	PERMSN_DT	|	date/time	|	29	|		|		|		|	EXIST_FLG	|		|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	EXIST_FLG	|	varchar2	|	1	|		|		|
|		|	EXIST_FLG	|	string	|	1	|		|		|		|	VIEW_FLG	|		|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	VIEW_FLG	|	varchar2	|	1	|		|		|
|		|	VIEW_FLG	|	string	|	1	|		|		|		|	UPDT_FLG	|		|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	UPDT_FLG	|	varchar2	|	1	|		|		|
|		|	UPDT_FLG	|	string	|	1	|		|		|		|	WK_SLCTN_FLG	|		|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	WK_SLCTN_FLG	|	varchar2	|	1	|		|		|
|		|	WK_SLCTN_FLG	|	string	|	1	|		|		|		|	LAST_MAINT_DT	|		|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	LAST_MAINT_DT	|	date	|	19	|		|		|
|		|	LAST_MAINT_DT	|	date/time	|	29	|		|		|		|		|	Y'	|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|	SYSDATE	|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|	$$etlcyckey	|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|		|	HDM_AWD_ROLE_PRIVILEGE	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	PRVL_TYPE	|	string	|	60	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	REC_CD	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ROLE_NM	|	string	|	60	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
