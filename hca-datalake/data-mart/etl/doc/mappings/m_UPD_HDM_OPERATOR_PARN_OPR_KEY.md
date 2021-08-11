|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	Update only	|		|		|		|		|		|		|		|
|	Refer SQ	|	OPERATOR_ID	|	string	|	20	|		|		|		|		|		|		|	Table Name: HDM.OPERATOR <br>Condition: OPR_ID = OPERATOR_ID AND DAY_KEY = IN_DAY_KEY<br>Ouput Column: OPR_KEY	|		|		|	OPR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FIRST_LOGON_DTE	|	date/time	|	29	|		|		|		|		|		|		|	Table  Name: HDM.CALENDAR <br>Condition: CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	IWS_ACCESS_CDE	|	string	|	1	|		|		|		|		|		|		|		|		|		|	OPR_ID	|	varchar2	|	25	|		|		|
|		|	LAST_LOGON_DTE	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	FIRST_LOGON_DT	|	date	|	19	|		|		|
|		|	DBO_OPERSECU_LASTMAINT	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	IWS_ACCESS_CD	|	varchar2	|	1	|		|		|
|		|	LASTMAINT_ID	|	string	|	20	|		|		|		|		|		|		|		|		|		|	LAST_LOGON_DT	|	date	|	19	|		|		|
|		|	MGMT_CODE	|	string	|	3	|		|		|		|		|		|		|		|		|		|	OPR_SECR_LAST_MAINT	|	date	|	19	|		|		|
|		|	NBR_OF_LOGONS	|	double	|	15	|		|		|		|		|		|		|		|		|		|	LAST_MAINT_OPR	|	varchar2	|	25	|		|		|
|		|	OPER_EXPIRED_CDE	|	string	|	1	|		|		|		|		|		|		|		|		|		|	LOGON_CNT	|	number	|	15	|		|		|
|		|	OPER_NAME	|	string	|	50	|		|		|		|		|		|		|		|		|		|	OPR_EXPN_FLG	|	varchar2	|	1	|		|		|
|		|	OPR_EXPT_STR_DT	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	OPR_NM	|	varchar2	|	50	|		|		|
|		|	OPR_LAS_PSW_RS_DT	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	EXPCTD_STRT_DT	|	date	|	19	|		|		|
|		|	PASSWORD_CHG_DTE	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	LAST_PSWD_RST_DT	|	date	|	19	|		|		|
|		|	PASSWORD_TRIES_CNT	|	double	|	15	|		|		|		|		|		|		|		|		|		|	PSWD_CHG_DT	|	date	|	19	|		|		|
|		|	TA3270_ACCE_CDE	|	string	|	1	|		|		|		|		|		|		|		|		|		|	PSWD_TRY_CNT	|	number	|	15	|		|		|
|		|	FACILITY_ID	|	string	|	20	|		|		|		|		|		|		|		|		|		|	TA3270_ACCESS_FLG	|	varchar2	|	1	|		|		|
|		|	SUB_FACILITY_NAME	|	string	|	100	|		|		|		|		|		|		|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	FACILITY_FUNC_ID	|	double	|	15	|		|		|		|		|		|		|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	AUTHOR_CDE	|	string	|	1	|		|		|		|		|		|		|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	DBO_OPERFUNC_LASTMAINT	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DBO_OPERFUNC_LASTMAINT_TIME	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	FUNCTION_ADD_CODE	|	string	|	1	|		|		|		|		|		|		|	Table Name: HDM.OPERATOR <br>Condition: OPR_ID = COPY_FROM_OPR_ID AND DAY_KEY = IN_DAY_KEY <br>Ouput Column: OPR_KEY	|		|		|	PARN_OPR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FUNCTION_BROWSE_CODE	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	FUNCTION_DELETE_CODE	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	FUNCTION_UPDATE_CODE	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	MINOR_ADD_CODE	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	MINOR_BROWSE_CODE	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	MINOR_DELETE_CODE	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	MINOR_UPDATE_CODE	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	PROCESS_DATE	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	COPY_FROM_OPR_ID	|	string	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
