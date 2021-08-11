|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|	// For new record<br>Use sequence generator to generate the auto increment number	|		|	"Update , if record (USR_ID)already present"	|	HDM_AWD_USER	|	USR_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|	refer SQ	|	USR_KEY	|	decimal	|	10	|		|		|		|	GRP_NM' from Source	|	GRP_NM	|		|		|	HDM_AWD_USER	|	GRP_NM	|	varchar2	|	255	|		|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|	USR_STAT_CD'  from Source	|	USR_STAT_CD	|		|		|	HDM_AWD_USER	|	USR_STAT_CD	|	varchar2	|	25	|		|		|		|
|		|	GRP_KEY	|	decimal	|	10	|		|		|		|	USR_STAT_DESC'  from Source	|	USR_STAT_DESC	|		|		|	HDM_AWD_USER	|	USR_STAT_DESC	|	varchar2	|	50	|		|		|		|
|		|	USR_STAT_KEY	|	decimal	|	10	|		|		|		|	USR_ID'  from Source	|	USR_ID	|		|		|	HDM_AWD_USER	|	USR_ID	|	varchar2	|	25	|		|		|		|
|		|	USR_ID	|	string	|	25	|		|		|		|	FIRST_NM'  from Source	|	FIRST_NM	|		|		|	HDM_AWD_USER	|	FIRST_NM	|	varchar2	|	60	|		|		|		|
|		|	FIRST_NM	|	string	|	60	|		|		|		|	MINT'  from Source	|	MINT	|		|		|	HDM_AWD_USER	|	MINT	|	varchar2	|	50	|		|		|		|
|		|	MINT	|	string	|	60	|		|		|		|	LAST_NM'  from Source	|	LAST_NM	|		|		|	HDM_AWD_USER	|	LAST_NM	|	varchar2	|	60	|		|		|		|
|		|	LAST_NM	|	string	|	60	|		|		|		|	TAX_ID' from Source	|	TAX_ID	|		|		|	HDM_AWD_USER	|	TAX_ID	|	number	|	15	|		|		|		|
|		|	TAX_ID	|	decimal	|	11	|		|		|		|	CLSFCN' from Source	|	CLSFCN	|		|		|	HDM_AWD_USER	|	CLSFCN	|	varchar2	|	25	|		|		|		|
|		|	CLSFCN	|	string	|	25	|		|		|		|	LCK_STAT' from Source	|	LCK_STAT	|		|		|	HDM_AWD_USER	|	LCK_STAT	|	varchar2	|	1	|		|		|		|
|		|	LCK_STAT	|	string	|	1	|		|		|		|	Q_FLG' from Source	|	Q_FLG	|		|		|	HDM_AWD_USER	|	Q_FLG	|	varchar2	|	1	|		|		|		|
|		|	Q_FLG	|	string	|	1	|		|		|		|	PHON_NBR' from Source	|	PHON_NBR	|		|		|	HDM_AWD_USER	|	PHON_NBR	|	varchar2	|	25	|		|		|		|
|		|	PHON_NBR	|	string	|	25	|		|		|		|	FAX_NBR' from Source	|	FAX_NBR	|		|		|	HDM_AWD_USER	|	FAX_NBR	|	varchar2	|	25	|		|		|		|
|		|	FAX_NBR	|	string	|	25	|		|		|		|	WK_SLCTN_CD' from Source	|	WK_SLCTN_CD	|		|		|	HDM_AWD_USER	|	WK_SLCTN_CD	|	number	|	15	|		|		|		|
|		|	WK_SLCTN_CD	|	decimal	|	11	|		|		|		|	FAX_DEPT' from Source	|	FAX_DEPT	|		|		|	HDM_AWD_USER	|	FAX_DEPT	|	varchar2	|	60	|		|		|		|
|		|	FAX_DEPT	|	string	|	60	|		|		|		|	ROUT_USR_ID' from Source	|	ROUT_USR_ID	|		|		|	HDM_AWD_USER	|	ROUT_USR_ID	|	varchar2	|	25	|		|		|		|
|		|	ROUT_USR_ID	|	string	|	25	|		|		|		|	SEC_LVL_CD' from Source	|	SEC_LVL_CD	|		|		|	HDM_AWD_USER	|	SEC_LVL_CD	|	number	|	15	|		|		|		|
|		|	SEC_LVL_CD	|	decimal	|	11	|		|		|		|	UT_CD' from Source	|	UT_CD	|		|		|	HDM_AWD_USER	|	UT_CD	|	varchar2	|	25	|		|		|		|
|		|	UT_CD	|	string	|	25	|		|		|		|	LAST_MAINT_DT'  from Source	|	LAST_MAINT_DT	|		|		|	HDM_AWD_USER	|	LAST_MAINT_DT	|	date	|	19	|		|		|		|
|		|	LAST_MAINT_DT	|	date/time	|	29	|		|		|		|	LAST_DT'  from Source	|	LAST_DT	|		|		|	HDM_AWD_USER	|	LAST_DT	|	date	|	19	|		|		|		|
|		|	LAST_DT	|	date/time	|	29	|		|		|		|	LAST_PSWD_DT' from Source	|	LAST_PSWD_DT	|		|		|	HDM_AWD_USER	|	LAST_PSWD_MAINT_DT	|	date	|	19	|		|		|		|
|		|	LAST_PSWD_DT	|	date/time	|	29	|		|		|		|	AWD_DTP_VRSN'  from Source	|	AWD_DTP_VRSN	|		|		|	HDM_AWD_USER	|	AWD_DESKTOP_VRSN	|	varchar2	|	25	|		|		|		|
|		|	AWD_DTP_VRSN	|	string	|	25	|		|		|		|	DATA_REVS'  from Source	|	DATA_REVS	|		|		|	HDM_AWD_USER	|	DATA_REVS	|	number	|	15	|		|		|		|
|		|	DATA_REVS	|	decimal	|	11	|		|		|		|	Default to 'Y'	|	Y'	|		|		|	HDM_AWD_USER	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	Current date	|	SYSDATE	|		|		|	HDM_AWD_USER	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|	ROW_STOP_DTTM	|	ROW_STOP_DTTM	|		|		|	HDM_AWD_USER	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|	HDM_AWD_USER	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|	HDM_AWD_USER	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	USR_STAT_CD	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	USR_STAT_DESC	|	string	|	255	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	GRP_NM	|	string	|	255	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
