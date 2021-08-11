|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	ISSUE_COUNT	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	Insert Only	|		|	INTERMEDIARY_ISSUES	|	INTRM_ISS_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ISSUE_DATE	|	date/time	|	29	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(ISSUE_DATE)<br><br>Table Name: HDM.CALENDAR<br>Condition:  CAL_DAY = CAL_DAY_IN<br>Return Column:DAY_KEY	|		|		|	INTERMEDIARY_ISSUES	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	DLR_ROLE_KEY	|	decimal	|	10	|		|		|		|		|		|	:LKP.LKP_HDM_DLR_KEY(DLR_ID)<br><br><br>Table Name: HDM.DEALER<br>Condition: DLR_ID = DLR_ID_IN<br>Return Column:DLR_KEY	|		|		|	INTERMEDIARY_ISSUES	|	DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SHR_PT_KEY	|	decimal	|	10	|		|		|		|		|		|	":LKP.LKP_INTRM_ISS_TYPE_KEY(LTRIM(RTRIM(DLR_ISSUE_TYPE)))<br><br><br>Override Query: SELECT   HDM.INTERMEDIARY_ISSUE_TYPE.INTRM_ISS_TYPE_KEY AS INTRM_ISS_TYPE_KEY<br>        ,LTRIM(RTRIM(HDM.INTERMEDIARY_ISSUE_TYPE.INTRM_ISS_TYPE)) AS INTRM_ISS_TYPE<br>  FROM   HDM.INTERMEDIARY_ISSUE_TYPE<br>Condition: INTRM_ISS_TYPE = INTRM_ISS_TYPE_IN<br>Return Column:INTRM_ISS_TYPE_KEY"	|		|		|	INTERMEDIARY_ISSUES	|	INTRM_ISS_TYPE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ISS_CREATN_DT	|	date/time	|	29	|		|		|		|	ISSUE_COUNT	|		|		|		|		|	INTERMEDIARY_ISSUES	|	ISS_CNT	|	number	|	15	|		|		|
|		|	ISS_DUE_DT	|	date/time	|	29	|		|		|		|		|	Y	|		|		|		|	INTERMEDIARY_ISSUES	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	OVERSIGHT_RPTB_FLG	|	string	|	1	|		|		|		|		|	SYSDATE	|		|		|		|	INTERMEDIARY_ISSUES	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	DLR_ISS_TYPE	|	string	|	255	|		|		|		|		|		|		|		|		|	INTERMEDIARY_ISSUES	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	ISS_STAT	|	string	|	25	|		|		|		|		|		|		|		|		|	INTERMEDIARY_ISSUES	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ISS_PRIO	|	string	|	25	|		|		|		|		|	6	|		|		|		|	INTERMEDIARY_ISSUES	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	ISS_TITL	|	string	|	255	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DEPT	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	PROJ_NM	|	string	|	255	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	RPTG_USR	|	string	|	255	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	BMG_ASSGNT	|	string	|	255	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ASSGND_USR	|	string	|	255	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
