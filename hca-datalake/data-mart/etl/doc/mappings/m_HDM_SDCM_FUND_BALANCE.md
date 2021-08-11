|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	SDCM_FUND_BAL_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	Insert Only	|		|	HDM_SDCM_FUND_BALANCE	|	FUND_BAL_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	HDM_SDCM_FUND_BALANCE	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SHR_CLS_FUND_KEY	|	decimal	|	10	|		|		|		|		|		|	Table Name: HDM.FUND<br>Condition: FUND_NBR = IN_FUND_NBR<br>Output Column: FUND_KEY	|		|		|	HDM_SDCM_FUND_BALANCE	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SHR_BAL_DT	|	date/time	|	29	|		|		|		|	OTST_SHRS	|		|		|		|		|	HDM_SDCM_FUND_BALANCE	|	TOT_FUND_SHRS	|	number	|	15	|		|		|
|		|	SPR_SHEET_DT	|	date/time	|	29	|		|		|		|	SHR_PR	|		|		|		|		|	HDM_SDCM_FUND_BALANCE	|	TOT_FUND_ASSET	|	number	|	15	|		|		|
|		|	SUB_AMT	|	double	|	15	|		|		|		|		|		|	Y'	|		|		|	HDM_SDCM_FUND_BALANCE	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	REDMPN_AMT	|	double	|	15	|		|		|		|		|		|	SYSDATE	|		|		|	HDM_SDCM_FUND_BALANCE	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	OTST_SHRS	|	double	|	15	|		|		|		|		|		|	NULL	|		|		|	HDM_SDCM_FUND_BALANCE	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	DIVD_AMT	|	double	|	15	|		|		|		|		|		|	$$etlcyckey	|		|		|	HDM_SDCM_FUND_BALANCE	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	REC_SHR_AMT	|	double	|	15	|		|		|		|		|		|	4	|		|		|	HDM_SDCM_FUND_BALANCE	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	DAILY_ACCRL_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DAILY_ACCRL_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DIVD_CLOSEOUT_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DIVD_CLOSEOUT_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DAILY_ADJ_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DAILY_ADJ_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	CERTFC_SHR_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	NON_CERTFC_SHR_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	FAIL_FILE_SALE_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	FREE_FILE_SALE_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	REDMPN_FAIL_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DIVD_FLG	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DIVD_ON__OFF_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	NSCC_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	NSCC_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SHR_PR	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SHR_CLS_FUND_KEY1	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
