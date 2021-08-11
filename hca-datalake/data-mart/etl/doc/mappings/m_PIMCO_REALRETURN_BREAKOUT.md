|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	Update record if already present	|		|		|		|		|		|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	ASOF_DATE	|	string	|	25	|		|		|		|		|		|	Table Name: HDM.PIMCO_SECTOR_BREAKOUT<br>Condition: PMC_SCTR_KEY = PMC_SCTR_KEY_in AND FUND_COMPST_KEY = FUND_COMPST_KEY_in AND DAY_KEY = DAY_KEY_in<br>Output Column: SCTR_BRK_OUT_KEY	|		|		|	PIMCO_SECTOR_BREAKOUT	|	SCTR_BRK_OUT_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	ACCT_NO	|	string	|	25	|		|		|		|		|		|	":LKP.LKP_SECTOR_KEY(<br>LTRIM(RTRIM(SECTOR_TYPE_CODE_in)),<br>LTRIM(RTRIM(LEVEL_1_GROUP_DESC_in)),<br>LTRIM(RTRIM(LEVEL_2_GROUP_DESC_in))<br>)<br>Table Name: HDM.PIMCO_SECTOR<br>Condition: SCTR_TYPE = SCTR_TYPE_in AND SCTR_LVL_DESC = SCTR_LVL_DESC_in AND SCTR_SUB_LVL_DESC = SCTR_SUB_LVL_DESC_in<br>Output COlumn: PMC_SCTR_KEY"	|		|		|	PIMCO_SECTOR_BREAKOUT	|	PMC_SCTR_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	SECTOR_TYPE_CODE	|	string	|	25	|		|		|		|		|		|	:LKP.LKP_FND_COMPST_KEY(LTRIM(RTRIM(ACCT_NO_in))).<br><br>Table Name: HDM.PIMCO_ACCOUNT_REFERENCE<br>Condition: PMC_ACCT_NBR = PMC_ACCT_NBR_IN<br>Output Column: FUND_COMPST_KEY	|		|		|	PIMCO_SECTOR_BREAKOUT	|	FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	LEVEL_1_GROUP_DESC	|	string	|	255	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(ASOF_DATE_v)<br><br>Table name : HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_in<br>Output Column: DAY_KEY	|		|		|	PIMCO_SECTOR_BREAKOUT	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	LEVEL_2_GROUP_DESC	|	string	|	255	|		|		|		|		|	TO_DECIMAL(ACCT_MV_AMT_in)	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	ACCT_MKT_VAL	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	ACCT_MV_AMT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_MV_PCT_in)/100	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	ACCT_MKT_VAL_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	ACCT_MV_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_DUR_PCT_in)/100	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	ACCT_DUR_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	ACCT_DUR_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_DWE_in)	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	ACCT_DWE	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	ACCT_DWE	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_MV_PCT_in)/100	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	BMK_MKT_VAL_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	BM_MV_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_DUR_PCT_in)/100	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	BMK_DUR_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	BM_DUR_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_DWE_in)	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	BMK_DWE	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	BM_DWE	|	string	|	25	|		|		|		|		|	TO_DECIMAL(VAR_MV_PCT_in)/100	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	VAR_MKT_VAL_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	VAR_MV_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(VAR_DUR_PCT_in)/100	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	VAR_DUR_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	VAR_DUR_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(VAR_DWE_in)	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	VAR_DWE	|	number	|	15	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|	VAR_DWE	|	string	|	25	|		|		|		|		|	Y'	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	STG_PIMCO_REALRETURN_BREAKOUT	|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_SECTOR_BREAKOUT	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
