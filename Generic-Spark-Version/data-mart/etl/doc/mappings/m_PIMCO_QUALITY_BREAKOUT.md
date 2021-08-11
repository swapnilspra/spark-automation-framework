|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update the record if already present, otherwise insert the record"	|		|		|		|		|		|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	ASOF_DATE	|	string	|	25	|		|		|		|	SEQ Number	|		|	<br>Table Name : HDM.PIMCO_QUALITY_BREAKOUT<br>Condition: PMC_QUAL_BUCKT_KEY = PMC_QUAL_BUCKT_KEY_in AND FUND_COMPST_KEY = FUND_COMPST_KEY_in AND DAY_KEY = DAY_KEY_in<br>Output Column: QUAL_BRK_OUT_KEY	|		|		|		|	QUAL_BRK_OUT_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	ACCT_NO	|	string	|	25	|		|		|		|		|		|	<br>//:LKP.LKP_PMC_QUAL_BUCKT_KEY(LTRIM(RTRIM(BUCKET_in)))<br>Table Name: HDM.PIMCO_QUALITY_BUCKET<br>Condition: QUAL_BUCKT_NM = QUAL_BUCKT_NM_in<br>Output Column: PMC_QUAL_BUCKT_KEY<br>	|		|		|		|	PMC_QUAL_BUCKT_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	BUCKET	|	string	|	25	|		|		|		|		|		|	//:LKP.LKP_FND_COMPST_KEY(LTRIM(RTRIM(ACCT_NO_in)))<br><br>Table Name: <br>Condition:<br>Output Column:	|		|		|		|	FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	ACCT_EXP_AMT	|	string	|	25	|		|		|		|		|		|	"//:LKP.LKP_DAY_KEY(TO_DATE(ASOF_DATE_in,'mm/dd/yyyy'))<br><br><br>Table Name: HDM.PIMCO_ACCOUNT_REFERENCE<br>Condition: PMC_ACCT_NBR = PMC_ACCT_NBR_IN<br>Output Column: FUND_COMPST_KEY"	|		|		|		|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	ACCT_EXP_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_EXP_AMT_in)	|		|		|		|		|	ACCT_EXPSR_AMT	|	number	|	15	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	ACCT_DUR_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_EXP_PCT_in)/100	|		|		|		|		|	ACCT_EXPSR_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	ACCT_DWE	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_DUR_PCT_in)/100	|		|		|		|		|	ACCT_DUR_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	BM_EXP_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_DWE_in)	|		|		|		|		|	ACCT_DWE	|	number	|	15	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	BM_DUR_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_EXP_PCT_in)/100	|		|		|		|		|	BMK_EXPSR_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	BM_DUR_YRS	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_DUR_PCT_in)/100	|		|		|		|		|	BMK_DUR_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	VAR_EXP_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_DUR_YRS_in)	|		|		|		|		|	BMK_DWE	|	number	|	15	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	VAR_DUR_PCT	|	string	|	25	|		|		|		|		|	TO_DECIMAL(VAR_EXP_PCT_in)/100	|		|		|		|		|	VAR_EXPSR_PCT	|	number	|	15	|		|		|
|	STG_PIMCO_QUALITY_BREAKOUT	|	VAR_DWE	|	string	|	25	|		|		|		|		|	TO_DECIMAL(VAR_DUR_PCT_in)/100	|		|		|		|		|	VAR_DUR_PCT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	TO_DECIMAL(VAR_DWE_in)	|		|		|		|		|	VAR_DWE	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|		|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
