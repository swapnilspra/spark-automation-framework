|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	STG_PIMCO_DERIV_EXPOSURE	|	ASOF_DATE	|	nstring	|	256	|		|		|		|	Seq Number	|		|		|	table Name : HDM.PIMCO_DERIVATIVES<br>Condition: DERVTV_BUCKT_KEY = DERVTV_BUCKT_KEY_in AND DAY_KEY = DAY_KEY_in AND FUND_COMPST_KEY = FUND_COMPST_KEY_in<br>output Column: DERVTV_KEY	|	"If record Already present, update the record"	|		|	DERVTV_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_DERIV_EXPOSURE	|	ACCT_NO	|	nstring	|	25	|		|		|		|		|		|	":LKP.LKP_DERVTV_BUCKT_KEY(UPPER(LTRIM(RTRIM(BUCKET_in))))<br><br>Override Quer: SELECT <br>HDM.PIMCO_DERIVATIVE_BUCKET.DERVTV_BUCKT_KEY as DERVTV_BUCKT_KEY, UPPER(HDM.PIMCO_DERIVATIVE_BUCKET.DERVTV_BUCKT_NM) as DERVTV_BUCKT_NM <br>FROM <br>HDM.PIMCO_DERIVATIVE_BUCKET<br><br>Condition: DERVTV_BUCKT_NM = DERVTV_BUCKT_NM_in<br>Output Column: DERVTV_BUCKT_KEY"	|		|		|		|	DERVTV_BUCKT_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_DERIV_EXPOSURE	|	BUCKET	|	nstring	|	256	|		|		|		|		|		|	":LKP.LKP_DAY_KEY(TO_DATE(LTRIM(RTRIM(ASOF_DATE_in)),'MM/DD/YYYY'))<br><br>Table Name : HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_in<br>Output Column: DAY_KEY<br>"	|		|		|		|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_DERIV_EXPOSURE	|	NOTIONAL	|	nstring	|	25	|		|		|		|		|		|	:LKP.LKP_FND_COMPST_KEY(TO_INTEGER(LTRIM(RTRIM(ACCT_NO_in))))<br><br>Table Name: HDM.PIMCO_ACCOUNT_REFERENCE<br>Condition: PMC_ACCT_NBR = PMC_ACCT_NBR_IN<br>Output Column: FUND_COMPST_KEY	|		|		|		|	FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_DERIV_EXPOSURE	|	MV_AMT	|	nstring	|	25	|		|		|		|		|	TO_DECIMAL(NOTIONAL_in)	|		|		|		|		|	NTIONAL_AMT	|	number	|	15	|		|		|
|	STG_PIMCO_DERIV_EXPOSURE	|	MV	|	nstring	|	25	|		|		|		|		|	TO_DECIMAL(MV_AMT_in)	|		|		|		|		|	MKT_VAL	|	number	|	15	|		|		|
|	STG_PIMCO_DERIV_EXPOSURE	|	DUR	|	nstring	|	25	|		|		|		|		|	TO_DECIMAL(MV_in)/100	|		|		|		|		|	MKT_VAL_PCT	|	number	|	15	|		|		|
|		|	DWE	|	nstring	|	25	|		|		|		|		|	TO_DECIMAL(DUR_in)/100	|		|		|		|		|	DUR_PCT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	TO_DECIMAL(DWE_in)	|		|		|		|		|	DWE	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
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
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
