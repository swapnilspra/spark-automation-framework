|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	PEND_TRDE_KEY	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.PENDING_TRADE<br>Condition: TXN_REF_ID = TXN_REF_ID_in<br>output Column: PEND_TRDE_KEY	|	Update if record already present	|		|	HDM_PENDING_TRADE	|	PEND_TRDE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|	"IIF(<br>ISNULL(:LKP.LKP_DLR_KEY(TO_CHAR(DLR_ID))),-1,<br>(:LKP.LKP_DLR_KEY(TO_CHAR(DLR_ID)))<br>)"	|	//LKP.LKP_DLR_KEY<br><br>Table Name: HDM.DEALER<br>Condition: DLR_ID = DLR_ID_IN<br>Return Column:DLR_KEY	|		|		|	HDM_PENDING_TRADE	|	DLR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DLR_ID	|	decimal	|	10	|		|		|		|		|	"IIF(<br>ISNULL(:LKP.LKP_FUND_KEY(FUND_NBR)),-1,<br>(:LKP.LKP_FUND_KEY(FUND_NBR))<br>)"	|	Table Name: HDM.FUND<br>Condition: FUND_NBR = FUND_NBR_in<br>Output Column: FUND_KEY	|		|		|	HDM_PENDING_TRADE	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ACCT_NBR	|	decimal	|	10	|		|		|		|		|	"IIF(<br>ISNULL(:LKP.LKP_ACCOUNT_KEY(ACCT_NBR,FUND_NBR)),-1,<br>(:LKP.LKP_ACCOUNT_KEY(ACCT_NBR,FUND_NBR))<br>)"	|	"Override Query: SELECT ACCOUNT.ACCT_KEY AS ACCT_KEY,<br>         ACCOUNT.ACCT_NBR AS ACCT_NBR,<br>         ACCOUNT.FUND_NBR AS FUND_NBR<br>    FROM HDM.ACCOUNT ACCOUNT,<br>         (SELECT TO_NUMBER (STG_DTO_HCA_SPOT_TXN.TA_ACCOUNT_NUM) AS ACCT_NBR,<br>                 TO_NUMBER (STG_DTO_HCA_SPOT_TXN.PORT_ID) AS FUND_NBR<br>            FROM STG.STG_DTO_HCA_SPOT_TXN) STAGING<br>   WHERE     ACCOUNT.ACCT_NBR = STAGING.ACCT_NBR<br>         AND ACCOUNT.FUND_NBR = STAGING.FUND_NBR<br>AND ACCOUNT.CURR_ROW_FLG = 'Y'<br>GROUP BY ACCOUNT.ACCT_KEY, ACCOUNT.ACCT_NBR, ACCOUNT.FUND_NBR<br>ORDER BY ACCOUNT.ACCT_NBR, ACCOUNT.FUND_NBR<br>--<br>Condition: ACCT_NBR = ACCT_NBR_in AND FUND_NBR = FUND_NBR_in<br><br>Return COlumn: ACCT_KEY"	|		|		|	HDM_PENDING_TRADE	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FUND_NBR	|	decimal	|	10	|		|		|		|		|	"IIF(<br>ISNULL(:LKP.LKP_TRANS_TYPE_KEY(FUND_TXN_SFX,FUND_TXN_CD)),-1,<br>(:LKP.LKP_TRANS_TYPE_KEY(FUND_TXN_SFX,FUND_TXN_CD))<br>)"	|	//LKP.LKP_TRANS_TYPE_KEY<br>Table Name: HDM.TRANSACTION_TYPE<br>Condition: TXN_SFX_CD = TXN_SFX_CD_in AND TXN_CD = TXN_CD_in<br>Output Column: TXN_TYPE_KEY<br>	|		|		|	HDM_PENDING_TRADE	|	TXN_TYPE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FUND_TXN_CD	|	decimal	|	10	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(TRDE_DT)<br><br>Table Name : HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_in<br>Return Column: DAY_KEY	|		|		|	HDM_PENDING_TRADE	|	TRDE_DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FUND_TXN_SFX	|	decimal	|	10	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(STTL_DT)<br>Table Name : HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_in<br>Return Column: DAY_KEY	|		|		|	HDM_PENDING_TRADE	|	STTL_DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	TXN_REF_ID	|	string	|	100	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(LOG_DT)<br><br>Table Name : HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_in<br>Return Column: DAY_KEY	|		|		|	HDM_PENDING_TRADE	|	LOG_DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	GRP_TXN_REF	|	string	|	100	|		|		|		|	TXN_REF_ID	|		|		|		|		|	HDM_PENDING_TRADE	|	TXN_REF_ID	|	varchar2	|	100	|		|		|
|		|	TRDE_DT	|	date/time	|	29	|		|		|		|	GRP_TXN_REF	|		|		|		|		|	HDM_PENDING_TRADE	|	GRP_TXN_REF	|	varchar2	|	100	|		|		|
|		|	STTL_DT	|	date/time	|	29	|		|		|		|	ENTRY_TYPE	|		|		|		|		|	HDM_PENDING_TRADE	|	ENTRY_TYPE	|	varchar2	|	60	|		|		|
|		|	LOG_DT	|	date/time	|	29	|		|		|		|	NCA_FLG	|		|		|		|		|	HDM_PENDING_TRADE	|	NCA_FLG	|	varchar2	|	1	|		|		|
|		|	ENTRY_TYPE	|	string	|	60	|		|		|		|	RVRS_FLG	|		|		|		|		|	HDM_PENDING_TRADE	|	RVRS_FLG	|	varchar2	|	1	|		|		|
|		|	NCA_FLG	|	string	|	1	|		|		|		|	TRDE_DELVY_FLG	|		|		|		|		|	HDM_PENDING_TRADE	|	TRDE_DELVY_FLG	|	varchar2	|	1	|		|		|
|		|	RVRS_FLG	|	string	|	1	|		|		|		|	REP_CD	|		|		|		|		|	HDM_PENDING_TRADE	|	REP_CD	|	varchar2	|	60	|		|		|
|		|	TRDE_DELVY_FLG	|	string	|	1	|		|		|		|	AFT_HRS_FLG	|		|		|		|		|	HDM_PENDING_TRADE	|	AFT_HRS_FLG	|	varchar2	|	1	|		|		|
|		|	REP_CD	|	string	|	60	|		|		|		|	NSCC_CONFRM_ID	|		|		|		|		|	HDM_PENDING_TRADE	|	NSCC_CONFRM_ID	|	varchar2	|	100	|		|		|
|		|	AFT_HRS_FLG	|	string	|	1	|		|		|		|	TRDE_SRC	|		|		|		|		|	HDM_PENDING_TRADE	|	TRDE_SRC	|	varchar2	|	60	|		|		|
|		|	NSCC_CONFRM_ID	|	string	|	100	|		|		|		|	DLN_LEVY	|		|		|		|		|	HDM_PENDING_TRADE	|	DLN_LEVY	|	varchar2	|	255	|		|		|
|		|	TRDE_SRC	|	string	|	60	|		|		|		|	PLN_ID	|		|		|		|		|	HDM_PENDING_TRADE	|	PLN_ID	|	varchar2	|	25	|		|		|
|		|	DLN_LEVY	|	string	|	255	|		|		|		|	TXN_PR	|		|		|		|		|	HDM_PENDING_TRADE	|	TXN_PR	|	number	|	15	|		|		|
|		|	PLN_ID	|	string	|	25	|		|		|		|	GR_CASH_VAL	|		|		|		|		|	HDM_PENDING_TRADE	|	GR_CASH_VAL	|	number	|	15	|		|		|
|		|	TXN_PR	|	double	|	15	|		|		|		|	CB_VAL	|		|		|		|		|	HDM_PENDING_TRADE	|	CB_VAL	|	number	|	15	|		|		|
|		|	GR_CASH_VAL	|	double	|	15	|		|		|		|	TXN_TAX_AMT	|		|		|		|		|	HDM_PENDING_TRADE	|	TXN_TAX_AMT	|	number	|	15	|		|		|
|		|	COST_BASIS_VAL	|	double	|	15	|		|		|		|	SHR_VAL	|		|		|		|		|	HDM_PENDING_TRADE	|	SHR_VAL	|	number	|	15	|		|		|
|		|	TXN_TAX_AMT	|	double	|	15	|		|		|		|	NET_CASH_VAL	|		|		|		|		|	HDM_PENDING_TRADE	|	NET_CASH_VAL	|	number	|	15	|		|		|
|		|	SHR_VAL	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|	HDM_PENDING_TRADE	|	LTST_BATCH_FLG	|	varchar2	|	1	|		|		|
|		|	NET_CASH_VAL	|	double	|	15	|		|		|		|		|		|		|		|		|	HDM_PENDING_TRADE	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	FILE_DTTM	|	date/time	|	29	|		|		|		|		|	SYSDATE	|		|		|		|	HDM_PENDING_TRADE	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	HDM_PENDING_TRADE	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|	HDM_PENDING_TRADE	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	HDM_PENDING_TRADE	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	SRC_SYS_USRID	|	string	|	12	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
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
