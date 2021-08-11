|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	refer SQ	|	SPR_SHEET_DT	|	date/time	|	29	|		|		|		|	Seq Number	|		|		|	Insert if new record<br><br>Update the record if already exists<br><br>	|	Table Name: HDM.SDCM_CASHFLOW<br>Condition:REG_KEY = in_REG_KEY AND DLR_KEY = in_DLR_KEY AND SHRHLDR_KEY = in_SHRHLDR_KEY AND ACCT_KEY = in_ACCT_KEY AND FUND_KEY = in_FUND_KEY AND SPR_SHEET_DT_KEY = in_SPR_SHEET_DT_KEY AND TRDE_DT_KEY = in_TRDE_DT_KEY	|	CASHFLOW_ACTIVITY	|	CASHFLOW_ACT_HIST_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	TRDE_DT	|	date/time	|	29	|		|		|		|		|		|	Table Name: HDM.FUND<br>Condition: FUND_NBR = IN_FUND_NBR <br>Output Column: FUND_KEY	|		|		|	CASHFLOW_ACTIVITY	|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	CONFIRM_DT	|	date/time	|	29	|		|		|		|		|		|	//:LKP.LKP_DM_DIM_CALENDAR(SPR_SHEET_DT)<br><br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	CASHFLOW_ACTIVITY	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SHRS	|	double	|	15	|		|		|		|		|	SUM(SUB_SHARES) for group by DAY_KEY and FUND_KEY	|		|		|		|	CASHFLOW_ACTIVITY	|	SUB_SHRS	|	number	|	15	|		|		|
|		|	OFRG_PR	|	double	|	15	|		|		|		|		|	SUM(SUB_AMT) for group by DAY_KEY and FUND_KEY	|		|		|		|	CASHFLOW_ACTIVITY	|	SUB_AMT	|	number	|	15	|		|		|
|		|	GR_AMT	|	double	|	15	|		|		|		|		|	SUM(REDMPN_SHRS) for group by DAY_KEY and FUND_KEY	|		|		|		|	CASHFLOW_ACTIVITY	|	REDMPN_SHRS	|	number	|	15	|		|		|
|		|	DLR_COMMSN_AMT1	|	double	|	15	|		|		|		|		|	SUM(REDMPN_AMT) for group by DAY_KEY and FUND_KEY	|		|		|		|	CASHFLOW_ACTIVITY	|	REDMPN_AMT	|	number	|	15	|		|		|
|		|	UNDR_WRTR_COMMSN_AMT	|	double	|	15	|		|		|		|		|	SUM(SUB_SHARES) -SUM(ABS(REDMPN_SHRS)) for group by DAY_KEY and FUND_KEY	|		|		|		|	CASHFLOW_ACTIVITY	|	NET_SHRS	|	number	|	15	|		|		|
|		|	NAV_AMT	|	double	|	15	|		|		|		|		|	SUM(SUB_AMT) + (SUM(REDMPN_AMT)-SUM(DLR_COMMSN_AMT+UNDR_WRTR_COMMSN_AMT+ADV_COMMSN_AMT)) for group by DAY_KEY and FUND_KEY	|		|		|		|	CASHFLOW_ACTIVITY	|	NET_CASHFLOW_AMT	|	number	|	15	|		|		|
|		|	TXN_TYPE_CD	|	decimal	|	1	|		|		|		|		|	Y'	|		|		|		|	CASHFLOW_ACTIVITY	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	CASH_CNTL_RECON_FLG	|	string	|	1	|		|		|		|		|	SYSDATE	|		|		|		|	CASHFLOW_ACTIVITY	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	ADV_COMMSN_AMT	|	double	|	15	|		|		|		|		|		|		|		|		|	CASHFLOW_ACTIVITY	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|	$$etlcyckey	|		|		|		|	CASHFLOW_ACTIVITY	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ACCT_NBR	|	decimal	|	20	|		|		|		|		|	4	|		|		|		|	CASHFLOW_ACTIVITY	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	DLR_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SHRHLDR_ROLE_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|		|		|	ETL_LOAD_ERROR	|	ERR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	CUST_ID	|	decimal	|	11	|		|		|		|		|	$$etlcyckey	|		|		|		|	ETL_LOAD_ERROR	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	CUST_ID_2	|	decimal	|	11	|		|		|		|		|	"IIF(<br>      (NOT ISNULL(v_FUND_KEY_ERR_MSG)),<br>                                                         v_FUND_KEY_ERR_MSG&vert;&vert;'FUND NUMBER:'&vert;&vert;TO_CHAR(FUND_NBR)<br>&vert;&vert;' AND SUPER SHEET DATE:'&vert;&vert;TO_CHAR(SPR_SHEET_DT,'MM/DD/YYYY')<br>,NULL)<br>"	|		|		|		|	ETL_LOAD_ERROR	|	ERR_MSG	|	varchar2	|	4000	|		|		|
|		|	ST_CD	|	string	|	3	|		|		|		|		|	SYSDATE	|		|		|		|	ETL_LOAD_ERROR	|	RUN_DT_TS	|	date	|	19	|		|		|
|		|	REG_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	CRTY_ISO_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	PMT_MTHD_CD	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
