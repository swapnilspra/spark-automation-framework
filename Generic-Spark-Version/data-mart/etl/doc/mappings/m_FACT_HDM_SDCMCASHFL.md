|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	refer SQ	|	SPR_SHEET_DT	|	date/time	|	29	|		|		|		|	Seq Number	|		|		|	Insert if new record<br><br>Update the record if already exists<br><br>	|	Table Name: HDM.SDCM_CASHFLOW <br>Condition:REG_KEY = in_REG_KEY AND DLR_KEY = in_DLR_KEY AND SHRHLDR_KEY = in_SHRHLDR_KEY AND ACCT_KEY = in_ACCT_KEY AND FUND_KEY = in_FUND_KEY AND SPR_SHEET_DT_KEY = in_SPR_SHEET_DT_KEY AND TRDE_DT_KEY = in_TRDE_DT_KEY	|	SDCM_CASHFLOW	|	SDCM_CASHFLOW_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	TRDE_DT	|	date/time	|	29	|		|		|		|		|	"IIF(ISNULL(REG_KEY),300,REG_KEY)"	|	Table Name: HDM.REGION<br>Condition: ST_CD = IN_ST_CD AND MKTG_REG_ID = IN_MKTG_REG_ID AND CRTY_CD = IN_CRTY_CD<br>Output Column: REG_KEY	|		|		|	SDCM_CASHFLOW	|	REG_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	CONFIRM_DT	|	date/time	|	29	|		|		|		|		|		|	Table Name: HDM.PAYMENT_METHOD<br>Condition: PMT_MTHD_CD = in_PMT_MTHD_CD<br>Output Column: PMT_MTHD_KEY	|		|		|	SDCM_CASHFLOW	|	PMT_MTHD_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SHRS	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.REGION<br>Condition: ST_CD = IN_ST_CD AND MKTG_REG_ID = IN_MKTG_REG_ID AND CRTY_CD = IN_CRTY_CD<br>Output Column: DLR_KEY	|		|		|	SDCM_CASHFLOW	|	DLR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	OFRG_PR	|	double	|	15	|		|		|		|		|	"IIF(ISNULL(SHRHLDR_KEY),-1,SHRHLDR_KEY)"	|	<br><br>Table Name: HDM.SHAREHOLDER<br>Condition:CUST_IDNFCN_NBR = IN_CUST_IDNFCN_NBR AND CUST_IDNFCN_NBR_2 = IN_CUST_IDNFCN_NBR_2<br>Output Column:SHRHLDR_KEY	|		|		|	SDCM_CASHFLOW	|	SHRHLDR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	GR_AMT	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.ACCOUNT<br>Condition: ACCT_NBR = IN_ACCT_NBR AND FUND_NBR = IN_FUND_NBR<br>Output Column: ACCT_KEY	|		|		|	SDCM_CASHFLOW	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DLR_COMMSN_AMT1	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.FUND<br>Condition: FUND_NBR = IN_FUND_NBR<br>Output Column: FUND_KEY	|		|		|	SDCM_CASHFLOW	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	UNDR_WRTR_COMMSN_AMT	|	double	|	15	|		|		|		|		|		|	//:LKP.LKP_DM_DIM_CALENDAR(SPR_SHEET_DT)<br><br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	SDCM_CASHFLOW	|	SPR_SHEET_DT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	NAV_AMT	|	double	|	15	|		|		|		|		|		|	//:LKP.LKP_DM_DIM_CALENDAR(TRDE_DT)<br><br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	SDCM_CASHFLOW	|	TRDE_DT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	TXN_TYPE_CD	|	decimal	|	1	|		|		|		|		|		|	//:LKP.LKP_DM_DIM_CALENDAR(CONFIRM_DT)<br><br><br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	SDCM_CASHFLOW	|	CONFIRM_DT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	CASH_CNTL_RECON_FLG	|	string	|	1	|		|		|		|		|	"IIF(ISNULL(in_SHRHLDR_ROLE_KEY),-1,in_SHRHLDR_ROLE_KEY)"	|		|		|		|	SDCM_CASHFLOW	|	SHRHLDR_ROLE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ADV_COMMSN_AMT	|	double	|	15	|		|		|		|	ACCT_NBR	|		|		|		|		|	SDCM_CASHFLOW	|	ACCT_NBR	|	number	|	15	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|	"IIF(TXN_TYPE_CD=0,SHRS,0)"	|		|		|		|	SDCM_CASHFLOW	|	SUB_SHRS	|	number	|	15	|		|		|
|		|	ACCT_NBR	|	decimal	|	20	|		|		|		|		|	"SUM(SUB_AMT) for group by REG_KEY, DLR_KEY, SHRHLDR_KEY, ACCT_KEY, FUND_KEY, SPR_SHEET_DT_KEY and TRDE_DT_KEY"	|		|		|		|	SDCM_CASHFLOW	|	SUB_AMT	|	number	|	15	|		|		|
|		|	DLR_ID	|	string	|	25	|		|		|		|		|	SUM(REDMPN_SHRS)	|		|		|		|	SDCM_CASHFLOW	|	REDMPN_SHRS	|	number	|	15	|		|		|
|		|	SHRHLDR_ROLE_KEY	|	decimal	|	10	|		|		|		|		|	SUM(REDMPN_AMT)	|		|		|		|	SDCM_CASHFLOW	|	REDMPN_AMT	|	number	|	15	|		|		|
|		|	CUST_ID	|	decimal	|	11	|		|		|		|		|	AVG(OFRG_PR)	|		|		|		|	SDCM_CASHFLOW	|	OFRG_PR	|	number	|	15	|		|		|
|		|	CUST_ID_2	|	decimal	|	11	|		|		|		|		|	SUM(NAV_AMT)	|		|		|		|	SDCM_CASHFLOW	|	NAV	|	number	|	15	|		|		|
|		|	ST_CD	|	string	|	3	|		|		|		|	DLR_COMMSN_AMT	|		|		|		|		|	SDCM_CASHFLOW	|	DLR_COMMSN_AMT	|	number	|	15	|		|		|
|		|	REG_KEY	|	decimal	|	10	|		|		|		|	UNDR_WRTR_COMMSN_AMT	|		|		|		|		|	SDCM_CASHFLOW	|	UNDR_WRTR_COMMSN_AMT	|	number	|	15	|		|		|
|		|	CRTY_ISO_CD	|	string	|	2	|		|		|		|	ADV_COMMSN_AMT	|		|		|		|		|	SDCM_CASHFLOW	|	ADV_COMMSN_AMT	|	number	|	15	|		|		|
|		|	PMT_MTHD_CD	|	string	|	1	|		|		|		|		|	SUM(SUB_SHARES) -SUM(ABS(REDMPN_SHRS))	|		|		|		|	SDCM_CASHFLOW	|	NET_SHRS	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	SUM(SUB_AMT) +SUM(REDMPN_AMT)-SUM(DLR_COMMSN_AMT+UNDR_WRTR_COMMSN_AMT+ADV_COMMSN_AMT)	|		|		|		|	SDCM_CASHFLOW	|	NET_CASHFLOW_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|	CASH_CNTL_RECON_FLG	|		|		|		|		|	SDCM_CASHFLOW	|	CASH_CNTL_RECON_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	SDCM_CASHFLOW	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	SDCM_CASHFLOW	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|	SDCM_CASHFLOW	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	4	|		|		|		|	SDCM_CASHFLOW	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	SDCM_CASHFLOW	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|	Table Name: HDM.ACCOUNT<br>Condition: ACCT_NBR = IN_ACCT_NBR AND FUND_NBR = IN_FUND_NBR<br>Output Column:HBR_DLR_ID	|		|		|	SDCM_CASHFLOW	|	HBR_DLR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|	Seq Number	|		|		|		|	"IIF(NOT ISNULL(v_ERR_MSG),'Y','N')"	|	ETL_LOAD_ERROR	|	ERR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|		|	ETL_LOAD_ERROR	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	"IIF(<br>      (NOT ISNULL(v_FUND_KEY_ERR_MSG)<br>                                                         OR<br>       NOT ISNULL(v_ACCT_KEY_ERR_MSG)<br>                                                          OR     <br>       NOT ISNULL(v_DLR_KEY_ERR_MSG)<br>                                                          OR <br>       NOT ISNULL(v_SHRHLDR_ROLE_KEY_ERR_MSG)<br>                                                         OR<br>       NOT ISNULL(v_SHRHLDR_KEY_ERR_MSG)),<br>(v_FUND_KEY_ERR_MSG&vert;&vert;v_ACCT_KEY_ERR_MSG&vert;&vert;v_DLR_KEY_ERR_MSG&vert;&vert;v_SHRHLDR_ROLE_KEY_ERR_MSG&vert;&vert;v_SHRHLDR_KEY_ERR_MSG&vert;&vert;' FOR CUSTOMER ACCOUNT:'&vert;&vert;TO_CHAR(ACCT_NBR)&vert;&vert;' AND FUND NUMBER:'&vert;&vert;TO_CHAR(FUND_NBR)&vert;&vert;TO_CHAR(DLR_ID))&vert;&vert;' AND SHAREHOLDER ID1:'&vert;&vert;CUST_ID&vert;&vert;' AND SHAREHOLDER ID2:'&vert;&vert;CUST_ID_2<br>,NULL)<br><br>"	|		|		|		|	ETL_LOAD_ERROR	|	ERR_MSG	|	varchar2	|	4000	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	ETL_LOAD_ERROR	|	RUN_DT_TS	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
