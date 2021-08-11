|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	TRDE_DT	|	date/time	|	29	|	9	|		|		|	Seq Number	|		|		|		|	//If record already exisit update the record<br>Table : HDM.CASHFLOW<br>Condition: EY AND DLR_KEY = in_DLR_KEY AND FUND_KEY = in_FUND_KEY AND ACCT_KEY = in_ACCT_KEY AND TRDE_DT_KEY = in_TRDE_DT_KEY AND SHRHLDR_KEY = in_SHRHLDR_KEY	|	CASHFLOW	|	CASHFLOW_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SHRS_TRANSACTED	|	double	|	15	|		|		|		|		|	"IIF(ISNULL(REG_KEY),300,REG_KEY)"	|	<br>Table :LKP_DM_DIM_REGION<br>Condition: ST_CD = IN_ST_CD AND MKTG_REG_ID = IN_MKTG_REG_ID AND CRTY_CD = IN_CRTY_CD<br>Output Column : REG_KEY	|		|		|	CASHFLOW	|	REG_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SHR_PR_AMT	|	double	|	15	|		|		|		|		|		|	<br>Table :HDM.FUND<br>Condition: DLR_ID = IN_DLR_ID<br>Output Column : DLR_KEY	|		|		|	CASHFLOW	|	DLR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	GR_TXN_AMT	|	double	|	15	|		|		|		|		|		|	<br>Table :LKP_DM_DIM_DEALER<br>Condition: FUND_NBR = IN_FUND_NBR<br>Output Column : FUND_KEY	|		|		|	CASHFLOW	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DLR_COMMSN_AMT	|	double	|	15	|		|		|		|		|		|	<br>Table :HDM.ACCOUNT<br>Condition: ACCT_NBR = IN_ACCT_NBR AND FUND_NBR = IN_FUND_NBR<br>Output Column : ACCT_KEY	|		|		|	CASHFLOW	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	UNDERWRITER_COMMSN_AMT	|	double	|	15	|		|		|		|		|		|	<br>Table :HDM.PAYMENT_METHOD<br>Condition: PMT_MTHD_CD = in_PMT_MTHD_CD<br>Output Column :PMT_MTHD_KEY	|		|		|	CASHFLOW	|	PMT_MTHD_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SHR_BAL_EFFCT_CD	|	string	|	1	|		|		|		|		|		|	//:LKP.LKP_DM_DIM_CALENDAR(TRDE_DT)<br><br>Table :LKP_DM_DIM_CALENDAR<br>Condition:CAL_DAY = TRDE_DT<br>Output Column :DAY_KEY	|		|		|	CASHFLOW	|	TRDE_DT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PMT_DT	|	date/time	|	29	|		|		|		|		|		|	<br>Table :LKP_DM_DIM_SHAREHOLDER<br>Condition:CUST_IDNFCN_NBR = IN_CUST_IDNFCN_NBR AND CUST_IDNFCN_NBR_2 = IN_CUST_IDNFCN_NBR_2<br>Output Column :SHRHLDR_KEY	|		|		|	CASHFLOW	|	SHRHLDR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FINCL_TXN_ADV_COMMSN	|	double	|	15	|		|		|		|		|		|	//:LKP.LKP_DM_DIM_CALENDAR(PMT_DT)<br><br>Table :LKP_DM_DIM_CALENDAR<br>Condition:CAL_DAY = PMT_DT<br>Output Column :DAY_KEY	|		|		|	CASHFLOW	|	CONFIRM_DT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|	"IIF(ISNULL(in_SHRHLDR_ROLE_KEY),-1,in_SHRHLDR_ROLE_KEY)"	|		|		|		|	CASHFLOW	|	SHRHLDR_ROLE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ACCT_NBR	|	decimal	|	20	|		|		|		|	ACCT_NBR	|		|		|		|		|	CASHFLOW	|	ACCT_NBR	|	number	|	15	|		|		|
|		|	DLR_ID	|	string	|	25	|		|		|		|	SUB_SHARES	|		|		|		|		|	CASHFLOW	|	SUB_SHRS	|	number	|	15	|		|		|
|		|	SHRHLDR_ROLE_KEY	|	decimal	|	10	|		|		|		|		|	"SUM(SUB_AMT) for group by REG_KEY, DLR_KEY, FUND_KEY,ACCT_KEY, TRDE_DT_KEY and SHRHLDR_KEY"	|		|		|		|	CASHFLOW	|	SUB_AMT	|	number	|	15	|		|		|
|		|	CUST_ID	|	decimal	|	11	|		|		|		|		|	"=-1*SUM(REDMPN_SHRS) for group by REG_KEY, DLR_KEY, FUND_KEY,ACCT_KEY, TRDE_DT_KEY and SHRHLDR_KEY"	|		|		|		|	CASHFLOW	|	REDMPN_SHRS	|	number	|	15	|		|		|
|		|	CUST_ID_2	|	decimal	|	11	|		|		|		|		|	"=-1*SUM(REDMPN_AMT) for group by REG_KEY, DLR_KEY, FUND_KEY,ACCT_KEY, TRDE_DT_KEY and SHRHLDR_KEY"	|		|		|		|	CASHFLOW	|	REDMPN_AMT	|	number	|	15	|		|		|
|		|	ST_CD	|	string	|	3	|		|		|		|		|	"AVG(SHR_PR_AMT) for group by REG_KEY, DLR_KEY, FUND_KEY,ACCT_KEY, TRDE_DT_KEY and SHRHLDR_KEY"	|		|		|		|	CASHFLOW	|	OFRG_PR	|	number	|	15	|		|		|
|		|	REG_KEY	|	decimal	|	10	|		|		|		|		|	"SUM(NAV_AMT) for group by REG_KEY, DLR_KEY, FUND_KEY,ACCT_KEY, TRDE_DT_KEY and SHRHLDR_KEY"	|		|		|		|	CASHFLOW	|	NAV	|	number	|	15	|		|		|
|		|	CRTY_ISO_CD	|	string	|	2	|		|		|		|	DLR_COMMSN_AMT	|		|		|		|		|	CASHFLOW	|	DLR_COMMSN_AMT	|	number	|	15	|		|		|
|		|	PMT_MTHD_CD	|	string	|	1	|		|		|		|	UNDERWRITER_COMMSN_AMT	|		|		|		|		|	CASHFLOW	|	UNDR_WRTR_COMMSN_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|	FINCL_TXN_ADV_COMMSN	|		|		|		|		|	CASHFLOW	|	ADV_COMMSN_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	"SUM(SUB_AMT) - (SUM(ABS(REDMPN_AMT))-SUM(DLR_COMMSN_AMT+UNDERWRITER_COMMSN_AMT+FINCL_TXN_ADV_COMMSN))<br>//for group by REG_KEY, DLR_KEY, FUND_KEY,ACCT_KEY, TRDE_DT_KEY and SHRHLDR_KEY"	|		|		|		|	CASHFLOW	|	NET_CASHFLOW_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	CASHFLOW	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	CASHFLOW	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	CASHFLOW	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	4	|		|		|		|	CASHFLOW	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|		|	CASHFLOW	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	"SUM(SUB_SHARES) -SUM(ABS(REDMPN_SHRS))<br>//for group by REG_KEY, DLR_KEY, FUND_KEY,ACCT_KEY, TRDE_DT_KEY and SHRHLDR_KEY"	|		|		|		|	CASHFLOW	|	NET_SHRS	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	CASHFLOW	|	CASH_CNTL_RECON_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	ERR_FLG = 'Y'	|		|		|		|		|		|		|
|		|		|		|		|		|		|		|	Seq number	|		|		|		|		|	ETL_LOAD_ERROR	|	ERR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|		|	ETL_LOAD_ERROR	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	"IIF(<br>      (NOT ISNULL(v_FUND_KEY_ERR_MSG)<br>                                                         OR<br>       NOT ISNULL(v_ACCT_KEY_ERR_MSG)<br>                                                          OR     <br>       NOT ISNULL(v_DLR_KEY_ERR_MSG)<br>                                                          OR <br>       NOT ISNULL(v_SHRHLDR_ROLE_KEY_ERR_MSG)<br>                                                         OR<br>       NOT ISNULL(v_SHRHLDR_KEY_ERR_MSG)),<br>(v_FUND_KEY_ERR_MSG&vert;&vert;v_ACCT_KEY_ERR_MSG&vert;&vert;v_DLR_KEY_ERR_MSG&vert;&vert;v_SHRHLDR_ROLE_KEY_ERR_MSG&vert;&vert;v_SHRHLDR_KEY_ERR_MSG&vert;&vert;<br>' FOR SHAREHOLDERID1:'&vert;&vert;TO_CHAR(CUST_ID)&vert;&vert;' AND  SHAREHOLDERID2:'&vert;&vert;TO_CHAR(CUST_ID_2)&vert;&vert;<br>'  AND CUSTOMER ACCOUNT:'&vert;&vert;TO_CHAR(ACCT_NBR)&vert;&vert;' AND FUND NUMBER:'&vert;&vert;TO_CHAR(FUND_NBR)&vert;&vert;TO_CHAR(DLR_ID)),NULL)<br><br><br>"	|		|		|		|	ETL_LOAD_ERROR	|	ERR_MSG	|	varchar2	|	4000	|		|		|
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
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
