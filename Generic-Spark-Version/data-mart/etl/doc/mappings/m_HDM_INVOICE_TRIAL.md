|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	INVC_TRL_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	Insert only	|		|	DEALER_INVOICE_TRIAL	|	DLR_INVC_TRL_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	FEE_TYPE_KEY	|	decimal	|	10	|		|		|		|		|		|	:LKP.LKP_DEALER_ROLE_KEY(DLR_ID)<br><br>Table Name: HDM.DEALER<br>Condition: DLR_ID = DLR_ID_IN<br>Return Column: DLR_KEY	|		|		|	DEALER_INVOICE_TRIAL	|	DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SHR_CLS_FUND_KEY	|	decimal	|	10	|		|		|		|		|		|	":LKP.LKP_ACCT_KEY(ACCT_NBR, FUND_NBR)<br><br>Table Name: HDM.ACCOUNT<br>Condition: ACCT_NBR = ACCT_NBR_IN AND FUND_NBR = FUND_NBR_IN<br>Return Column:ACCT_KEY"	|		|		|	DEALER_INVOICE_TRIAL	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	DLR_ROLE_KEY	|	decimal	|	10	|		|		|		|		|		|	:LKP.LKP_SHR_CLS_FUND_KEY(FUND_NBR)<br><br>Table Name: HDM.FUND<br>Condition: FUND_NBR = FUND_NBR_IN<br>Return Column:FUND_KEY	|		|		|	DEALER_INVOICE_TRIAL	|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	CUST_ACCT_KEY	|	decimal	|	10	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(INVC_PER)<br><br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_IN<br>Return Column: DAY_KEY	|		|		|	DEALER_INVOICE_TRIAL	|	INVC_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	INVC_PER	|	date/time	|	29	|		|		|		|		|		|	:LKP.LKP_DKR_FEE_TYPE(FEE_TYPE_CD)<br><br>Table Name: HDM.DEALER_FEE_TYPE<br>Condition: DLR_FEE_TYPE_CD = DLR_FEE_TYPE_CD_IN<br>Return Column:DLR_FEE_TYPE_KEY	|		|		|	DEALER_INVOICE_TRIAL	|	DLR_FEE_TYPE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PMT_PER	|	date/time	|	29	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(PMT_PER)<br><br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_IN<br>Return Column: DAY_KEY	|		|		|	DEALER_INVOICE_TRIAL	|	PMT_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	BUS_LINE	|	string	|	255	|		|		|		|	FEE_RT	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	FEE_RT	|	number	|	15	|		|		|
|		|	FEE_TYPE_CD	|	string	|	255	|		|		|		|	TOT_FEE_AMT	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	TOT_FEE_AMT	|	number	|	15	|		|		|
|		|	FUND_NBR	|	string	|	255	|		|		|		|	INVC_NBR	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	INVC_NBR	|	varchar2	|	25	|		|		|
|		|	DLR_ID	|	string	|	255	|		|		|		|	INVC_DT	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	INVC_DT	|	date	|	19	|		|		|
|		|	ACCT_NBR	|	string	|	255	|		|		|		|	BUS_LINE	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	BUS_LINE	|	varchar2	|	255	|		|		|
|		|	INVC_AVG_ASSET	|	double	|	15	|		|		|		|	INVC_AVG_ASSET	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	INVC_AVG_ASSET	|	number	|	15	|		|		|
|		|	POSN_CNT	|	double	|	15	|		|		|		|	POSN_CNT	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	POSN_CNT	|	number	|	15	|		|		|
|		|	FEE_RT	|	double	|	15	|		|		|		|	PER_ACCT_FEE	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	PER_ACCT_FEE	|	number	|	15	|		|		|
|		|	PER_ACCT_FEE	|	double	|	15	|		|		|		|	DLR_PORTAL_ASSET	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	DLR_PORTAL_ASSET	|	number	|	15	|		|		|
|		|	TOT_FEE_AMT	|	double	|	15	|		|		|		|	PORTAL_POSN_CNT	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	PORTAL_POSN_CNT	|	number	|	15	|		|		|
|		|	DLR_PORTAL_ASSET	|	double	|	15	|		|		|		|	INVC_FREQ_FLG	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	INVC_FREQ_FLG	|	varchar2	|	1	|		|		|
|		|	PORTAL_POSN_CNT	|	double	|	15	|		|		|		|	REC_SPLT_FLG	|		|		|		|		|	DEALER_INVOICE_TRIAL	|	REC_SPLT_FLG	|	varchar2	|	1	|		|		|
|		|	INVC_FREQ_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	DEALER_INVOICE_TRIAL	|	DLR_CAT	|	varchar2	|	255	|		|		|
|		|	INVC_NBR	|	string	|	25	|		|		|		|		|	Y'	|		|		|		|	DEALER_INVOICE_TRIAL	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	INVC_DT	|	date/time	|	29	|		|		|		|		|	SYSDATE	|		|		|		|	DEALER_INVOICE_TRIAL	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|	DEALER_INVOICE_TRIAL	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	DEALER_INVOICE_TRIAL	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|	DEALER_INVOICE_TRIAL	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	REC_SPLT_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
