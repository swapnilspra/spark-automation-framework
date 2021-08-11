|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	DLR_PMT_SUM_KEY	|	decimal	|	10	|		|		|		|		|	//DLR_FEE_TYPE_KEY<br>:LKP.LKP_DEALER_FEE_TYPE_KEY(COMPNSN_FEE_CD)<br>Table Name: HDM.DEALER_FEE_TYPE<br>Condition: DLR_FEE_TYPE_CD = DLR_FEE_TYPE_CD_IN<br>Return Column:DLR_FEE_TYPE_KEY<br><br>//DAY_KEY<br>:LKP.LKP_DAY_KEY(PER_END_DT)<br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_IN<br>Return Column:DAY_KEY<br><br>//FUND_KEY<br>:LKP.LKP_FUND_KEY(FUND_NBR)<br>Table Name: HDM.FUND<br>Condition: FUND_NBR = FUND_NBR_IN<br>Return Column:FUND_KEY<br><br>//DLR_KEY<br>:LKP.LKP_DEALER_KEY(TO_CHAR(DLR_ID))<br>Table Name: HDM.DEALER<br>Condition: DLR_ID = DLR_ID_IN<br>Return Column:DLR_KEY<br>	|	Table Name: HDM.DEALER_PAYMENT_SUMMARY<br>Condition: DLR_FEE_TYPE_KEY = DLR_FEE_TYPE_KEY_IN AND DAY_KEY = DAY_KEY_IN AND FUND_KEY = FUND_KEY_IN AND DLR_KEY = DLR_KEY_IN AND WK_ORD_NBR = WK_ORD_NBR_IN<br><br>Output Column: DLR_PMT_SUM_KEY	|	Only Insert	|		|	HDM_DEALER_PAYMENT_SUMMARY	|	DLR_PMT_SUM_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	DLR_FEE_TYPE_KEY from above lookup	|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	DLR_FEE_TYPE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SHR_CLS_FUND_KEY	|	decimal	|	10	|		|		|		|		|		|	DAY_KEY from above lookup	|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DLR_ROLE_KEY	|	decimal	|	10	|		|		|		|		|		|	FUND_KEY from above lookup	|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DLR_PMT_MTHD_KEY	|	decimal	|	10	|		|		|		|		|		|	DLR_KEY from above lookup	|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	DLR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FEE_TYPE_KEY	|	decimal	|	10	|		|		|		|		|		|	DLR_PMT_MTHD_KEY from above lookup	|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	DLR_PMT_MTHD_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PMT_PER_BEG_DT	|	date/time	|	29	|		|		|		|		|		|	//:LKP.LKP_DLR_PAMT_METHOD_CD(PAMT_METHOD_CD)<br><br>Table Name: HDM.DEALER_PAYMENT_METHOD<br>Condition: PMT_MTHD_CD = PMT_MTHD_CD_IN<br>Ouptut Column: DLR_PMT_MTHD_KEY	|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	WK_ORD_NBR	|	varchar2	|	25	|		|		|
|		|	PMT_PER_END_DT	|	date/time	|	29	|		|		|		|	COMPNSN_PYEE_TYPE_CD	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	COMPNSN_PYEE_TYPE_CD	|	varchar2	|	25	|		|		|
|		|	WK_ORD_NBR	|	string	|	25	|		|		|		|	PRE_AGREED_SCHED_OVRD_TYPE	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	PRE_AGREED_SCHED_OVRD_TYPE	|	varchar2	|	25	|		|		|
|		|	COMPNSN_PYEE_TYPE_CD	|	string	|	10	|		|		|		|	PRE_AGREED_SHR_RT	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	PRE_AGREE_SHR_RT	|	number	|	15	|		|		|
|		|	PRE_AGREED_SCHED_OVRD_TYPE	|	string	|	25	|		|		|		|	PRE_AGREE_AVG_ASSET	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	PRE_AGREE_AVG_ASSET	|	number	|	15	|		|		|
|		|	PRE_AGREED_SHR_RT	|	double	|	15	|		|		|		|	PRE_AGREE_COMPNSN	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	PRE_AGREE_COMPNSN	|	number	|	15	|		|		|
|		|	PRE_AGREE_AVG_ASSET	|	double	|	15	|		|		|		|	ELIGBL_COMPNSN_OVRD_TYPE	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	ELIGBL_COMPNSN_OVRD_TYPE	|	varchar2	|	25	|		|		|
|		|	PRE_AGREE_COMPNSN	|	double	|	15	|		|		|		|	ELIGBL_SHR_RT	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	ELIGBL_SHR_RT	|	number	|	15	|		|		|
|		|	ELIGBL_COMPNSN_OVRD_TYPE	|	string	|	25	|		|		|		|	ELIGBL_AVG_ASSET	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	ELIGBL_AVG_ASSET	|	number	|	15	|		|		|
|		|	ELIGBL_SHR_RT	|	double	|	15	|		|		|		|	ELIGBL_COMPNSN	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	ELIGBL_COMPNSN	|	number	|	15	|		|		|
|		|	ELIGBL_AVG_ASSET	|	double	|	15	|		|		|		|	TOT_COMPNSN	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	TOT_COMPNSN	|	number	|	15	|		|		|
|		|	ELIGBL_COMPNSN	|	double	|	15	|		|		|		|		|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	TOT_COMPNSN	|	double	|	15	|		|		|		|		|	SESSSTARTTIME	|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	DLR_ID	|	string	|	25	|		|		|		|		|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|	TOT_COMPNSN - PREVIOUS_MONTH_COMPSN	|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	COMPNSN_DIFF	|	number	|	15	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|	PREVIOUS_MONTH_COMPSN	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	PREV_PER_COMPNSN	|	number	|	15	|		|		|
|		|	FEE_TYPE_CD	|	string	|	10	|		|		|		|		|	ELIGBL_AVG_ASSET - PREVIOUS_MONTH_AVG_ASSET	|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	AVG_ASSET_DIFF	|	number	|	15	|		|		|
|		|	PAMT_METHOD_CD	|	string	|	10	|		|		|		|		|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	PREV_PER_ASSET	|	number	|	15	|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|	TRL_MODE_FLG	|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	TRL_MODE_FLG	|	varchar2	|	1	|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|	":LKP.LKP_DAY_KEY(<br>ADD_TO_DATE(PMT_PER_END_DT,'MM',1)<br>)"	|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	PMT_DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	TRL_MODE_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	HDM_DEALER_PAYMENT_SUMMARY	|	BUS_LINE	|	varchar2	|	25	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
