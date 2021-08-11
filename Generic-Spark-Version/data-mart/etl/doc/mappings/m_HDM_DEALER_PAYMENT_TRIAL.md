|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	DLR_PMT_DET_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	Insert Only	|		|	DEALER_PAYMENT_TRIAL	|	DLR_PMT_TRL_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	:LKP.LKP_DEALER_KEY(TO_CHAR(DLR_ID))<br>Table Name:<br>Condition:<br>Return Column:	|		|		|	DEALER_PAYMENT_TRIAL	|	DLR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|		|	:LKP.LKP_FUND_KEY(FUND_NBR)<br>Table Name:HDM.FUND<br>Condition: HDM.FUND<br>Return Column:FUND_KEY	|		|		|	DEALER_PAYMENT_TRIAL	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ACCT_NBR	|	decimal	|	10	|		|		|		|		|		|	":LKP.LKP_DLR_BRANCH_KEY(TO_CHAR(DLR_ID),TO_CHAR(BRANCH_ID))<br>Table Name:HDM.DEALER_BRANCH<br>Condition: DLR_ID = DLR_ID_IN AND BRANCH_ID = BRANCH_ID_IN<br>Return Column:DLR_BRANCH_KEY"	|		|		|	DEALER_PAYMENT_TRIAL	|	DLR_BRANCH_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DLR_ID	|	decimal	|	10	|		|		|		|		|		|	:LKP.LKP_DEALER_FEE_TYPE_KEY(COMPNSN_FEE_CD)<br>Table Name:HDM.DEALER_FEE_TYPE<br>Condition: DLR_FEE_TYPE_CD = DLR_FEE_TYPE_CD_IN<br>Return Column:DLR_FEE_TYPE_KEY	|		|		|	DEALER_PAYMENT_TRIAL	|	DLR_FEE_TYPE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DLR_PMT_MTHD_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|	DLR_PMT_MTHD_KEY	|	DEALER_PAYMENT_TRIAL	|	DLR_PMT_MTHD_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PER_END_DT	|	date/time	|	29	|		|		|		|		|		|	":LKP.LKP_ACCT_KEY(ACCT_NBR, FUND_NBR)<br>Table Name:<br>Condition:<br>Return Column:"	|		|		|	DEALER_PAYMENT_TRIAL	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	BRANCH_ID	|	string	|	25	|		|		|		|		|		|	:LKP.LKP_PAYOUT_TYPE_KEY(PAYOUT_TYPE_FLG)<br>Table Name :HDM.DEALER_PAYOUT_TYPE<br>Condition: PAYOUT_TYPE_CD = PAYOUT_TYPE_CD_IN<br>Return Column: PAYOUT_TYPE_KEY	|		|		|	DEALER_PAYMENT_TRIAL	|	PAYOUT_TYPE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PMT_WK_ORD	|	string	|	25	|		|		|		|		|		|	":LKP.LKP_DAY_KEY(LAST_DAY(ADD_TO_DATE(PER_END_DT,'MM',1)))<br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_IN<br>Return Column:DAY_KEY"	|		|		|	DEALER_PAYMENT_TRIAL	|	PMT_DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	NSCC_IND_FLG	|	string	|	1	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(PER_END_DT)<br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_IN<br>Return Column:DAY_KEY	|		|		|	DEALER_PAYMENT_TRIAL	|	INVC_DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	NSCC_NM_USE_FLG	|	string	|	1	|		|		|		|	PMT_WK_ORD	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	WK_ORD_ID	|	varchar2	|	25	|		|		|
|		|	COMPNSN_PYAEE_TYPE_CD	|	string	|	3	|		|		|		|	NSCC_IND_FLG	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	NSCC_FLG	|	varchar2	|	1	|		|		|
|		|	COMPNSN_FEE_CD	|	string	|	4	|		|		|		|	NSCC_NM_USE_FLG	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	NSCC_NM_USE_FLG	|	varchar2	|	1	|		|		|
|		|	SPLT_COMPNSN_FLG	|	string	|	1	|		|		|		|	COMPNSN_FEE_CD	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	PMT_OFST_CD	|	varchar2	|	25	|		|		|
|		|	PREV_COMPNSN_SCHED_OVRD	|	string	|	4	|		|		|		|	COMPNSN_PYAEE_TYPE_CD	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	PAYEE_TYPE	|	varchar2	|	3	|		|		|
|		|	ELIGBL_COMPNSN_SCHED_OVRD	|	string	|	4	|		|		|		|	SPLT_COMPNSN_FLG	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	SPLT_COMPNSN_FLG	|	varchar2	|	1	|		|		|
|		|	PAYOUT_TYPE_FLG	|	string	|	1	|		|		|		|	PREV_COMPNSN_SCHED_OVRD	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	PREV_COMPNSN_OVRD_TYPE	|	varchar2	|	4	|		|		|
|		|	PRE_AGREED_BASIS_POINTS	|	double	|	15	|		|		|		|	ELIGBL_COMPNSN_SCHED_OVRD	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	ELIGBL_COMPNSN_OVRD_TYPE	|	varchar2	|	4	|		|		|
|		|	PRE_AGREED_ASSET	|	double	|	15	|		|		|		|	PRE_AGREED_BASIS_POINTS	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	PRE_AGRD_SHR_RT	|	number	|	15	|		|		|
|		|	PRE_AGREED_COMPNSN	|	double	|	15	|		|		|		|	PRE_AGREED_ASSET	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	PRE_AGRD_ASSET	|	number	|	15	|		|		|
|		|	PREV_SPLT_COMPNSN	|	double	|	15	|		|		|		|	PRE_AGREED_COMPNSN	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	PRE_AGRD_FEE	|	number	|	15	|		|		|
|		|	TOT_ELIGBL_BASIS_POINTS	|	double	|	15	|		|		|		|	PREV_SPLT_COMPNSN	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	PREV_SPLT_FEE	|	number	|	15	|		|		|
|		|	TOT_ELIGBL_ASSETS	|	double	|	15	|		|		|		|	TOT_ELIGBL_BASIS_POINTS	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	TOT_ELIGBL_RT	|	number	|	15	|		|		|
|		|	TOT_ELIGBL_COMPNSN	|	double	|	15	|		|		|		|	TOT_ELIGBL_ASSETS	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	TOT_ELIGBL_ASSET	|	number	|	15	|		|		|
|		|	TOT_COMPNSN	|	double	|	15	|		|		|		|	TOT_ELIGBL_COMPNSN	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	TOT_ELIGBL_FEE	|	number	|	15	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|	TOT_COMPNSN	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	TOT_COMPNSN	|	number	|	15	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|	AVG_ELIGBL_BP	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	AVG_ELIGBL_BP	|	number	|	15	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|	AVG_ELIGBL_ASSET	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	AVG_ELIGBL_ASSET	|	number	|	15	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	AVG_ELIGBL_FEES	|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	AVG_ELIGBL_FEES	|	number	|	15	|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|	SESSSTARTTIME	|		|		|		|	DEALER_PAYMENT_TRIAL	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	AVG_ELIGBL_BP	|	double	|	15	|		|		|		|		|	0'	|		|		|		|	DEALER_PAYMENT_TRIAL	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	AVG_ELIGBL_ASSET	|	double	|	15	|		|		|		|		|		|		|		|		|	DEALER_PAYMENT_TRIAL	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	AVG_ELIGBL_FEES	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
