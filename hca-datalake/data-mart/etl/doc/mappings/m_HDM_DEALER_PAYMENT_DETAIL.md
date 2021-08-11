|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	DLR_PMT_DET_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	SEQ Number	|		|	HDM_DEALER_PAYMENT_DETAIL	|	DLR_PMT_DET_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	//:LKP.LKP_DAY_KEY(PER_END_DT)<br><br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_IN<br>Return Column: DAY_KEY	|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|		|	// :LKP.LKP_DEALER_KEY(TO_CHAR(DLR_ID))<br><br>Table Name: HDM.DEALER<br>Condition: DLR_ID = DLR_ID_IN<br>Return Column: DLR_KEY	|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	DLR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ACCT_NBR	|	decimal	|	10	|		|		|		|		|		|	// :LKP.LKP_FUND_KEY(FUND_NBR)<br><br>Table Name: HDM.FUND<br>Condition: FUND_NBR = FUND_NBR_IN<br>Return Column: FUND_KEY	|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DLR_ID	|	decimal	|	10	|		|		|		|		|		|	":LKP.LKP_DLR_BRANCH_KEY(TO_CHAR(DLR_ID),TO_CHAR(BRANCH_ID))<br><br>Table Name: HDM.DEALER_BRANCH<br>Condition: DLR_ID = DLR_ID_IN AND BRANCH_ID = BRANCH_ID_IN<br>Return Column: DLR_BRANCH_KEY"	|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	DLR_BRANCH_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	DLR_PMT_MTHD_KEY	|	decimal	|	10	|		|		|		|		|		|	:LKP.LKP_DEALER_FEE_TYPE_KEY(COMPNSN_FEE_CD)<br><br>Table Name: HDM.DEALER_FEE_TYPE<br>Condition: DLR_FEE_TYPE_CD = DLR_FEE_TYPE_CD_IN<br>Return Column: DLR_FEE_TYPE_KEY	|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	DLR_FEE_TYPE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PER_END_DT	|	date/time	|	29	|		|		|		|	DLR_PMT_MTHD_KEY	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	DLR_PMT_MTHD_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	BRANCH_ID	|	string	|	25	|		|		|		|		|		|	":LKP.LKP_ACCT_KEY(ACCT_NBR, FUND_NBR)<br><br>Table Name: HDM.ACCOUNT<br>Condition: ACCT_NBR = ACCT_NBR_IN AND FUND_NBR = FUND_NBR_IN<br>Return Column: ACCT_KEY"	|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PMT_WK_ORD	|	string	|	25	|		|		|		|	PMT_WK_ORD	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	WK_ORD_ID	|	varchar2	|	25	|		|		|
|		|	NSCC_IND_FLG	|	string	|	1	|		|		|		|	NSCC_IND_FLG	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	NSCC_FLG	|	varchar2	|	1	|		|		|
|		|	NSCC_NM_USE_FLG	|	string	|	1	|		|		|		|	NSCC_NM_USE_FLG	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	NSCC_NM_USE_FLG	|	varchar2	|	1	|		|		|
|		|	COMPNSN_PYAEE_TYPE_CD	|	string	|	3	|		|		|		|	COMPNSN_PYAEE_TYPE_CD	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	PMT_OFST_CD	|	varchar2	|	25	|		|		|
|		|	COMPNSN_FEE_CD	|	string	|	4	|		|		|		|	COMPNSN_FEE_CD	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	PAYEE_TYPE	|	varchar2	|	3	|		|		|
|		|	SPLT_COMPNSN_FLG	|	string	|	1	|		|		|		|	SPLT_COMPNSN_FLG	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	SPLT_COMPNSN_FLG	|	varchar2	|	1	|		|		|
|		|	PREV_COMPNSN_SCHED_OVRD	|	string	|	4	|		|		|		|	PREV_COMPNSN_SCHED_OVRD	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	PREV_COMPNSN_OVRD_TYPE	|	varchar2	|	4	|		|		|
|		|	ELIGBL_COMPNSN_SCHED_OVRD	|	string	|	4	|		|		|		|	ELIGBL_COMPNSN_SCHED_OVRD	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	ELIGBL_COMPNSN_OVRD_TYPE	|	varchar2	|	4	|		|		|
|		|	PAYOUT_TYPE_FLG	|	string	|	1	|		|		|		|		|		|	:LKP.LKP_PAYOUT_TYPE_KEY(PAYOUT_TYPE_FLG)<br><br>Table Name: HDM.DEALER_PAYOUT_TYPE<br>Condition: PAYOUT_TYPE_CD = PAYOUT_TYPE_CD_IN<br>Return Column: PAYOUT_TYPE_KEY	|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	PAYOUT_TYPE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PRE_AGREED_BASIS_POINTS	|	double	|	15	|		|		|		|	PRE_AGREED_BASIS_POINTS	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	PRE_AGREED_SHR_RT	|	number	|	15	|		|		|
|		|	PRE_AGREED_ASSET	|	double	|	15	|		|		|		|	PRE_AGREED_ASSET	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	PRE_AGREED_ASSET	|	number	|	15	|		|		|
|		|	PRE_AGREED_COMPNSN	|	double	|	15	|		|		|		|	PRE_AGREED_COMPNSN	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	PRE_AGREED_FEE	|	number	|	15	|		|		|
|		|	PREV_SPLT_COMPNSN	|	double	|	15	|		|		|		|	PREV_SPLT_COMPNSN	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	PREV_SPLT_FEE	|	number	|	15	|		|		|
|		|	TOT_ELIGBL_BASIS_POINTS	|	double	|	15	|		|		|		|	TOT_ELIGBL_BASIS_POINTS	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	TOT_ELIGBL_RT	|	number	|	15	|		|		|
|		|	TOT_ELIGBL_ASSETS	|	double	|	15	|		|		|		|	TOT_ELIGBL_ASSETS	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	TOT_ELIGBL_ASSET	|	number	|	15	|		|		|
|		|	TOT_ELIGBL_COMPNSN	|	double	|	15	|		|		|		|	TOT_ELIGBL_COMPNSN	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	TOT_ELIGBL_FEE	|	number	|	15	|		|		|
|		|	TOT_COMPNSN	|	double	|	15	|		|		|		|	TOT_COMPNSN	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	TOT_COMPNSN	|	number	|	15	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|	Y'	|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|	SYSDATE	|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|	AVG_ELIGBL_BP	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	AVG_ELIGBL_BP	|	number	|	15	|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|	AVG_ELIGBL_ASSET	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	AVG_ELIGBL_ASSET	|	number	|	15	|		|		|
|		|	AVG_ELIGBL_BP	|	double	|	15	|		|		|		|	AVG_ELIGBL_FEES	|		|		|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	AVG_ELIGBL_FEES	|	number	|	15	|		|		|
|		|	AVG_ELIGBL_ASSET	|	double	|	15	|		|		|		|		|		|	":LKP.LKP_DAY_KEY(LAST_DAY(ADD_TO_DATE(PER_END_DT,'MM',1)))"	|		|		|	HDM_DEALER_PAYMENT_DETAIL	|	PMT_DAY_KEY	|	"number(p,s)"	|	10	|		|		|
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
