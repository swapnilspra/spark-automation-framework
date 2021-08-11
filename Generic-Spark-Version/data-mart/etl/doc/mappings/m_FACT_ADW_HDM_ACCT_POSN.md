|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	ACCT_BAL_DT	|	date/time	|	29	|	9	|		|		|	Seq Number	|		|		|		|	//If record already exisit update the record<br>Table : HDM.CASHFLOW<br>Condition: EY AND DLR_KEY = in_DLR_KEY AND FUND_KEY = in_FUND_KEY AND ACCT_KEY = in_ACCT_KEY AND TRDE_DT_KEY = in_TRDE_DT_KEY AND SHRHLDR_KEY = in_SHRHLDR_KEY	|	ACCOUNT_POSITION	|	ACCT_POSN_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	TOT_SHRS	|	double	|	15	|		|		|		|		|		|	Table Name: LKP_DM_DIM_ACCOUNT<br>Condition: ACCT_NBR = IN_ACCT_NBR AND FUND_NBR = IN_FUND_NBR<br>Output Column: ACCT_KEY	|		|		|		|	ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	UNISSUED_SHRS	|	double	|	15	|		|		|		|		|		|	Table Name: LKP_DM_DIM_FUND<br>Condition: FUND_NBR = IN_FUND_NBR <br>Output Column: FUND_KEY	|		|		|		|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ISSD_SHRS	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.SHAREHOLDER<br>Condition: CUST_IDNFCN_NBR = IN_CUST_IDNFCN_NBR AND CUST_IDNFCN_NBR_2 = IN_CUST_IDNFCN_NBR_2<br>Output Column: SHRHLDR_KEY	|		|		|		|	SHRHLDR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	NAV	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.DEALER<br>Condition: DLR_ID = IN_DLR_ID<br>Output Column: DLR_KEY	|		|		|		|	DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	RSRVD_SHRS	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|		|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ACCRURED_DIVD_AMT	|	double	|	15	|		|		|		|	ACCT_NBR	|		|		|		|		|		|	ACCT_NBR	|	number	|	15	|		|		|
|		|	ESCRW_SHRS	|	double	|	15	|		|		|		|		|	"IIF(ISNULL(SHRHLDR_ROLE_KEY),-1,SHRHLDR_ROLE_KEY)"	|		|		|		|		|	SHRHLDR_ROLE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	COLLCTD_BAL_AMT	|	double	|	15	|		|		|		|	TOT_SHRS	|		|		|		|		|		|	TOT_SHRS	|	number	|	15	|		|		|
|		|	CURR_INCM_DIVIDENDS	|	double	|	15	|		|		|		|	UN_ISSD_SHRS	|		|		|		|		|		|	UN_ISSD_SHRS	|	number	|	15	|		|		|
|		|	CURR_LONG_TERM_CAP_GAINS	|	double	|	15	|		|		|		|	ISSD_SHRS	|		|		|		|		|		|	ISSD_SHRS	|	number	|	15	|		|		|
|		|	CURR_SHRT_TERM_CAP_GAINS	|	double	|	15	|		|		|		|	NAV	|		|		|		|		|		|	NAV	|	number	|	15	|		|		|
|		|	CURR_FORGN_WITHHOLDINGS	|	double	|	15	|		|		|		|	RSRVD_SHRS	|		|		|		|		|		|	RSRVD_SHRS	|	number	|	15	|		|		|
|		|	CURR_DIVD_WITHHOLDINGS	|	double	|	15	|		|		|		|	ACCRD_DIVD_AMT	|		|		|		|		|		|	ACCRD_DIVD_AMT	|	number	|	15	|		|		|
|		|	CURR_FORGN_TAX_PAID_AMT	|	double	|	15	|		|		|		|	ESCRW_SHRS	|		|		|		|		|		|	ESCRW_SHRS	|	number	|	15	|		|		|
|		|	CURR_YR_1099B_GR_REDMPN_AMT	|	double	|	15	|		|		|		|	COLLCTD_SHRS	|		|		|		|		|		|	COLLCTD_SHRS	|	number	|	15	|		|		|
|		|	CURR_YR_1099B_GR_WHG_AMT	|	double	|	15	|		|		|		|	COLLCTD_BAL_AMT	|		|		|		|		|		|	COLLCTD_BAL_AMT	|	number	|	15	|		|		|
|		|	COLLCTD_SHRS	|	double	|	15	|		|		|		|	CURR_INCM_DIVD	|		|		|		|		|		|	CURR_INCM_DIVD	|	number	|	15	|		|		|
|		|	COLLCTD_SHRS_NET_CHG_CNT	|	double	|	15	|		|		|		|	CURR_LONG_TERM_CAP_GAIN	|		|		|		|		|		|	CURR_LONG_TERM_CAP_GAIN	|	number	|	15	|		|		|
|		|	TOT_SHRS_NET_CHG_CNT	|	double	|	15	|		|		|		|	CURR_SHRT_TERM_CAP_GAIN	|		|		|		|		|		|	CURR_SHRT_TERM_CAP_GAIN	|	number	|	15	|		|		|
|		|	DSGNTD_WHG_AMT	|	double	|	15	|		|		|		|	CURR_FORGN_W_HOLDG	|		|		|		|		|		|	CURR_FORGN_W_HOLDG	|	number	|	15	|		|		|
|		|	DSGNTD_WHG_RT	|	double	|	15	|		|		|		|	CURR_DIVD_W_HOLDG	|		|		|		|		|		|	CURR_DIVD_W_HOLDG	|	number	|	15	|		|		|
|		|	DIVD_WHG_AMT	|	double	|	15	|		|		|		|	CURR_FORGN_TAX_PAID_AMT	|		|		|		|		|		|	CURR_FORGN_TAX_PAID_AMT	|	number	|	15	|		|		|
|		|	DIVD_WHG_RT	|	double	|	15	|		|		|		|	CURR_YR_1099B_GR_REDMPN_AMT	|		|		|		|		|		|	CURR_YR_1099B_GR_REDMPN_AMT	|	number	|	15	|		|		|
|		|	SHRT_TERM_CAP_GAIN_WHG_AMT	|	double	|	15	|		|		|		|	CURR_YR_1099B_GR_W_HOLDG_AMT	|		|		|		|		|		|	CURR_YR_1099B_GR_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|	SHRT_TERM_CAP_GAIN_WHG_RT	|	double	|	15	|		|		|		|	COLLCTD_SHRS_NET_CHG_CNT	|		|		|		|		|		|	COLLCTD_SHRS_NET_CHG_CNT	|	number	|	15	|		|		|
|		|	LONG_TERM_CAP_GAIN_WHG_AMT	|	double	|	15	|		|		|		|	TOT_SHRS_NET_CHG_CNT	|		|		|		|		|		|	TOT_SHRS_NET_CHG_CNT	|	number	|	15	|		|		|
|		|	LONG_TERM_CAP_GAIN_WHG_RT	|	double	|	15	|		|		|		|	DSGNTD_W_HOLDG_AMT	|		|		|		|		|		|	DSGNTD_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	DSGNTD_W_HOLDG_RT	|		|		|		|		|		|	DSGNTD_W_HOLDG_RT	|	number	|	15	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|	DIVD_W_HOLDG_AMT	|		|		|		|		|		|	DIVD_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|	ACCT_NBR	|	decimal	|	20	|		|		|		|	DIVD_W_HOLDG_RT	|		|		|		|		|		|	DIVD_W_HOLDG_RT	|	number	|	15	|		|		|
|		|	DLR_ID	|	string	|	25	|		|		|		|	SHRT_TERM_CAP_GAIN_W_HOLDG_AMT	|		|		|		|		|		|	SHRT_TERM_CAP_GAIN_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|	BRANCH_ID	|	string	|	25	|		|		|		|	SHRT_TERM_CAP_GAIN_W_HOLDG_RT	|		|		|		|		|		|	SHRT_TERM_CAP_GAIN_W_HOLDG_RT	|	number	|	15	|		|		|
|		|	SHRHLDR_ROLE_KEY	|	decimal	|	10	|		|		|		|	LONG_TERM_CAP_GAIN_W_HOLDG_AMT	|		|		|		|		|		|	LONG_TERM_CAP_GAIN_W_HOLDG_AMT	|	number	|	15	|		|		|
|		|	CUST_ID	|	decimal	|	11	|		|		|		|	LONG_TERM_CAP_GAIN_W_HOLDG_RT	|		|		|		|		|		|	LONG_TERM_CAP_GAIN_W_HOLDG_RT	|	number	|	15	|		|		|
|		|	CUST_ID_2	|	decimal	|	11	|		|		|		|		|	"GET_DATE_PART(ACCT_BAL_DT,'YYYY')"	|		|		|		|		|	PARTITION_YR	|	"number(p,s)"	|	4	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|	CURR_ROW_FLG	|		|		|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	4	|		|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|	Table Name: HDM.DEALER_BRANCH<br>Condition:DLR_ID = IN_DLR_ID AND BRANCH_ID = IN_BRANCH_ID<br>Output Column: DLR_BRANCH_KEY	|		|		|		|	DLR_BRANCH_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	TOT_SHRS*NAV	|		|		|		|		|	TOT_BAL_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	TO_CHAR(ROUND(IN_HBR_DLR_ID))	|		|		|		|		|	HBR_DLR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|	Table Name: LKP_DM_DIM_ACCOUNT<br>Condition:ACCT_NBR = IN_ACCT_NBR AND FUND_NBR = IN_FUND_NBR<br>Output Column: ACCT_KEY	|		|		|		|	ACCT_STAT_CD	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	"//Write only if Err_MSG is not NULL<br>IIF(NOT ISNULL(v_ERR_MSG),'Y','N')"	|		|		|		|		|		|
|		|		|		|		|		|		|		|	Seq Number	|		|		|		|		|		|	ETL_LOAD_ERROR	|	ERR_KEY	|	"number(p,s)"	|	10	|		|
|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|		|		|	ETL_LOAD_ERROR	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|
|		|		|		|		|		|		|		|		|	"IIF(<br>       (NOT ISNULL(v_SHRHLDR_ROLE_KEY_ERR_MSG)<br>                                             OR<br>       NOT ISNULL(v_SHRHLDR_KEY_ERR_MSG)),<br>(v_SHRHLDR_ROLE_KEY_ERR_MSG&vert;&vert;v_SHRHLDR_KEY_ERR_MSG&vert;&vert;' FOR CUSTOMER ACCOUNT:'&vert;&vert;TO_CHAR(ACCT_NBR)&vert;&vert;<br>' AND SHAREHOLDER1:'&vert;&vert;TO_CHAR(CUST_ID)&vert;&vert;' AND SHAREHOLDER2:'&vert;&vert;TO_CHAR(CUST_ID_2)&vert;&vert;<br>' AND FUND NUMBER:'&vert;&vert;TO_CHAR(FUND_NBR)),NULL)<br>"	|		|		|		|		|	ETL_LOAD_ERROR	|	ERR_MSG	|	varchar2	|	4000	|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|	ETL_LOAD_ERROR	|	RUN_DT_TS	|	date	|	19	|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
