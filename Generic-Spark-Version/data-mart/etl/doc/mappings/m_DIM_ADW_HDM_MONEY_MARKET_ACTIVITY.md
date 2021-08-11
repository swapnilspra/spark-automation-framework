|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	MONY_MKT_ACT_KEY	|	decimal	|	10	|	9	|		|		|	Seq Number	|		|		|		|	//If record already exisit update the record<br>Table : HDMMONEY_MARKET_ACTIVITY<br>Condition: DAY_KEY = DAY_KEY1 AND FUND_KEY = FUND_KEY1<br>	|	HDMMONEY_MARKET_ACTIVITY	|	MONY_MKT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|	FUND_KEY	|	//FUND_KEY<br>Table Name: HDM.FUND<br>Condtion: FUND_NBR = IN_FUND_NBR<br>output column: FUND_KEY	|		|		|		|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SHR_CLS_FUND_KEY	|	decimal	|	10	|		|		|		|		|	DAY_KEY	|	//DAY_KEY<br>Table Name: HDM.CALENDAR<br>Condtion: CAL_DAY = IN_CAL_DAY<br>output column: Day_Key	|		|		|		|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	MONY_MKT_DT	|	date/time	|	29	|		|		|		|	GAIN_LOSS_AMT	|	GAIN_LOSS_AMT	|		|		|		|		|	GAIN_LOSS_AMT	|	number	|	15	|		|		|
|		|	GAIN_LOSS_AMT	|	double	|	15	|		|		|		|	DSTRBTN_AMT	|	DSTRBTN_AMT	|		|		|		|		|	DSTRBTN_AMT	|	number	|	15	|		|		|
|		|	DSTRBTN_AMT	|	double	|	15	|		|		|		|	DIVD_PAYBL_AMT	|	DIVD_PAYBL_AMT	|		|		|		|		|	DIVD_PAYBL_AMT	|	number	|	15	|		|		|
|		|	DIVD_PAYBL_AMT	|	double	|	15	|		|		|		|	BASIS_POINTS	|	BASIS_POINTS	|		|		|		|		|	BASIS_POINTS	|	number	|	15	|		|		|
|		|	BASIS_POINTS	|	double	|	15	|		|		|		|	DAYS_TO_MTRY	|	DAYS_TO_MTRY	|		|		|		|		|	DAYS_TO_MTRY	|	number	|	15	|		|		|
|		|	DAYS_TO_MTRY	|	double	|	15	|		|		|		|	INTR_W_GAIN_LOSS	|	INTR_W_GAIN_LOSS	|		|		|		|		|	INTR_W_GAIN_LOSS	|	number	|	15	|		|		|
|		|	INRT_W_GAIN_LOSS	|	double	|	15	|		|		|		|	INTR_WO_GAIN_LOSS	|	INTR_WO_GAIN_LOSS	|		|		|		|		|	INTR_WO_GAIN_LOSS	|	number	|	15	|		|		|
|		|	INRT_WO_GAIN_LOSS	|	double	|	15	|		|		|		|	'Y'	|	'Y'	|		|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	SYSDATE	|	SYSDATE	|		|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|	4	|	4	|		|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|	WEIGHTED_AVG_LIFE	|	WEIGHTED_AVG_LIFE	|		|		|		|		|	WEIGHTED_AVG_LIFE	|	number	|	15	|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|	NET_ASSET_AT_COST	|	NET_ASSET_AT_COST	|		|		|		|		|	NET_ASSET_AT_COST	|	number	|	15	|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|	NET_ASSET_AT_MKT1	|	NET_ASSET_AT_MKT1	|		|		|		|		|	NET_ASSET_AT_MKT	|	number	|	15	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|	INVMT_AT_COST	|	INVMT_AT_COST	|		|		|		|		|	INVMT_AT_COST	|	number	|	15	|		|		|
|		|	WEIGHTED_AVG_LIFE	|	double	|	15	|		|		|		|	INVMT_AT_VAL	|	INVMT_AT_VAL	|		|		|		|		|	INVMT_AT_VAL	|	number	|	15	|		|		|
|		|	NET_ASSET_AT_COST	|	double	|	15	|		|		|		|	MKT_BASED_PR	|	MKT_BASED_PR	|		|		|		|		|	MKT_BASED_PR	|	number	|	15	|		|		|
|		|	INVMT_AT_COST	|	double	|	15	|		|		|		|	SUB_W_DRV_IN_AMT	|	SUB_W_DRV_IN_AMT	|		|		|		|		|	SUB_W_DRV_IN_AMT	|	number	|	15	|		|		|
|		|	INVMT_AT_VAL	|	double	|	15	|		|		|		|	REDMPN_AMT	|	REDMPN_AMT	|		|		|		|		|	REDMPN_AMT	|	number	|	15	|		|		|
|		|	MKT_BASED_PR	|	double	|	15	|		|		|		|	NET_SHRHLDR_CASH_FLOW_AMT	|	NET_SHRHLDR_CASH_FLOW_AMT	|		|		|		|		|	NET_SHRHLDR_CASH_FLOW_AMT	|	number	|	15	|		|		|
|		|	SUB_W_DRV_IN_AMT	|	double	|	15	|		|		|		|	DAILY_LQD_ASSET_AMT	|	DAILY_LQD_ASSET_AMT	|		|		|		|		|	DAILY_LQD_ASSET_AMT	|	number	|	15	|		|		|
|		|	REDMPN_AMT	|	double	|	15	|		|		|		|	DAILY_LQD_ASSET_RT	|	DAILY_LQD_ASSET_RT	|		|		|		|		|	DAILY_LQD_ASSET_RT	|	number	|	15	|		|		|
|		|	NET_SHRHLDR_CASH_FLOW_AMT	|	double	|	15	|		|		|		|	WKL_LQD_ASSET_AMT	|	WKL_LQD_ASSET_AMT	|		|		|		|		|	WKL_LQD_ASSET_AMT	|	number	|	15	|		|		|
|		|	DAILY_LQD_ASSET_AMT	|	double	|	15	|		|		|		|	WKL_LQD_ASSET_RT	|	WKL_LQD_ASSET_RT	|		|		|		|		|	WKL_LQD_ASSET_RT	|	number	|	15	|		|		|
|		|	DAILY_LQD_ASSET_RT	|	double	|	15	|		|		|		|	SHDW_NAV	|	SHDW_NAV	|		|		|		|		|	SHDW_NAV	|	number	|	15	|		|		|
|		|	WKL_LQD_ASSET_AMT	|	double	|	15	|		|		|		|	CNST_NAV	|	CNST_NAV	|		|		|		|		|	CNST_NAV	|	number	|	15	|		|		|
|		|	WKL_LQD_ASSET_RT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SHDW_NAV	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	CNST_NAV	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	NET_ASSET_AT_MKT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
