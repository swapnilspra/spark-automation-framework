|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	FUND_EXPNS_RATIO_KEY	|	decimal	|	10	|	9	|		|		|	Seq Number	|		|		|		|	//If record already exisit update the record<br>Table : HDM.FUND_EXPENSE_RATIO<br>Condition: FUND_KEY = IN_FUND_KEY AND EFFTV_END_DT = IN_EFFTV_END_DT AND EFFTV_STRT_DT = IN_EFFTV_STRT_DT AND PER_FLG = IN_PER_FLG	|	HDM.FUND_EXPENSE_RATIO	|	FUND_EXPNS_RATIO_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	EXPNS_RATIO_WAVR_TYPE_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|	EXPNS_RATIO_WAVR_TYPE_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SHR_CLS_FUND_KEY	|	decimal	|	10	|		|		|		|		|		|	//FUND_KEY<br>Table Name: LKP_DM_DIM_FUND<br>Condition: FUND_NBR = IN_FUND_NBR <br>Ouput Column : FUND_KEY	|		|		|		|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SEMI_ANNL_RPT_DT	|	date/time	|	29	|		|		|		|	SEMI_ANNL_RPT_DT	|		|		|		|		|		|	SEMI_ANNL_RPT_DT	|	date	|	19	|		|		|
|		|	EFFTV_STRT_DT	|	date/time	|	29	|		|		|		|	EFFTV_STRT_DT	|		|		|		|		|		|	EFFTV_STRT_DT	|	date	|	19	|		|		|
|		|	EFFTV_END_DT	|	date/time	|	29	|		|		|		|	EFFTV_END_DT	|		|		|		|		|		|	EFFTV_END_DT	|	date	|	19	|		|		|
|		|	GR_EXPNS_RATIO	|	double	|	15	|		|		|		|	GR_EXPNS_RATIO	|		|		|		|		|		|	GR_EXPNS_RATIO	|	number	|	15	|		|		|
|		|	NET_EXPNS_RATIO	|	double	|	15	|		|		|		|	NET_EXPNS_RATIO	|		|		|		|		|		|	NET_EXPNS_RATIO	|	number	|	15	|		|		|
|		|	EXPNS_WAVD_RATIO	|	double	|	15	|		|		|		|	EXPNS_WAVR_RATIO	|		|		|		|		|		|	EXPNS_WAVR_RATIO	|	number	|	15	|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	CAYMAN_RATIO	|		|		|		|		|		|	CAYMAN_RATIO	|	number	|	15	|		|		|
|		|	CAYMAN_RATIO	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|	SYSDATE	|		|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|	$$etlcyckey	|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|	4	|		|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|	MANUAL_ENTRY_FLG	|	char	|	1	|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|	PER_FLG	|		|		|		|		|		|	PER_FLG	|	varchar2	|	1	|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|	PASS_THRU_MGMT_FEE_WAVR	|		|		|		|		|		|	PASS_THRU_MGMT_FEE_WAVR	|	number	|	15	|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	MANUAL_ENTRY_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	PER_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	PASS_THRU_MGMT_FEE_WAVR	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
