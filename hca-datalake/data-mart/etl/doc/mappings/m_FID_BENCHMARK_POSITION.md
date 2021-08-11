|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	IDX_NM	|	string	|	255	|	9	|		|		|	BLUESKY_TXN_FLG	|		|	":LKP.LKP_FID_BENCHMARK_POSITION(<br>:LKP.LKP_FIDUCIARY_CUSTOMER(CLNT_NM),<br>:LKP.LKP_DAY_KEY(RPT_DT),<br>:LKP.LKP_VW_FIDUCIARY_BENCHMARK_INDEX(IDX_NM),<br>:LKP.LKP_VW_FIDUCIARY_FUND(FUND_NM)<br>)<br><br>Table Name: HDM.FID_BENCHMARK_POSITION<br>Condition:  FID_CUST_KEY = IN_FID_CUST_KEY AND DAY_KEY = IN_DAY_KEY AND BMK_IDX_KEY = IN_EXT_BMK_IDX_KEY AND FUND_KEY = IN_FUND_KEY<br>Return Column: BMK_POSN_KEY<br><br>//:LKP.LKP_FIDUCIARY_CUSTOMER(CLNT_NM)<br><br>Table Name: HDM.VW_FIDUCIARY_BENCHMARK_INDEX<br>Condition:  IDX_NM = IDX_NM1<br>Return Column: EXT_BMK_IDX_KEY<br><br>//:LKP.LKP_DAY_KEY(RPT_DT),<br><br>Table Name: HDM.CALENDAR<br>Condition:  CAL_DAY = IN_CAL_DAY<br>Return Column: DAY_KEY<br><br>//:LKP.LKP_VW_FIDUCIARY_BENCHMARK_INDEX(IDX_NM),<br><br>Table Name: HDM.VW_FIDUCIARY_BENCHMARK_INDEX<br>Condition:  IDX_NM = IDX_NM1<br>Return Column: EXT_BMK_IDX_KEY<br><br>//:LKP.LKP_VW_FIDUCIARY_FUND(FUND_NM)<br><br>Table Name: HDM.VW_FIDUCIARY_CUSTOMER<br>Condition:  FID_CUST_NM = CLNT_NM<br>Return Column:FID_CUST_KEY"	|	Update the existing records	|	Table Name : FID_BENCHMARK_POSITION<br>	|	FID_BENCHMARK_POSITION	|	BMK_POSN_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	CLNT_NM	|	string	|	255	|		|		|		|		|		|	<br>//:LKP.LKP_FIDUCIARY_CUSTOMER(CLNT_NM)<br><br>Table Name: HDM.VW_FIDUCIARY_BENCHMARK_INDEX<br>Condition:  IDX_NM = IDX_NM1<br>Return Column: EXT_BMK_IDX_KEY	|		|		|	FID_BENCHMARK_POSITION	|	FID_CUST_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	RPT_DT	|	date/time	|	29	|		|		|		|		|		|	"//:LKP.LKP_DAY_KEY(RPT_DT),<br><br>Table Name: HDM.CALENDAR<br>Condition:  CAL_DAY = IN_CAL_DAY<br>Return Column: DAY_KEY"	|		|		|	FID_BENCHMARK_POSITION	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	FUND_NM	|	string	|	150	|		|		|		|		|		|	<br>//:LKP.LKP_FIDUCIARY_CUSTOMER(CLNT_NM)<br><br>Table Name: HDM.VW_FIDUCIARY_BENCHMARK_INDEX<br>Condition:  IDX_NM = IDX_NM1<br>Return Column: EXT_BMK_IDX_KEY	|		|		|	FID_BENCHMARK_POSITION	|	BMK_IDX_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	MO_RET	|	double	|	15	|		|		|		|		|		|	//:LKP.LKP_VW_FIDUCIARY_FUND(FUND_NM)<br><br>Table Name: HDM.VW_FIDUCIARY_CUSTOMER<br>Condition:  FID_CUST_NM = CLNT_NM<br>Return Column:FID_CUST_KEY	|		|		|	FID_BENCHMARK_POSITION	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|	MO_RET	|		|		|		|		|	FID_BENCHMARK_POSITION	|	MO_RET	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|	Y'	|		|		|	FID_BENCHMARK_POSITION	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|	FID_BENCHMARK_POSITION	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FID_BENCHMARK_POSITION	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FID_BENCHMARK_POSITION	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FID_BENCHMARK_POSITION	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
