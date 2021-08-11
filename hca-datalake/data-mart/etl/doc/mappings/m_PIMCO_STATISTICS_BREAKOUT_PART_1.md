|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	Update record if already present	|		|		|		|		|		|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	ASOF_DATE	|	string	|	25	|		|		|		|	Seq Number	|		|	Table Name: HDM.PIMCO_STATISTICS<br>Condition: DAY_KEY = DAY_KEY_in AND FUND_COMPST_KEY = FUND_COMPST_KEY_in<br>Output Column: PMC_STATS_KEY	|		|		|	PIMCO_STATISTICS	|	PMC_STATS_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	ACCT_NO	|	string	|	25	|		|		|		|		|		|	":LKP.LKP_DAY_KEY(<br>TO_DATE(LTRIM(RTRIM(ASOF_DATE)),'MM/DD/YYYY')<br>)<br>Table Name:HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_in<br>Output Colum: DAY_KEY"	|		|		|	PIMCO_STATISTICS	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	MKT_VAL_TOTAL	|	string	|	25	|		|		|		|		|		|	:LKP.LKP_FND_COMPST_KEY(<br>LTRIM(RTRIM(ACCT_NO))<br>)<br><br>Table Name: HDM.PIMCO_ACCOUNT_REFERENCE<br>Condition: PMC_ACCT_NBR = PMC_ACCT_NBR_IN<br>Output Colum: FUND_COMPST_KEY	|		|		|	PIMCO_STATISTICS	|	FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	QTRLY_CONTRIB	|	string	|	25	|		|		|		|		|	lTRIM(RTRIM(AVG_QLTY))	|		|		|		|	PIMCO_STATISTICS	|	MKT_VAL	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	ESTIMATED_INCOME	|	string	|	25	|		|		|		|		|	TO_DECIMAL(QTRLY_CONTRIB)	|		|		|		|	PIMCO_STATISTICS	|	QTRLY_CONTRBN	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	ESTIMATED_YTM	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ESTIMATED_INCOME)	|		|		|		|	PIMCO_STATISTICS	|	EST_INCM	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	DURATION	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ESTIMATED_YTM)	|		|		|		|	PIMCO_STATISTICS	|	EST_YTM	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	AVG_MATURITY	|	string	|	25	|		|		|		|		|	TO_DECIMAL(DURATION)	|		|		|		|	PIMCO_STATISTICS	|	EFFTV_DUR	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	AVG_QLTY	|	string	|	25	|		|		|		|		|	TO_DECIMAL(AVG_MATURITY)	|		|		|		|	PIMCO_STATISTICS	|	AVG_MTRY	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	AVG_COUPON	|	string	|	25	|		|		|		|		|	LTRIM(RTRIM(AVG_QLTY))	|		|		|		|	PIMCO_STATISTICS	|	AVG_QUAL	|	nvarchar2	|	25	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	CURRENT_YIELD	|	string	|	25	|		|		|		|		|	TO_DECIMAL(AVG_COUPON)	|		|		|		|	PIMCO_STATISTICS	|	AVG_COUPN	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	FIELD12	|	string	|	25	|		|		|		|		|	TO_DECIMAL(CURRENT_YIELD)	|		|		|		|	PIMCO_STATISTICS	|	CURR_YLD	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	ASOF_DATE1	|	string	|	25	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_AVG_QUAL	|	nvarchar2	|	25	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	ACCT_NO1	|	string	|	25	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_EFFTV_DUR	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	BOGIE_AVG_QLTY	|	string	|	25	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_CURR_YLD	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	DURATION1	|	string	|	25	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_PMC_DUR	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	BENCHMARK_CURRENT_YIELD	|	string	|	25	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_EST_YTM	|	number	|	15	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	BENCHMARK_DURATION_PIMCO_YRS	|	string	|	25	|		|		|		|		|	Y'	|		|		|		|	PIMCO_STATISTICS	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	STG_PIMCO_STATISTICS_BREAKOUT	|	BENCHMARK_EST_YLD_TO_MATURITY	|	string	|	25	|		|		|		|		|	SYSDATE	|		|		|		|	PIMCO_STATISTICS	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	TOT_NET_ASSET	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	FINC_ASSET	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	NET_ASSET_N_CASH	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	FUND_ISS_CNT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_ISS_CNT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_AVG_MKT_COUPN	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_WGTA_MTRY	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
