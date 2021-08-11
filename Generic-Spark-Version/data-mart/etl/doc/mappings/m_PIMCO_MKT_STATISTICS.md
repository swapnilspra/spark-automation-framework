|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update the record if already exits, otherwise insert"	|		|		|		|		|		|		|		|
|	STG_PIMCO_MKT_STATISTICS	|	ASOF_DATE	|	nstring	|	30	|		|		|		|	Seq Number	|		|	Table Name: HDM.PIMCO_STATISTICS<br>Condition: FUND_COMPST_KEY = FUND_COMPST_KEY1 AND DAY_KEY = DAY_KEY1<br>Output Column: PMC_STATS_KEY	|		|		|	PIMCO_STATISTICS	|	PMC_STATS_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_MKT_STATISTICS	|	ACCT_NO	|	nstring	|	30	|		|		|		|		|		|	Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = ASOF_DATE_out<br>Output Column: DAY_KEY	|		|		|	PIMCO_STATISTICS	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_MKT_STATISTICS	|	TOTAL_NET_ASSETS_ALL_CLASSES	|	nstring	|	100	|		|		|		|		|		|	Table Name: HDM.PIMCO_ACCOUNT_REFERENCE<br>Condition: PMC_ACCT_NBR = ACCT_NO<br>Output Column: FUND_COMPST_KEY	|		|		|	PIMCO_STATISTICS	|	FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	STG_PIMCO_MKT_STATISTICS	|	FIXED_INCOME_ASSETS	|	nstring	|	100	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	MKT_VAL	|	number	|	15	|		|		|
|	STG_PIMCO_MKT_STATISTICS	|	CASH_OTHER_ASSETS_LESS_LIAB	|	nstring	|	100	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	QTRLY_CONTRBN	|	number	|	15	|		|		|
|	STG_PIMCO_MKT_STATISTICS	|	NUMBER_OF_ISSUES_FUND	|	nstring	|	30	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	EST_INCM	|	number	|	15	|		|		|
|	STG_PIMCO_MKT_STATISTICS	|	NUMBER_OF_ISSUES_BENCHMARK	|	nstring	|	30	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	EST_YTM	|	number	|	15	|		|		|
|	STG_PIMCO_MKT_STATISTICS	|	AVG_MARKET_COUPON_BENCHMARK	|	nstring	|	30	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	EFFTV_DUR	|	number	|	15	|		|		|
|	STG_PIMCO_MKT_STATISTICS	|	WEIGHTED_AVGMATURITY_BENCHMARK	|	nstring	|	30	|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	AVG_MTRY	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	AVG_QUAL	|	nvarchar2	|	25	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	AVG_COUPN	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	CURR_YLD	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_AVG_QUAL	|	nvarchar2	|	25	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_EFFTV_DUR	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_CURR_YLD	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_PMC_DUR	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	BMK_EST_YTM	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|	PIMCO_STATISTICS	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	PIMCO_STATISTICS	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_STATISTICS	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	to_decimal(TOTAL_NET_ASSETS_ALL_CLASSES)	|		|		|		|	PIMCO_STATISTICS	|	TOT_NET_ASSET	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	to_decimal(FIXED_INCOME_ASSETS)	|		|		|		|	PIMCO_STATISTICS	|	FINC_ASSET	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	to_decimal(CASH_OTHER_ASSETS_LESS_LIAB)	|		|		|		|	PIMCO_STATISTICS	|	NET_ASSET_N_CASH	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	to_decimal(NUMBER_OF_ISSUES_FUND)	|		|		|		|	PIMCO_STATISTICS	|	FUND_ISS_CNT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	to_decimal(NUMBER_OF_ISSUES_BENCHMARK)	|		|		|		|	PIMCO_STATISTICS	|	BMK_ISS_CNT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	to_decimal(AVG_MARKET_COUPON_BENCHMARK)	|		|		|		|	PIMCO_STATISTICS	|	BMK_AVG_MKT_COUPN	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	to_decimal(WEIGHTED_AVGMATURITY_BENCHMARK)	|		|		|		|	PIMCO_STATISTICS	|	BMK_WGTA_MTRY	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
