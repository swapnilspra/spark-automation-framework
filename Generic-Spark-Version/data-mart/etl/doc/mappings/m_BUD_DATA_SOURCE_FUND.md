|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_DATA_SOURCE_FUND<br>Condition: DATA_SRC_DIM_KEY = DATA_SRC_DIM_KEY1 AND BUD_FUND_COMPST_KEY = BUD_FUND_COMPST_KEY1<br>Output Column: BUD_DATA_SRC_FUND_KEY	|		|		|	BUD_DATA_SOURCE_FUND	|	BUD_DATA_SRC_FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	FUND_CMP	|	string	|	20	|		|		|		|		|		|	Table Name: HDM.BUD_DATA_SOURCE_DIM <br>Condition: DATA_SRC_ID = DATA_SOURCE_ID_out<br>Output Column: DATA_SRC_DIM_KEY	|		|		|	BUD_DATA_SOURCE_FUND	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	AVG_MONTHLY_TNA	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUD_FUND_COMPOSITE<br>Condition: FUND_COMPST_ID = FUND_CMP_out<br>Output Column: BUD_FUND_COMPST_KEY	|		|		|	BUD_DATA_SOURCE_FUND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	FEE_REQUIRED_MINIMUM	|	double	|	15	|		|		|		|	AVG_MONTHLY_TNA	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	AVG_MTHLY_TNA	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	FUND_NAME	|	string	|	80	|		|		|		|	ISSUER_PERCENT	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	ISSR_PCT	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	FUND_ORDER	|	double	|	15	|		|		|		|	RATE	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	RT	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	ISSUER_PERCENT	|	double	|	15	|		|		|		|		|	"iif(isnull(ltrim(rtrim(REQUIRES_FEE))),0,iif(ltrim(rtrim(REQUIRES_FEE))='false',0,1))"	|		|		|		|	BUD_DATA_SOURCE_FUND	|	REQS_FEE_FLG	|	"number(p,s)"	|	38	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	RATE	|	double	|	15	|		|		|		|	SOURCE_VALUE	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	SRC_VAL	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	REQUIRES_FEE	|	string	|	5	|		|		|		|	TOTAL_FEE	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	TOT_FEE	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	RIC_PERCENT	|	double	|	15	|		|		|		|	OUTPUT_VALUE	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	OTPT_VAL	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	SOURCE_VALUE	|	double	|	15	|		|		|		|	ADJUSTMENT	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	ADJMT	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	TOTAL_FEE	|	double	|	15	|		|		|		|		|	ltrim(rtrim(NOTES))	|		|		|		|	BUD_DATA_SOURCE_FUND	|	DATA_SRC_FUND_NTE	|	nvarchar2	|	2000	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	TOTAL_ISSUER_MARKET_CAP	|	double	|	15	|		|		|		|	SUBSIDIARY_MULTIPLIER	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	SBY_MULTR	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	OUTPUT_VALUE	|	double	|	15	|		|		|		|	HARBOR_NBR_ACCOUNTS	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	HBR_NBR_ACCTS	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	ADJUSTMENT	|	double	|	15	|		|		|		|	BROADRIDGE_NBR_ACCOUNTS	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	BROADRIDGE_NBR_ACCTS	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	NOTES	|	string	|	300	|		|		|		|	REINVESTMENTS	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	REINVS	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	SUBSIDIARY_MULTIPLIER	|	double	|	15	|		|		|		|	BILLABLE_ACTIVITY	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	BLLBL_ACT	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	HARBOR_NBR_ACCOUNTS	|	double	|	15	|		|		|		|	CAPSTOCK_ADJUSTMENTS	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	CAPSTK_ADJMTS	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	BROADRIDGE_NBR_ACCOUNTS	|	double	|	15	|		|		|		|	TNA_VALUE	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	TNA_VAL	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	REINVESTMENTS	|	double	|	15	|		|		|		|	RESIDUAL_VALUE	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	RSDL_VAL	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	BILLABLE_ACTIVITY	|	double	|	15	|		|		|		|	SUBSCRIPTIONS	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	SUBS	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	CAPSTOCK_ADJUSTMENTS	|	double	|	15	|		|		|		|	REDEMPTIONS	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	REDMPNS	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	TNA_VALUE	|	double	|	15	|		|		|		|	NET_SALES_ACTIVITY	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	NET_SALES_ACT	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	VERSION	|	double	|	15	|		|		|		|	ANNUAL_NET_SALES_ACTIVITY	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	ANNL_NET_SALES_ACT	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	RESIDUAL_VALUE	|	double	|	15	|		|		|		|	SUBSCRIPTIONS_INCLUDING_ADJ	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	SUBS_INCLUDING_ADJMTS	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	SUBSCRIPTIONS	|	double	|	15	|		|		|		|	AVG_DAILY_TNA	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	AVG_DAILY_TNA	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	REDEMPTIONS	|	double	|	15	|		|		|		|	ADJUSTED_AVG_DAILY_TNA	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	ADJ_AVG_DAILY_TNA	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	NET_SALES_ACTIVITY	|	double	|	15	|		|		|		|	ALLOCATION_ADJUSTMENT	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	ALLOCN_ADJMT	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	ANNUAL_NET_SALES_ACTIVITY	|	double	|	15	|		|		|		|	ORIGINAL_ALLOCATION	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	ORIG_ALLOCN	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	SUBSCRIPTIONS_INCLUDING_ADJ	|	double	|	15	|		|		|		|	GREATEST_COMMON_DENOMINATOR	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	GREATEST_COMMN_DENOMR	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	AVG_DAILY_TNA	|	double	|	15	|		|		|		|	TOTAL_AVG_DAILY_TNA	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	TOT_AVG_DAILY_TNA	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	ADJUSTED_AVG_DAILY_TNA	|	double	|	15	|		|		|		|	TOTAL_COMPENSATION	|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	TOT_COMPNSN	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	ALLOCATION_ADJUSTMENT	|	double	|	15	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_DATA_SOURCE_FUND	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	ORIGINAL_ALLOCATION	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|	BUD_DATA_SOURCE_FUND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	GREATEST_COMMON_DENOMINATOR	|	double	|	15	|		|		|		|		|	SYSDATE	|		|		|		|	BUD_DATA_SOURCE_FUND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	TOTAL_AVG_DAILY_TNA	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	VW_BDGT_DATA_SOURCE_FUND	|	TOTAL_COMPENSATION	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_DATA_SOURCE_FUND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
