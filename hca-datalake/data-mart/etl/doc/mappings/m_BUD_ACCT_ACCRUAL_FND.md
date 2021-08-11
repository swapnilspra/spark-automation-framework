|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_ID	|	string	|	100	|		|		|		|	auto increment the key value by '1' for each insert	|	Use sequence generator to generate the value for each data insert	|		|	If ACCRL_ID is already available then update the columns 'CURR_ACCT_ACCRL_FUND_KEY' and 'ROW_STRT_DTTM'	|	BUD_ACCT_ACCRUAL_FND	|	ACCT_ACCURAL_FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_SOURCE_BUDGET_ID	|	string	|	60	|		|		|		|		|		|	//ACCT_ACCRL_KEY<br>Table : HDM.BUD_ACCT_ACCRUAL<br>Condition: ACCRL_ID = ACCRUAL_ID_out (From Source)<br>Ouput Column : ACCT_ACCRL_KEY<br>	|		|	BUD_ACCT_ACCRUAL_FND	|	ACCT_ACCRL_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_SOURCE_ASSUMPTION_ID	|	string	|	100	|		|		|		|		|		|	//SRC_BUD_KEY<br>Table : HDM.BUDGET<br>Condition: ACCRL_ID = BUD_ID = ACCRUAL_SOURCE_BUDGET_ID_out (From Source)<br>Ouput Column : BUD_KEY<br>	|		|	BUD_ACCT_ACCRUAL_FND	|	SRC_BUD_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_SOURCE	|	string	|	30	|		|		|		|		|		|	//SRC_ASSMPN_CHG_KEY<br>Table : HDM.BUD_ASMPT_CHANGE<br>Condition: ASSMPN_ID = ACCRUAL_SOURCE_ASSUMPTION_ID_out (From Source)<br>Ouput Column : ASSMPN_CHG_KEY<br>	|		|	BUD_ACCT_ACCRUAL_FND	|	SRC_ASSMPN_CHG_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FUND_CMP	|	string	|	20	|		|		|		|		|		|	//EXPNS_LINE_ITEM_KEY<br>Table : HDM.BUD_EXPENSE_LINE_ITEM<br>Condition: EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out (From Source)<br>Ouput Column :EXPNS_LINE_ITEM_KEY<br>	|		|	BUD_ACCT_ACCRUAL_FND	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FUND_NAME	|	string	|	80	|		|		|		|		|		|	//BUD_FUND_COMPST_KEY<br>Table : HDM.BUD_FUND_COMPOSITE<br>Condition: FUND_COMPST_ID = FUND_CMP (From Source)<br>Ouput Column :BUD_FUND_COMPST_KEY<br>	|		|	BUD_ACCT_ACCRUAL_FND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FUND_ORDER	|	double	|	15	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL_FND	|	CURR_ACCT_ACCRL_FUND_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|		|		|	ltrim(rtrim(ACCRUAL_ID))	|		|	BUD_ACCT_ACCRUAL_FND	|	ACCRL_ID	|	nvarchar2	|	255	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|		|		|	DAILY_ACCRUAL	|		|	BUD_ACCT_ACCRUAL_FND	|	DAILY_ACCRL	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|		|	ORIGINAL_BUDGET	|		|	BUD_ACCT_ACCRUAL_FND	|	ORIG_BUD	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_STATUS	|	string	|	30	|		|		|		|		|		|	REVISED_BUDGET	|		|	BUD_ACCT_ACCRUAL_FND	|	RVSD_BUD	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	NOTES	|	string	|	1000	|		|		|		|		|		|	REMAINING_TO_ACCRUE	|		|	BUD_ACCT_ACCRUAL_FND	|	REMNG_TO_ACCRL	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	DAILY_ACCRUAL	|	double	|	15	|		|		|		|		|		|	FYTD_EXPENSE	|		|	BUD_ACCT_ACCRUAL_FND	|	FYTD_EXPNS	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ORIGINAL_BUDGET	|	double	|	15	|		|		|		|		|		|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|	BUD_ACCT_ACCRUAL_FND	|	EFFTV_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	REVISED_BUDGET	|	double	|	15	|		|		|		|		|		|	"to_date(ACCRUAL_START_DATE,'YYYY-MM-DD')"	|		|	BUD_ACCT_ACCRUAL_FND	|	ACCRL_STRT_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	REMAINING_TO_ACCRUE	|	double	|	15	|		|		|		|		|		|	"to_date(ACCRUE_BY_DATE,'YYYY-MM-DD')"	|		|	BUD_ACCT_ACCRUAL_FND	|	ACCRL_BY_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	FYTD_EXPENSE	|	double	|	15	|		|		|		|		|		|	"to_date(SSB_EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|	BUD_ACCT_ACCRUAL_FND	|	SSB_EFFTV_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|		|	NBR_ACCRUAL_DAYS	|		|	BUD_ACCT_ACCRUAL_FND	|	NBR_ACCRL_DAYS	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUAL_START_DATE	|	string	|	30	|		|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|	BUD_ACCT_ACCRUAL_FND	|	VRSN	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	ACCRUE_BY_DATE	|	string	|	30	|		|		|		|		|		|	Y'	|		|	BUD_ACCT_ACCRUAL_FND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	SSB_EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|		|	sysdate	|		|	BUD_ACCT_ACCRUAL_FND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	NBR_ACCRUAL_DAYS	|	double	|	15	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL_FND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	INSERT_USER	|	string	|	30	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL_FND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	INSERT_DATE	|	string	|	30	|		|		|		|		|		|		|		|	BUD_ACCT_ACCRUAL_FND	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	CURRENT_DAILY_ACCRUAL_ID	|	string	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	CURRENT_DAILY_ACCRUAL_RATE	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	PAYABLE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	EXPENSE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_ACCRUAL	|	VERSION	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
