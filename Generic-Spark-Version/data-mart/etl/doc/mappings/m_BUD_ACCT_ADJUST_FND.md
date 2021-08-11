|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	Updaye if record already present	|		|		|		|		|		|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	ADJUSTMENT_ID	|	string	|	100	|		|		|		|	SEQ Number	|		|	Table Name: HDM.BUD_ACCT_ADJUST_FND<br>Condition: ADJMT_ID = ADJUSTMENT_ID_out<br>Output COlumn: ACCT_ADJMT_KEY	|		|		|	BUD_ACCT_ADJUST_FND	|	ACCT_ADJMT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	FUND_CMP	|	string	|	20	|		|		|		|		|		|	Table Name: HDM.BUD_FUND_COMPOSITE <br>Condition: FUND_COMPST_ID = FUND_CMP_out<br>Output COlumn: BUD_FUND_COMPST_KEY 	|		|		|	BUD_ACCT_ADJUST_FND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	FUND_NAME	|	string	|	80	|		|		|		|		|		|	Table Name: HDM.BUDGET<br>Condition: BUD_ID = SOURCE_BUDGET_ID_out <br>Output COlumn: BUD_KEY	|		|		|	BUD_ACCT_ADJUST_FND	|	BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	FUND_ORDER	|	double	|	15	|		|		|		|		|		|	Table Name: HDM.BUD_EXPENSE_LINE_ITEM<br>Condition: EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out <br>Output COlumn: EXPNS_LINE_ITEM_KEY 	|		|		|	BUD_ACCT_ADJUST_FND	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_ASMPT_CHANGE <br>Condition: ASSMPN_ID = SOURCE_ASSUMPTION_ID_out<br>Output COlumn: ASSMPN_CHG_KEY	|		|		|	BUD_ACCT_ADJUST_FND	|	ASSMPN_CHG_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = EXPENSE_ACCOUNT_ID_out<br>Output COlumn: BUD_ACCT_KEY	|		|		|	BUD_ACCT_ADJUST_FND	|	EXPNS_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	EXPENSE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = PAYABLE_ACCOUNT_ID_out<br>Output COlumn: BUD_ACCT_KEY	|		|		|	BUD_ACCT_ADJUST_FND	|	PAYBL_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	PAYABLE_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = DEBIT_ACCOUNT_ID_out<br>Output COlumn: BUD_ACCT_KEY	|		|		|	BUD_ACCT_ADJUST_FND	|	DBT_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	DEBIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|	Table Name: HDM.BUD_ACCOUNT<br>Condition: ACCT_ID = CREDIT_ACCOUNT_ID_out<br>Output COlumn: BUD_ACCT_KEY	|		|		|	BUD_ACCT_ADJUST_FND	|	CR_ACCT_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	CREDIT_ACCOUNT_ID	|	string	|	30	|		|		|		|		|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	CAT_ACCRL_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	FISCAL_YEAR	|	double	|	15	|		|		|		|		|	ltrim(rtrim(ADJUSTMENT_ID))	|		|		|		|	BUD_ACCT_ADJUST_FND	|	ADJMT_ID	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	ADJUSTMENT_AMOUNT	|	double	|	15	|		|		|		|	FISCAL_YEAR	|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	FISCL_YR	|	number	|	15	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	ACTIVITY_STATUS	|	string	|	30	|		|		|		|	ADJUSTMENT_AMOUNT	|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	ADJMT_AMT	|	number	|	15	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	NOTES	|	string	|	4000	|		|		|		|	ACTIVITY_STATUS	|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	ACT_STAT	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	EFFECTIVE_DATE	|	string	|	30	|		|		|		|	NOTES	|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	NTES	|	nvarchar2	|	4000	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	SSB_EFFECTIVE_DATE	|	string	|	30	|		|		|		|		|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_ACCT_ADJUST_FND	|	EFFTV_DT	|	date	|	19	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	INSERT_TS	|	string	|	30	|		|		|		|		|	"to_date(SSB_EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_ACCT_ADJUST_FND	|	SSB_EFFTV_DT	|	date	|	19	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	INSERT_USER	|	string	|	30	|		|		|		|	ADJUSTMENT_TYPE	|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	ADJMT_TYPE	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	VERSION	|	double	|	15	|		|		|		|	INSERT_USER	|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	INSR_USR	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	SOURCE_ASSUMPTION_ID	|	string	|	100	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TS)/1000)"	|		|		|		|	BUD_ACCT_ADJUST_FND	|	INSR_TS	|	date	|	19	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	SOURCE_BUDGET_ID	|	string	|	100	|		|		|		|	VERSION	|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	VRSN	|	number	|	15	|		|		|
|	VW_BDGT_ACCT_ADJUSTMENT	|	ADJUSTMENT_TYPE	|	string	|	30	|		|		|		|		|	Y'	|		|		|		|	BUD_ACCT_ADJUST_FND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	BUD_ACCT_ADJUST_FND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_ACCT_ADJUST_FND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
