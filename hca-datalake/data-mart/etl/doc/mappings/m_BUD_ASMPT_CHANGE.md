|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	VW_BDGT_ASSUMPTION_CHANGE	|	ASSUMPTION_ID	|	string	|	100	|		|		|		|		|	Use sequence generator to generate the key value	|		|		|	BUD_ASMPT_CHANGE	|	ASSMPN_CHG_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	ADJUSTMENT_TYPE	|	string	|	20	|		|		|		|	direct move of 'ADJUSTMENT_TYPE' from Source	|	ADJUSTMENT_TYPE	|		|		|		|	ADJMT_TYPE	|	nvarchar2	|	25	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	ALLOCATION_TYPE	|	string	|	30	|		|		|		|	direct move of 'CHANGE_AMOUNT' from Source	|	CHANGE_AMOUNT	|		|		|		|	CHG_AMT	|	number	|	15	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	CHANGE_AMOUNT	|	double	|	15	|		|		|		|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|		|	EFFTV_DT	|	date	|	19	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	EFFECTIVE_DATE	|	string	|	20	|		|		|		|	direct move of 'FISCAL_YEAR' from source	|	FISCAL_YEAR	|		|		|		|	FISCL_YR	|	number	|	15	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	EXPENSE_CATEGORY	|	string	|	30	|		|		|		|	"iif(isnull(EDITABLE),0,iif(lower(EDITABLE)='false',0,1))"	|	"iif(isnull(EDITABLE),0,iif(lower(EDITABLE)='false',0,1))"	|		|		|		|	EDITBL_FLG	|	"number(p,s)"	|	38	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	FISCAL_YEAR	|	double	|	15	|		|		|		|	direct move of 'NOTES' from Source	|	NOTES	|		|		|		|	ASSMPN_CHG_NTES	|	nvarchar2	|	255	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|	remove empty space on 'ASSUMPTION_ID'	|	ltrim(rtrim(ASSUMPTION_ID))	|		|		|		|	ASSMPN_ID	|	nvarchar2	|	255	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	EDITABLE	|	string	|	5	|		|		|		|	LIABILITY_ADJUSTMENT_BUDGET_ID	|	LIABILITY_ADJUSTMENT_BUDGET_ID	|		|		|		|	LIAB_ADJ_BUD_ID	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|	direct move of 'INSERT_USER' from Source	|	INSERT_USER	|		|		|		|	INSR_USR	|	nvarchar2	|	30	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	NOTES	|	string	|	1000	|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	INSR_TS	|	date	|	19	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	VERSION	|	double	|	15	|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	VRSN	|	number	|	15	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	LIABILITY_ADJUSTMENT_BUDGET_ID	|	string	|	30	|		|		|		|	Default to 'Y'	|	'Y'	|		|		|		|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	INSERT_USER	|	string	|	30	|		|		|		|	sysdate	|	sysdate	|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|	VW_BDGT_ASSUMPTION_CHANGE	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|	remove empty space on 'DATA_SOURCE_ID'	|	ltrim(rtrim(DATA_SOURCE_ID))	|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	Update	|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|	Table: HDM.BUD_ASMPT_CHANGE<br>Condition : ASSMPN_ID = ASSUMPTION_ID_out (from source)<br>Output Column : ASSMPN_CHG_KEY	|	Update the records if exits	|	BUD_ASMPT_CHANGE	|	ASSMPN_CHG_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|	direct move of 'ADJUSTMENT_TYPE' from Source	|	ADJUSTMENT_TYPE	|		|		|		|	ADJMT_TYPE	|	nvarchar2	|	25	|		|		|		|
|		|		|		|		|		|		|		|	direct move of 'CHANGE_AMOUNT' from Source	|	CHANGE_AMOUNT	|		|		|		|	CHG_AMT	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|	"to_date(EFFECTIVE_DATE,'YYYY-MM-DD')"	|		|		|		|	EFFTV_DT	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|	direct move of 'FISCAL_YEAR' from source	|	FISCAL_YEAR	|		|		|		|	FISCL_YR	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	"iif(isnull(EDITABLE),0,iif(lower(EDITABLE)='false',0,1))"	|	"iif(isnull(EDITABLE),0,iif(lower(EDITABLE)='false',0,1))"	|		|		|		|	EDITBL_FLG	|	"number(p,s)"	|	38	|		|		|		|
|		|		|		|		|		|		|		|	direct move of 'NOTES' from Source	|	NOTES	|		|		|		|	ASSMPN_CHG_NTES	|	nvarchar2	|	255	|		|		|		|
|		|		|		|		|		|		|		|	remove empty space on 'ASSUMPTION_ID'	|	ltrim(rtrim(ASSUMPTION_ID))	|		|		|		|	ASSMPN_ID	|	nvarchar2	|	255	|		|		|		|
|		|		|		|		|		|		|		|	LIABILITY_ADJUSTMENT_BUDGET_ID	|	LIABILITY_ADJUSTMENT_BUDGET_ID	|		|		|		|	LIAB_ADJ_BUD_ID	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	direct move of 'INSERT_USER' from Source	|	INSERT_USER	|		|		|		|	INSR_USR	|	nvarchar2	|	30	|		|		|		|
|		|		|		|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	INSR_TS	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	VRSN	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	Default to 'Y'	|	'Y'	|		|		|		|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|	sysdate	|	sysdate	|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|	remove empty space on 'DATA_SOURCE_ID'	|	ltrim(rtrim(DATA_SOURCE_ID))	|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
