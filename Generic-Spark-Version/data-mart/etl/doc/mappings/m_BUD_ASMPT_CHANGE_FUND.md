|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update, if record alredy present else insert it"	|		|		|		|		|		|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	ASSUMPTION_ID	|	string	|	100	|		|		|		|		|		|	Table Name: HDM.BUD_ASMPT_CHANGE_FUND<br>Condition: ASSMPN_CHG_KEY = ASSMPN_CHG_KEY1 AND BUD_FUND_COMPST_KEY = BUD_FUND_COMPST_KEY1<br>Output Column: ASSMPN_CHG_FUND_KEY	|		|		|	BUD_ASMPT_CHANGE_FUND	|	ASSMPN_CHG_FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	LINE_ITEM_ID	|	string	|	100	|		|		|		|		|		|	<br><br>Table Name: HDM.BUD_FUND_COMPOSITE<br>Condition: FUND_COMPST_ID = FUND_CMP_out<br>Output Column: BUD_FUND_COMPST_KEY<br>	|		|		|	BUD_ASMPT_CHANGE_FUND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	DATA_SOURCE_ID	|	string	|	100	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	FUND_CMP	|	string	|	20	|		|		|		|		|		|	<br>Table Name: HDM.BUD_ASMPT_CHANGE <br>Condition: ASSMPN_ID = ASSUMPTION_ID_out<br>Output Column: ASSMPN_CHG_KEY	|		|		|	BUD_ASMPT_CHANGE_FUND	|	ASSMPN_CHG_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	FUND_NAME	|	string	|	80	|		|		|		|		|		|	<br>Table Name: HDM.BUD_EXPENSE_LINE_ITEM<br>Condition: EXPNS_LINE_ITEM_ID = LINE_ITEM_ID_out<br>Output Column: EXPNS_LINE_ITEM_KEY	|		|		|	BUD_ASMPT_CHANGE_FUND	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	FUND_ORDER	|	double	|	15	|		|		|		|		|		|	<br>Table Name: HDM.BUD_DATA_SOURCE_DIM<br>Condition: DATA_SRC_ID = DATA_SOURCE_ID_out <br>Output Column: DATA_SRC_DIM_KEY	|		|		|	BUD_ASMPT_CHANGE_FUND	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	RATE	|	double	|	15	|		|		|		|	RATE	|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	RT	|	number	|	15	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	ALLOCATION	|	double	|	15	|		|		|		|	ALLOCATION	|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ALLOCN	|	number	|	15	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	DATA_SOURCE_VALUE	|	double	|	15	|		|		|		|	DATA_SOURCE_VALUE	|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	DATA_SRC_VAL	|	number	|	15	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	NOTES	|	string	|	1000	|		|		|		|	NOTES	|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ASSMPN_CHG_PER_FUND_NTES	|	nvarchar2	|	255	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	ADJUSTMENT	|	double	|	15	|		|		|		|	ADJUSTMENT	|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ADJMT	|	number	|	15	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	RESIDUAL_VALUE	|	double	|	15	|		|		|		|	RESIDUAL_VALUE	|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	RSDL_VAL	|	number	|	15	|		|		|
|	VW_BDGT_ASSUMPTION_CHNG_FUND	|	VERSION	|	double	|	15	|		|		|		|		|	"iif(isnull(VERSION),0,VERSION)"	|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	VRSN	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|	Only for Update	|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|	ASSMPN_CHG_FUND_KEY	|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ASSMPN_CHG_FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	ASSMPN_CHG_FUND_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	BUD_FUND_COMPST_KEY	|	decimal	|	10	|		|		|		|		|		|	//BUD_KEY<br>Table Name: HDM.BUDGET<br>Condition: BUD_ID = BUDGET_ID <br>Output Column: BUD_KEY<br><br>//BUDGET_ID<br>Table Name: BDGT.VW_BDGT_BUDGET<br>Condition: ASSUMPTION_CHANGE_IDS = ASSMPN_ID<br>Output Column: BUDGET_ID<br><br>//ASSMPN_ID <br>Table Name: HDM.BUD_ASMPT_CHANGE<br>COndition: ASSMPN_CHG_KEY = ASSMPN_CHG_KEY1 <br>Output COlumn: ASSMPN_ID	|		|		|	BUD_ASMPT_CHANGE_FUND	|	BUD_KEY	|	"number(p,s)"	|	10	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	BUD_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ASSMPN_CHG_KEY	|	"number(p,s)"	|	10	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	ASSMPN_CHG_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	EXPNS_LINE_ITEM_KEY	|	"number(p,s)"	|	10	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	EXPNS_LINE_ITEM_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	DATA_SRC_DIM_KEY	|	"number(p,s)"	|	10	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	DATA_SRC_DIM_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	RT	|	number	|	15	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	RT	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ALLOCN	|	number	|	15	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	ALLOCN	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	DATA_SRC_VAL	|	number	|	15	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	DATA_SRC_VAL	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ASSMPN_CHG_PER_FUND_NTES	|	nvarchar2	|	255	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	ASSMPN_CHG_PER_FUND_NTES	|	nstring	|	1000	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ADJMT	|	number	|	15	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	ADJMT	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	RSDL_VAL	|	number	|	15	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	RSDL_VAL	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	VRSN	|	number	|	15	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	VRSN	|	double	|	15	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	CURR_ROW_FLG	|	nstring	|	1	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	BUD_ASMPT_CHANGE_FUND	|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	BUD_ASMPT_CHANGE_FUND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
