|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	FUND_DIVD_KEY	|	decimal	|	10	|		|		|		|	Increment the key value by'1' for each new record	|	Increment the key value by'1' for each new record	|		|	If the record already present for 'COMPST_CD'  then update the existing record	|		|	FUND_DIVD_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|	As per Lookup Logic	|	FUND_KEY	|	//FUND_KEY<br>Table : HDM.FUND<br>Condition : FUND_NBR = IN_FUND_NBR <br>Output Column: FUND_KEY	|		|		|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	SHR_CLS_FUND_KEY	|	decimal	|	10	|		|		|		|	As per Lookup Logic	|	REC_DAY_KEY	|	//REC_DAY_KEY<br>Table :HDM.CALENDAR<br>Condition : CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	REC_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	DIVD_REC_DT	|	date/time	|	29	|		|		|		|	As per Lookup Logic	|	REINVEST_DAY_KEY	|	//REC_DAY_KEY<br>Table :HDM.CALENDAR<br>Condition : CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	REINVEST_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	DIVD_REINVEST_DT	|	date/time	|	29	|		|		|		|	As per Lookup Logic	|	PAYBL_DAY_KEY	|	//REC_DAY_KEY<br>Table :HDM.CALENDAR<br>Condition : CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|		|	PAYBL_DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	PAYBL_DT	|	date/time	|	29	|		|		|		|	TOT_DIVD From Source	|	TOT_DIVD	|		|		|		|	TOT_DIVD	|	number	|	15	|		|		|		|
|		|	NET_INVMT_INCM	|	double	|	15	|		|		|		|	NET_INVMT_INCM From Source	|	NET_INVMT_INCM	|		|		|		|	NET_INVMT_INCM	|	number	|	15	|		|		|		|
|		|	SHRS_OF_REC	|	double	|	15	|		|		|		|	SHRS_OF_REC From Source	|	SHRS_OF_REC	|		|		|		|	SHRS_OF_REC	|	number	|	15	|		|		|		|
|		|	REINVEST_PR	|	double	|	15	|		|		|		|	RE_INVST_PR From Source	|	RE_INVST_PR	|		|		|		|	RE_INVST_PR	|	number	|	15	|		|		|		|
|		|	ORDN_INCM	|	double	|	15	|		|		|		|	ORDN_INCM	|	ORDN_INCM	|		|		|		|	ORDN_INCM	|	number	|	15	|		|		|		|
|		|	TOT_SHRT_TERM_GAINS	|	double	|	15	|		|		|		|	TOT_SHRT_TERM_GAINS From Source	|	TOT_SHRT_TERM_GAINS	|		|		|		|	TOT_SHRT_TERM_GAINS	|	number	|	15	|		|		|		|
|		|	TOT_MID_TERM_GAINS	|	double	|	15	|		|		|		|	TOT_MID_TERM_GAINS From Source	|	TOT_MID_TERM_GAINS	|		|		|		|	TOT_MID_TERM_GAINS	|	number	|	15	|		|		|		|
|		|	TOT_LONG_TERM_GAINS	|	double	|	15	|		|		|		|	TOT_LONG_TERM_GAINS From Source	|	TOT_LONG_TERM_GAINS	|		|		|		|	TOT_LONG_TERM_GAINS	|	number	|	15	|		|		|		|
|		|	SHRT_TERM_CAP_GAINS	|	double	|	15	|		|		|		|	SHRT_TERM_CAP_GAINS From Source	|	SHRT_TERM_CAP_GAINS	|		|		|		|	SHRT_TERM_CAP_GAINS	|	number	|	15	|		|		|		|
|		|	MID_TERM_CAP_GAINS	|	double	|	15	|		|		|		|	MID_TERM_CAP_GAINS From Source	|	MID_TERM_CAP_GAINS	|		|		|		|	MID_TERM_CAP_GAINS	|	number	|	15	|		|		|		|
|		|	LONG_TERM_CAP_GAINS	|	double	|	15	|		|		|		|	LONG_TERM_CAP_GAINS From Source	|	LONG_TERM_CAP_GAINS	|		|		|		|	LONG_TERM_CAP_GAINS	|	number	|	15	|		|		|		|
|		|	NON_TXBL_DISCNT	|	double	|	15	|		|		|		|	NON_TXBL_DISCNT From Source	|	NON_TXBL_DISCNT	|		|		|		|	NON_TXBL_DISCNT	|	number	|	15	|		|		|		|
|		|	TOT_DIVIDEND	|	double	|	15	|		|		|		|	FORGN_TAX_CR From Source	|	FORGN_TAX_CR	|		|		|		|	FORGN_TAX_CR	|	number	|	15	|		|		|		|
|		|	FORGN_TAX_CR	|	double	|	15	|		|		|		|	RE_INVST_IDX From Source	|	RE_INVST_IDX	|		|		|		|	RE_INVST_IDX	|	number	|	15	|		|		|		|
|		|	REINVEST_IDX	|	double	|	15	|		|		|		|	EST_DIVD_FLG From Source	|	EST_DIVD_FLG	|		|		|		|	EST_DIVD_FLG	|	varchar2	|	1	|		|		|		|
|		|	EST_DIVD_FLG	|	string	|	1	|		|		|		|	Default to 'Y'	|	'Y'	|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	Current Date	|	SYSDATE	|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|	ROW_STOP_DTTM  From Source	|	ROW_STOP_DTTM	|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|	Default to '4'	|	4	|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SHR_CLS_FUND_KEY1	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
