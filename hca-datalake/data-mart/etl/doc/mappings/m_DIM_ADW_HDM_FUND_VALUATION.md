|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	FUND_VLTN_KEY	|	decimal	|	10	|		|		|		|	Increment the key value by'1' for each new record	|	Increment the key value by'1' for each new record	|		|	If the record already present for FUND_KEY = in_FUND_KEY AND DAY_KEY = in_DAY_KEY then update the existing record	|	HDMFUND_VALUATION	|	FUND_VLTN_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|	As per lookup Logic	|	FUND_KEY	|	//FUND_KEY<br>Table: HDM.FUND<br>Condition : FUND_NBR = IN_FUND_NBR<br>Output Column: FUND_KEY	|		|	HDMFUND_VALUATION	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	FUND_PR_TYPE_KEY	|	string	|	3	|		|		|		|	As per lookup Logic	|	DAY_KEY	|	//FUND_KEY<br>Table: HDM.CALENDAR<br>Condition :CAL_DAY = IN_CAL_DAY<br>Output Column: DAY_KEY	|		|	HDMFUND_VALUATION	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	SHR_CLS_FUND_KEY	|	decimal	|	10	|		|		|		|	NAV_AMT from source	|	NAV_AMT	|		|		|	HDMFUND_VALUATION	|	NAV_AMT	|	number	|	15	|		|		|		|
|		|	VLTN_DT	|	date/time	|	29	|		|		|		|	TOT_SHRS from source	|	TOT_SHRS	|		|		|	HDMFUND_VALUATION	|	TOT_SHRS	|	number	|	15	|		|		|		|
|		|	NAV_AMT	|	decimal	|	19	|		|		|		|	PR_PER_SHR from source	|	PR_PER_SHR	|		|		|	HDMFUND_VALUATION	|	PR_PER_SHR	|	number	|	15	|		|		|		|
|		|	TOT_SHR_QTY	|	decimal	|	15	|		|		|		|	WLTH_IDX from Source	|	WLTH_IDX	|		|		|	HDMFUND_VALUATION	|	WLTH_IDX	|	"number(p,s)"	|	27	|		|		|		|
|		|	RT_PER_SHR_AMT	|	decimal	|	19	|		|		|		|	ASOF_PR from Source	|	ASOF_PR	|		|		|	HDMFUND_VALUATION	|	ASOF_PR	|	number	|	15	|		|		|		|
|		|	WLTH_IDX	|	decimal	|	27	|		|		|		|	ASOF_WLTH_IDX from Source	|	ASOF_WLTH_IDX	|		|		|	HDMFUND_VALUATION	|	ASOF_WLTH_IDX	|	"number(p,s)"	|	27	|		|		|		|
|		|	ASOF_PR	|	double	|	15	|		|		|		|	Y'	|	Y'	|		|		|	HDMFUND_VALUATION	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|		|
|		|	ASOF_WLTH_IDX	|	decimal	|	27	|		|		|		|	Default to 'Y'	|	Y'	|		|		|	HDMFUND_VALUATION	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	Current Date	|	SYSDATE	|		|		|	HDMFUND_VALUATION	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|	ROW_STOP_DTTM  from Source	|	ROW_STOP_DTTM	|		|		|	HDMFUND_VALUATION	|	ROW_STOP_DTTM	|	date	|	10	|		|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|	HDMFUND_VALUATION	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	15	|		|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|	Default to '4'	|	4'	|		|		|	HDMFUND_VALUATION	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|	DAILY_PR_DIFF  from Source	|	DAILY_PR_DIFF	|		|		|	HDMFUND_VALUATION	|	DAILY_PR_DIFF	|	number	|	15	|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|	HSG_PR_REVS  from Source	|	HSG_PR_REVS	|		|		|	HDMFUND_VALUATION	|	HSG_PR_REVS	|	number	|	15	|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SHR_CLS_FUND_KEY1	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DAILY_PR_DIFF	|	decimal	|	19	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	HSG_PR_REVS	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
