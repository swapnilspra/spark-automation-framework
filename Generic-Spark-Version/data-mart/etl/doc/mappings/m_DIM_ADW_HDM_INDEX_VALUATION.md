|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	IDX_VLTN_KEY	|	decimal	|	10	|		|		|		|	Increment the key value by'1' for each new record	|	Increment the key value by'1' for each new record	|		|	If the record already present for BMK_IDX_Key AND DAY_KEY then update the existing record	|	INDEX_VALUATION	|	IDX_VLTN_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|	As per lookup logic	|	BMK_IDX_KEY	|	Table Name : HDM.BENCHMARK_INDEX<br>Condition: IDX_ID = in_IDX_ID<br>Output Column : BMK_IDX_KEY	|		|	INDEX_VALUATION	|	BMK_IDX_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	IDX_KEY	|	decimal	|	10	|		|		|		|	As per lookup logic	|	DAY_KEY	|	Table Name : HDM.CALENDAR<br>Condition: CAL_DAY = IN_CAL_DAY<br>Output Column : DAY_KEY	|		|	INDEX_VALUATION	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	VLTN_DT	|	date/time	|	29	|		|		|		|	IDX_VLTN_AMT from Source	|	IDX_VLTN_AMT	|		|		|	INDEX_VALUATION	|	IDX_VLTN_AMT	|	number	|	15	|		|		|		|
|		|	IDX_VAL_AMT	|	decimal	|	19	|		|		|		|	1*v_PREV_WLTH_IDX*(DAILY_RET + 1)<br><br>//v_PREV_WLTH_IDX<br>if PREV_WLTH_IDX is not valid then '1'	|	"1*v_PREV_WLTH_IDX*(DAILY_RET + 1)<br><br>//v_PREV_WLTH_IDX<br>IIF(ISNULL(PREV_WLTH_IDX),1,PREV_WLTH_IDX)"	|		|		|	INDEX_VALUATION	|	WLTH_IDX	|	number	|	15	|		|		|		|
|		|	IDX_ID	|	string	|	25	|		|		|		|	DAILY_RET from source	|	DAILY_RET	|		|		|	INDEX_VALUATION	|	DAILY_RET	|	number	|	15	|		|		|		|
|		|	PREV_VLTN_DT	|	date/time	|	29	|		|		|		|	Default to 'Y'	|	'Y'	|		|		|	INDEX_VALUATION	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|	Current date	|	SYSDATE	|		|		|	INDEX_VALUATION	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|	INDEX_VALUATION	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|	4	|	4	|		|		|	INDEX_VALUATION	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	DAILY_RET	|	decimal	|	10	|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|	INDEX_VALUATION	|	ETL_LOAD_CYC_KEY	|	Varchar2	|	1	|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|	Default to '4	|	4	|		|		|	INDEX_VALUATION	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
