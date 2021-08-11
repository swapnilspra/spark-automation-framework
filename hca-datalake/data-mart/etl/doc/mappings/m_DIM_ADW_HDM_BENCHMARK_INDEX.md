|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	refer SQ	|	IDX_KEY	|	decimal	|	10	|		|		|		|	Auto Increment the key value by '1' for each new record	|	//For New Record<br>Use sequence generator for auo increment key value	|		|	Table Name: HDM.BENCHMARK_INDEX <br>Condition : IDX_ID = IDX_ID1<br>Output Column : BMK_IDX_KEY	|		|	BMK_IDX_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|	IDX_ID from Source	|	IDX_ID	|		|		|		|	IDX_ID	|	varchar2	|	25	|		|		|		|
|		|	IDX_TYPE_KEY	|	decimal	|	10	|		|		|		|	IDX_NM from Source	|	IDX_NM	|		|	Update the records if already exists	|		|	IDX_NM	|	varchar2	|	150	|		|		|		|
|		|	IDX_ID	|	string	|	25	|		|		|		|	IDX_SHRT_NM from Source	|	IDX_SHRT_NM	|		|		|		|	IDX_SHRT_NM	|	varchar2	|	12	|		|		|		|
|		|	IDX_NM	|	string	|	150	|		|		|		|	ROBECO_IDX_ID from Source	|	ROBECO_IDX_ID	|		|		|		|	ROBECO_IDX_ID	|	number	|	15	|		|		|		|
|		|	IDX_ABRV_NM	|	string	|	12	|		|		|		|	BLOOMBERG_SYM	|	BLOOMBERG_SYM	|		|		|		|	BLOOMBERG_SYM	|	varchar2	|	25	|		|		|		|
|		|	PBL_ID	|	string	|	25	|		|		|		|	IDX_TYPE	|	IDX_TYPE	|		|		|		|	IDX_TYPE	|	number	|	15	|		|		|		|
|		|	ROBECO_IDX_ID	|	decimal	|	11	|		|		|		|	IDX_TYPE_DESC	|	IDX_TYPE_DESC	|		|		|		|	IDX_TYPE_DESC	|	varchar2	|	60	|		|		|		|
|		|	BLOOMBERG_SYM	|	string	|	25	|		|		|		|	BMK_LEGACY_ID	|	BMK_LEGACY_ID	|		|		|		|	BMK_LEGACY_ID	|	number	|	15	|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	'Y'	|	'Y'	|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|	sysdate	|	sysdate	|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|	4	|	4	|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	IDX_TYPE_DESC	|	string	|	60	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	IDX_TYPE_CD	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
