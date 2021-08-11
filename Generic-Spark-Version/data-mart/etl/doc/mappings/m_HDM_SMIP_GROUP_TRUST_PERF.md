|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	GROUP_TRUST_PERF	|	GRP_TRST_PERF_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	Insert Only	|		|	SMIP_GROUP_TRUST_PERF	|	GRP_TRST_PERF_KEY	|	"number(p,s)"	|	10	|		|		|
|	GROUP_TRUST_PERF	|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	":LKP.LKP_SMIP_METRICS(TM_PER)<br><br>Override Query: SELECT SMIP_METRICS.SMIP_MTRIC_KEY as SMIP_MTRIC_KEY, SMIP_METRICS.PER_NM as PER_NM FROM HDM.SMIP_METRICS SMIP_METRICS<br>WHERE <br>SMIP_METRICS.MTRIC_CD = 'GRT' AND SMIP_METRICS.CURR_ROW_FLG = 'Y'<br>Condition: PER_NM = PER_NM_IN<br>Output Column: SMIP_MTRIC_KEY"	|		|		|	SMIP_GROUP_TRUST_PERF	|	SMIP_MTRIC_KEY	|	"number(p,s)"	|	10	|		|		|
|	GROUP_TRUST_PERF	|	TM_PER	|	string	|	10	|		|		|		|		|		|	":LKP.LKP_DAY_KEY(PERF_DT)<br><br>Overrirde Query: SELECT CALENDAR.DAY_KEY as DAY_KEY, CALENDAR.CAL_DAY as CAL_DAY FROM HDM.CALENDAR CALENDAR<br>WHERE <br>CALENDAR.CURR_ROW_FLG = 'Y' AND CALENDAR.BUS_DAY_FLG = 'Y'<br>Condition: CAL_DAY = CAL_DAY_IN<br>Output Column: Day_Key"	|		|		|	SMIP_GROUP_TRUST_PERF	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|	GROUP_TRUST_PERF	|	PERF_DT	|	date/time	|	29	|		|		|		|	GRP_TRST_PERF	|		|		|		|		|	SMIP_GROUP_TRUST_PERF	|	GRP_TRST_PERF	|	number	|	15	|		|		|
|	GROUP_TRUST_PERF	|	GRP_TRST_PERF	|	double	|	15	|		|		|		|	IDX_PERF	|		|		|		|		|	SMIP_GROUP_TRUST_PERF	|	BMK_PERF	|	number	|	15	|		|		|
|	GROUP_TRUST_PERF	|	IDX_PERF	|	double	|	15	|		|		|		|		|	(GRP_TRST_PERF - IDX_PERF)	|		|		|		|	SMIP_GROUP_TRUST_PERF	|	PERF_DIFF	|	number	|	15	|		|		|
|	GROUP_TRUST_PERF	|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|	Y'	|		|		|		|	SMIP_GROUP_TRUST_PERF	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|	GROUP_TRUST_PERF	|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|	SYSDATE	|		|		|		|	SMIP_GROUP_TRUST_PERF	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|	GROUP_TRUST_PERF	|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|		|	SMIP_GROUP_TRUST_PERF	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|	GROUP_TRUST_PERF	|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|	SMIP_GROUP_TRUST_PERF	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|	GROUP_TRUST_PERF	|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|	ADW'	|		|		|		|	SMIP_GROUP_TRUST_PERF	|	SRC_SYS_ID	|	number	|	15	|		|		|
|	GROUP_TRUST_PERF	|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	GROUP_TRUST_PERF	|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
