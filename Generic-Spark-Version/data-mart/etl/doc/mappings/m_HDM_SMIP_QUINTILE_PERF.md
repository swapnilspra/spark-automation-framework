|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	QNTL_PERF_KEY	|	decimal	|	10	|		|		|		|	Seq Number	|		|		|	Inser Only	|		|		|	SMIP_QUNITILE_PERF_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|	"IIF(TM_PER = 'INCP',<br>:LKP.LKP_SMIP_METRICS_NEW_FUND(TM_PER), :LKP.LKP_SMIP_METRICS(TM_PER))"	|	"LKP.LKP_SMIP_METRICS_NEW_FUND<br><br>Override Quer:SELECT SMIP_METRICS.SMIP_MTRIC_KEY as SMIP_MTRIC_KEY, SMIP_METRICS.PER_NM as PER_NM FROM HDM.SMIP_METRICS SMIP_METRICS<br>WHERE <br>SMIP_METRICS.PER_NM = 'INCP' <br>AND SMIP_METRICS.MTRIC_CD = 'NFQ' <br>AND SMIP_METRICS.CURR_ROW_FLG = 'Y'<br>Condition: PER_NM = PER_NM_IN<br>Output Coulumn:SMIP_MTRIC_KEY<br><br>//:LKP.LKP_SMIP_METRICS<br><br><br>Override Query : SELECT SMIP_METRICS.SMIP_MTRIC_KEY as SMIP_MTRIC_KEY, SMIP_METRICS.PER_NM as PER_NM FROM HDM.SMIP_METRICS SMIP_METRICS<br>WHERE <br>SMIP_METRICS.MTRIC_CD = 'QNT' AND SMIP_METRICS.CURR_ROW_FLG = 'Y'<br>Condition: PER_NM = PER_NM_IN<br>Output Coulumn: SMIP_MTRIC_KEY<br>"	|		|		|		|	SMIP_MTRIC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SHR_CLS_FUND_KEY	|	decimal	|	10	|		|		|		|		|		|	:LKP.LKP_FUND_KEY(SHR_CLS_FUND_KEY)<br><br><br>Table Name:HDM.FUND<br>Condition: FUND_NBR = FUND_NBR_IN<br>Output Coulumn: FUND_KEY	|		|		|		|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	IDX_KEY	|	decimal	|	10	|		|		|		|		|		|	":LKP.LKP_BENCHMARK_IDX_KEY(IDX_ID), <br><br><br>Table Name:HDM.BENCHMARK_INDEX<br>Condition:  IDX_ID = IDX_ID_IN<br>Output Coulumn:BMK_IDX_KEY"	|		|		|		|	BMK_IDX_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	STRT_DT	|	date/time	|	29	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(END_DT)<br><br><br>Table Name:<br>Condition: <br>Output Coulumn	|		|		|		|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	END_DT	|	date/time	|	29	|		|		|		|	FUND__RET_OUT	|		|		|		|		|		|	FUND_RET	|	char	|	18	|		|		|
|		|	TM_PER	|	string	|	10	|		|		|		|	IDX_RET	|		|		|		|		|		|	IDX_RET	|	number	|	15	|		|		|
|		|	IDX_ID	|	string	|	10	|		|		|		|	FUND_RANK	|		|		|		|		|		|	FUND_PEER_GRP_RANK	|	number	|	15	|		|		|
|		|	FUND__RET	|	double	|	15	|		|		|		|	IDX_RANK	|		|		|		|		|		|	IDX_PEER_GRP_RANK	|	number	|	15	|		|		|
|		|	FUND_RANK	|	double	|	15	|		|		|		|	FUND_QNTL_RANK	|		|		|		|		|		|	FUND_PEER_GRP_QNTL	|	number	|	15	|		|		|
|		|	FUND_QNTL_RANK	|	double	|	15	|		|		|		|	IDX_QNTL_RANK	|		|		|		|		|		|	IDX_PEER_GRP_QNTL	|	number	|	15	|		|		|
|		|	IDX_RET	|	double	|	15	|		|		|		|	GRP_RET_AVG	|		|		|		|		|		|	PEER_GRP_AVG	|	number	|	15	|		|		|
|		|	IDX_RANK	|	double	|	15	|		|		|		|	GRP_POP_CNT	|		|		|		|		|		|	PEER_GRP_CNT	|	number	|	15	|		|		|
|		|	IDX_QNTL_RANK	|	double	|	15	|		|		|		|		|	Y'	|		|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	GRP_RET_AVG	|	double	|	15	|		|		|		|		|	SYSDATE	|		|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	GRP_POP_CNT	|	double	|	15	|		|		|		|		|		|		|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SRC_SYS_DEL_FLG	|	string	|	1	|		|		|		|		|		|		|	SRC_SYS_USERID	|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	SRC_SYS_USERID	|	string	|	12	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	ETL_LOAD_CYC_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SRC_SYS_ID	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
