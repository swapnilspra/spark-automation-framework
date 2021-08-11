|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	COMPST_FUND_KEY	|	decimal	|	10	|		|		|		|	Increment the key value by'1' for each new record	|	Increment the key value by'1' for each new record	|		|	If the record already present for 'COMPST_CD'  then update the existing record	|		|	FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	COMPST_FUND_ID	|	decimal	|	11	|		|		|		|	Default to 'All'	|	'All'	|		|		|		|	ALL_COMPST_CD	|	varchar2	|	10	|		|		|		|
|		|	ASSET_SBCLS_KEY	|	decimal	|	2	|		|		|		|	Default to 'All Composites'	|	'All Composites'	|		|		|		|	ALL_COMPST_DESC	|	varchar2	|	20	|		|		|		|
|		|	FUND_MGR_ROLE_KEY	|	decimal	|	10	|		|		|		|	ASSET_CLS_FULL_NM from Source	|	ASSET_CLS_FULL_NM	|		|		|		|	ASSET_CLS_NM	|	varchar2	|	40	|		|		|		|
|		|	INVMT_STYL_KEY	|	decimal	|	10	|		|		|		|	ASSET_CLS_DESC From Source	|	ASSET_CLS_DESC	|		|		|		|	ASSET_CLS_DESC	|	varchar2	|	255	|		|		|		|
|		|	PERF_CALCULATION_MTHD_KEY	|	decimal	|	10	|		|		|		|	ASSET_SBCLS_FULL_NM From Source	|	ASSET_SBCLS_FULL_NM	|		|		|		|	SUB_CLS_NM	|	varchar2	|	40	|		|		|		|
|		|	COMPST_FUND_FULL_NM	|	string	|	60	|		|		|		|	ASSET_SBCLS_DESC From Source	|	ASSET_SBCLS_DESC	|		|		|		|	SUB_CLS_DESC	|	varchar2	|	255	|		|		|		|
|		|	COMPST_FUND_ABRV_NM	|	string	|	12	|		|		|		|	INVMT_STYL_NM From Source	|	INVMT_STYL_NM	|		|		|		|	STYL_NM	|	varchar2	|	40	|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	INVMT_STYL_DESC From Source	|	INVMT_STYL_DESC	|		|		|		|	STYL_DESC	|	varchar2	|	255	|		|		|		|
|		|	COMPST_ORD	|	double	|	15	|		|		|		|	COMPST_FUND_FULL_NM	|	COMPST_FUND_FULL_NM	|		|		|		|	COMPST_CD	|	"number(p,s)"	|	10	|		|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|	COMPST_FUND_ABRV_NM From Source	|	COMPST_FUND_ABRV_NM	|		|		|		|	COMPST_NM	|	varchar2	|	255	|		|		|		|
|		|	DQ_LVL_CD	|	string	|	3	|		|		|		|	PERF_CALCULATION_MTHD_NM From Source	|	PERF_CALCULATION_MTHD_NM	|		|		|		|	PERF_CD	|	varchar2	|	20	|		|		|		|
|		|	ASSET_SBCLS_FULL_NM	|	string	|	60	|		|		|		|	PERF_CALCULATION_DESC From Source	|	PERF_CALCULATION_DESC	|		|		|		|	PERF_NM	|	varchar2	|	255	|		|		|		|
|		|	ASSET_SBCLS_DESC	|	string	|	255	|		|		|		|	COMPST_ORD From Source	|	COMPST_ORD	|		|		|		|	COMPST_ORD	|	number	|	15	|		|		|		|
|		|	ASSET_CLS_FULL_NM	|	string	|	60	|		|		|		|	FUND_STAT_NM From Source	|	FUND_STAT_NM	|		|		|		|	COMPST_STAT_CD	|	varchar2	|	1	|		|		|		|
|		|	ASSET_CLS_DESC	|	string	|	255	|		|		|		|	INITCAP(FUND_STAT_DESC)	|	INITCAP(FUND_STAT_DESC)	|		|		|		|	COMPST_STAT_DESC	|	varchar2	|	50	|		|		|		|
|		|	INVMT_STYL_NM	|	string	|	60	|		|		|		|	Current Date	|	SYSDATE	|		|		|		|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|	INVMT_STYL_DESC	|	string	|	255	|		|		|		|	ROW_STOP_DTTM From Source	|	ROW_STOP_DTTM	|		|		|		|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|	PERF_CALCULATION_MTHD_NM	|	string	|	60	|		|		|		|	CURR_ROW_FLG From Source	|	CURR_ROW_FLG	|		|		|		|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|		|
|		|	PERF_CALCULATION_DESC	|	string	|	255	|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	FUND_STAT_NM	|	string	|	60	|		|		|		|	4	|	4	|		|		|		|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|	FUND_STAT_DESC	|	string	|	255	|		|		|		|	COMPST_SRC_ID From Source	|	COMPST_SRC_ID	|		|		|		|	COMPST_SRC_ID	|	"number(p,s)"	|	11	|		|		|		|
|		|	COMPST_SRC_ID	|	decimal	|	11	|		|		|		|	FUND_STYL From Source	|	FUND_STYL	|		|		|		|	FUND_STYL	|	varchar2	|	100	|		|		|		|
|		|	FUND_STYL	|	string	|	100	|		|		|		|	RISK_RET_ORD From Source	|	RISK_RET_ORD	|		|		|		|	RISK_RET_ORD	|	number	|	15	|		|		|		|
|		|	RISK_RET_ORD	|	double	|	15	|		|		|		|	COMPST_INCPN_DT From Source	|	COMPST_INCPN_DT	|		|		|		|	COMPST_INCPN_DT	|	date	|	19	|		|		|		|
|		|	COMPST_INCPN_DT	|	date/time	|	29	|		|		|		|	SALES_CAT From Source	|	SALES_CAT	|		|		|		|	SALES_CAT	|	varchar2	|	60	|		|		|		|
|		|	SALES_CAT	|	string	|	60	|		|		|		|	ST_STR_FUND_NBR From Source	|	ST_STR_FUND_NBR	|		|		|		|	ST_STR_FUND_NBR	|	varchar2	|	25	|		|		|		|
|		|	ST_STR_FUND_NBR	|	string	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
