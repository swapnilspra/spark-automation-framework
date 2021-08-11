|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	"Update if data already present, else inert"	|		|		|		|		|		|		|		|
|	VW_BDGT_FUND	|	FUND_CMP	|	string	|	20	|		|		|		|		|		|	Table Name: HDM.BUD_FUND_COMPOSITE<br>Condition: FUND_COMPST_ID = FUND_CMP_out<br>Output Column: BUD_FUND_COMPST_KEY	|		|		|	BUD_FUND_COMPOSITE	|	BUD_FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|	VW_BDGT_FUND	|	ASSET_TYPE	|	string	|	20	|		|		|		|		|	ltrim(rtrim(ASSET_TYPE))	|		|		|		|	BUD_FUND_COMPOSITE	|	ASSET_TYPE	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_FUND	|	ASSET_TYPE_DESCRIPTION	|	string	|	20	|		|		|		|		|	ltrim(rtrim(ASSET_TYPE_DESCRIPTION))	|		|		|		|	BUD_FUND_COMPOSITE	|	ASSET_TYPE_DESC	|	nvarchar2	|	255	|		|		|
|	VW_BDGT_FUND	|	INCEPTION_DATE	|	string	|	20	|		|		|		|		|	ltrim(rtrim(FUND_NAME))	|		|		|		|	BUD_FUND_COMPOSITE	|	FUND_NM	|	nvarchar2	|	100	|		|		|
|	VW_BDGT_FUND	|	FUND_NAME	|	string	|	80	|		|		|		|	FUND_ORDER	|		|		|		|		|	BUD_FUND_COMPOSITE	|	FUND_ORD	|	number	|	15	|		|		|
|	VW_BDGT_FUND	|	FUND_ORDER	|	double	|	15	|		|		|		|		|	ltrim(rtrim(SSB_CODE))	|		|		|		|	BUD_FUND_COMPOSITE	|	SSB_CD	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_FUND	|	SSB_CODE	|	string	|	5	|		|		|		|		|	ltrim(rtrim(SSB_SHORT_CODE))	|		|		|		|	BUD_FUND_COMPOSITE	|	SSB_SHRT_CD	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_FUND	|	SSB_SHORT_CODE	|	string	|	5	|		|		|		|		|	ltrim(rtrim(SUBSIDIARY_OF))	|		|		|		|	BUD_FUND_COMPOSITE	|	PARN_COMPST_ID	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_FUND	|	CLOSED_DATE	|	string	|	20	|		|		|		|		|	ltrim(rtrim(FUND_CMP))	|		|		|		|	BUD_FUND_COMPOSITE	|	FUND_COMPST_ID	|	nvarchar2	|	25	|		|		|
|	VW_BDGT_FUND	|	SUBSIDIARY_OF	|	string	|	20	|		|		|		|		|	"to_date(INCEPTION_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_FUND_COMPOSITE	|	INCPN_DT	|	date	|	19	|		|		|
|	VW_BDGT_FUND	|	INSERT_USER	|	string	|	30	|		|		|		|		|	"to_date(CLOSED_DATE,'YYYY-MM-DD')"	|		|		|		|	BUD_FUND_COMPOSITE	|	CLOSD_DT	|	date	|	19	|		|		|
|	VW_BDGT_FUND	|	INSERT_TIMESTAMP	|	string	|	30	|		|		|		|	INSERT_USER	|		|		|		|		|	BUD_FUND_COMPOSITE	|	INSR_USR	|	nvarchar2	|	30	|		|		|
|	VW_BDGT_FUND	|	VERSION	|	double	|	15	|		|		|		|		|	"ADD_TO_DATE(to_date('1970-01-01', 'YYYY-MM-DD'),'SS',to_bigint(INSERT_TIMESTAMP)/1000)"	|		|		|		|	BUD_FUND_COMPOSITE	|	INSR_TS	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|	VERSION	|		|		|		|		|	BUD_FUND_COMPOSITE	|	VRSN	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	BUD_FUND_COMPOSITE	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_FUND_COMPOSITE	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|	BUD_FUND_COMPOSITE	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_FUND_COMPOSITE	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	BUD_FUND_COMPOSITE	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
