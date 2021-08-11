|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ query	|	DLR_ID	|	string	|	25	|		|		|		|		|	\\\\If new record <br>Sequence generator used to generate the auto increment for each new row insert<br>	|	Tabke : HDM.DEALER<br>Condition : DLR_ID = IN_DLR_ID<br>output column : DLR_KEY	|	"If record already present, update the existing record"	|	DEALER	|	DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|	DLR_BRANCH_ESTB_DT	|	date/time	|	29	|		|		|		|	Default to 'All'	|	'All'	|		|		|	DEALER	|	DLR_TOP_LVL	|	varchar2	|	3	|		|	Y	|		|
|		|	DLR_BRANCH_CLOSED_DT	|	date/time	|	29	|		|		|		|	Default to 'All Dealers'	|	'All Dealers'	|		|		|	DEALER	|	DLR_TOP_LVL_DESC	|	varchar2	|	60	|		|	Y	|		|
|		|	SELLG_AGRMT_RQRD_FLG	|	string	|	1	|		|		|		|	DLR_GRP_ID	|	DLR_GRP_ID	|		|		|	DEALER	|	DLR_GRP_ID	|	varchar2	|	25	|		|		|		|
|		|	SELLG_AGRMT_12B1_FLG	|	string	|	1	|		|		|		|	If DLR_GRP_ID is valid then populate DLR_GRP_NM otherwise populate PARTY_FRMTD_NM	|	"IIF((DLR_GRP_ID='0' OR ISNULL(DLR_GRP_ID)), PARTY_FRMTD_NM,DLR_GRP_NM)"	|		|		|	DEALER	|	DLR_GRP_NM	|	varchar2	|	60	|		|		|		|
|		|	RECV_12B1_TAPE_FLG	|	string	|	1	|		|		|		|	DLR_ID	|	DLR_ID	|		|		|	DEALER	|	DLR_ID	|	varchar2	|	25	|		|		|		|
|		|	AUTO_DIVD_CHK_FLG	|	string	|	1	|		|		|		|	DLR_BRANCH_ESTB_DT	|	DLR_BRANCH_ESTB_DT	|		|		|	DEALER	|	BRANCH_ESTB_DT	|	date	|	19	|		|		|		|
|		|	WIRE_INCL_ACCRL_FLG	|	string	|	1	|		|		|		|	DLR_BRANCH_CLOSED_DT	|	DLR_BRANCH_CLOSED_DT	|		|		|	DEALER	|	BRANCH_CLOSED_DT	|	date	|	19	|		|		|		|
|		|	SIPC_INSD_FLG	|	string	|	1	|		|		|		|	SELLG_AGRMT_RQRD_FLG	|	SELLG_AGRMT_RQRD_FLG	|		|		|	DEALER	|	SELLG_AGRMT_RQRD_FLG	|	varchar2	|	1	|		|		|		|
|		|	DLR_FDIC_SELL_FLG	|	string	|	1	|		|		|		|	SELLG_AGRMT_12B1_FLG	|	SELLG_AGRMT_12B1_FLG	|		|		|	DEALER	|	SELLG_AGRMT_12B1_FLG	|	varchar2	|	1	|		|		|		|
|		|	SEC_ID	|	decimal	|	11	|		|		|		|	FREQ_CD	|	FREQ_CD	|		|		|	DEALER	|	PAYOUT_FREQ_CD	|	varchar2	|	1	|		|		|		|
|		|	TRSTE_DFLT_DLR_ID	|	decimal	|	11	|		|		|		|	FREQ_DESC	|	FREQ_DESC	|		|		|	DEALER	|	PAYOUT_FREQ_DESC	|	varchar2	|	60	|		|		|		|
|		|	GRP_SALES_CHNL_ID	|	string	|	25	|		|		|		|	STMT_FREQ_CD	|	STMT_FREQ_CD	|		|		|	DEALER	|	STMT_FREQ_CD	|	varchar2	|	2	|		|		|		|
|		|	GRP_FINCL_INST_ID	|	decimal	|	11	|		|		|		|	FREQ_DESC1	|	FREQ_DESC1	|		|		|	DEALER	|	STMT_FREQ_DESC	|	varchar2	|	60	|		|		|		|
|		|	DIVD_CONFIRM_SPRSN_CD	|	string	|	1	|		|		|		|	RECV_12B1_TAPE_FLG	|	RECV_12B1_TAPE_FLG	|		|		|	DEALER	|	RECV_12B1_TAPE_FLG	|	varchar2	|	1	|		|		|		|
|		|	DAILY_CONFIRM_SPRSN_CD	|	string	|	1	|		|		|		|	AUTO_DIVD_CHK_FLG	|	AUTO_DIVD_CHK_FLG	|		|		|	DEALER	|	AUTO_DIVD_CHK_FLG	|	varchar2	|	1	|		|		|		|
|		|	ONLN_INVAL_REP_HNDLG_CD	|	string	|	1	|		|		|		|	WIRE_INCL_ACCRL_FLG	|	WIRE_INCL_ACCRL_FLG	|		|		|	DEALER	|	WIRE_INCL_ACCRL_FLG	|	varchar2	|	1	|		|		|		|
|		|	DLR_REP_ST_LIC_VLDN_CD	|	string	|	1	|		|		|		|	SIPC_INSD_FLG	|	SIPC_INSD_FLG	|		|		|	DEALER	|	SIPC_INSD_FLG	|	varchar2	|	1	|		|		|		|
|		|	DLR_INVAL_REP_ST_LIC_ACTN_CD	|	string	|	1	|		|		|		|	DLR_FDIC_SELL_FLG	|	DLR_FDIC_SELL_FLG	|		|		|	DEALER	|	DLR_FDIC_SELL_FLG	|	varchar2	|	1	|		|		|		|
|		|	NASD_REGSTN_VLDN_LIC_CD	|	string	|	1	|		|		|		|	SEC_ID	|	SEC_ID	|		|		|	DEALER	|	SEC_ID	|	number	|	15	|		|		|		|
|		|	INVAL_REP_NASD_REGISTERED_CD	|	string	|	1	|		|		|		|	MBRS_TYPE_CD	|	MBRS_TYPE_CD	|		|		|	DEALER	|	MBR_SHIP_TYPE_CD	|	varchar2	|	2	|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	TYPE_DESC	|	TYPE_DESC	|		|		|	DEALER	|	MBR_SHIP_TYPE	|	varchar2	|	60	|		|		|		|
|		|	SPR_ONBS_FLG	|	string	|	1	|		|		|		|	REP_HNDLG_CD	|	REP_HNDLG_CD	|		|		|	DEALER	|	INVAL_REP_HNDLG_CD	|	varchar2	|	1	|		|		|		|
|		|	ONBS_FLG	|	string	|	1	|		|		|		|	HNDLG_DESC	|	HNDLG_DESC	|		|		|	DEALER	|	INVAL_REP_HNDLG_DESC	|	varchar2	|	60	|		|		|		|
|		|	INTRL_CNTL_DOC_DT	|	date/time	|	29	|		|		|		|	SLIP_ADDR_CD	|	SLIP_ADDR_CD	|		|		|	DEALER	|	INVMT_SLIP_ADDR_CD	|	varchar2	|	1	|		|		|		|
|		|	LTST_CNTL_AUDT_RECPT_DT	|	date/time	|	29	|		|		|		|	SLIP_ADDR_DESC	|	SLIP_ADDR_DESC	|		|		|	DEALER	|	INVMT_SLIP_ADDR	|	varchar2	|	60	|		|		|		|
|		|	DLR_GRP_ID	|	string	|	25	|		|		|		|	TRSTE_DFLT_DLR_ID	|	TRSTE_DFLT_DLR_ID	|		|		|	DEALER	|	TRSTE_DFLT_DLR_ID	|	number	|	15	|		|		|		|
|		|	DLR_GRP_NM	|	string	|	60	|		|		|		|	GRP_SALES_CHNL_ID	|	GRP_SALES_CHNL_ID	|		|		|	DEALER	|	GRP_SALES_CHNL_ID	|	varchar2	|	25	|		|		|		|
|		|	FREQ_CD	|	string	|	1	|		|		|		|	GRP_FINCL_INST_ID	|	GRP_FINCL_INST_ID	|		|		|	DEALER	|	GRP_FINCL_INST_ID	|	number	|	15	|		|		|		|
|		|	FREQ_DESC	|	string	|	60	|		|		|		|	DIVD_CONFIRM_SPRSN_CD	|	DIVD_CONFIRM_SPRSN_CD	|		|		|	DEALER	|	DIVD_CONFIRM_SPRSN_FLG	|	varchar2	|	1	|		|		|		|
|		|	STMT_FREQ_CD	|	string	|	2	|		|		|		|	DAILY_CONFIRM_SPRSN_CD	|	DAILY_CONFIRM_SPRSN_CD	|		|		|	DEALER	|	DAILY_CONFIRM_SPRSN_FLG	|	varchar2	|	1	|		|		|		|
|		|	FREQ_DESC1	|	string	|	255	|		|		|		|	ONLN_INVAL_REP_HNDLG_CD	|	ONLN_INVAL_REP_HNDLG_CD	|		|		|	DEALER	|	ONLN_INVAL_REP_HNDLG_FLG	|	varchar2	|	1	|		|		|		|
|		|	MBRS_TYPE_CD	|	string	|	2	|		|		|		|	DLR_REP_ST_LIC_VLDN_CD	|	DLR_REP_ST_LIC_VLDN_CD	|		|		|	DEALER	|	DLR_REP_ST_LIC_VLDN_FLG	|	varchar2	|	1	|		|		|		|
|		|	TYPE_DESC	|	string	|	255	|		|		|		|	DLR_INVAL_REP_ST_LIC_ACTN_CD	|	DLR_INVAL_REP_ST_LIC_ACTN_CD	|		|		|	DEALER	|	DLR_INVAL_REP_ST_LIC_ACTN_FLG	|	varchar2	|	1	|		|		|		|
|		|	REP_HNDLG_CD	|	string	|	1	|		|		|		|	NASD_REGSTN_VLDN_LIC_CD	|	NASD_REGSTN_VLDN_LIC_CD	|		|		|	DEALER	|	NASD_REGSTN_VLDN_LIC_FLG	|	varchar2	|	1	|		|		|		|
|		|	HNDLG_DESC	|	string	|	255	|		|		|		|	INVAL_REP_NASD_REGISTERED_CD	|	INVAL_REP_NASD_REGISTERED_CD	|		|		|	DEALER	|	INVAL_REP_NASD_REGSTR_FLG	|	varchar2	|	1	|		|		|		|
|		|	SLIP_ADDR_CD	|	string	|	1	|		|		|		|	SPR_ONBS_FLG	|	SPR_ONBS_FLG	|		|		|	DEALER	|	DLR_NM	|	varchar2	|	60	|		|		|		|
|		|	SLIP_ADDR_DESC	|	string	|	255	|		|		|		|	LOCN_LINE_1_ADDR	|	LOCN_LINE_1_ADDR	|		|		|	DEALER	|	ADDR_LINE_1	|	varchar2	|	60	|		|		|		|
|		|	PARTY_FRMTD_NM	|	string	|	60	|		|		|		|	LOCN_LINE_2_ADDR	|	LOCN_LINE_2_ADDR	|		|		|	DEALER	|	ADDR_LINE_2	|	varchar2	|	60	|		|		|		|
|		|	LOCN_LINE_1_ADDR	|	string	|	60	|		|		|		|	LOCN_LINE_3_ADDR	|	LOCN_LINE_3_ADDR	|		|		|	DEALER	|	ADDR_LINE_3	|	varchar2	|	60	|		|		|		|
|		|	LOCN_LINE_2_ADDR	|	string	|	60	|		|		|		|	LOCN_LINE_4_ADDR	|	LOCN_LINE_4_ADDR	|		|		|	DEALER	|	ADDR_LINE_4	|	varchar2	|	60	|		|		|		|
|		|	LOCN_LINE_3_ADDR	|	string	|	60	|		|		|		|	CITY_NM	|	CITY_NM	|		|		|	DEALER	|	CITY_NM	|	varchar2	|	60	|		|		|		|
|		|	LOCN_LINE_4_ADDR	|	string	|	60	|		|		|		|	ST_CD	|	ST_CD	|		|		|	DEALER	|	ST	|	varchar2	|	2	|		|		|		|
|		|	CITY_NM	|	string	|	60	|		|		|		|	POSTL_ZIP_CD	|	POSTL_ZIP_CD	|		|		|	DEALER	|	POSTL_ZIP_CD	|	"number(p,s)"	|	5	|		|		|		|
|		|	POSTL_ZIP_CD	|	decimal	|	38	|		|		|		|	CRTY_FULL_NM	|	CRTY_FULL_NM	|		|		|	DEALER	|	CRTY	|	varchar2	|	60	|		|		|		|
|		|	CRTY_FULL_NM	|	string	|	60	|		|		|		|	sysdate	|	sysdate	|		|		|	DEALER	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|	ST_CD	|	string	|	3	|		|		|		|		|		|		|		|	DEALER	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|	Y'	|	Y'	|		|		|	DEALER	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|	DEALER	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|		|	4	|		|		|	DEALER	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
