|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	refer SQ query	|	DLR_CHG_CNTL_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|	Seq Number	|	DEALER_CHANGE_CONTROL	|	DLR_CHG_CNTL_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|	//:LKP.LKP_DLR_KEY(TO_INTEGER(DLR_ROLE_KEY))<br>Table Name: HDM.DEALER<br>Condition: DLR_ID = DLR_ID1<br>Return Column: DLR_KEY	|		|		|	DEALER_CHANGE_CONTROL	|	DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	DLR_ROLE_KEY	|	decimal	|	10	|		|		|		|		|		|	//:LKP.LKP_DAY_KEY(TRUNC(CHG_CNTL_DT))<br><br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY1<br>Return Column: DAY_KEY	|		|		|	DEALER_CHANGE_CONTROL	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	CHG_CNTL_ID	|	double	|	15	|		|		|		|	CHG_CNTL_ID	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CHG_CNTL_ID	|	number	|	15	|		|		|
|		|	CHG_CNTL_DT	|	date/time	|	29	|		|		|		|	CHG_TYPE	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CHG_TYPE	|	nvarchar2	|	255	|		|		|
|		|	CHG_DESC	|	nstring	|	4000	|		|		|		|	CHG_DESC	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CHG_DESC	|	nvarchar2	|	4000	|		|		|
|		|	CHG_TYPE	|	nstring	|	255	|		|		|		|	RECVG_DLR_NM	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	RECVG_DLR_NM	|	nvarchar2	|	255	|		|		|
|		|	RECVG_DLR_NM	|	nstring	|	255	|		|		|		|	RECVG_DLR_ACCT_TYPE	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	RECVG_DLR_ACCT_TYPE	|	nvarchar2	|	60	|		|		|
|		|	RECVG_DLR_ACCT_TYPE	|	nstring	|	60	|		|		|		|	ACCT_MOVE_CNT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	ACCT_MOVE_CNT	|	number	|	15	|		|		|
|		|	ACCT_MOVE_CNT	|	double	|	15	|		|		|		|	BSKY_NTFN_FLG	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	BSKY_NTFN_FLG	|	nvarchar2	|	1	|		|		|
|		|	BLUESKY_NTFN_FLG	|	nstring	|	1	|		|		|		|	HBR_IMPCT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	HBR_IMPCT	|	nvarchar2	|	1000	|		|		|
|		|	HBR_IMPCT	|	nstring	|	1000	|		|		|		|	CUSIP_LIST	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CUSIP_LIST	|	nvarchar2	|	255	|		|		|
|		|	CUSIP_LIST	|	nstring	|	255	|		|		|		|	DELVY_DLR_MTRX_LVL	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	DELVY_DLR_MTRX_LVL	|	nvarchar2	|	60	|		|		|
|		|	DELVY_DLR_MTRX_LVL	|	nstring	|	60	|		|		|		|	DELVY_DLR_ACCT_TYPE	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	DELVY_DLR_ACCT_TYPE	|	nvarchar2	|	60	|		|		|
|		|	DELVY_DLR_ACCT_TYPE	|	nstring	|	60	|		|		|		|	ASSET_MOVE_VAL	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	ASSET_MOVE_VAL	|	number	|	15	|		|		|
|		|	ASSET_MOVE_VAL	|	double	|	15	|		|		|		|	DELVY_DLR_PLFM_NM	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	DELVY_DLR_PLFM_NM	|	nvarchar2	|	255	|		|		|
|		|	DELVY_DLR_PLFM_NM	|	nstring	|	255	|		|		|		|	DELVY_DLR_FEES	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	DELVY_DLR_FEES	|	number	|	15	|		|		|
|		|	DELVY_DLR_FEES	|	double	|	15	|		|		|		|	RECVG_DLR_ID	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	RECVG_DLR_ID	|	nvarchar2	|	25	|		|		|
|		|	RECVG_DLR_ID	|	nstring	|	25	|		|		|		|	RECVG_DLR_MTRX_LVL	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	RECVG_DLR_MTRX_LVL	|	nvarchar2	|	60	|		|		|
|		|	RECVG_DLR_MTRX_LVL	|	nstring	|	60	|		|		|		|	RECVG_DLR_FEES	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	RECVG_DLR_FEES	|	number	|	15	|		|		|
|		|	RECVG_DLR_FEES	|	double	|	15	|		|		|		|	CNVRSN_STRT_DT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CNVRSN_STRT_DT	|	date	|	19	|		|		|
|		|	CNVRSN_STRT_DT	|	date/time	|	29	|		|		|		|	CNVRSN_END_DT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CNVRSN_END_DT	|	date	|	19	|		|		|
|		|	CNVRSN_END_DT	|	date/time	|	29	|		|		|		|	CHG_RSN	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CHG_RSN	|	nvarchar2	|	4000	|		|		|
|		|	CHG_RSN	|	nstring	|	255	|		|		|		|	CHG_CONTNT_TYPE	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CHG_CONTNT_TYPE	|	nvarchar2	|	60	|		|		|
|		|	CHG_CONTNT_TYPE	|	nstring	|	60	|		|		|		|	CONTCT_NM	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CONTCT_NM	|	nvarchar2	|	255	|		|		|
|		|	CONTCT_NM	|	nstring	|	255	|		|		|		|	UFID_STAT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	UFID_STAT	|	nvarchar2	|	60	|		|		|
|		|	UFID_STAT	|	nstring	|	60	|		|		|		|	DLR_GRPING_STAT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	DLR_GRPING_STAT	|	nvarchar2	|	60	|		|		|
|		|	DLR_GRPING_STAT	|	nstring	|	60	|		|		|		|	DLR_PROF_STAT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	DLR_PROF_STAT	|	nvarchar2	|	60	|		|		|
|		|	DLR_PROF_STAT	|	nstring	|	60	|		|		|		|	MTF_STAT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	MTF_STAT	|	nvarchar2	|	60	|		|		|
|		|	MTF_STAT	|	nstring	|	60	|		|		|		|	LIVE_LINK_STAT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	LIVE_LINK_STAT	|	nvarchar2	|	60	|		|		|
|		|	LIVE_LINK_STAT	|	nstring	|	60	|		|		|		|	RFRSER_RQST	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	RFRSER_RQST	|	nvarchar2	|	4000	|		|		|
|		|	RFRSER_RQST	|	nstring	|	255	|		|		|		|	COMMFEE_CMMT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	COMMFEE_CMMT	|	nvarchar2	|	4000	|		|		|
|		|	COMMFEE_CMMT	|	nstring	|	255	|		|		|		|	CUMDISC_CD	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CUMDISC_CD	|	nvarchar2	|	25	|		|		|
|		|	CUMDISC_CD	|	nstring	|	25	|		|		|		|	MTF_INLSN_FLG	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	MTF_INLSN_FLG	|	nvarchar2	|	1	|		|		|
|		|	MTF_INLSN_FLG	|	nstring	|	1	|		|		|		|	AWD_SEND_VAL	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	AWD_SEND_VAL	|	nvarchar2	|	25	|		|		|
|		|	AWD_SEND_VAL	|	nstring	|	25	|		|		|		|	MTF_EXCLN_FLG	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	MTF_EXCLN_FLG	|	nvarchar2	|	1	|		|		|
|		|	MTF_EXCLN_FLG	|	nstring	|	1	|		|		|		|	EXCLN_CMMTS	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	EXCLN_CMMTS	|	nvarchar2	|	4000	|		|		|
|		|	EXCLN_CMMTS	|	nstring	|	255	|		|		|		|	INLSN_CMMTS	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	INLSN_CMMTS	|	nvarchar2	|	4000	|		|		|
|		|	INLSN_CMMTS	|	nstring	|	255	|		|		|		|	CNVRSN_STAT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CNVRSN_STAT	|	nvarchar2	|	60	|		|		|
|		|	CNVRSN_STAT	|	nstring	|	60	|		|		|		|	DLR_TOT_ASSET_PCT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	DLR_TOT_ASSET_PCT	|	number	|	15	|		|		|
|		|	DLR_TOT_ASSET_PCT	|	double	|	15	|		|		|		|	CHG_CNTL_WKFL	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CHG_CNTL_WKFL	|	nvarchar2	|	25	|		|		|
|		|	CHG_CNTL_WKFL	|	nstring	|	25	|		|		|		|	FEE_IMPCT_FLG	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	FEE_IMPCT_FLG	|	nvarchar2	|	1	|		|		|
|		|	FEE_IMPCT_FLG	|	nstring	|	1	|		|		|		|	CREATR	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CREATR	|	nvarchar2	|	255	|		|		|
|		|	CREATR	|	nstring	|	255	|		|		|		|	MODFR	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	MODFR	|	nvarchar2	|	255	|		|		|
|		|	MODFR	|	nstring	|	255	|		|		|		|	CREATN_DT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CREATN_DT	|	date	|	19	|		|		|
|		|	CREATN_DT	|	date/time	|	29	|		|		|		|	CHG_DT	|		|		|		|		|	DEALER_CHANGE_CONTROL	|	CHG_DT	|	date	|	19	|		|		|
|		|	CHG_DT	|	date/time	|	29	|		|		|		|		|	Y'	|		|		|		|	DEALER_CHANGE_CONTROL	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|	CURR_ROW_FLG	|	nstring	|	1	|		|		|		|		|	SYSDATE	|		|		|		|	DEALER_CHANGE_CONTROL	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	ROW_STOP_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|		|	DEALER_CHANGE_CONTROL	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	SRC_SYS_DEL_FLG	|	nstring	|	1	|		|		|		|		|		|		|		|		|	DEALER_CHANGE_CONTROL	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
