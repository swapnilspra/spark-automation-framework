|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	ACCT_NBR	|	decimal	|	20	|		|		|		|	// for new record<br>Use sequence generator to generate the auto increment value	|	// for new record<br>Use sequence generator to generate the auto increment value	|		|	If record already presents Update the records	|	ACCOUNT	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	ACCT_STRT_DT	|	date/time	|	29	|		|		|		|	'All'	|	'All'	|		|		|	ACCOUNT	|	ACCT_TOP_LVL_CD	|	varchar2	|	3	|		|		|		|
|		|	TAX_ID	|	double	|	15	|		|		|		|	'All Accounts'	|	'All Accounts'	|		|		|	ACCOUNT	|	ACCT_TOP_LVL_DESC	|	varchar2	|	60	|		|		|		|
|		|	ACCT_LAST_MAINT_DT	|	date/time	|	29	|		|		|		|	ACCT_TYPE_CD	|	ACCT_TYPE_CD	|		|		|	ACCOUNT	|	ACCT_TYPE_CD	|	number	|	15	|		|		|		|
|		|	PRE_AUTHZD_CHECKING_ACCT	|	decimal	|	11	|		|		|		|	CUST_ACCT_TYPE_NM	|	CUST_ACCT_TYPE_NM	|		|		|	ACCOUNT	|	ACCT_TYPE	|	varchar2	|	60	|		|		|		|
|		|	SWP_ACCT	|	double	|	15	|		|		|		|	ACCT_NBR	|	ACCT_NBR	|		|		|	ACCOUNT	|	ACCT_NBR	|	number	|	15	|		|		|		|
|		|	ACH_ACCT	|	double	|	15	|		|		|		|	FUND_NBR	|	FUND_NBR	|		|		|	ACCOUNT	|	FUND_NBR	|	"number(p,s)"	|	7	|		|		|		|
|		|	REINVEST_TO_ANOTHER_ACCT	|	double	|	15	|		|		|		|	TAX_ID	|	TAX_ID	|		|		|	ACCOUNT	|	TAX_ID	|	number	|	15	|		|		|		|
|		|	MONITORED_VIP_ACCT	|	double	|	15	|		|		|		|	CUST_ACCT_TYPE_NM	|	CUST_ACCT_TYPE_NM	|		|		|	ACCOUNT	|	ACCT_STAT_CD	|	"number(p,s)"	|	1	|		|		|		|
|		|	FIDUCIARY_ACCT_ID	|	double	|	15	|		|		|		|	CUST_ACCT_STAT_CD	|	CUST_ACCT_STAT_CD	|		|		|	ACCOUNT	|	ACCT_STAT	|	varchar2	|	60	|		|		|		|
|		|	MAILG_FLG	|	string	|	1	|		|		|		|	CUST_ACCT_STAT_NM	|	CUST_ACCT_STAT_NM	|		|		|	ACCOUNT	|	ACCT_STRT_DT	|	date	|	19	|		|		|		|
|		|	INTERESTED_PARTY_FLG	|	string	|	1	|		|		|		|	ACCT_LAST_MAINT_DT	|	ACCT_LAST_MAINT_DT	|		|		|	ACCOUNT	|	LAST_MNTN_DT	|	date	|	19	|		|		|		|
|		|	MGMT_CO_EMPL_FLG	|	string	|	1	|		|		|		|	"IIF(ISNULL(DLR_LVL_CNTL_CD),-1,DLR_LVL_CNTL_CD)"	|	"IIF(ISNULL(DLR_LVL_CNTL_CD),-1,DLR_LVL_CNTL_CD)"	|		|		|	ACCOUNT	|	DLR_LVL_CNTL_CD	|	"number(p,s)"	|	1	|		|		|		|
|		|	TRST_CO_NBR	|	double	|	15	|		|		|		|	INITCAP(in_CNTL_DESC)	|	INITCAP(in_CNTL_DESC)	|		|		|	ACCOUNT	|	DLR_LVL_CNTL_DESC	|	varchar2	|	60	|		|		|		|
|		|	TPA_NBR	|	double	|	15	|		|		|		|	PRE_AUTHZD_CHECKING_ACCT	|	PRE_AUTHZD_CHECKING_ACCT	|		|		|	ACCOUNT	|	PRE_AUTHZD_CHK_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	CUSTODIAN_ID	|	double	|	15	|		|		|		|	SWP_ACCT	|	SWP_ACCT	|		|		|	ACCOUNT	|	SWP_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	TPA_ID	|	double	|	15	|		|		|		|	ACH_ACCT	|	ACH_ACCT	|		|		|	ACCOUNT	|	ACH_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	EXT_PLN_ID	|	string	|	9	|		|		|		|	REINVEST_TO_ANOTHER_ACCT	|	REINVEST_TO_ANOTHER_ACCT	|		|		|	ACCOUNT	|	RE_INVST_TO_ANOTHER_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	ALPHA_CD	|	string	|	10	|		|		|		|	MONITORED_VIP_ACCT	|	MONITORED_VIP_ACCT	|		|		|	ACCOUNT	|	MONITORED_VIP_ACCT	|	"number(p,s)"	|	1	|		|		|		|
|		|	CUST_BIN	|	string	|	20	|		|		|		|	FIDUCIARY_ACCT_ID	|	FIDUCIARY_ACCT_ID	|		|		|	ACCOUNT	|	FIDUCIARY_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	STOP_MAIL_DT	|	date/time	|	29	|		|		|		|	MAILG_FLG	|	MAILG_FLG	|		|		|	ACCOUNT	|	MAILG_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	INTERNET_ACCT_FLG	|	string	|	1	|		|		|		|	INTERESTED_PARTY_FLG	|	INTERESTED_PARTY_FLG	|		|		|	ACCOUNT	|	INTR_PARTY_FLG	|	varchar2	|	1	|		|		|		|
|		|	GRND_FATHER_ACCT_FLG	|	string	|	1	|		|		|		|	MGMT_CO_EMPL_FLG	|	MGMT_CO_EMPL_FLG	|		|		|	ACCOUNT	|	MGMT_CO_EMPL_FLG	|	varchar2	|	1	|		|		|		|
|		|	PLN_CLSFCN_FLG	|	string	|	1	|		|		|		|	TRST_CO_NBR	|	TRST_CO_NBR	|		|		|	ACCOUNT	|	TRST_CO_NBR	|	number	|	15	|		|		|		|
|		|	MKT_CLSFCN_KEY	|	decimal	|	10	|		|		|		|	TPA_NBR	|	TPA_NBR	|		|		|	ACCOUNT	|	TPA_NBR	|	number	|	15	|		|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|	CUSTODIAN_ID	|	CUSTODIAN_ID	|		|		|	ACCOUNT	|	CUSTODIAN_ID	|	number	|	15	|		|		|		|
|		|	CHK_WRITING_ACCT	|	double	|	15	|		|		|		|	THRD_PARTY_ADMIN_ID	|	THRD_PARTY_ADMIN_ID	|		|		|	ACCOUNT	|	TPA_ID	|	number	|	15	|		|		|		|
|		|	ACCT_ESTB_DT	|	date/time	|	29	|		|		|		|	EXT_PLN_ID	|	EXT_PLN_ID	|		|		|	ACCOUNT	|	EXT_PLN_ID	|	varchar2	|	25	|		|		|		|
|		|	EXPEDITED_REDMPN_ACCT	|	decimal	|	1	|		|		|		|	TIN_CD	|	TIN_CD	|		|		|	ACCOUNT	|	TIN_CD	|	"number(p,s)"	|	1	|		|		|		|
|		|	SUB_ACCT	|	decimal	|	1	|		|		|		|	INITCAP(in_TIN_DESC)	|	INITCAP(in_TIN_DESC)	|		|		|	ACCOUNT	|	TIN_DESC	|	varchar2	|	60	|		|		|		|
|		|	FORGN_TAX_RT	|	double	|	15	|		|		|		|	ALPHA_CD	|	ALPHA_CD	|		|		|	ACCOUNT	|	ALPHA_CD	|	varchar2	|	10	|		|		|		|
|		|	CUM_DISCNT_NBR	|	double	|	15	|		|		|		|	"IIF(ISNULL(CAP_GAIN_DSTRBTN_TYPE_FLG),-1,CAP_GAIN_DSTRBTN_TYPE_FLG)"	|	"IIF(ISNULL(CAP_GAIN_DSTRBTN_TYPE_FLG),-1,CAP_GAIN_DSTRBTN_TYPE_FLG)"	|		|		|	ACCOUNT	|	CAP_GAINS_DSTRBTN_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	LTR_OF_INTEND_NBR	|	double	|	15	|		|		|		|	CHK_WRITING_ACCT	|	CHK_WRITING_ACCT	|		|		|	ACCOUNT	|	CHK_WRITING_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	TIMER_ACCT_ID	|	double	|	15	|		|		|		|	DIVD_DSTRBTN_OPT_FLG	|	DIVD_DSTRBTN_OPT_FLG	|		|		|	ACCOUNT	|	DIVD_DSTRBTN_OPT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	GPURCH_ACCT	|	decimal	|	1	|		|		|		|	EXPEDITED_REDMPN_ACCT	|	EXPEDITED_REDMPN_ACCT	|		|		|	ACCOUNT	|	EXPEDITED_REDMPN_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	EXPEDITED_XCHG_ACCT	|	decimal	|	1	|		|		|		|	SUB_ACCT	|	SUB_ACCT	|		|		|	ACCOUNT	|	SUB_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	PNLTY_WHG_ACCT	|	decimal	|	1	|		|		|		|	FORGN_TAX_RT	|	FORGN_TAX_RT	|		|		|	ACCOUNT	|	FORGN_TAX_RT	|	number	|	15	|		|		|		|
|		|	CERTFC_ISSUANCE_CD	|	decimal	|	1	|		|		|		|	CUM_DISCNT_NBR	|	CUM_DISCNT_NBR	|		|		|	ACCOUNT	|	CUM_DISCNT_NBR	|	number	|	15	|		|		|		|
|		|	STOP_XFER_FLG	|	decimal	|	1	|		|		|		|	LTR_OF_INTEND_NBR	|	LTR_OF_INTEND_NBR	|		|		|	ACCOUNT	|	LTR_OF_INTEND_NBR	|	number	|	15	|		|		|		|
|		|	CAP_GAIN_DSTRBTN_TYPE_FLG	|	decimal	|	1	|		|		|		|	TIMER_ACCT_ID	|	TIMER_ACCT_ID	|		|		|	ACCOUNT	|	TIMER_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	DIVD_DSTRBTN_OPT_FLG	|	decimal	|	1	|		|		|		|	GPURCH_ACCT	|	GPURCH_ACCT	|		|		|	ACCOUNT	|	GPURCH_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	BLUE_SKY_XMPTN_FLG	|	decimal	|	1	|		|		|		|	EXPEDITED_XCHG_ACCT	|	EXPEDITED_XCHG_ACCT	|		|		|	ACCOUNT	|	EXPDT_XCHG_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	BNK_CARD_ISSD	|	decimal	|	1	|		|		|		|	PNLTY_WHG_ACCT	|	PNLTY_WHG_ACCT	|		|		|	ACCOUNT	|	PNLTY_WHG_ACCT_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	NAV_ACCT_ID	|	string	|	1	|		|		|		|	CERTFC_ISSUANCE_CD	|	CERTFC_ISSUANCE_CD	|		|		|	ACCOUNT	|	CERTFC_ISSUANCE_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	SHR_ACCT_PHON_CHK_REDMPN_FLG	|	string	|	1	|		|		|		|	STOP_XFER_FLG	|	STOP_XFER_FLG	|		|		|	ACCOUNT	|	STOP_XFER_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	SHR_ACCT_HOUSE_ACCT_FLG	|	string	|	1	|		|		|		|	BLUE_SKY_XMPTN_FLG	|	BLUE_SKY_XMPTN_FLG	|		|		|	ACCOUNT	|	BLUESKY_XMPTN_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	DIVD_MAIL_ACCT_ID	|	decimal	|	1	|		|		|		|	BNK_CARD_ISSD	|	BNK_CARD_ISSD	|		|		|	ACCOUNT	|	BNK_CARD_ISSD_FLG	|	"number(p,s)"	|	1	|		|		|		|
|		|	STOP_PURCH_ACCT_ID	|	decimal	|	1	|		|		|		|	NAV_ACCT_ID	|	NAV_ACCT_ID	|		|		|	ACCOUNT	|	NAV_ACCT_FLG	|	varchar2	|	1	|		|		|		|
|		|	STOP_MAIL_ACCT_ID	|	decimal	|	1	|		|		|		|	SHR_ACCT_PHON_CHK_REDMPN_FLG	|	SHR_ACCT_PHON_CHK_REDMPN_FLG	|		|		|	ACCOUNT	|	PHON_CHK_REDMPN_FLG	|	varchar2	|	1	|		|		|		|
|		|	FRACTIONAL_CHK_ID	|	decimal	|	1	|		|		|		|	SHR_ACCT_PHON_CHK_REDMPN_FLG	|	SHR_ACCT_PHON_CHK_REDMPN_FLG	|		|		|	ACCOUNT	|	HOUSE_ACCT_FLG	|	varchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
