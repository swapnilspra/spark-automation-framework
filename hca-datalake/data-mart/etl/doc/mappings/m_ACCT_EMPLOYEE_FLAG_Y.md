|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Please refer the join condition	|	FUND	|	decimal	|	10	|	0	|		|		|	Default to 'AC'	|	'AC'	|		|		|	ACCT_MAINT_UPDATE	|	TRANS_CLASSIFICATION_CODE1	|	string	|	2	|		|		|		|
|		|	ACCOUNT	|	nstring	|	11	|	0	|		|		|	Default to '1'	|	'1'	|		|		|	ACCT_MAINT_UPDATE	|	TRANS_SEQUENCE_NUMBER1	|	string	|	1	|		|		|		|
|		|	LOAD_DATE	|	date/time	|	29	|	9	|		|		|	Default to  '02'	|	'02'	|		|		|	ACCT_MAINT_UPDATE	|	TRANS_TYPE_CODE	|	string	|	2	|		|		|		|
|		|	LOAD_TIME	|	date/time	|	29	|	9	|		|		|	Default to  '000'	|	'000'	|		|		|	ACCT_MAINT_UPDATE	|	TRANS_SUBTYPE_CODE	|	string	|	3	|		|		|		|
|		|	ACCT_ENTRY_METHOD	|	nstring	|	1	|	0	|		|		|	Default to  '  '	|	'  '	|		|		|	ACCT_MAINT_UPDATE	|	FILLER1	|	string	|	2	|		|		|		|
|		|	ACCT_ORIGIN_ID	|	decimal	|	5	|	0	|		|		|	add prefix of '0' to 'FUND'(incoming field)	|	"lpad(to_char(FUND_#),7,'0')"	|		|		|	ACCT_MAINT_UPDATE	|	FUND_CODE	|	string	|	7	|		|		|		|
|		|	ACCT_SETUP_DTE	|	date/time	|	29	|	9	|		|		|	Default to  'F'	|	'F'	|		|		|	ACCT_MAINT_UPDATE	|	CUSIP_FUND_CODE	|	string	|	1	|		|		|		|
|		|	ACHFLAG	|	nstring	|	1	|	0	|		|		|	Default to  '         '	|	'         '	|		|		|	ACCT_MAINT_UPDATE	|	FILLER2	|	string	|	9	|		|		|		|
|		|	AFFIL_BRANCH	|	nstring	|	9	|	0	|		|		|	add prefix of '0' to 'ACCOUNT'(incoming field)	|	"LPAD(to_char(ACCOUNT_NUMBER_in),11,'0')"	|		|		|	ACCT_MAINT_UPDATE	|	ACCOUNT_NUMBER	|	string	|	11	|		|		|		|
|		|	AFFIL_DEALER	|	decimal	|	10	|	0	|		|		|	Default to  'F'	|	'F'	|		|		|	ACCT_MAINT_UPDATE	|	CUSTOMER_ACCOUNT_CODE	|	string	|	1	|		|		|		|
|		|	AFFIL_REP	|	nstring	|	9	|	0	|		|		|	Default to  ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	DEALER_CONTROL_LEVEL_CODE	|	string	|	1	|		|		|		|
|		|	AFFIL_ROLE	|	nstring	|	1	|	0	|		|		|	Default to  '       '	|	'       '	|		|		|	ACCT_MAINT_UPDATE	|	DEALER_NUMBER	|	string	|	7	|		|		|		|
|		|	ALPHACODE	|	nstring	|	10	|	0	|		|		|	Default to  '         '	|	'         '	|		|		|	ACCT_MAINT_UPDATE	|	DEALER_BRANCH	|	string	|	9	|		|		|		|
|		|	AUDIORESP	|	nstring	|	1	|	0	|		|		|	to_char(SOCIALCODE) (Incoming field)	|	to_char(SOCIALCODE)	|		|		|	ACCT_MAINT_UPDATE	|	SOCIAL_CODE	|	string	|	3	|		|		|		|
|		|	BEGSHRSISS	|	double	|	15	|	0	|		|		|	Default to  '         '	|	'         '	|		|		|	ACCT_MAINT_UPDATE	|	SOCIAL_SEC_NO	|	string	|	9	|		|		|		|
|		|	BEGSHRSUISS	|	double	|	15	|	0	|		|		|	Default to  ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	SS_CODE	|	string	|	1	|		|		|		|
|		|	BLUESKY	|	nstring	|	1	|	0	|		|		|	Default to  ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	ACH_FLAG	|	string	|	1	|		|		|		|
|		|	CAPGAINS	|	nstring	|	1	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	CONFIRM_PRINT_CDE	|	string	|	1	|		|		|		|
|		|	CHECKDIGIT	|	nstring	|	1	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	ADDRESS_CHANGE_NOTIFY_CDE	|	string	|	1	|		|		|		|
|		|	CHECKWRITE	|	nstring	|	1	|	0	|		|		|	Default to  ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	DEALER_NUMBER_CHANGE_CDE	|	string	|	1	|		|		|		|
|		|	COMMISSION_CALC	|	nstring	|	1	|	0	|		|		|	Default to  '       '	|	'       '	|		|		|	ACCT_MAINT_UPDATE	|	BATCH_NUMBER	|	string	|	7	|		|		|		|
|		|	CUMDISC	|	decimal	|	10	|	0	|		|		|		|		|		|		|	ACCT_MAINT_UPDATE	|	EXTERNAL_CLASSIFICATION_CODE	|	string	|	2	|		|		|		|
|		|	CUSTODIAN_ID	|	decimal	|	10	|	0	|		|		|	chr(010)	|	chr(010)	|		|		|	ACCT_MAINT_UPDATE	|	FILLER3	|	string	|	1	|		|		|		|
|		|	CUSTOM_SALUTATION	|	nstring	|	35	|	0	|		|		|		|		|		|		|	ACCT_MAINT_UPDATE	|	NEW_ACCOUNT_CDE	|	string	|	1	|		|		|		|
|		|	DATEEST	|	date/time	|	29	|	9	|		|		|		|		|		|		|	ACCT_MAINT_UPDATE	|	FILLER4	|	string	|	16	|		|		|		|
|		|	DEALER	|	decimal	|	10	|	0	|		|		|		|		|		|		|	ACCT_MAINT_UPDATE	|	ACCOUNT_TRANSACTION_SORT_AREA	|	string	|	60	|		|		|		|
|		|	DEALERBRCH	|	nstring	|	9	|	0	|		|		|	Default to 'AC'	|	'AC'	|		|		|	ACCT_MAINT_UPDATE	|	TRANS_CLASSIFICATION_CODE2	|	string	|	2	|		|		|		|
|		|	DEALERCNTL	|	nstring	|	1	|	0	|		|		|	Default to '2'	|	'2'	|		|		|	ACCT_MAINT_UPDATE	|	TRANS_SEQUENCE_NUMBER2	|	string	|	1	|		|		|		|
|		|	DEBITCARD	|	nstring	|	1	|	0	|		|		|	Default to  '     '	|	'     '	|		|		|	ACCT_MAINT_UPDATE	|	ZIP_DELIVERY_OFFICE_CODE	|	string	|	5	|		|		|		|
|		|	DIVMAIL	|	nstring	|	1	|	0	|		|		|	Default to '  '	|	'  '	|		|		|	ACCT_MAINT_UPDATE	|	FILLER5	|	string	|	2	|		|		|		|
|		|	DIVTYPE	|	nstring	|	1	|	0	|		|		|	Default to '   '	|	'   '	|		|		|	ACCT_MAINT_UPDATE	|	FOREIGN_TAX_RATE	|	string	|	3	|		|		|		|
|		|	ESCHEAT	|	nstring	|	1	|	0	|		|		|	Default to '          '	|	'          '	|		|		|	ACCT_MAINT_UPDATE	|	ALPHA_CODE	|	string	|	10	|		|		|		|
|		|	ESCROWSHRS	|	double	|	15	|	0	|		|		|	Default to '         '	|	'         '	|		|		|	ACCT_MAINT_UPDATE	|	CUMULATIVE_DISCOUNT_NUMBR	|	string	|	9	|		|		|		|
|		|	EXPACCT	|	nstring	|	1	|	0	|		|		|	Default to '         '	|	'         '	|		|		|	ACCT_MAINT_UPDATE	|	LOI_NUMBER	|	string	|	9	|		|		|		|
|		|	EXPEXCH	|	nstring	|	1	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	LINE_CODE	|	string	|	1	|		|		|		|
|		|	FIDUCIARY	|	nstring	|	1	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	DIVIDEND_OPTION_CODE	|	string	|	1	|		|		|		|
|		|	FOREIGNTAX	|	double	|	15	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	CAPITAL_GAINS_OPTION_CODE	|	string	|	1	|		|		|		|
|		|	FRACCHECK	|	nstring	|	1	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	EXPEDITED_EXCHANGE_CODE	|	string	|	1	|		|		|		|
|		|	ISSUECODE	|	nstring	|	1	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	EXPEDITED_REDEMPTION_CODE	|	string	|	1	|		|		|		|
|		|	LASTMAINT	|	date/time	|	29	|	9	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	SUB_ACCOUNT_CODE	|	string	|	1	|		|		|		|
|		|	LINECODE	|	decimal	|	5	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	PENALTY_WITHHOLD_OVERRIDE	|	string	|	1	|		|		|		|
|		|	LISTBILL	|	nstring	|	1	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	ADDR_CHG_CODE	|	string	|	1	|		|		|		|
|		|	LOIFUNDGRP	|	decimal	|	10	|	0	|		|		|		|		|		|		|	ACCT_MAINT_UPDATE	|	FROM_CUSIP_NUMBER	|	string	|	7	|		|		|		|
|		|	LOINUMBER	|	decimal	|	10	|	0	|		|		|	Default to '  '	|	'  '	|		|		|	ACCT_MAINT_UPDATE	|	FILLER6	|	string	|	2	|		|		|		|
|		|	MAILFLAG	|	nstring	|	1	|	0	|		|		|	Default to '       '	|	'       '	|		|		|	ACCT_MAINT_UPDATE	|	FROM_FUND_CODE	|	string	|	7	|		|		|		|
|		|	MGMTCODE	|	nstring	|	2	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	FROM_CUSIP_FUND_CODE	|	string	|	1	|		|		|		|
|		|	MONITORVIP	|	nstring	|	1	|	0	|		|		|	Default to '         '	|	'         '	|		|		|	ACCT_MAINT_UPDATE	|	FILLER7	|	string	|	9	|		|		|		|
|		|	MULTICWR	|	nstring	|	1	|	0	|		|		|	Default to '           '	|	'           '	|		|		|	ACCT_MAINT_UPDATE	|	FROM_ACCOUNT_NUMBER	|	string	|	11	|		|		|		|
|		|	NAVCODE	|	nstring	|	1	|	0	|		|		|	Default to ' '	|	' '	|		|		|	ACCT_MAINT_UPDATE	|	FROM_CUSTOMER_ACCOUNT_CODE	|	string	|	1	|		|		|		|
|		|	NEW_ACCT_RSN_MKT	|	decimal	|	10	|	0	|		|		|	chr(010)	|	chr(010)	|		|		|	ACCT_MAINT_UPDATE	|	FILLER8	|	string	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
