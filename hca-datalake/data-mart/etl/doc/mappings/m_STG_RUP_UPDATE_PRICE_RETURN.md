|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Order No	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	UPR_PRICE_RETURN_UPDATE	|	UPR_PRICE_RETURN_UPDATE	|		|		|	1	|		|		|		|	Filter the record if FUND is NULL	|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	RECORD_TYPE_CDE	|	string	|	3	|	5	|		|		|	Not Used	|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	RECORD_TYPE_SEQUENCE_NBR	|	string	|	3	|	5	|		|		|	Not Used	|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	DETAIL_RECORD	|		|		|	5	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	MGMT_CODE	|	string	|	2	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPP_PRICE_UPD_RET	|	MANAGEMENT_CODE	|	varchar2	|	2	|	N	|	N	|		|
|	XML File	|	CUSIP_NUMBER	|	string	|	9	|	10	|		|		|	Direct Move (Trim the spaces)	|		|	LTRIM(RTRIM(CUSIP_NUMBER))	|		|	STG_UPP_PRICE_UPD_RET	|	CUSIP_NUMBER	|	varchar2	|	9	|	N	|	N	|		|
|	XML File	|	FILLER1	|		|		|	10	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	FILLER2	|	string	|	2	|	15	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	FUND_CODE_C	|		|		|	15	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	FUND_CODE	|	number	|	7	|	20	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	CUSIP_FUND_CODE	|	string	|	1	|	10	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|	CUSIP_INDICATOR	|	varchar2	|	1	|	N	|	N	|		|
|	XML File	|	NEW_FUND_CDE	|	string	|	1	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPP_PRICE_UPD_RET	|	NEW_FUND_CODE	|	varchar2	|	1	|	N	|	N	|		|
|	XML File	|	ACTION_TYPE_CDE	|	string	|	1	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPP_PRICE_UPD_RET	|	ACTION_TYPE_CODE	|	varchar2	|	1	|	N	|	N	|		|
|	XML File	|	UPDATE_REC_STATUS_CDE	|	string	|	1	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPP_PRICE_UPD_RET	|	UPD_REC_STATUS_CODE	|	varchar2	|	1	|	N	|	N	|		|
|	XML File	|	ERROR_CNT	|	number	|	2	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPP_PRICE_UPD_RET	|	ERROR_COUNT	|	"number(p,s)"	|	2	|	N	|	N	|		|
|	XML File	|	GENERIC_DETAIL_AREA	|	string	|	117	|	10	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	UPDATE_PRICE_INFO_1	|		|		|	10	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	PRICE_DATE	|		|		|	15	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	PRICE_DATE_YYYY	|		|		|	20	|		|		|	"Contatenate the columns Price,Century, Price_Year, Price_Month, Price_Date for the date format (YYYYMMDD)"	|		|	"TO_DATE(<br>LPAD(TO_CHAR(PRICE_CENTURY),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(PRICE_YEAR),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(PRICE_MONTH),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(PRICE_DAY),2,'0')<br>,'YYYYMMDD')"	|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	PRICE_CENTURY	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|	PRICE_DATE	|	date	|	19	|	N	|	N	|		|
|	XML File	|	PRICE_YEAR	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	PRICE_MONTH	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	PRICE_DAY	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	VERIFICATION_DTE1	|		|		|	15	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	VERIFICATION_YYYY_DTE1	|		|		|	20	|		|		|		|		|		|		|	STG_UPP_PRICE_UPD_RET	|		|		|		|		|		|		|
|	XML File	|	VERIFICATION_CC_DTE1	|	number	|	2	|	25	|		|		|	"Concatenate the columns VERIFICATION_CC_DTE, VERIFICATION_YY_DTE, VERIFICATION_MM_DTE, VERIFICATION_DD_DTE for the date format (YYYYMMDD)"	|		|	"TO_DATE(<br>LPAD(TO_CHAR(VERIFICATION_CC_DTE1),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(VERIFICATION_YY_DTE1),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(VERIFICATION_MM_DTE1),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(VERIFICATION_DD_DTE1),2,'0')<br>,'YYYYMMDD')"	|		|	STG_UPP_PRICE_UPD_RET	|	VERIFICATION_DATE	|	date	|	19	|	N	|	N	|		|
|	XML File	|	VERIFICATION_YY_DTE1	|	number	|	2	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	XML File	|	VERIFICATION_MM_DTE1	|	number	|	2	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	XML File	|	VERIFICATION_DD_DTE1	|	number	|	2	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	XML File	|	NAV_PER_SHARE_C	|		|		|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	XML File	|	NAV_PER_SHARE	|	number	|	9	|	20	|		|		|	Direct Move	|		|		|		|		|	NAV_PER_SHARE	|	"number(p,s)"	|	9	|	N	|	N	|		|
|	XML File	|	NAV_PER_SHARE_NET_CHNG_AMT_C	|		|		|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	XML File	|	NAV_PER_SHARE_NET_CHNG_AMT	|	number	|	9	|	20	|		|		|	Direct Move	|		|		|		|		|	NAV_NET_CHANGE	|	"number(p,s)"	|	9	|	N	|	N	|		|
|	XML File	|	NAV_PER_SHR_NET_CHNG_SIGN_CDE	|	string	|	1	|	15	|		|		|	Direct Move	|		|		|		|		|	NAV_NET_CHANGE_SIGN	|	varchar2	|	1	|	N	|	N	|		|
|	XML File	|	FILLER3	|	string	|	82	|	15	|		|		|	Process Start Time	|		|	SESSSTARTTIME	|		|		|	INSERT_TIMESTAMP	|	timestamp	|	26	|	N	|		|		|
|	XML File	|	UPDATE_RATE_INFO_1	|		|		|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	XML File	|	SUPER_DATE1	|		|		|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SUPER_DATE_YYYY1	|		|		|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SUPER_DATE_CC1	|	number	|	2	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SUPER_DATE_YY1	|	number	|	2	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SUPER_DATE_MM1	|	number	|	2	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	SUPER_DATE_DD1	|	number	|	2	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	VERIFICATION_DTE	|		|		|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	VERIFICATION_YYYY_DTE	|		|		|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	VERIFICATION_CC_DTE	|	number	|	2	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	VERIFICATION_YY_DTE	|	number	|	2	|	25	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	VERIFICATION_MM_DTE	|	number	|	2	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	VERIFICATION_DD_DTE	|	number	|	2	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DAILY_DIVIDEND_RATE_C	|		|		|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|	DAILY_DIVIDEND_RATE	|	number	|	11	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
