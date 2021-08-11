|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Order No	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	UPR_PRICE_RETURN_UPDATE	|	UPR_PRICE_RETURN_UPDATE	|		|		|	1	|		|		|		|	(RECORD_TYPE_CDE = 'UPP' OR RECORD_TYPE_CDE = 'RUP' )  AND RECORD_TYPE_SEQUENCE_NBR = '001'	|		|		|		|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	RECORD_TYPE_CDE	|	string	|	3	|	5	|		|		|	Not Used	|		|		|		|		|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	RECORD_TYPE_SEQUENCE_NBR	|	string	|	3	|	5	|		|		|	Not Used	|		|		|		|		|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	DETAIL_RECORD	|		|		|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	MGMT_CODE	|	string	|	2	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPR_RATE_UPD_RET	|	MANAGEMENTCODE	|	varchar2	|	2	|	N	|	N	|		|
|	UPR_PRICE_RETURN_UPDATE	|	CUSIP_NUMBER	|	string	|	9	|	10	|		|		|	Direct Move (Trim the spaces)	|		|	LTRIM(RTRIM(CUSIP_NUMBER))	|		|	STG_UPR_RATE_UPD_RET	|	CUSIP_NUMBER	|	varchar2	|	9	|	N	|	N	|		|
|	UPR_PRICE_RETURN_UPDATE	|	FILLER1	|		|		|	10	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	FILLER2	|	string	|	2	|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	FUND_CODE_C	|		|		|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	FUND_CODE	|	number	|	7	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	CUSIP_FUND_CODE	|	string	|	1	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPR_RATE_UPD_RET	|	CUSIP_INDICATOR	|	varchar2	|	1	|	N	|	N	|		|
|	UPR_PRICE_RETURN_UPDATE	|	NEW_FUND_CDE	|	string	|	1	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPR_RATE_UPD_RET	|	NEW_FUND_CODE	|	varchar2	|	1	|	N	|	N	|		|
|	UPR_PRICE_RETURN_UPDATE	|	ACTION_TYPE_CDE	|	string	|	1	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPR_RATE_UPD_RET	|	ACTION_TYPE_CODE	|	varchar2	|	1	|	N	|	N	|		|
|	UPR_PRICE_RETURN_UPDATE	|	UPDATE_REC_STATUS_CDE	|	string	|	1	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPR_RATE_UPD_RET	|	UPD_REC_STATUS_CODE	|	varchar2	|	1	|	N	|	N	|		|
|	UPR_PRICE_RETURN_UPDATE	|	ERROR_CNT	|	number	|	2	|	10	|		|		|	Direct Move	|		|		|		|	STG_UPR_RATE_UPD_RET	|	ERROR_COUNT	|	"number(p,s)"	|	2	|	N	|	N	|		|
|	UPR_PRICE_RETURN_UPDATE	|	GENERIC_DETAIL_AREA	|	string	|	117	|	10	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	UPDATE_PRICE_INFO_1	|		|		|	10	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	PRICE_DATE	|		|		|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	PRICE_DATE_YYYY	|		|		|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	PRICE_CENTURY	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	PRICE_YEAR	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	PRICE_MONTH	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	PRICE_DAY	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_DTE1	|		|		|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_YYYY_DTE1	|		|		|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_CC_DTE1	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_YY_DTE1	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_MM_DTE1	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_DD_DTE1	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	NAV_PER_SHARE_C	|		|		|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	NAV_PER_SHARE	|	number	|	9	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	NAV_PER_SHARE_NET_CHNG_AMT_C	|		|		|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	NAV_PER_SHARE_NET_CHNG_AMT	|	number	|	9	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	NAV_PER_SHR_NET_CHNG_SIGN_CDE	|	string	|	1	|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	FILLER3	|	string	|	82	|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	UPDATE_RATE_INFO_1	|		|		|	10	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	SUPER_DATE1	|		|		|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	SUPER_DATE_YYYY1	|		|		|	20	|		|		|	Concatenate of date fields and populate in the format of YYYYMMDD	|		|	"TO_DATE(<br>LPAD(TO_CHAR(SUPERSHEET_CENTURY),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(SUPERSHEET_YEAR),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(SUPERSHEET_MONTH),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(SUPERSHEET_DAY),2,'0')<br>,'YYYYMMDD')"	|		|	STG_UPR_RATE_UPD_RET	|	SUPER_SHEET_DATE	|	date	|	9	|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	SUPER_DATE_CC1	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	SUPER_DATE_YY1	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	SUPER_DATE_MM1	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	SUPER_DATE_DD1	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_DTE	|		|		|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_YYYY_DTE	|		|		|	20	|		|		|	Concatenate of date fields and populate in the format of YYYYMMDD	|		|	"TO_DATE(<br>LPAD(TO_CHAR(VERIFICATION_CC_DTE),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(VERIFICATION_YY_DTE),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(VERIFICATION_MM_DTE),2,'0')&vert;&vert;<br>LPAD(TO_CHAR(VERIFICATION_DD_DTE),2,'0')<br>,'YYYYMMDD')"	|		|	STG_UPR_RATE_UPD_RET	|	VERIFICATION_DATE	|	date	|	9	|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_CC_DTE	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_YY_DTE	|	number	|	2	|	25	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_MM_DTE	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	VERIFICATION_DD_DTE	|	number	|	2	|	20	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	DAILY_DIVIDEND_RATE_C	|		|		|	15	|		|		|		|		|		|		|	STG_UPR_RATE_UPD_RET	|		|		|		|		|		|		|
|	UPR_PRICE_RETURN_UPDATE	|	DAILY_DIVIDEND_RATE	|	number	|	11	|	20	|		|		|	Direct Move	|		|		|		|	STG_UPR_RATE_UPD_RET	|	DAILY_DIVIDEND_RATE	|	"number(p,s)"	|	11	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
