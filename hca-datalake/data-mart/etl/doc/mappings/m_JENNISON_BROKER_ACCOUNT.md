|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Dummy	|	Dummy	|		|		|		|		|		|		|	'ACCOUNT_FUND'	|		|	Write Header	|		|	BROKER_ACCOUNT	|	ACCOUNT_FUND	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'ESTABLISHED_DATE'	|		|		|		|	BROKER_ACCOUNT	|	ESTABLISHED_DATE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'BROKER_CODE'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_CODE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'CORPORATE_AP_ID'	|		|		|		|	BROKER_ACCOUNT	|	CORPORATE_AP_ID	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'ACCOUNT_NAME'	|		|		|		|	BROKER_ACCOUNT	|	ACCOUNT_NAME	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'ACCOUNT_DESCRIPTION'	|		|		|		|	BROKER_ACCOUNT	|	ACCOUNT_DESCRIPTION	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'CLOSED_DATE'	|		|		|		|	BROKER_ACCOUNT	|	CLOSED_DATE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'TAX_ID'	|		|		|		|	BROKER_ACCOUNT	|	TAX_ID	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'NOT_MONITORED'	|		|		|		|	BROKER_ACCOUNT	|	NOT_MONITORED	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'ACCOUNT_TYPE'	|		|		|		|	BROKER_ACCOUNT	|	ACCOUNT_TYPE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'BROKER_CONTACT'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_CONTACT	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'BROKER_ADDRESS1'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_ADDRESS1	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'BROKER_ADDRESS2'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_ADDRESS2	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'BROKER_CITY'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_CITY	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'BROKER_STATE'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_STATE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'BROKER_ZIP'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_ZIP	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'BROKER_PHONE'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_PHONE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'BROKER_COUNTRY_CODE'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_COUNTRY_CODE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'COMMENTS'	|		|		|		|	BROKER_ACCOUNT	|	COMMENTS	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'FUND_CUSIP'	|		|		|		|	BROKER_ACCOUNT	|	FUND_CUSIP	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'ACCOUNT_STATUS'	|		|		|		|	BROKER_ACCOUNT	|	ACCOUNT_STATUS	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	Detaill Records	|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	Refer SQ	|	ACCT_TYPE	|	string	|	60	|		|		|		|		|	TO_CHAR(ACCT_NBR) &vert;&vert; TO_CHAR(FUND_NBR)	|	Override Query: SELECT <br> TO_NUMBER(HDWSYS.USER_SECURITY.DECRYPT(HEXTORAW(SUB_ADVISOR_EMPLOYEE.TAX_ID))) as TAX_ID_CONVERTED<br>FROM<br> DBO.SUB_ADVISOR_EMPLOYEE<br>WHERE<br>SUB_ADVISOR_EMPLOYEE.EMPLOYEE_ACTIVE='Y'<br><br>Condition: TAX_ID_CONVERTED = TAX_ID_IN<br>output COlumn: TAX_ID_CONVERTED	|	<br><br>Filter the records<br><br>NOT ISNULL(:LKP.LKP_TAX_ID(TAX_ID))<br><br>	|	Sort All the records with all the fields as key column	|	BROKER_ACCOUNT	|	ACCOUNT_FUND	|	string	|	100	|		|		|
|		|	ACCT_NBR	|	double	|	15	|		|		|		|		|	"TO_CHAR(ACCT_STRT_DT,'MM/DD/YYYY')"	|		|		|		|	BROKER_ACCOUNT	|	ESTABLISHED_DATE	|	string	|	100	|		|		|
|		|	TAX_ID	|	double	|	15	|		|		|		|		|	HARBOR'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_CODE	|	string	|	100	|		|		|
|		|	ACCT_STAT_CD	|	decimal	|	1	|		|		|		|		|	TO_CHAR(TAX_ID_IN)	|		|		|		|	BROKER_ACCOUNT	|	CORPORATE_AP_ID	|	string	|	100	|		|		|
|		|	ACCT_STAT	|	string	|	60	|		|		|		|	REGSTN_LINE_1	|		|		|		|		|	BROKER_ACCOUNT	|	ACCOUNT_NAME	|	string	|	100	|		|		|
|		|	ACCT_STRT_DT	|	date/time	|	29	|		|		|		|		|	ACCT_TYPE	|		|		|		|	BROKER_ACCOUNT	|	ACCOUNT_DESCRIPTION	|	string	|	100	|		|		|
|		|	REGSTN_LINE_1	|	string	|	50	|		|		|		|		|	"IIF(ACCT_STAT_CD=1,TO_CHAR(ACCT_CLSD_DT,'MM/DD/YYYY'),null) "	|		|		|		|	BROKER_ACCOUNT	|	CLOSED_DATE	|	string	|	100	|		|		|
|		|	ACCT_CLSD_DT	|	date/time	|	29	|		|		|		|		|	TO_CHAR(TAX_ID_IN)	|		|		|		|	BROKER_ACCOUNT	|	TAX_ID	|	string	|	100	|		|		|
|		|	FUND_CUSIP	|	string	|	10	|		|		|		|		|	N'	|		|		|		|	BROKER_ACCOUNT	|	NOT_MONITORED	|	string	|	100	|		|		|
|		|	FUND_NBR	|	string	|	10	|		|		|		|		|	ACCT_TYPE_CODE	|		|		|		|	BROKER_ACCOUNT	|	ACCOUNT_TYPE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_CONTACT	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'111 SOUTH WACKER Drive '	|		|		|		|	BROKER_ACCOUNT	|	BROKER_ADDRESS1	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	34th Floor'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_ADDRESS2	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	CHICAGO'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_CITY	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	'IL'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_STATE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	60606'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_ZIP	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	1-800-422-1050'	|		|		|		|	BROKER_ACCOUNT	|	BROKER_PHONE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	 '	|		|		|		|	BROKER_ACCOUNT	|	BROKER_COUNTRY_CODE	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	 ' 	|		|		|		|	BROKER_ACCOUNT	|	COMMENTS	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|	FUND_CUSIP	|		|		|		|		|	BROKER_ACCOUNT	|	FUND_CUSIP	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|	"IIF(UPPER(ACCT_STAT) = 'CLOSED STATUS', 'CLOSED','OPEN')"	|		|		|		|	BROKER_ACCOUNT	|	ACCOUNT_STATUS	|	string	|	100	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
