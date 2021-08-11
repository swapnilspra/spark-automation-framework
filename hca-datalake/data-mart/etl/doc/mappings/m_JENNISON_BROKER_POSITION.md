|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	TAX_ID	|	double	|	15	|		|		|		|		|		|	Override Query: SELECT <br> TO_NUMBER(HDWSYS.USER_SECURITY.DECRYPT(HEXTORAW(SUB_ADVISOR_EMPLOYEE.TAX_ID))) as TAX_ID_CONVERTED<br>FROM<br> DBO.SUB_ADVISOR_EMPLOYEE<br>WHERE<br>SUB_ADVISOR_EMPLOYEE.EMPLOYEE_ACTIVE='Y'<br><br>Condition: TAX_ID_CONVERTED = TAX_ID_IN<br>Return Column: TAX_ID_CONVERTED	|	//Filter Condition<br><br>NOT ISNULL(:LKP.LKP_TAX_ID(TAX_ID))	|		|		|		|		|		|		|		|
|		|	ACCT_NBR	|	double	|	15	|		|		|		|	ACCT_NBR	|		|		|		|		|	BROKER_POSITION_FLAT_FILE	|	ACCOUNT_NUMBER	|	double	|	10	|		|		|
|		|	QUOT_SYM	|	string	|	8	|		|		|		|		|	'HARBOR'	|		|		|		|	BROKER_POSITION_FLAT_FILE	|	BROKER_CODE	|	string	|	10	|		|		|
|		|	CUSIP_ID	|	string	|	9	|		|		|		|	QUOT_SYM	|		|		|		|		|	BROKER_POSITION_FLAT_FILE	|	QUOT_SYMBOL	|	string	|	10	|		|		|
|		|	TOT_SHRS	|	decimal	|	14	|		|		|		|	CUSIP_ID	|		|		|		|		|	BROKER_POSITION_FLAT_FILE	|	FUND_CUSIP	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|	TOT_SHRS	|		|		|		|		|	BROKER_POSITION_FLAT_FILE	|	TOT_SHARES	|	nstring	|	14	|		|		|
|		|		|		|		|		|		|		|		|	''	|		|		|		|	BROKER_POSITION_FLAT_FILE	|	SEDOL	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	''	|		|		|		|	BROKER_POSITION_FLAT_FILE	|	ISIN	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	''	|		|		|		|	BROKER_POSITION_FLAT_FILE	|	RIC_CODE	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	''	|		|		|		|	BROKER_POSITION_FLAT_FILE	|	CORP_SEC_ID	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'' 	|		|		|		|	BROKER_POSITION_FLAT_FILE	|	SECURITY_NAME	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'' 	|		|		|		|	BROKER_POSITION_FLAT_FILE	|	SECURITY_NAME2	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
