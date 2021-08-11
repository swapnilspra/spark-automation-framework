|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	QUOT_SYM	|	string	|	8	|		|		|		|		|		|	Override Query: SELECT <br> TO_NUMBER(HDWSYS.USER_SECURITY.DECRYPT(HEXTORAW(SUB_ADVISOR_EMPLOYEE.TAX_ID))) as TAX_ID_CONVERTED<br>FROM<br> DBO.SUB_ADVISOR_EMPLOYEE<br>WHERE<br>SUB_ADVISOR_EMPLOYEE.EMPLOYEE_ACTIVE='Y'<br><br>Condition: TAX_ID_CONVERTED = TAX_ID_IN<br>Return COlumn : TAX_ID_CONVERTED	|	NOT ISNULL(:LKP.LKP_TAX_ID(TAX_ID))	|	Filter the record which does not match the lookup	|		|		|		|		|		|		|
|		|	CUSIP_ID	|	string	|	9	|		|		|		|	QUOT_SYM	|		|		|		|		|	BROKER_CONFIRM	|	QUOT_SYM	|	string	|	10	|		|		|
|		|	FUND_NM	|	string	|	50	|		|		|		|	CUSIP_ID	|		|		|		|		|		|	CUSIP_ID	|	string	|	10	|		|		|
|		|	ACCT_NBR	|	double	|	15	|		|		|		|		|	'	|		|		|		|		|	SEDOL	|	string	|	10	|		|		|
|		|	TXN_TYPE_DESC	|	string	|	60	|		|		|		|		|	'	|		|		|		|		|	ISIN	|	string	|	10	|		|		|
|		|	CAL_DAY_EFFTV	|	date/time	|	29	|		|		|		|		|	'MUT'	|		|		|		|		|	SECURITY_TYPE	|	string	|	10	|		|		|
|		|	CAL_DAY_STTL	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|	RIC_CODE	|	string	|	10	|		|		|
|		|	SHRS	|	decimal	|	15	|		|		|		|		|	'	|		|		|		|		|	CORPORATE_SECURITY_ID	|	string	|	10	|		|		|
|		|	SHR_PR	|	decimal	|	15	|		|		|		|	FUND_NM	|		|		|		|		|		|	SECURITY_NAME	|	string	|	10	|		|		|
|		|	GR_AMT	|	double	|	15	|		|		|		|	ACCT_NBR	|		|		|		|		|		|	ACCOUNT_NUMBER	|	double	|	10	|		|		|
|		|	NET_AMT	|	double	|	15	|		|		|		|		|	HARBOR'	|		|		|		|		|	BROKER_CODE	|	string	|	10	|		|		|
|		|	TAX_ID	|	double	|	15	|		|		|		|		|	"IIF(SHR_BAL_EFFCT_FLG = 'A','BUY','SELL')"	|		|		|		|		|	TXN_TYPE_DESC	|	string	|	10	|		|		|
|		|	SHR_BAL_EFFCT_FLG	|	string	|	1	|		|		|		|	CAL_DAY	|		|		|		|		|		|	CAL_DAY	|	datetime	|	29	|		|		|
|		|		|		|		|		|		|		|	CAL_DAY1	|		|		|		|		|		|	CAL_DAY1	|	datetime	|	29	|		|		|
|		|		|		|		|		|		|		|	SUM_SHRS	|		|		|		|		|		|	SHARES	|	double	|	10	|		|		|
|		|		|		|		|		|		|		|	SHR_PR	|		|		|		|		|		|	SHR_PR	|	double	|	10	|		|		|
|		|		|		|		|		|		|		|	GR_AMT	|		|		|		|		|		|	PRINCIPAL_AMOUNT	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|	NET_AMT	|		|		|		|		|		|	NET_AMOUNT	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	COMMISION	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	INTEREST	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	SECURITY_FEE	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	OTHER_FEE	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	OTHER_FEE_2	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	OTHER_FEE_3	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	"IIF(TXN_TYPE_DESC = 'Cancel', 'Y','N')"	|		|		|		|		|	CANCEL_FLAG	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	N'	|		|		|		|		|	CORRECT_FLAG	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	COMMENTS	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	TRADE_ID	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	TRADE_SHORT_SALE	|	string	|	15	|		|		|
|		|		|		|		|		|		|		|		|	'	|		|		|		|		|	OPEN_CLOSE_OPTION	|	string	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
