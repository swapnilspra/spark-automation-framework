|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|	Updaye if record already present	|		|		|		|		|		|		|		|
|	CCT_invoice	|	Key	|	string	|	25	|		|		|		|	SEQ Number	|		|	Table Name: HDM.CCT_TAX_SUMMARY <br>Condition: HBR_GRP_KEY = HBR_GRP_KEY1 AND SPEND_CAT_KEY = SPEND_CAT_KEY1 AND SUPPL_KEY = SUPPL_KEY1 AND DAY_KEY = DAY_KEY1 <br>Output Column: TAX_SUM_KEY 	|		|		|	CCT_TAX_SUMMARY	|	TAX_SUM_KEY	|	"number(p,s)"	|	10	|		|		|
|	CCT_invoice	|	Company	|	string	|	255	|		|		|		|		|		|	Table Name: HDM.CCT_HARBOR_GROUP <br>Condition: CO_NM = Company1 <br>Output Column: HBR_GRP_KEY 	|		|		|	CCT_TAX_SUMMARY	|	HBR_GRP_KEY	|	"number(p,s)"	|	10	|		|		|
|	CCT_invoice	|	Amount	|	decimal	|	16	|		|		|		|		|		|	Table Name: HDM.CCT_SPEND_CATEGORY <br>Condition: SPEND_CAT = Spend_Category1 <br>Output Column: SPEND_CAT_KEY 	|		|		|	CCT_TAX_SUMMARY	|	SPEND_CAT_KEY	|	"number(p,s)"	|	10	|		|		|
|	CCT_invoice	|	Line_Memo	|	string	|	1000	|		|		|		|		|		|	Table Name: HDM.CCT_SUPPLIER <br>Condition: SUPPL_NM = Supplier1 <br>Output Column: SUPPL_KEY	|		|		|	CCT_TAX_SUMMARY	|	SUPPL_KEY	|	"number(p,s)"	|	10	|		|		|
|	CCT_invoice	|	Spend_Category	|	string	|	255	|		|		|		|		|		|	Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = Date_Paid1<br>Output Column: DAY_KEY 	|		|		|	CCT_TAX_SUMMARY	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|	CCT_invoice	|	Supplier	|	string	|	255	|		|		|		|		|	ltrim(rtrim(Key))	|		|		|		|	CCT_TAX_SUMMARY	|	KEY_REF	|	varchar2	|	25	|		|		|
|	CCT_invoice	|	Supplier_Reference_Number	|	string	|	60	|		|		|		|		|	ltrim(rtrim(Line_Memo))	|		|		|		|	CCT_TAX_SUMMARY	|	LINE_MEMO	|	varchar2	|	1000	|		|		|
|	CCT_invoice	|	Date_Paid	|	string	|	20	|		|		|		|		|	ltrim(rtrim(Supplier_Reference_Number))	|		|		|		|	CCT_TAX_SUMMARY	|	SUPPL_REF_NBR	|	varchar2	|	60	|		|		|
|	CCT_invoice	|	Product_invoice_description	|	string	|	4000	|		|		|		|		|	ltrim(rtrim(Product_invoice_description))	|		|		|		|	CCT_TAX_SUMMARY	|	INVC_LINE_DESC	|	varchar2	|	4000	|		|		|
|	CCT_invoice	|	Tax_Analysis_Conclusion	|	string	|	4000	|		|		|		|		|	ltrim(rtrim(Tax_Analysis_Conclusion))	|		|		|		|	CCT_TAX_SUMMARY	|	TAX_ANLYS_CONCLSN	|	varchar2	|	4000	|		|		|
|	CCT_invoice	|	TOTAL	|	decimal	|	16	|		|		|		|	Amount	|		|		|		|		|	CCT_TAX_SUMMARY	|	INVC_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|	TOTAL	|		|		|		|		|	CCT_TAX_SUMMARY	|	TOT_TAX_AMT	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|	CCT_TAX_SUMMARY	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	CCT_TAX_SUMMARY	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	CCT_TAX_SUMMARY	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	CCT_TAX_SUMMARY	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	CCT_TAX_SUMMARY	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
