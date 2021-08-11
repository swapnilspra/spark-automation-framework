|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	ACCT_NBR	|	decimal	|	20	|		|		|		|	Seq Number	|		|		|	Update if the reocrd already present	|	Table Name: HDM.SHDR_ACCT_FUND<br>Condition: ACCT_KEY = IN_ACCT_KEY AND FUND_KEY = IN_FUND_KEY AND SHRHLDR_KEY = IN_SHRHLDR_KEY AND REG_KEY = IN_REG_KEY AND DLR_KEY = IN_DLR_KEY AND DLR_BRANCH_KEY = IN_DLR_BRANCH_KEY AND PRIM_SHRHLDR_FLG = IN_PRIM_SHRHLDR_FLG	|	SHDR_ACCT_FUND	|	SHRHLDR_ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	BRANCH_ID	|	string	|	25	|		|		|		|		|	"IIF(ISNULL(BRANCH_KEY),-1,BRANCH_KEY)"	|	Table Name: HDM.DEALER_BRANCH<br>Condition: DLR_ID = IN_DLR_ID AND BRANCH_ID = IN_BRANCH_ID<br>Output Column : DLR_BRANCH_KEY	|		|		|	SHDR_ACCT_FUND	|	DLR_BRANCH_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|		|	"IIF(ISNULL(in_FUND_KEY),-1,in_FUND_KEY)"	|	Table Name: HDM.FUND<br>Condition: DLR_ID = FUND_NBR = IN_FUND_NBR<br>Output Column : FUND_KEY	|		|		|	SHDR_ACCT_FUND	|	FUND_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	TXBL_PARTY	|	string	|	1	|		|		|		|		|	"IIF(ISNULL(in_ACCT_KEY),-1,in_ACCT_KEY)"	|	Table Name: HDM.ACCOUNT<br>Condition: ACCT_NBR = IN_ACCT_NBR AND FUND_NBR = IN_FUND_NBR<br>Output Column :ACCT_KEY	|		|		|	SHDR_ACCT_FUND	|	ACCT_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	SHRHLDR_ROLE_KEY	|	decimal	|	10	|		|		|		|		|	"IIF(ISNULL(in_SHRHLDR_KEY),-1,in_SHRHLDR_KEY)"	|	Table Name: HDM.SHAREHOLDER<br>Condition: ACCT_NBR =CUST_IDNFCN_NBR = IN_CUST_IDNFCN_NBR AND CUST_IDNFCN_NBR_2 = IN_CUST_IDNFCN_NBR_2<br>Output Column :SHRHLDR_KEY	|		|		|	SHDR_ACCT_FUND	|	SHRHLDR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	CUST_ID	|	decimal	|	11	|		|		|		|		|	"IIF(ISNULL(in_DLR_KEY),-1,in_DLR_KEY)"	|	Table Name: HDM.DEALER<br>Condition: DLR_ID = IN_DLR_ID<br>Output Column :DLR_KEY	|		|		|	SHDR_ACCT_FUND	|	DLR_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	CUST_ID_2	|	decimal	|	11	|		|		|		|		|	"IIF(ISNULL(REG_KEY),300,REG_KEY)"	|	Table Name: HDM.REGION<br>Condition: ST_CD = IN_ST_CD AND MKTG_REG_ID = IN_MKTG_REG_ID AND CRTY_CD = IN_CRTY_CD<br>Output Column :REG_KEY	|		|		|	SHDR_ACCT_FUND	|	REG_KEY	|	"number(p,s)"	|	10	|		|	Y	|
|		|	DLR_ID	|	string	|	25	|		|		|		|		|	"IIF(TXBL_PARTY='Y','Y','N')"	|		|		|		|	SHDR_ACCT_FUND	|	PRIM_SHRHLDR_FLG	|	varchar2	|	1	|		|		|
|		|	ST_CD	|	string	|	3	|		|		|		|		|	SYSDATE	|		|		|		|	SHDR_ACCT_FUND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|	REG_KEY	|	decimal	|	10	|		|		|		|		|		|		|		|		|	SHDR_ACCT_FUND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|	CRTY_ISO_CD	|	string	|	2	|		|		|		|		|	$$etlcyckey	|		|		|		|	SHDR_ACCT_FUND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	ACCT_FUND_KEY	|	decimal	|	11	|		|		|		|		|		|		|		|		|	SHDR_ACCT_FUND	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|	EMAIL_ADDR	|	string	|	60	|		|		|		|		|	'Y'	|		|		|		|	SHDR_ACCT_FUND	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|	EMAIL_CONSNT_FLG	|	string	|	1	|		|		|		|	EMAIL_ADDR	|		|		|		|		|	SHDR_ACCT_FUND	|	EMAIL_ADDR	|	varchar2	|	255	|		|		|
|		|		|		|		|		|		|		|	EMAIL_CONSNT_FLG	|		|		|		|		|	SHDR_ACCT_FUND	|	EMAIL_CNSNT_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|	Seq Number	|		|		|		|		|		|	ERR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	"IIF(<br>      (NOT ISNULL(v_DLR_KEY_ERR_MG)<br>                                                         OR<br>       NOT ISNULL(v_BRANCH_KEY_ERR_MG)<br>                                                         OR<br>       NOT ISNULL(v_SHRHLDR_KEY_ERR_MG)<br>                                                         OR<br>       NOT ISNULL(v_ACCT_KEY_ERR_MG)<br>                                                          OR<br>       NOT ISNULL(v_FUND_KEY_ERR_MG)),<br>(v_DLR_KEY_ERR_MG&vert;&vert;' '&vert;&vert;v_BRANCH_KEY_ERR_MG&vert;&vert;' '&vert;&vert;v_SHRHLDR_KEY_ERR_MG&vert;&vert;' '&vert;&vert;<br>v_ACCT_KEY_ERR_MG&vert;&vert;' '&vert;&vert;v_FUND_KEY_ERR_MG),NULL)"	|		|		|		|		|	ERR_MSG	|	varchar2	|	4000	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|		|	RUN_DT_TS	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
