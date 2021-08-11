|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	SHR_CLS_FUND_KEY	|	decimal	|	10	|		|		|		|	//For new Insert<br>Increment the key value by '1'	|	//For New Insert<br>Used sequence generator to get the auto increment key	|		|	//If record already exisit update the record<br>Table : HDM.FUND<br>Condition: FUND_NBR = IN_FUND_NBR	|	FUND	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|	Default to 'All'	|	'All'	|		|		|	FUND	|	ALL_FUND_CD	|	nvarchar2	|	10	|		|		|		|
|		|	CURR_ROW_FLG	|	string	|	1	|		|		|		|	Default to 'All Funds'	|	'All Funds'	|		|		|	FUND	|	ALL_FUND_DESC	|	nvarchar2	|	25	|		|		|		|
|		|	FUND_NBR	|	decimal	|	11	|		|		|		|	FUND_COMPST_KEY from source	|		|	//FUND_COMPST_KEY<br>Table: HDM.FUND_COMPOSITE<br>Condition: COMPST_CD = IN_COMPST_CD <br>Output Column : FUND_COMPST_KEY	|		|	FUND	|	FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|	FUND_ABRV	|	string	|	50	|		|		|		|	SHR_CLS_CD from source	|	SHR_CLS_CD	|		|		|	FUND	|	SHR_CLS_CD	|	number	|	15	|		|		|		|
|		|	DIVD_FREQ_KEY	|	decimal	|	10	|		|		|		|	SHR_CLS_DESC from source	|	SHR_CLS_DESC	|		|		|	FUND	|	SHR_CLS_DESC	|	nvarchar2	|	100	|		|		|		|
|		|	CAP_GAIN_FREQ_KEY	|	decimal	|	10	|		|		|		|	COMPST_FUND_FULL_NM from source	|	COMPST_FUND_FULL_NM	|		|		|	FUND	|	FUND_NM	|	nvarchar2	|	50	|		|		|		|
|		|	REG_ACCT_MIN_INIT_INVMT_AMT	|	decimal	|	19	|		|		|		|	SECR_NM from source	|	SECR_NM	|		|		|	FUND	|	FUND_LONG_DESC	|	nvarchar2	|	255	|		|		|		|
|		|	IRA_ACCT_MIN_INIT_INVMT_AMT	|	decimal	|	19	|		|		|		|	FUND_ABRV from source	|	FUND_ABRV	|		|		|	FUND	|	FUND_ABRV	|	nvarchar2	|	50	|		|		|		|
|		|	UGMA_ACCT_MIN_INIT_INVMT_AMT	|	decimal	|	19	|		|		|		|	FUND_STAT_CD from source	|	FUND_STAT_CD	|		|		|	FUND	|	FUND_STAT_CD	|	nvarchar2	|	1	|		|		|		|
|		|	FUND_ORD_SEQ_NBR	|	decimal	|	38	|		|		|		|	Covert the 'FUND_STAT_DESC' 1st character as Upper and remaining in lower case	|	"UPPER(SUBSTR(FUND_STAT_DESC,1,1))&vert;&vert;LOWER(SUBSTR(FUND_STAT_DESC,2,255))"	|	Table : ADW.FUND_STATUS<br>Condition: FUND_STAT_CD = IN_FUND_STAT_CD<br>Output Column: FUND_STAT_DESC	|		|	FUND	|	FUND_STAT_DESC	|	nvarchar2	|	50	|		|		|		|
|		|	ADDR_ID	|	double	|	15	|		|		|		|	FUND_INCPN_DT from source	|	FUND_INCPN_DT	|		|		|	FUND	|	FUND_INCPN_DT	|	date	|	19	|		|		|		|
|		|	COMPST_FUND_KEY	|	decimal	|	10	|		|		|		|	FUND_CLOSED_DT from source	|	FUND_CLOSED_DT	|		|		|	FUND	|	FUND_CLOS_DT	|	date	|	19	|		|		|		|
|		|	COMPST_FUND_ID	|	decimal	|	11	|		|		|		|	FUND_NBR from source	|	FUND_NBR	|		|		|	FUND	|	FUND_NBR	|	number	|	15	|		|		|		|
|		|	COMPST_FUND_FULL_NM	|	string	|	60	|		|		|		|	FUND_ORD_SEQ_NBR from source	|	FUND_ORD_SEQ_NBR	|		|		|	FUND	|	FUND_SORT_ORD	|	number	|	15	|		|		|		|
|		|	INVMT_SECR_KEY	|	decimal	|	10	|		|		|		|	ADDR_ID from source	|	ADDR_ID	|		|		|	FUND	|	ADDR_ID	|	number	|	15	|		|		|		|
|		|	FUND_INCPN_DT	|	date/time	|	29	|		|		|		|	CUSIP_ID from source	|	CUSIP_ID	|		|		|	FUND	|	CUSIP_ID	|	nvarchar2	|	9	|		|		|		|
|		|	FUND_CLOSED_DT	|	date/time	|	29	|		|		|		|	QUOT_SYM_CD from source	|	QUOT_SYM_CD	|		|		|	FUND	|	QUOT_SYM	|	nvarchar2	|	8	|		|		|		|
|		|	ST_STR_FUND_ID	|	string	|	4	|		|		|		|	ST_STR_FUND_ID from source	|	ST_STR_FUND_ID	|		|		|	FUND	|	ST_STR_FUND_NBR	|	nvarchar2	|	4	|		|		|		|
|		|	FUND_TAX_ID	|	decimal	|	11	|		|		|		|	FUND_TAX_ID from source	|	FUND_TAX_ID	|		|		|	FUND	|	TAX_ID	|	nvarchar2	|	9	|		|		|		|
|		|	RISK_LVL_CD	|	decimal	|	11	|		|		|		|	RISK_LVL_CD from source	|	RISK_LVL_CD	|		|		|	FUND	|	RISK_LVL	|	number	|	15	|		|		|		|
|		|	FUND_STAT_CD	|	decimal	|	10	|		|		|		|	FREQ_CD from lookup	|	FREQ_CD	|	"Table : ADW.PAYOUT_FREQUENCY<br>Condition: FREQ_KEY = IN_DIVD_FREQ_KEY<br>Output Column: FREQ_CD, FREQ_DESC"	|		|	FUND	|	DIVD_FREQ_CD	|	nvarchar2	|	1	|		|		|		|
|		|	SHR_CLS_KEY	|	decimal	|	1	|		|		|		|	FREQ_DESC from lookup	|	FREQ_DESC	|		|		|	FUND	|	DIVD_FREQ_DESC	|	nvarchar2	|	100	|		|		|		|
|		|	SHR_CLS_CD	|	decimal	|	11	|		|		|		|	FREQ_CD1 from Lookup	|		|	"Table : ADW.PAYOUT_FREQUENCY<br>Condition: FREQ_KEY = IN_CAP_GAIN_FREQ_KEY<br>Output Column: FREQ_CD, FREQ_DESC"	|		|	FUND	|	CAP_GAIN_FREQ_CD	|	nvarchar2	|	1	|		|		|		|
|		|	SHR_CLS_DESC	|	string	|	255	|		|		|		|	FREQ_DESC1 from Lookup	|		|		|		|	FUND	|	CAP_GAIN_FREQ_DESC	|	nvarchar2	|	100	|		|		|		|
|		|	INVMT_SECR_KEY1	|	decimal	|	10	|		|		|		|	REG_ACCT_MIN_INIT_INVMT_AMT from source	|	REG_ACCT_MIN_INIT_INVMT_AMT	|		|		|	FUND	|	REG_ACCT_INIT_INVMT	|	number	|	15	|		|		|		|
|		|	SECR_NM	|	string	|	255	|		|		|		|	IRA_ACCT_MIN_INIT_INVMT_AMT from source	|	IRA_ACCT_MIN_INIT_INVMT_AMT	|		|		|	FUND	|	IRA_ACCT_INIT_INVMT	|	number	|	15	|		|		|		|
|		|	CUSIP_ID	|	decimal	|	11	|		|		|		|	UGMA_ACCT_MIN_INIT_INVMT_AMT from source	|	UGMA_ACCT_MIN_INIT_INVMT_AMT	|		|		|	FUND	|	UGMA_ACCT_INIT_INVMT	|	number	|	15	|		|		|		|
|		|	QUOT_SYM_CD	|	string	|	6	|		|		|		|	Current date	|	SYSDATE	|		|		|	FUND	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|		|	PERF_INCPN_DT	|	date/time	|	29	|		|		|		|		|		|		|		|	FUND	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|	CURR_ROW_FLG from source	|	CURR_ROW_FLG	|		|		|	FUND	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|		|		|		|		|		|		|		|	Default to '4'	|	4	|		|		|	FUND	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	$$etlcyckey	|	$$etlcyckey	|		|		|	FUND	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|		|		|		|		|		|		|		|	PERF_INCPN_DT from source	|	PERF_INCPN_DT	|		|		|	FUND	|	PERF_INCPN_DT	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
