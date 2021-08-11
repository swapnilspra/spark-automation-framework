|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|	CLIENT_NAME	|	string	|	200	|		|		|		|	Seq Number	|	//FUND_Key_v	|	":LKP.LKP_FIDUCIARY_POSITION(<br>:LKP.LKP_HDM_FIDUCIARY_CUSTOMER(CLIENT_NAME),<br>:LKP.LKP_DAY_KEY(RPT_DATE),<br>FUND_KEY_v<br>--:LKP.LKP_VW_FIDUCIARY_FUND_BY_NAME(UPPER(LTRIM(RTRIM(FUND_NAME))))<br>)<br><br>//:LKP.LKP_FIDUCIARY_POSITION<br><br>Table Name: HDM.FIDUCIARY_POSITION<br>Condition : FID_CUST_KEY = FID_CUST_KEY1 AND DAY_KEY = DAY_KEY1 AND FUND_KEY = FUND_KEY1<br>Return COlumn : FID_POSN_KEY<br><br>//:LKP.LKP_HDM_FIDUCIARY_CUSTOMER(CLIENT_NAME)<br><br>Table Name: HDM.FIDUCIARY_CUSTOMER <br>Condition : FID_CUST_NM = CLIENT_NAME<br>Return Column: FID_CUST_KEY<br><br>//:LKP.LKP_DAY_KEY(RPT_DATE)<br><br>Table Name: HDM.CALENDAR<br>Condition: CAL_DAY = IN_CAL_DAY<br>Return COlumn: DAY_KEY<br><br>//:LKP.LKP_VW_FIDUCIARY_FUND_BY_NAME(UPPER(LTRIM(RTRIM(FUND_NAME))))<br><br>Override Query: SELECT HDM.VW_FIDUCIARY_FUND.FUND_KEY as FUND_KEY, <br>       HDM.VW_FIDUCIARY_FUND.FUND_NBR as FUND_NBR <br>FROM HDM.VW_FIDUCIARY_FUND<br>WHERE HDM.VW_FIDUCIARY_FUND.FUND_NBR IS NOT NULL<br>ORDER BY HDM.VW_FIDUCIARY_FUND.FUND_SORT_ORD<br><br>COndition: FUND_NBR = FUND_NBR_IN<br>Return Column: FUND_KEY"	|	"If record already present , then update the record"	|		|	FIDUCIARY_POSITION	|	FID_POSN_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	FUND_NAME	|	string	|	200	|		|		|		|		|		|	:LKP.LKP_HDM_FIDUCIARY_CUSTOMER(CLIENT_NAME)	|		|		|	FIDUCIARY_POSITION	|	FID_CUST_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	RPT_DATE	|	date/time	|	29	|		|		|		|		|		|	:LKP.LKP_DAY_KEY(RPT_DATE)	|		|		|	FIDUCIARY_POSITION	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	NAV_VAL	|	double	|	15	|		|		|		|		|	"IIF(IS_NUMBER(FUND_NAME),<br>:LKP.LKP_VW_FIDUCIARY_FUND_BY_FNDNBR(TO_INTEGER(FUND_NAME)),<br>:LKP.LKP_VW_FIDUCIARY_FUND_BY_NAME(UPPER(LTRIM(RTRIM(FUND_NAME))))<br>)"	|	"//:LKP.LKP_VW_FIDUCIARY_FUND_BY_FNDNBR(TO_INTEGER(FUND_NAME)<br><br>Override Query : SELECT HDM.VW_FIDUCIARY_FUND.FUND_KEY as FUND_KEY, <br>       HDM.VW_FIDUCIARY_FUND.FUND_NBR as FUND_NBR <br>FROM HDM.VW_FIDUCIARY_FUND<br>WHERE HDM.VW_FIDUCIARY_FUND.FUND_NBR IS NOT NULL<br>ORDER BY HDM.VW_FIDUCIARY_FUND.FUND_SORT_ORD<br><br>Condition: FUND_NBR = FUND_NBR_IN<br>Return COlun: FUND_KEY<br><br>//:LKP.LKP_VW_FIDUCIARY_FUND_BY_NAME(UPPER(LTRIM(RTRIM(FUND_NAME))))<br><br>Override Query: SELECT HDM.VW_FIDUCIARY_FUND.FUND_KEY as FUND_KEY, <br>       HDM.VW_FIDUCIARY_FUND.FUND_NBR as FUND_NBR <br>FROM HDM.VW_FIDUCIARY_FUND<br>WHERE HDM.VW_FIDUCIARY_FUND.FUND_NBR IS NOT NULL<br>ORDER BY HDM.VW_FIDUCIARY_FUND.FUND_SORT_ORD"	|		|		|	FIDUCIARY_POSITION	|	FUND_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	SHARES	|	double	|	15	|		|		|		|	NAV_VAL	|		|		|		|		|	FIDUCIARY_POSITION	|	NAV	|	number	|	15	|		|		|
|		|	MKT_VAL	|	double	|	15	|		|		|		|	SHARES	|		|		|		|		|	FIDUCIARY_POSITION	|	SHRS	|	number	|	15	|		|		|
|		|	ONE_MNTH_RET	|	double	|	15	|		|		|		|	MKT_VAL	|		|		|		|		|	FIDUCIARY_POSITION	|	MKT_VAL	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|	ONE_MNTH_RET	|		|		|		|		|	FIDUCIARY_POSITION	|	MO_RET	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|	FIDUCIARY_POSITION	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	FIDUCIARY_POSITION	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FIDUCIARY_POSITION	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FIDUCIARY_POSITION	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FIDUCIARY_POSITION	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
