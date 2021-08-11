|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ	|	ASOF_DATE	|	string	|	25	|		|		|		|		|		|		|		|	If record already present then update the recpord	|		|		|		|		|		|		|
|		|	ACCT_NO	|	string	|	25	|		|		|		|		|		|	"//:LKP.LKP_FND_COMPST_KEY(ACCT_NO)<br><br>Tabke Name:HDM.PIMCO_ACCOUNT_REFERENCE<br>Condition: PMC_ACCT_NBR = PMC_ACCT_NBR_IN<br>output COlumn: FUND_COMPST_KEY<br><br>//:LKP.LKP_PMC_REG_KEY(TO_INTEGER(RPT_REGION_CODE),<br>TO_INTEGER( COUNTRY_NO))<br><br>Override Query : SELECT PIMCO_REGION.PMC_REG_KEY AS PMC_REG_KEY,<br>         NVL(PIMCO_REGION.REG_CD,0)    AS REG_CD,<br>         NVL(PIMCO_REGION.CRTY_ID,0)   AS CRTY_ID<br>    FROM HDM.PIMCO_REGION<br>   WHERE     PIMCO_REGION.CURR_ROW_FLG = 'Y'<br>GROUP BY PIMCO_REGION.PMC_REG_KEY, PIMCO_REGION.REG_CD, PIMCO_REGION.CRTY_ID<br>Condition:  REG_CD = REG_CD_IN AND CRTY_ID = CRTY_ID_IN<br>output COlumn: PMC_REG_KEY<br><br><br>//:LKP.LKP_DAY_KEY(TO_DATE(ASOF_DATE,'MM/DD/YYYY'))<br><br>Table Name : HDM.CALENDAR<br>Condition: CAL_DAY = CAL_DAY_in<br>Ouput Column: DAY_KEY"	|	"Override Query : SELECT HDM.PIMCO_COUNTRY_BREAKOUT.CRTY_BRK_OUT_KEY as CRTY_BRK_OUT_KEY, HDM.PIMCO_COUNTRY_BREAKOUT.ACCT_EXT_DUR_YR as ACCT_EXT_DUR_YR, HDM.PIMCO_COUNTRY_BREAKOUT.ACCT_EXT_EM_SPRD as ACCT_EXT_EM_SPRD, HDM.PIMCO_COUNTRY_BREAKOUT.ACCT_LOC_DUR_YR as ACCT_LOC_DUR_YR, HDM.PIMCO_COUNTRY_BREAKOUT.ACCT_CURR_EXPSR as ACCT_CURR_EXPSR, HDM.PIMCO_COUNTRY_BREAKOUT.BMK_EXT_DUR_YR as BMK_EXT_DUR_YR, HDM.PIMCO_COUNTRY_BREAKOUT.BMK_EXT_EM_SPRD as BMK_EXT_EM_SPRD, HDM.PIMCO_COUNTRY_BREAKOUT.BMK_LOC_DUR_YR as BMK_LOC_DUR_YR, HDM.PIMCO_COUNTRY_BREAKOUT.BMK_CURR_EXPSR as BMK_CURR_EXPSR, HDM.PIMCO_COUNTRY_BREAKOUT.VAR_EXT_DUR_YR as VAR_EXT_DUR_YR, HDM.PIMCO_COUNTRY_BREAKOUT.VAR_EXT_EM_SPRD as VAR_EXT_EM_SPRD, HDM.PIMCO_COUNTRY_BREAKOUT.VAR_LOC_DUR_YR as VAR_LOC_DUR_YR, HDM.PIMCO_COUNTRY_BREAKOUT.VAR_CURR_EXPSR as VAR_CURR_EXPSR, HDM.PIMCO_COUNTRY_BREAKOUT.CURR_ROW_FLG as CURR_ROW_FLG, HDM.PIMCO_COUNTRY_BREAKOUT.ROW_STRT_DTTM as ROW_STRT_DTTM, HDM.PIMCO_COUNTRY_BREAKOUT.FUND_COMPST_KEY as FUND_COMPST_KEY, HDM.PIMCO_COUNTRY_BREAKOUT.PMC_REG_KEY as PMC_REG_KEY, HDM.PIMCO_COUNTRY_BREAKOUT.DAY_KEY as DAY_KEY FROM HDM.PIMCO_COUNTRY_BREAKOUT<br>WHERE HDM.PIMCO_COUNTRY_BREAKOUT.CURR_ROW_FLG = 'Y'<br><br>COndition: FUND_COMPST_KEY = FUND_COMPST_KEY_in AND PMC_REG_KEY = PMC_REG_KEY_in AND DAY_KEY = DAY_KEY_in<br><br>Output COlumn: CRTY_BRK_OUT_KEY"	|	If record already present then update the recpord	|	PIMCO_COUNTRY_BREAKOUT	|	CRTY_BRK_OUT_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	PM_EM	|	string	|	25	|		|		|		|		|		|	as per aboe lookup	|		|		|	PIMCO_COUNTRY_BREAKOUT	|	FUND_COMPST_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	RPT_REGION_CODE	|	string	|	25	|		|		|		|		|		|	as per aboe lookup	|		|		|	PIMCO_COUNTRY_BREAKOUT	|	PMC_REG_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	RPT_REGION_DESC	|	string	|	255	|		|		|		|		|		|	as per aboe lookup	|		|		|	PIMCO_COUNTRY_BREAKOUT	|	DAY_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	COUNTRY_NO	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_DUR_YRS_in)	|		|		|	TO_DECIMAL(ACCT_DUR_YRS_in)	|	PIMCO_COUNTRY_BREAKOUT	|	ACCT_EXT_DUR_YR	|	number	|	15	|		|		|
|		|	COUNTRY_DESCRIPTION	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_EM_SPREAD_in)	|		|		|	TO_DECIMAL(ACCT_EM_SPREAD_in)	|	PIMCO_COUNTRY_BREAKOUT	|	ACCT_EXT_EM_SPRD	|	number	|	15	|		|		|
|		|	ACCT_DUR_YRS	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_DUR_YRS1_in)	|		|		|	TO_DECIMAL(ACCT_DUR_YRS1_in)	|	PIMCO_COUNTRY_BREAKOUT	|	ACCT_LOC_DUR_YR	|	number	|	15	|		|		|
|		|	ACCT_EM_SPREAD	|	string	|	25	|		|		|		|		|	TO_DECIMAL(ACCT_CURREXP_in)/100	|		|		|	TO_DECIMAL(ACCT_CURREXP_in)/100	|	PIMCO_COUNTRY_BREAKOUT	|	ACCT_CURR_EXPSR	|	number	|	15	|		|		|
|		|	ACCT_DUR_YRS1	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_DUR_YRS_in)	|		|		|	TO_DECIMAL(BM_DUR_YRS_in)	|	PIMCO_COUNTRY_BREAKOUT	|	BMK_EXT_DUR_YR	|	number	|	15	|		|		|
|		|	ACCT_CURREXP	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_EM_SPREAD_in)	|		|		|	TO_DECIMAL(BM_EM_SPREAD_in)	|	PIMCO_COUNTRY_BREAKOUT	|	BMK_EXT_EM_SPRD	|	number	|	15	|		|		|
|		|	BM_DUR_YRS	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_DUR_YRS1_in)	|		|		|	TO_DECIMAL(BM_DUR_YRS1_in)	|	PIMCO_COUNTRY_BREAKOUT	|	BMK_LOC_DUR_YR	|	number	|	15	|		|		|
|		|	BM_EM_SPREAD	|	string	|	25	|		|		|		|		|	TO_DECIMAL(BM_CURREXP_in)/100	|		|		|	TO_DECIMAL(BM_CURREXP_in)/100	|	PIMCO_COUNTRY_BREAKOUT	|	BMK_CURR_EXPSR	|	number	|	15	|		|		|
|		|	BM_DUR_YRS1	|	string	|	25	|		|		|		|		|	TO_DECIMAL(VAR_DUR_YRS_in)	|		|		|	TO_DECIMAL(VAR_DUR_YRS_in)	|	PIMCO_COUNTRY_BREAKOUT	|	VAR_EXT_DUR_YR	|	number	|	15	|		|		|
|		|	BM_CURREXP	|	string	|	25	|		|		|		|		|	TO_DECIMAL(VAR_EM_SPREAD_in)	|		|		|	TO_DECIMAL(VAR_EM_SPREAD_in)	|	PIMCO_COUNTRY_BREAKOUT	|	VAR_EXT_EM_SPRD	|	number	|	15	|		|		|
|		|	VAR_DUR_YRS	|	string	|	25	|		|		|		|		|	TO_DECIMAL(VAR_DUR_YRS1_in)	|		|		|	TO_DECIMAL(VAR_DUR_YRS1_in)	|	PIMCO_COUNTRY_BREAKOUT	|	VAR_LOC_DUR_YR	|	number	|	15	|		|		|
|		|	VAR_EM_SPREAD	|	string	|	25	|		|		|		|		|	TO_DECIMAL(VAR_CURREXP_in)/100	|		|		|	TO_DECIMAL(VAR_CURREXP_in)/100	|	PIMCO_COUNTRY_BREAKOUT	|	VAR_CURR_EXPSR	|	number	|	15	|		|		|
|		|	VAR_DUR_YRS1	|	string	|	25	|		|		|		|		|	Y'	|		|		|	Y'	|	PIMCO_COUNTRY_BREAKOUT	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|
|		|	VAR_CURREXP	|	string	|	25	|		|		|		|		|	SYSDATE	|		|		|	SYSDATE	|	PIMCO_COUNTRY_BREAKOUT	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_COUNTRY_BREAKOUT	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_COUNTRY_BREAKOUT	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	PIMCO_COUNTRY_BREAKOUT	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
