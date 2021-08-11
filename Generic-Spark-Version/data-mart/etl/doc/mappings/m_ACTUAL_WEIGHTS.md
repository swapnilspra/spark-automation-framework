|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	WEIGHTS	|	TYPE	|	string	|	2	|		|		|		|		|		|		|	Insert Only	|	(NOT ISNULL(LTRIM(RTRIM(TICKER_SYMB))) OR<br>NOT IS_SPACES(LTRIM(RTRIM(TICKER_SYMB))) ) AND<br>TYPE = 'DD' AND<br>(NOT ISNULL(INVEST_IND) OR NOT IS_SPACES(INVEST_IND))	|		|		|		|		|		|		|
|	WEIGHTS	|	FUND_ID	|	string	|	8	|		|		|		|	SEQ Number	|		|		|		|		|	FND_ACTUAL_WEIGHTS	|	ACTUAL_WT_ID	|	number	|	15	|		|		|
|	WEIGHTS	|	BASE_CNTRY_CD	|	string	|	2	|		|		|		|		|		|	"//:LKP.LKP_WEIGHT_HDR(FUND_ID,TO_DATE($$DATE_IN,'dd-mon-yy'))<br><br>Table Name: DBO.FND_ACTUAL_WEIGHT_HDR <br>Override Query : SELECT A.ACTUAL_HDR_ID AS ACTUAL_HDR_ID<br>     , B.STATE_STR_FUND_NBR AS TGT_FND_NBR<br>     , A.ACTUAL_WEIGHT_DATE AS ACTUAL_WEIGHT_DATE<br>FROM DBO.FND_ACTUAL_WEIGHT_HDR A, DBO.FND_FUND B<br>WHERE A.TGT_FND_ID = B.ID<br><br>Output COlumn: TGT_FND_NBR = FND_NBR_IN AND ACTUAL_WEIGHT_DATE = CALEN_DT_IN"	|		|		|	FND_ACTUAL_WEIGHTS	|	ACTUAL_HDR_ID	|	number	|	15	|		|		|
|	WEIGHTS	|	CRNCY_CD_BASE	|	string	|	3	|		|		|		|		|		|	"//:LKP.LKP_FND_ID_WITH_QUOT_SYM(LTRIM(RTRIM(TICKER_SYMB)))<br><br>Table Name : DBO.FND_FUND<br>Override Query : SELECT DBO.FND_FUND.ID as ID, LTRIM(RTRIM(DBO.FND_FUND.QUOT_SYM)) as QUOT_SYM FROM DBO.FND_FUND <br>Output Column: QUOT_SYM"	|		|		|	FND_ACTUAL_WEIGHTS	|	FND_ID	|	number	|	15	|		|		|
|	WEIGHTS	|	CALEN_DT	|	string	|	19	|		|		|		|		|	"(TOTAL_BASE_MKT_VAL*:LKP.LKP_FND_PRICE(FND_ID_IN,ACTUAL_WEIGHT_DT)) /:LKP.LKP_TOT_WEIGHTS_NON_CASH(EXISTING_HDR_ID)<br><br>//TOTAL_BASE_MKT_VAL<br>SUM(BASE_MKT_VAL)"	|	":LKP.LKP_FND_PRICE(FND_ID_IN,ACTUAL_WEIGHT_DT)<br>Table Name: DBO.FND_PRICE<br>Condition :  FND_ID = FND_ID_IN AND PRICE_DATE = PRICE_DT_IN<br>Output Column: PRICE<br><br>:LKP.LKP_TOT_WEIGHTS_NON_CASH(EXISTING_HDR_ID)<br>Table Name: DBO.FND_ACTUAL_TOT_WEIGHTS <br>Override Query: SELECT SUM (DBO.FND_ACTUAL_TOT_WEIGHTS.TOT_WEIGHT) AS TOT_WEIGHT<br>     , DBO.FND_ACTUAL_TOT_WEIGHTS.ACTUAL_HDR_ID AS ACTUAL_HDR_ID<br>FROM DBO.FND_ACTUAL_TOT_WEIGHTS<br>WHERE INDICATOR = 'M'<br>GROUP BY DBO.FND_ACTUAL_TOT_WEIGHTS.ACTUAL_HDR_ID<br>Condition: ACTUAL_HDR_ID = ACTUAL_HDR_ID_IN <br>Output COlumn: TOT_WEIGHT"	|		|		|	FND_ACTUAL_WEIGHTS	|	WEIGHT	|	number	|	15	|		|		|
|	WEIGHTS	|	ASSET_ID	|	string	|	9	|		|		|		|		|	'HDWSYS'	|		|		|		|	FND_ACTUAL_WEIGHTS	|	INSERT_USER	|	varchar2	|	20	|		|		|
|	WEIGHTS	|	ALT_ASSET_ID_TYPE_CD1	|	string	|	3	|		|		|		|		|	SESSSTARTTIME	|		|		|		|	FND_ACTUAL_WEIGHTS	|	INSERT_TIMESTAMP	|	date	|	19	|		|		|
|	WEIGHTS	|	ALT_ASSET_ID1	|	string	|	25	|		|		|		|		|		|		|		|		|	FND_ACTUAL_WEIGHTS	|	UPDATE_USER	|	varchar2	|	20	|		|		|
|	WEIGHTS	|	ALT_ASSET_ID_TYPE_CD2	|	string	|	3	|		|		|		|		|		|		|		|		|	FND_ACTUAL_WEIGHTS	|	UPDATE_TIMESTAMP	|	date	|	19	|		|		|
|	WEIGHTS	|	ALT_ASSET_ID2	|	string	|	50	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	DTC_CUSIP	|	string	|	9	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	TICKER_SYMB	|	string	|	20	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	ISSUE_LONG_NM	|	string	|	60	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	INVEST_IND	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	ASSET_GRP_CD	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	POS_TYPE_CD	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	INVEST_TYPE_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	STATE_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	INCORP_CNTRY_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	ISSUE_CNTRY_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	TRD_CNTRY_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	NRATAX_CNTRY_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	ISSUE_DT	|	string	|	8	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	POS_MTRTY_DT	|	string	|	8	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	DAYS_TOMAT_CNT	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	ORIG_CPN_RT	|	string	|	16	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	POS_INT_RT	|	string	|	16	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	VAR_RT_CHG_DT	|	string	|	8	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	VAR_RT_FREQ_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	MOODY_RTG	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	SNP_RTG	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	MJR_INDSTY_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	MNR_INDSTY_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	POOL_TYPE_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	LOT_ACCT_NUM	|	string	|	6	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	REPO_NUM	|	string	|	6	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	TOT_LOTS	|	string	|	5	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	TRD_DT	|	string	|	8	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	SETTLE_DT	|	string	|	8	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	ISSUE_CLS_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	RPT_CLS_CD	|	string	|	2	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	CALL_PUT_IND	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	ORG_STRIKE_PRC	|	string	|	16	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	CRNT_STRIKE_PRC	|	string	|	16	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	SHRPAR_QTY	|	string	|	17	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	CONTRACTS_QTY	|	string	|	17	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	ORG_FACE_POS_QTY	|	string	|	17	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	UNTCST_BTL	|	string	|	16	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	UNTCST_LTL	|	string	|	16	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	WEIGHTS	|	CST_ORG_BTL	|	string	|	17	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
