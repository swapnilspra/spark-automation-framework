|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|	Generate the auto increment key by 1. for each insert	|	Use sequence generator to generate the value	|		|		|	BROKER_POSITION_Insert	|	BRKR_POSN_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|		|		|		|		|		|		|		|		|	"//Lookup POSN_TYPE_KEY<br>Overrride Query : SELECT HDM.POSITION_TYPE.POSN_TYPE_KEY as POSN_TYPE_KEY, LTRIM(RTRIM(HDM.POSITION_TYPE.POSN_CD)) as POSN_CD FROM HDM.POSITION_TYPE<br><br>Condition: POSN_CD = POSITION_TYP_OUT (From Source)<br><br>Ouput Column: POSN_CD<br><br>//Lookup Final<br>Override query: SELECT HDM.BROKER.BRKR_KEY as BRKR_KEY, HDM.BROKER.POSN_TYPE_KEY as POSN_TYPE_KEY, LTRIM(RTRIM(HDM.BROKER.ST_STR_NBR)) as ST_STR_NBR, LTRIM(RTRIM(HDM.BROKER.BRKR_NM)) as BRKR_NM FROM HDM.BROKER<br><br>Condition: POSN_TYPE_KEY = POSN_TYPE_KEY1 <br>AND ST_STR_NBR = FUND_OUT (From source)<br>AND BRKR_NM = BRKR_NM_OUT (From Source)<br><br>Ouput COlumn : BRKR_KEY"	|		|	BROKER_POSITION_Insert	|	BRKR_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|	BRKRCONFIRM	|	PROCESS_DT	|	string	|	15	|		|		|		|	Convert the date to 'MM/DD/YYYY' format	|	"TO_DATE(PROCESS_DT,'MM/DD/YYYY')"	|	"//Lookup POSN_TYPE_KEY<br>Overrride Query : SELECT HDM.POSITION_TYPE.POSN_TYPE_KEY as POSN_TYPE_KEY, LTRIM(RTRIM(HDM.POSITION_TYPE.POSN_CD)) as POSN_CD FROM HDM.POSITION_TYPE<br><br>Condition: POSN_CD = POSITION_TYP_OUT (From Source)<br><br>Ouput Column: POSN_CD<br><br>//Lookup<br>Table Name: HDM.CALENDAR<br><br>Condition : CAL_DAY = IN_CAL_DAY"	|		|	BROKER_POSITION_Insert	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|	BRKRCONFIRM	|	SNO	|	double	|	15	|		|		|		|	Direct Move	|	SNO	|		|		|	BROKER_POSITION_Insert	|	SER_NBR	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	FUND	|	string	|	7	|		|		|		|	Remove space on SUB_ADV_FND_ID 	|	LTRIM(RTRIM(SUB_ADV_FND_ID)) 	|		|		|	BROKER_POSITION_Insert	|	SUB_ADVSR_FUND_ID	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	SUB_ADV_FND_ID	|	string	|	18	|		|		|		|	Remove space on CUSIP 	|	LTRIM(RTRIM(CUSIP)) 	|		|		|	BROKER_POSITION_Insert	|	ASSET_ID	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	BRKR_NM	|	string	|	150	|		|		|		|	Remove space on LX_LN_BLOOM_ID	|	LTRIM(RTRIM(LX_LN_BLOOM_ID))	|		|		|	BROKER_POSITION_Insert	|	THRD_PARTY_ID	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	CUSIP	|	string	|	20	|		|		|		|	Remove space on OTH_IDEN	|	LTRIM(RTRIM(OTH_IDEN))	|		|		|	BROKER_POSITION_Insert	|	OTHR_ID	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	LX_LN_BLOOM_ID	|	string	|	25	|		|		|		|	Remove space on TRADE_ID	|	LTRIM(RTRIM(TRADE_ID))	|		|		|	BROKER_POSITION_Insert	|	TRDE_ID	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	OTH_IDEN	|	string	|	25	|		|		|		|	Remove space on ASSET_DESC	|	LTRIM(RTRIM(ASSET_DESC))	|		|		|	BROKER_POSITION_Insert	|	ASSET_DESC	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	TRADE_ID	|	string	|	25	|		|		|		|	Remove space on PRODUCTS	|	LTRIM(RTRIM(PRODUCTS))	|		|		|	BROKER_POSITION_Insert	|	PROD_NM	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	ASSET_DESC	|	string	|	100	|		|		|		|	Convert to decimal 'QUANTITY'	|	TO_DECIMAL(LTRIM(RTRIM(QUANTITY))) 	|		|		|	BROKER_POSITION_Insert	|	ASSET_QTY	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	PRODUCTS	|	string	|	255	|		|		|		|	"TO_DATE(LTRIM(RTRIM(TRADE_DT)),'MM/DD/YYYY')"	|	"TO_DATE(LTRIM(RTRIM(TRADE_DT)),'MM/DD/YYYY')"	|		|		|	BROKER_POSITION_Insert	|	TRDE_DT	|	date	|	19	|		|		|		|
|	BRKRCONFIRM	|	QUANTITY	|	string	|	32	|		|		|		|	"TO_DATE(LTRIM(RTRIM(SETTLE_DT)),'MM/DD/YYYY')"	|	"TO_DATE(LTRIM(RTRIM(SETTLE_DT)),'MM/DD/YYYY')"	|		|		|	BROKER_POSITION_Insert	|	STTL_DT	|	date	|	19	|		|		|		|
|	BRKRCONFIRM	|	TRADE_DT	|	string	|	15	|		|		|		|	LTRIM(RTRIM(SALE_PURCHASE_IND))	|	LTRIM(RTRIM(SALE_PURCHASE_IND))	|		|		|	BROKER_POSITION_Insert	|	TXN_TYPE	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	SETTLE_DT	|	string	|	15	|		|		|		|	Convert to decimal 'PURCHASE_PRICE'	|	TO_DECIMAL(LTRIM(RTRIM(PURCHASE_PRICE)))	|		|		|	BROKER_POSITION_Insert	|	PURCH_PR	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	SALE_PURCHASE_IND	|	string	|	11	|		|		|		|	Convert to decimal ''VENDOR_PRICE'	|	TO_DECIMAL(LTRIM(RTRIM(VENDOR_PRICE)))	|		|		|	BROKER_POSITION_Insert	|	VEND_PR	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	PURCHASE_PRICE	|	string	|	32	|		|		|		|	Convert to decimal 'MARKET_VALUE'	|	TO_DECIMAL(LTRIM(RTRIM(MARKET_VALUE)))	|		|		|	BROKER_POSITION_Insert	|	MKT_VAL	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	VENDOR_PRICE	|	string	|	32	|		|		|		|	COUPON	|	COUPON	|		|		|	BROKER_POSITION_Insert	|	COUPN	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	MARKET_VALUE	|	string	|	32	|		|		|		|	"TO_DATE(LTRIM(RTRIM(MATURITY_DT)),'MM/DD/YYYY')"	|	"TO_DATE(LTRIM(RTRIM(MATURITY_DT)),'MM/DD/YYYY')"	|		|		|	BROKER_POSITION_Insert	|	MTRY_DT	|	date	|	19	|		|		|		|
|	BRKRCONFIRM	|	POSITION_TYP	|	string	|	8	|		|		|		|	Convert to decimal  'TRADED_AMT'	|	TO_DECIMAL(LTRIM(RTRIM(TRADED_AMT)))	|		|		|	BROKER_POSITION_Insert	|	TRDE_AMT	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	COUPON	|	double	|	15	|		|		|		|	Convert to decimal 'SETTL_AMT'	|	TO_DECIMAL(LTRIM(RTRIM(SETTL_AMT)))	|		|		|	BROKER_POSITION_Insert	|	STTL_AMT	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	MATURITY_DT	|	string	|	15	|		|		|		|	Convert to decimal 'PLEDGED_AMT'	|	TO_DECIMAL(LTRIM(RTRIM(PLEDGED_AMT)))	|		|		|	BROKER_POSITION_Insert	|	PLDG_AMT	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	TRADED_AMT	|	string	|	32	|		|		|		|	Convert to decimal  'UNSETTLED_AMT'	|	TO_DECIMAL(LTRIM(RTRIM(UNSETTLED_AMT)))	|		|		|	BROKER_POSITION_Insert	|	UN_STTLD_AMT	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|	SETTL_AMT	|	string	|	32	|		|		|		|	Remove space on CONTACT	|	LTRIM(RTRIM(CONTACT))	|		|		|	BROKER_POSITION_Insert	|	CONTCT_NM	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	PLEDGED_AMT	|	string	|	32	|		|		|		|	Remove space on PHONE	|	LTRIM(RTRIM(PHONE))	|		|		|	BROKER_POSITION_Insert	|	PHON_DET	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	UNSETTLED_AMT	|	string	|	32	|		|		|		|	Remove space on FAX	|	LTRIM(RTRIM(FAX))	|		|		|	BROKER_POSITION_Insert	|	FAX_DET	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	CONTACT	|	string	|	100	|		|		|		|	Remove space on ADDRESS	|	LTRIM(RTRIM(ADDRESS))	|		|		|	BROKER_POSITION_Insert	|	ADDR	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	PHONE	|	string	|	100	|		|		|		|	Remove space on E_MAIL	|	LTRIM(RTRIM(E_MAIL))	|		|		|	BROKER_POSITION_Insert	|	EMAIL_ID	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	FAX	|	string	|	100	|		|		|		|	Remove space on  NOTES	|	LTRIM(RTRIM(NOTES))	|		|		|	BROKER_POSITION_Insert	|	NOTES	|	nvarchar2	|	255	|		|		|		|
|	BRKRCONFIRM	|	ADDRESS	|	string	|	255	|		|		|		|		|		|		|		|	BROKER_POSITION_Insert	|	CURR_ROW_FLG	|	nvarchar2	|	1	|		|		|		|
|	BRKRCONFIRM	|	E_MAIL	|	string	|	255	|		|		|		|		|		|		|		|	BROKER_POSITION_Insert	|	ROW_STRT_DTTM	|	date	|	19	|		|		|		|
|	BRKRCONFIRM	|	NOTES	|	string	|	255	|		|		|		|		|		|		|		|	BROKER_POSITION_Insert	|	ROW_STOP_DTTM	|	date	|	19	|		|		|		|
|	BRKRCONFIRM	|	ROW_STRT_DTTM	|	date/time	|	29	|		|		|		|		|		|		|		|	BROKER_POSITION_Insert	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|		|
|	BRKRCONFIRM	|		|		|		|		|		|		|		|		|		|		|	BROKER_POSITION_Insert	|	SRC_SYS_ID	|	number	|	15	|		|		|		|
|	BRKRCONFIRM	|		|		|		|		|		|		|		|		|		|		|	UPDATE	|		|		|		|		|		|		|
|	BRKRCONFIRM	|		|		|		|		|		|		|		|		|		|		|	BROKER_POSITION_Update	|	BRKR_POSN_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|		|		|		|		|		|		|		|		|	"//Lookup POSN_TYPE_KEY<br>Overrride Query : SELECT HDM.POSITION_TYPE.POSN_TYPE_KEY as POSN_TYPE_KEY, LTRIM(RTRIM(HDM.POSITION_TYPE.POSN_CD)) as POSN_CD FROM HDM.POSITION_TYPE<br><br>Condition: POSN_CD = POSITION_TYP_OUT (From Source)<br><br>Ouput Column: POSN_CD<br><br>//Lookup Final<br>Override query: SELECT HDM.BROKER.BRKR_KEY as BRKR_KEY, HDM.BROKER.POSN_TYPE_KEY as POSN_TYPE_KEY, LTRIM(RTRIM(HDM.BROKER.ST_STR_NBR)) as ST_STR_NBR, LTRIM(RTRIM(HDM.BROKER.BRKR_NM)) as BRKR_NM FROM HDM.BROKER<br><br>Condition: POSN_TYPE_KEY = POSN_TYPE_KEY1 <br>AND ST_STR_NBR = FUND_OUT (From source)<br>AND BRKR_NM = BRKR_NM_OUT (From Source)<br><br>Ouput COlumn : BRKR_KEY"	|		|	BROKER_POSITION_Update	|	BRKR_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|		|		|		|		|		|		|	Convert the date to 'MM/DD/YYYY' format	|	"TO_DATE(PROCESS_DT,'MM/DD/YYYY')"	|	"//Lookup POSN_TYPE_KEY<br>Overrride Query : SELECT HDM.POSITION_TYPE.POSN_TYPE_KEY as POSN_TYPE_KEY, LTRIM(RTRIM(HDM.POSITION_TYPE.POSN_CD)) as POSN_CD FROM HDM.POSITION_TYPE<br><br>Condition: POSN_CD = POSITION_TYP_OUT (From Source)<br><br>Ouput Column: POSN_CD<br><br>//Lookup<br>Table Name: HDM.CALENDAR<br><br>Condition : CAL_DAY = IN_CAL_DAY"	|		|	BROKER_POSITION_Update	|	DAY_KEY	|	"number(p,s)"	|	10	|		|	Y	|		|
|		|		|		|		|		|		|		|	Direct Move	|	SNO	|		|		|	BROKER_POSITION_Update	|	SER_NBR	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	Remove space on SUB_ADV_FND_ID 	|	LTRIM(RTRIM(SUB_ADV_FND_ID)) 	|		|		|	BROKER_POSITION_Update	|	SUB_ADVSR_FUND_ID	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	Remove space on CUSIP 	|	LTRIM(RTRIM(CUSIP)) 	|		|		|	BROKER_POSITION_Update	|	ASSET_ID	|	nvarchar2	|	255	|		|		|		|
|		|		|		|		|		|		|		|	Remove space on LX_LN_BLOOM_ID	|	LTRIM(RTRIM(LX_LN_BLOOM_ID))	|		|		|	BROKER_POSITION_Update	|	THRD_PARTY_ID	|	nvarchar2	|	255	|		|		|		|
|		|		|		|		|		|		|		|	Remove space on OTH_IDEN	|	LTRIM(RTRIM(OTH_IDEN))	|		|		|	BROKER_POSITION_Update	|	OTHR_ID	|	nvarchar2	|	255	|		|		|		|
|		|		|		|		|		|		|		|	Remove space on TRADE_ID	|	LTRIM(RTRIM(TRADE_ID))	|		|		|	BROKER_POSITION_Update	|	TRDE_ID	|	nvarchar2	|	255	|		|		|		|
|		|		|		|		|		|		|		|	Remove space on ASSET_DESC	|	LTRIM(RTRIM(ASSET_DESC))	|		|		|	BROKER_POSITION_Update	|	ASSET_DESC	|	nvarchar2	|	255	|		|		|		|
|		|		|		|		|		|		|		|	Remove space on PRODUCTS	|	LTRIM(RTRIM(PRODUCTS))	|		|		|	BROKER_POSITION_Update	|	PROD_NM	|	nvarchar2	|	255	|		|		|		|
|		|		|		|		|		|		|		|	Convert to decimal 'QUANTITY'	|	TO_DECIMAL(LTRIM(RTRIM(QUANTITY))) 	|		|		|	BROKER_POSITION_Update	|	ASSET_QTY	|	number	|	15	|		|		|		|
|		|		|		|		|		|		|		|	"TO_DATE(LTRIM(RTRIM(TRADE_DT)),'MM/DD/YYYY')"	|	"TO_DATE(LTRIM(RTRIM(TRADE_DT)),'MM/DD/YYYY')"	|		|		|	BROKER_POSITION_Update	|	TRDE_DT	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|	"TO_DATE(LTRIM(RTRIM(SETTLE_DT)),'MM/DD/YYYY')"	|	"TO_DATE(LTRIM(RTRIM(SETTLE_DT)),'MM/DD/YYYY')"	|		|		|	BROKER_POSITION_Update	|	STTL_DT	|	date	|	19	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
