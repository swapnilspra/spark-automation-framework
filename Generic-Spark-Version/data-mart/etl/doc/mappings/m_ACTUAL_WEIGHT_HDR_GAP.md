|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|		|		|		|		|	For Update	|		|		|		|		|		|
|	Refer SQ	|	ACTUAL_HDR_ID	|	double	|	15	|		|		|		|		|	"//ACTUAL_WEIGHT_DATE_NEXT_DAY_v<br>ADD_TO_DATE(<br>trunc(to_date(ACTUAL_WEIGHT_DATE_in,'mm/dd/yyyy hh24:mi:ss')),'DD',v)"	|	"//(:LKP.LKP_TARGET(TGT_FND_ID_in,ACTUAL_WEIGHT_DATE_NEXT_DAY))"	|		|	"BUSINESS_DAY_FLG=''<br>AND <br>NOT ISNULL(:LKP.LKP_TARGET(TGT_FND_ID_in,ACTUAL_WEIGHT_DATE_NEXT_DAY))"	|	FND_ACTUAL_WEIGHT_HDR	|	ACTUAL_HDR_ID	|	number	|	15	|		|		|
|		|	TGT_FND_ID	|	double	|	15	|		|		|		|	TGT_FND_ID	|		|		|	convert single row to 5 rows	|		|	FND_ACTUAL_WEIGHT_HDR	|	TGT_FND_ID	|	number	|	15	|		|		|
|		|	ACTUAL_WEIGHT_DATE	|	date/time	|	29	|		|		|		|		|	"//ACTUAL_WEIGHT_DATE_NEXT_DAY_v<br>ADD_TO_DATE(<br>trunc(to_date(ACTUAL_WEIGHT_DATE_in,'mm/dd/yyyy hh24:mi:ss')),'DD',v)"	|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	ACTUAL_WEIGHT_DATE	|	date	|	19	|		|		|
|		|	INSERT_TIMESTAMP	|	date/time	|	29	|		|		|		|		|		|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	INSERT_TIMESTAMP	|	date	|	19	|		|		|
|		|	INSERT_USER	|	string	|	20	|		|		|		|		|		|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	INSERT_USER	|	varchar2	|	20	|		|		|
|		|	UPDATE_TIMESTAMP	|	date/time	|	29	|		|		|		|		|	SYSDATE	|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	UPDATE_TIMESTAMP	|	date	|	19	|		|		|
|		|	UPDATE_USER	|	string	|	20	|		|		|		|		|	'HDWSYS'	|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	UPDATE_USER	|	varchar2	|	20	|		|		|
|		|	IS_PUBLISHED	|	string	|	1	|		|		|		|		|		|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	IS_PUBLISHED	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|	"//:LKP.LKP_TARGET(TGT_FND_ID_in,ACTUAL_WEIGHT_DATE_NEXT_DAY<br><br>Table Name: FND_ACTUAL_WEIGHT_HDR <br>Condition: TGT_FND_ID = TGT_FND_ID_IN AND ACTUAL_WEIGHT_DATE = ACTUAL_WEIGHT_DATE_IN <br>Output Column: ACTUAL_HDR_ID<br>"	|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	For Insert	|		|		|		|		|		|
|		|		|		|		|		|		|		|		|	"//ACTUAL_WEIGHT_DATE_NEXT_DAY_v<br>ADD_TO_DATE(<br>trunc(to_date(ACTUAL_WEIGHT_DATE_in,'mm/dd/yyyy hh24:mi:ss')),'DD',v)"	|	"//(:LKP.LKP_TARGET(TGT_FND_ID_in,ACTUAL_WEIGHT_DATE_NEXT_DAY))"	|		|	"BUSINESS_DAY_FLG=''<br>AND <br>ISNULL(:LKP.LKP_TARGET(TGT_FND_ID_in,ACTUAL_WEIGHT_DATE_NEXT_DAY))"	|	FND_ACTUAL_WEIGHT_HDR	|	ACTUAL_HDR_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|	TGT_FND_ID	|		|		|	convert single row to 5 rows	|		|	FND_ACTUAL_WEIGHT_HDR	|	TGT_FND_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|		|	"//ACTUAL_WEIGHT_DATE_NEXT_DAY_v<br>ADD_TO_DATE(<br>trunc(to_date(ACTUAL_WEIGHT_DATE_in,'mm/dd/yyyy hh24:mi:ss')),'DD',v)"	|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	ACTUAL_WEIGHT_DATE	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	INSERT_TIMESTAMP	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	INSERT_USER	|	varchar2	|	20	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	UPDATE_TIMESTAMP	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|	'HDWSYS'	|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	UPDATE_USER	|	varchar2	|	20	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	FND_ACTUAL_WEIGHT_HDR	|	IS_PUBLISHED	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
