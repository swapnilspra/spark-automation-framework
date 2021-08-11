|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	STG_DST_PRICE	|	FND_ID	|	double	|	15	|	N	|		|		|	Direct Move	|		|		|		|		|	FND_ID	|	number	|	15	|	N	|	Y	|		|
|	STG_DST_PRICE	|	PRICE_DATE	|	date/time	|	29	|	N	|		|		|	Direct Move	|		|		|		|		|	PRICE_DATE	|	date	|	19	|	N	|	Y	|		|
|	STG_DST_PRICE	|	PRICE	|	decimal	|	"19,4"	|		|		|		|	Direct Move	|		|		|		|		|	PRICE	|	"number(p,s)"	|	19	|	Y	|	N	|		|
|	STG_DST_PRICE	|	N/A	|		|		|		|		|		|	// Previous Price <br><br>Lookup FND_Price with FND_ID<br>If 'Price' found then<br>get Price from lookup table<br>else<br> Map 'Price' from source	|	"iif(isnull(PREVIOUS_PRICE_in),PRICE,PREVIOUS_PRICE_in)"	|	//PREVIOUS_PRICE_in<br>select the table 'FND_Price' for the value 'FND_ID'	|		|		|	PREVIOUS_PRICE	|	"number(p,s)"	|	19	|	Y	|	N	|		|
|	STG_DST_PRICE	|	DOLLAR_CHANGE	|	decimal	|	"19,4"	|	N	|		|		|	PRICE - PREVIOUS_PRICE	|	PRICE - PREVIOUS_PRICE_v	|		|		|		|	DOLLAR_CHANGE	|	"number(p,s)"	|	19	|	Y	|	N	|		|
|	STG_DST_PRICE	|	N/A	|		|		|		|		|		|	"// PRCT_CHANGE <br>(PRICE - PREVIOUS_PRICE_v+ORD_INCOME_v+CAP_GAINS_SHORT_v+CAP_GAINS_LONG_v + NON_TAXABLE_DIS_v) / PREVIOUS_PRICE_v<br><br>Lookup FND_DIVIDED with FND_ID and PRICE DATE,<br>if ORD_INCOME found then<br>  ORD_INCOME<br>else<br> 0<br> <br>if CAP_GAINS_SHORT found then<br>  CAP_GAINS_SHORT<br>else<br> 0<br><br>if CAP_GAINS_LONG found then<br>  CAP_GAINS_LONG<br>else<br> 0<br><br>if NON_TAXABLE_DIS found then<br>  NON_TAXABLE_DIS<br>else<br> 0"	|	"(PRICE - PREVIOUS_PRICE_v+ORD_INCOME_v+CAP_GAINS_SHORT_v+CAP_GAINS_LONG_v + NON_TAXABLE_DIS_v) / PREVIOUS_PRICE_v<br><br><br>//ORD_INCOME_v<br>iif(isnull(ORD_INCOME),0,ORD_INCOME)<br><br>//CAP_GAINS_SHORT_v<br>iif(isnull(CAP_GAINS_SHORT),0,CAP_GAINS_SHORT)<br><br>//CAP_GAINS_LONG_v<br>iif(isnull(CAP_GAINS_LONG),0,CAP_GAINS_LONG)<br><br>//NON_TAXABLE_DIS_v<br>iif(isnull(NON_TAXABLE_DIS),0,NON_TAXABLE_DIS)<br>"	|	"Select <br>CAP_GAINS_SHORT,<br>CAP_GAINS_LONG,<br>NON_TAXABLE_DIS<br>from <br>FND_DIVIDEND Where<br>FND_ID = ID_in (FND_ID from source)<br> AND REINVEST_DATE = DATE_in (PRICE_DATE from Source)"	|		|	Add Target Table Name	|	PRCT_CHANGE	|	"number(p,s)"	|	38	|	Y	|	N	|		|
|	STG_DST_PRICE	|	N/A	|		|		|		|		|		|	If PRCT_CHANGE (Above field) is between THRESHOLD_HIGH and THRESHOLD_LOW then 0 else 1<br><br>//driving high and low threshold values<br>Lkp_Fnd_Threshold  with FND_ID <br>	|	"iif(TODAY_PRCT_CHANGE_in <= THRESHOLD_HIGH_in and TODAY_PRCT_CHANGE_in >=  THRESHOLD_LOW_in,0,1)"	|	"Select<br>THRESHOLD_HIGH,<br>THRESHOLD_LOW<br>from<br>FND_THRESHOLD<br>where<br>FND_ID = ID_in (FND_ID from source)"	|		|		|	FLG_THRESHOLD	|	"number(p,s)"	|	1	|	Y	|	N	|		|
|		|		|		|		|		|		|		|	Default '0'	|		|		|		|		|	FLG_MATCH_DST	|	"number(p,s)"	|	1	|	Y	|	N	|		|
|		|	INSERT_TIMESTAMP	|	date/time	|	29	|		|		|		|		|		|		|		|		|	INSERT_TIMESTAMP	|	date	|	19	|	Y	|	N	|		|
|		|	INSERT_USER	|	string	|	20	|		|		|		|		|		|		|		|		|	INSERT_USER	|	varchar2	|	20	|	Y	|	N	|		|
|		|	FLG_WRONG_DATE	|	decimal	|	1	|		|		|		|	If current -  PRICE_DATE (day) is 0 then 0<br>else '1'	|	"iif(date_diff(DATE_in,trunc(sysdate,'DDD'),'DDD')=0 ,0,1)<br><br>//DATE_in = 'Price_Date' from source"	|		|		|		|	FLG_WRONG_DATE	|	"number(p,s)"	|	1	|	Y	|	N	|		|
|		|	N/A	|		|		|		|		|		|	((1+YTD_CHANGE_v1)*(1+PRCT_CHANGE))-1<br><br>//YTD_CHANGE<br>Lookup FND_Price table with FND_ID<br>If YTD_CHANGE' found then<br>YTD_CHANGE<br>else<br>0<br><br>	|	"((1+YTD_CHANGE_v1)*(1+TODAY_PRCT_CHANGE_in))-1<br><br>//YTD_CHANGE_v1<br>iif(isnull(YESTERDAY_YTD_CHANGE_in),0,YESTERDAY_YTD_CHANGE_in)<br>"	|	//YESTERDAY_YTD_CHANGE_in<br>select YTD_CHANGE from  'FND_Price' for the value 'FND_ID' (From source)	|		|		|	YTD_CHANGE	|	"number(p,s)"	|	38	|	Y	|	N	|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
