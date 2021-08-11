|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	FND_RETURN 	|	RETURN_DATE	|	date/time	|	29	|	N	|		|		|	Input Field	|		|		|		|		|		|		|		|		|		|		|
|	FND_RETURN 	|	RETURN	|	double	|	15	|	N	|		|		|	Input Field	|		|		|		|		|		|		|		|		|		|		|
|	FND_RETURN 	|	NAME	|	string	|	6	|	N	|		|		|	Input Field	|		|		|		|		|		|		|		|		|		|		|
|	FND_RETURN 	|	PUBLIC_FND_NUMBER	|	decimal	|	4	|	N	|		|		|	Input Field	|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|	Default to 'UT'	|	UT'	|		|		|	TIP_INBOUND_WRK5644	|	RECORD_TYPE_CDE	|	string	|	2	|	N	|	Y	|		|
|		|		|		|		|		|		|		|	Default to  '1'	|	'1'	|		|		|	TIP_INBOUND_WRK5644	|	RECORD_TYPE_SEQUENCE_NBR	|	string	|	1	|	N	|	Y	|		|
|		|		|		|		|		|		|		|	Default to  'A'	|	'A'	|		|		|	TIP_INBOUND_WRK5644	|	ACTION_TYPE_CDE	|	string	|	1	|	Y	|	N	|		|
|		|		|		|		|		|		|		|	Default to  'HB'	|	'HB'	|		|		|	TIP_INBOUND_WRK5644	|	MGMT_CODE	|	string	|	2	|	Y	|	N	|		|
|		|		|		|		|		|		|		|	Default to blanks	|	'  '	|		|		|	TIP_INBOUND_WRK5644	|	FILLER1	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	Add prefix of '0'	|	"LPAD(to_char(PUBLIC_FND_NUMBER_in),7,'0')"	|		|		|	TIP_INBOUND_WRK5644	|	FUND_CODE	|	string	|	7	|		|		|		|
|		|		|		|		|		|		|		|	Default to  'F'	|	'F'	|		|		|	TIP_INBOUND_WRK5644	|	CUSIP_FUND_CODE	|	string	|	1	|		|		|		|
|		|		|		|		|		|		|		|	Default to  '0000004'	|	'0000004'	|		|		|	TIP_INBOUND_WRK5644	|	YIELD_TYPE_ID	|	string	|	7	|		|		|		|
|		|		|		|		|		|		|		|	"iif(NAME_in='QTR','00014') <br>iif(NAME_in='YR1','00005') <br>iif(NAME_in='YR3','00006') <br>iif(NAME_in='YR5','00002')<br>iif(NAME_in='YR10','00003')<br>iif(NAME_in='INCP','00004')"	|	"iif(NAME_in='QTR','00014', iif(NAME_in='YR1','00005', iif(NAME_in='YR3','00006', iif(NAME_in='YR5','00002', iif(NAME_in='YR10','00003', iif(NAME_in='INCP','00004'))))))"	|		|		|	TIP_INBOUND_WRK5644	|	TIMEFRAME_ID	|	string	|	5	|		|		|		|
|		|		|		|		|		|		|		|	"Get the century from ""Return date"""	|	"lpad(substr(to_char(RETURN_DATE_in,'mm/dd/yyyy'),7,2),2,'0')"	|		|		|	TIP_INBOUND_WRK5644	|	EFFECTIVE_CC_DTE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"Get the year from ""Return date"""	|	"lpad(substr(to_char(RETURN_DATE_in,'mm/dd/yyyy'),9,2),2,'0')"	|		|		|	TIP_INBOUND_WRK5644	|	EFFECTIVE_YY_DTE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"Get the month from ""Return date"""	|	"lpad(substr(to_char(RETURN_DATE_in,'mm/dd/yyyy'),1,2),2,'0')"	|		|		|	TIP_INBOUND_WRK5644	|	EFFECTIVE_MM_DTE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"Get the date from ""Return date"""	|	"lpad(substr(to_char(RETURN_DATE_in,'mm/dd/yyyy'),4,2),2,'0')"	|		|		|	TIP_INBOUND_WRK5644	|	EFFECTIVE_DD_DTE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"convert the ""Return_In"" as positive and add 0 in both prefix and suffix"	|	"CONCAT(YIELD_RTRN_LEFT_v,YIELD_RTRN_RIGHT_v)<br><br>//YIELD_RTRN_LEFT_v<br>lpad(to_char(trunc(YIELD_RTRN_ABS_v)),5,'0')<br><br>//YIELD_RTRN_RIGHT_v<br>rpad(substr(to_char(round(YIELD_RTRN_ABS_v-trunc(YIELD_RTRN_ABS_v),2)),3,10),10,'0')<br><br>//YIELD_RTRN_ABS_v<br>iif(RETURN_in >= 0,round(RETURN_in*100,2),round((0-RETURN_in)*100,2))<br>"	|		|		|	TIP_INBOUND_WRK5644	|	YIELD_RTRN_RTE	|	string	|	15	|		|		|		|
|		|		|		|		|		|		|		|	assign the symbol '+' or '-'	|	"iif(RETURN_in>0,'+','-')"	|		|		|	TIP_INBOUND_WRK5644	|	YIELD_RTRN_RTE_SIGN_CDE	|	string	|	1	|		|		|		|
|		|		|		|		|		|		|		|	Default to '000000000000000'	|	'000000000000000'	|		|		|	TIP_INBOUND_WRK5644	|	YIELD_RTRN_AMT	|	string	|	15	|		|		|		|
|		|		|		|		|		|		|		|	Default to  '+'	|	'+'	|		|		|	TIP_INBOUND_WRK5644	|	YIELD_RTRN_AMT_SIGN_CDE	|	string	|	1	|		|		|		|
|		|		|		|		|		|		|		|	Default to  'N'	|	'N'	|		|		|	TIP_INBOUND_WRK5644	|	FUTURE_EFFECTIVE_DATE_CD	|	string	|	1	|		|		|		|
|		|		|		|		|		|		|		|	Default to blanks	|	'           '	|		|		|	TIP_INBOUND_WRK5644	|	FILLER0	|	string	|	11	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
