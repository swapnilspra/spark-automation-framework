|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|	Default to '99'	|	'99'	|		|		|	TRANS_BASE_TRAILER	|	TRANS_CLASSIFICATION_CODE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	Default to '1'	|	'1'	|		|		|	TRANS_BASE_TRAILER	|	TRANS_SEQUENCE_NUMBER	|	string	|	1	|		|		|		|
|		|		|		|		|		|		|		|	"substr(to_char(sysdate,'mm/dd/yyyy'),-4,2)"	|	"substr(to_char(sysdate,'mm/dd/yyyy'),-4,2)"	|		|		|	TRANS_BASE_TRAILER	|	TRADE_CC_DATE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"substr(to_char(sysdate,'mm/dd/yyyy'),-2,2)"	|	"substr(to_char(sysdate,'mm/dd/yyyy'),-2,2)"	|		|		|	TRANS_BASE_TRAILER	|	TRADE_YY_DATE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"substr(to_char(sysdate,'mm/dd/yyyy'),1,2)"	|	"substr(to_char(sysdate,'mm/dd/yyyy'),1,2)"	|		|		|	TRANS_BASE_TRAILER	|	TRADE_MM_DATE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"substr(to_char(sysdate,'mm/dd/yyyy'),4,2)"	|	"substr(to_char(sysdate,'mm/dd/yyyy'),4,2)"	|		|		|	TRANS_BASE_TRAILER	|	TRADE_DD_DATE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	Default to  '0000000'	|	'0000000'	|		|		|	TRANS_BASE_TRAILER	|	TOTAL_NEW_ACCOUNTS	|	string	|	7	|		|		|		|
|		|		|		|		|		|		|		|	Default to  '000000000'	|	'000000000'	|		|		|	TRANS_BASE_TRAILER	|	TOTAL_REDEMPTIONS	|	string	|	9	|		|		|		|
|		|		|		|		|		|		|		|	Default to  '000000000'	|	'000000000'	|		|		|	TRANS_BASE_TRAILER	|	TOTAL_PURCHASES	|	string	|	9	|		|		|		|
|		|		|		|		|		|		|		|	Default to  '00000000000000000'	|	'00000000000000000'	|		|		|	TRANS_BASE_TRAILER	|	TOTAL_REDEMPTION_SHARES	|	string	|	17	|		|		|		|
|		|		|		|		|		|		|		|	Default to  '00000000000000000'	|	'00000000000000000'	|		|		|	TRANS_BASE_TRAILER	|	TOTAL_PURCHASE_DOLLARS	|	string	|	17	|		|		|		|
|	TIP_RECORD_CNT	|	COUNT	|	String	|	9	|		|		|		|	Add prefix of '0' to COUNT'	|	"lpad(COUNT,9,'0')"	|		|		|	TRANS_BASE_TRAILER	|	TOTAL_ACCOUNT_UPDATES	|	string	|	9	|		|		|		|
|		|		|		|		|		|		|		|	' '	|	' '	|		|		|	TRANS_BASE_TRAILER	|	FILLER1	|	string	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	TRANS_BASE_TRAILER	|	EXTERNAL_CLASSIFICATION_CODE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	TRANS_BASE_TRAILER	|	ORDER_PROCESSING_CODE	|	string	|	1	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	TRANS_BASE_TRAILER	|	FILLER2	|	string	|	17	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	TRANS_BASE_TRAILER	|	TRAILER_SORT_AREA	|	string	|	60	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
