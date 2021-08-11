|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|		|		|		|		|		|		|		|		|	'99'	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	RECORD_TYPE_CDE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|		|	'1'	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	RECORD_TYPE_SEQUENCE_NBR	|	string	|	1	|		|		|		|
|	fnd_return	|	RETURN_DATE	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|	"Get Century from ""Retrun Date "" field"	|	"substr(to_char(RETURN_DATE,'mm/dd/YYYY'),7,2)"	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	TRANSMISSION_CC_DTE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"Get Year from ""Retrun Date "" field"	|	"substr(to_char(RETURN_DATE,'mm/dd/YYYY'),9,2)"	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	TRANSMISSION_YY_DTE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"Get Month from ""Retrun Date "" field"	|	"substr(to_char(RETURN_DATE,'mm/dd/YYYY'),1,2)"	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	TRANSMISSION_MM_DTE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	"Get Date from ""Retrun Date "" field"	|	"substr(to_char(RETURN_DATE,'mm/dd/YYYY'),4,2)"	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	TRANSMISSION_DD_DTE	|	string	|	2	|		|		|		|
|		|		|		|		|		|		|		|	Add prefix of '0' to by incrementing $$LINECNT + 1	|	"lpad(to_char($$LINECNT+1),9,'0')"	|		|	$$LINECNT - Configured as param varailable	|	TIP_INBOUND_WRK5644_TRAILER	|	TOTAL_RECORD_CNT	|	string	|	9	|		|		|		|
|		|		|		|		|		|		|		|	Default to '000000000'	|	'000000000'	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	UPDT_PRICE_TOTAL_RECORD_CNT	|	string	|	9	|		|		|		|
|		|		|		|		|		|		|		|	Default to '000000000'	|	'000000000'	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	UPDT_RATE_TOTAL_RECORD_CNT	|	string	|	9	|		|		|		|
|		|		|		|		|		|		|		|	Default to '000000000'	|	'000000000'	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	UPDT_YIELD_TOTAL_RECORD_CNT	|	string	|	9	|		|		|		|
|		|		|		|		|		|		|		|	Add prefix of '0' to by incrementing $$LINECNT	|	"lpad(to_char($$LINECNT),9,'0')"	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	UPDT_YIELD_TBL_TOT_REC_CNT	|	string	|	9	|		|		|		|
|		|		|		|		|		|		|		|	Default to balnks ('                        ')	|	'                        '	|		|		|	TIP_INBOUND_WRK5644_TRAILER	|	FILLER0	|	string	|	24	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
