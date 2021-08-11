|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	VW_10KGROWTH_CHART	|	DATE_1	|	date	|	19	|		|		|		|	convert string of date in the format 'YYYY-MM-DD'	|	"TO_CHAR(DATE_IN,'yyyy-mm-dd')"	|		|		|	TEN_K_CHART	|	date	|	string	|	10	|		|		|		|
|	FND_DIVIDEND	|	FUND_NAME	|	varchar2	|	50	|		|		|		|	Trim the empty space in FUND_NAME	|	LTRIM(RTRIM(FUND_NAME_IN))	|		|		|	TEN_K_CHART	|	fund_name	|	string	|	255	|		|		|		|
|	FND_DIVIDEND	|	FUND_CLASS	|	varchar2	|	100	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	FUND_QUOTE	|	varchar2	|	8	|		|		|		|	Reove the Empty space in 'FUND_QUOTE'	|	LTRIM(RTRIM(FUND_QUOTE_IN))	|		|		|	TEN_K_CHART	|	fund_quote	|	string	|	25	|		|		|		|
|	FND_DIVIDEND	|	FUND_CMP	|	"number(p,s)"	|	10	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	FUND_SORT	|	number	|	15	|		|		|		|		|		|		|		|		|		|		|	13	|		|		|		|
|	FND_DIVIDEND	|	WEALTH_INDEX	|	"number(p,s)"	|	27	|		|		|		|	"ROUND(WEALTH_INDEX_IN,11)"	|	"ROUND(WEALTH_INDEX_IN,11)"	|		|		|	TEN_K_CHART	|	wealth_index	|	string	|	13	|		|		|		|
|	FND_DIVIDEND	|	BENCHMARK_NAME_1	|	varchar2	|	150	|		|		|		|	Reove the Empty space in 'BENCHMARK_NAME_1'	|	LTRIM(RTRIM(BENCHMARK_NAME_1_IN))	|		|		|	TEN_K_CHART	|	benchmark_name_1	|	string	|	255	|		|		|		|
|	FND_DIVIDEND	|	WEALTH_INDEX_1	|	number	|	15	|		|		|		|	"ROUND(WEALTH_INDEX_1_IN,11)"	|	"ROUND(WEALTH_INDEX_1_IN,11)"	|		|		|	TEN_K_CHART	|	wealth_index_1	|	string	|	9	|		|		|		|
|	FND_DIVIDEND	|	BENCHMARK_NAME_2	|	varchar2	|	150	|		|		|		|	Reove the Empty space in 'BENCHMARK_NAME_2'	|	LTRIM(RTRIM(BENCHMARK_NAME_2_IN))	|		|		|	TEN_K_CHART	|	benchmark_name_2	|	string	|	255	|		|		|		|
|	FND_DIVIDEND	|	WEALTH_INDEX_2	|	number	|	15	|		|		|		|	IF WEALTH_INDEX_2 is valid then ROUND(WEALTH_INDEX_2 else populate NULL	|	"IIF (NOT ISNULL(WEALTH_INDEX_2_IN),ROUND(WEALTH_INDEX_2_IN,11),NULL)"	|		|		|	TEN_K_CHART	|	wealth_index_2	|	string	|	9	|		|		|		|
|	FND_DIVIDEND	|	BENCHMARK_NAME_3	|	varchar2	|	150	|		|		|		|	Reove the Empty space in 'BENCHMARK_NAME_3'	|	LTRIM(RTRIM(BENCHMARK_NAME_3_IN))	|		|		|	TEN_K_CHART	|	benchmark_name_3	|	string	|	255	|		|		|		|
|		|	WEALTH_INDEX_3	|	number	|	15	|		|		|		|	IF WEALTH_INDEX_3 is valid then ROUND(WEALTH_INDEX_3 else populate NULL	|	"IIF (NOT ISNULL(WEALTH_INDEX_3_IN),ROUND(WEALTH_INDEX_3_IN,11),NULL)"	|		|		|	TEN_K_CHART	|	wealth_index_3	|	string	|	9	|		|		|		|
|		|	BENCHMARK_NAME_4	|	varchar2	|	305	|		|		|		|	Reove the Empty space in 'BENCHMARK_NAME_4'	|	LTRIM(RTRIM(BENCHMARK_NAME_4_IN))	|		|		|	TEN_K_CHART	|	benchmark_name_4	|	string	|	255	|		|		|		|
|		|	WEALTH_INDEX_4	|	number	|	15	|		|		|		|	IF WEALTH_INDEX_4 is valid then ROUND(WEALTH_INDEX_4 else populate NULL	|	"IIF (NOT ISNULL(WEALTH_INDEX_4_IN),ROUND(WEALTH_INDEX_4_IN,11),NULL)"	|		|		|	TEN_K_CHART	|	wealth_index_4	|	string	|	9	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
