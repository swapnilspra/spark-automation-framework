|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Open_Trades	|	Trade_Date	|	string	|	50	|	N	|		|		|	Direct Move	|	Filter the record if FUND is NULL	|		|		|	FND_OPEN_TRADES	|	TRADE_DATE	|	varchar2	|	50	|	N	|	Y	|		|
|	Open_Trades	|	Settle_Date	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	SETTLE_DATE	|	varchar2	|	50	|	N	|	Y	|		|
|	Open_Trades	|	Security_Name	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	SECURITY_NAME	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Fund	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	FUND	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Fund_Name	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	FUND_NAME	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Sedol	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	SEDOL	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Isin	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	ISIN	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Ticker	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	TICKER	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Trade_Type	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	TRADE_TYPE	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Share_Quantity	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	SHARE_QUANTITY	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Net_Amount	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	NET_AMOUNT	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Settle_Loc	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	SETTLE_LOC	|	varchar2	|	50	|	Y	|	N	|		|
|	Open_Trades	|	Currency	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	CURRENCY	|	varchar2	|	50	|		|		|		|
|	Open_Trades	|	Executing_Broker	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	EXECUTING_BROKER	|	varchar2	|	50	|		|		|		|
|	Open_Trades	|	ISSUER	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	ISSUER	|	varchar2	|	50	|		|		|		|
|	Open_Trades	|	ID_BB_COMPANY	|	string	|	50	|	N	|		|		|	Direct Move	|		|		|		|	FND_OPEN_TRADES	|	ID_BB_COMPANY	|	varchar2	|	50	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
