|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|	BK	|		|		|		|		|	Table Name	|	Target Table Name	|	Data Type	|	Length	|	Nullable	|	PK	|		|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	FND_DIVIDEND	|	ID	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	FND_ID	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	RECORD_DATE	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	REINVEST_DATE	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	PAYABLE_DATE	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	NET_INVEST_INCOME	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	TOTAL_GAINS_SHORT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	TOTAL_GAINS_LONG	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	ORD_INCOME	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	SHRS_OF_RECORD	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	CAP_GAINS_SHORT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	CAP_GAINS_LONG	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	REINVEST_PRICE	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	NON_TAXABLE_DIS	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	ESTIMATED_DIVIDEND	|	string	|	1	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	TOTAL_GAINS_MID	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	CAP_GAINS_MID	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	FOREIGN_TAX_CREDIT	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	REINVEST_INDEX	|	double	|	15	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	INSERT_TIMESTAMP	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	INSERT_USER	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	UPDATE_TIMESTAMP	|	date/time	|	29	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	UPDATE_USER	|	string	|	30	|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|	FND_DIVIDEND	|	TICKER	|	string	|	8	|		|		|		|	Direct Move	|	TICKER	|		|		|	SSB_DISTRIB	|	TICKER	|	string	|	5	|		|		|		|
|	FND_DIVIDEND	|	CUSIP	|	string	|	9	|		|		|		|	Direct Move	|	CUSIP	|		|		|	SSB_DISTRIB	|	CUSIP	|	string	|	9	|		|		|		|
|	FND_DIVIDEND	|	FUND_NAME	|	string	|	50	|		|		|		|	Direct Move	|	FUND_NAME	|		|		|	SSB_DISTRIB	|	FUND_NAME	|	string	|	50	|		|		|		|
|	FND_DIVIDEND	|	MCH_FUND_ID	|	string	|	5	|		|		|		|	Direct Move	|	MCH_FUND_ID	|		|		|	SSB_DISTRIB	|	MCH_FUND_ID	|	string	|	5	|		|		|		|
|	FND_DIVIDEND	|	MCH_BASIS	|	decimal	|	16	|		|		|		|	Direct Move	|	MCH_BASIS	|		|		|	SSB_DISTRIB	|	MCH_BASIS	|	number	|	16	|		|		|		|
|	FND_DIVIDEND	|	MANAGEMENT_CODE	|	string	|	10	|		|		|		|	Direct Move	|	MANAGEMENT_CODE	|		|		|	SSB_DISTRIB	|	MANAGEMENT_CODE	|	string	|	10	|		|		|		|
|	FND_DIVIDEND	|	TA_NUMBER	|	decimal	|	4	|		|		|		|	Direct Move	|	TA_NUMBER	|		|		|	SSB_DISTRIB	|	TA_NUMBER	|	number	|	4	|		|		|		|
|	FND_DIVIDEND	|	RECORD_DATE1	|	string	|	10	|		|		|		|	Direct Move	|	RECORD_DATE	|		|		|	SSB_DISTRIB	|	RECORD_DATE	|	string	|	10	|		|		|		|
|	FND_DIVIDEND	|	EX_DATE	|	string	|	10	|		|		|		|	Direct Move	|	EX_DATE	|		|		|	SSB_DISTRIB	|	EX_DATE	|	string	|	10	|		|		|		|
|	FND_DIVIDEND	|	REINVEST_DATE1	|	string	|	10	|		|		|		|	Direct Move	|	REINVEST_DATE	|		|		|	SSB_DISTRIB	|	REINVEST_DATE	|	string	|	10	|		|		|		|
|	FND_DIVIDEND	|	PAYABLE_DATE1	|	string	|	10	|		|		|		|	Direct Move	|	PAYABLE_DATE	|		|		|	SSB_DISTRIB	|	PAYABLE_DATE	|	string	|	10	|		|		|		|
|	FND_DIVIDEND	|	RECORD_DATE_SHARES	|	double	|	15	|		|		|		|	iif RECORD_DATE_SHARES=0 then NULL otherwise 'RECORD_DATE_SHARES'	|	"iif(RECORD_DATE_SHARES=0,NULL,RECORD_DATE_SHARES)"	|		|		|	SSB_DISTRIB	|	RECORD_DATE_SHARES	|	double	|	16	|		|		|		|
|	FND_DIVIDEND	|	ORD_INCOME1	|	double	|	15	|		|		|		|	if ORD_INCOME=0then NULL otherwise 'ORD_INCOME'	|	"iif(ORD_INCOME=0,NULL,ORD_INCOME)"	|		|		|	SSB_DISTRIB	|	ORD_INCOME	|	double	|	16	|		|		|		|
|	FND_DIVIDEND	|	ORD_DISTRIBUTION_PER_SHARE	|	double	|	15	|		|		|		|	if ORD_DISTRIBUTION_PER_SHARE=0 then NULL otherwise 'ORD_DISTRIBUTION_PER_SHARE'	|	"iif(ORD_DISTRIBUTION_PER_SHARE=0,NULL,ORD_DISTRIBUTION_PER_SHARE)"	|		|		|	SSB_DISTRIB	|	ORD_DISTRIBUTION_PER_SHARE	|	double	|	16	|		|		|		|
|	FND_DIVIDEND	|	SHORT_TERM_DISTRIB	|	double	|	15	|		|		|		|	if SHORT_TERM_DISTRIB=0 then NULL otherwise 'SHORT_TERM_DISTRIB'	|	"iif(SHORT_TERM_DISTRIB=0,NULL,SHORT_TERM_DISTRIB)"	|		|		|	SSB_DISTRIB	|	SHORT_TERM_DISTRIB	|	double	|	16	|		|		|		|
|	FND_DIVIDEND	|	SHORT_TERM_DISTRIB_PER_SHARE	|	double	|	15	|		|		|		|	iif SHORT_TERM_DISTRIB_PER_SHARE=0 then NULL otherwise 'SHORT_TERM_DISTRIB_PER_SHARE'	|	"iif(SHORT_TERM_DISTRIB_PER_SHARE=0,NULL,SHORT_TERM_DISTRIB_PER_SHARE)"	|		|		|	SSB_DISTRIB	|	SHORT_TERM_DISTRIB_PER_SHARE	|	double	|	16	|		|		|		|
|	FND_DIVIDEND	|	LONG_TERM_DISTRIB	|	double	|	15	|		|		|		|	if LONG_TERM_DISTRIB=0 then NULL otherwise 'LONG_TERM_DISTRIB'	|	"iif(LONG_TERM_DISTRIB=0,NULL,LONG_TERM_DISTRIB)"	|		|		|	SSB_DISTRIB	|	LONG_TERM_DISTRIB	|	double	|	16	|		|		|		|
|	FND_DIVIDEND	|	LONG_TERM_DISTRIB_PER_SHARE	|	double	|	15	|		|		|		|	if LONG_TERM_DISTRIB_PER_SHARE=0 then NULL otherwise 'LONG_TERM_DISTRIB_PER_SHARE'	|	"iif(LONG_TERM_DISTRIB_PER_SHARE=0,NULL,LONG_TERM_DISTRIB_PER_SHARE)"	|		|		|	SSB_DISTRIB	|	LONG_TERM_DISTRIB_PER_SHARE	|	double	|	16	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	SSB_DISTRIB	|	CAP_GAINS_DISTRIB	|	double	|	16	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	SSB_DISTRIB	|	CAP_TAINS_DISTRIB_PER_SHARE	|	double	|	16	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	SSB_DISTRIB	|	ROC_DISTRIB	|	double	|	16	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	SSB_DISTRIB	|	ROC_DISTRIB_PER_SHARE	|	double	|	16	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	SSB_DISTRIB	|	SPECIAL_DISTRIB	|	double	|	16	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	SSB_DISTRIB	|	SPEICAL_DISTRIB_PER_SHARE	|	double	|	16	|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
