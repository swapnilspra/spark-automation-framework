| Table | Project | Source | path | file/table/code | incude |    Comment    | FREQUENCY |  File Type |	Full or Delta |
| ----- | ------- | ------ | ---- | --------------- | ------ | ------------- | --------- |  --------- | ------------- |
|       |         |        |      |                 |        |               |           |            |               |
|ACCOUNT|Shareholder|DST|dstprod|DTO.HCA.AMP.TIP09.\*|||Business Days|COBOL|Full|
|ACCOUNT_CATEGORY|Shareholder|DST||||onetime setup.  Copy data||Delimited Text|Full|
|ACCOUNT_POSITION|Shareholder|DST|dstprod|DTO.HCA.APR.TIP03.\*|||Business Days|COBOL|Delta|
|ACCOUNT_POSITION_HIST|Shareholder|||||No ETL required.  Historical data.||||||
|ACCOUNT_SETUP_YEAR|Shareholder|||||Onetime setup.  Copy data||Delimited Text|Full|
|ACCOUNT_STATISTICS|Shareholder|||||||||					
|AFTER_TAX_CALCULATION|Fund|Internal||pkg_after_tax|||Annual|||
|AFTER_TAX_RETURN|Fund|Internal||pkg_after_tax|||Annual|||
|AFTER_TAX_WEALTH_INDEX|Fund|Internal||pkg_after_tax|||Annual|||
|AWD_PRIVILEGE_TYPE|AWD|DST||||||Delimited Text|Full| 				
|AWD_QUEUE|AWD|DST||||||Delimited Text|Full| 				
|AWD_RECORD_TYPE|AWD|DST||||||Delimited Text|Full|  				
|AWD_ROLE|AWD|DST||||||Delimited Text|Full|  			
|AWD_ROLE_PRIVILEGE|AWD|DST||||||Delimited Text|Full|
|AWD_USER|AWD|DST||||||Delimited Text|Full|  
|AWD_USER_QUEUE|AWD|DST||||||Delimited Text|Full| 
|AWD_USER_ROLE|AWD|DST||||||Delimited Text|Full|
|AWD_USER_WORK_TYPE|AWD|DST||||||Delimited Text|Full| 	
|AWD_WORK_TYPE|AWD|DST||||||Delimited Text|Full|
|BALANCE_CATEGORY|Shareholder|Internal||||Onetime setup.  Copy data||Delimited Text|Full|
|BENCHMARK_INDEX|Fund|hcprod|fund|benchmark.csv|||Week days|Delimited Text|Full|
|BLUESKY_TRANSACTIONS|Shareholder|DST|dstprod|DTO.HCA.BSK.TIP88.\*|||Business Days|COBOL|Delta|
|BROKER|Broker Confirm|	nternal||||||Delimited Text|Delta|	
|BROKER_BACKUP|Broker Confirm||||N|||||
|BROKER_POSITION|Broker Confirm|Internal||||||Delimited Text|Delta| 
|BROKER_POSITION_BACKUP|Broker Confirm||||N|||||
|BUDGET|Budget|Internal|budget-files|bdgt_budget.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_ACCOUNT|Budget|Internal|budget-files|bdgt_account.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_ACCT_ACCRUAL|Budget|Internal|budget-files|bdgt_acct_accrual.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_ACCT_ACCRUAL_FND|Budget|Internal|budget-files|bdgt_acct_accrual.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full| 
|BUD_ACCT_ADJUST_FND|Budget|Internal|budget-files|bdgt_acct_adjustment.csv, bdgt_category_accrual.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_ACCT_TRANSFER|Budget|Internal|budget-files|bdgt_acct_transfer.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_ACCT_TRANSFER_FND|Budget|Internal|budget-files|bdgt_acct_transfer.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_ASMPT_CHANGE|Budget|Internal|budget-files|bdgt_assumption_change.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_ASMPT_CHANGE_FUND|Budget|Internal|budget-files|bdgt_assumption_change_fund.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full| 
|BUD_CAT_ACCRUAL_DIM|Budget|Internal|budget-files|bdgt_category_accrual.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_CAT_ACCRUAL_FACT|Budget|Internal|budget-files|bdgt_category_accrual.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_CAT_ACCT_ACCRUAL|Budget|Internal|budget-files|bdgt_category_accrual.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_DATA_SOURCE_DIM|Budget|Internal|budget-files|bdgt_data_source.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_DATA_SOURCE_FACT|Budget|Internal|budget-files|bdgt_data_source.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_DATA_SOURCE_FUND|Budget|Internal|budget-files|bdgt_data_source_fund.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_EXPENSE_LINE_ITEM|Budget|Internal|budget-files|bdgt_line_item.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|  
|BUD_FUND_COMPOSITE|Budget|Internal|budget-files|bdgt_fund.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full| 
|BUD_FUND_MULTIPLIER|Budget|Internal|budget-files|bdgt_fund_multiplier.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full| 
|BUD_INVOICE|Budget|Internal|budget-files|bdgt_invc_alloc.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_INVOICE_ALLOC|Budget|Internal|budget-files|bdgt_invc_alloc.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_INVOICE_ALLOC_FUND|Budget|	Internal|budget-files|bdgt_invc_alloc_fund.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_LIABILITY|Budget|Internal|budget-files|bdgt_liability.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_LIABILITY_FUND|Budget|Internal|budget-files|bdgt_liability_fund.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_LINEITEM_ALLOC|Budget|Internal|budget-file|bdgt_line_item_alloc.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_LINEITEM_ALLOC_FUND|Budget|Internal|budget-files|bdgt_line_item_alloc_fund.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_PMT_DIFF|Budget|Internal|budget-files|bdgt_payment_diff.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_PMT_DIFF_FND_INVC|Budget|Internal|	budget-files|bdgt_payment_diff_fund.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_PMT_DIFF_FUND|Budget|Internal|budget-files|	bdgt_payment_diff_fund.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUD_TRUSTEE_ELECTION|Budget|Internal|budget-files|bdgt_trustee_election.csv||Users of budget application triggers a process to generate the source files.  This is always on demand.|on demand|Delimited Text|Full|
|BUSINESS_LINE|Fees|Internal|dealer-invoice,dealer-invoice-trial|invoice_trial.csv,invoice.csv||||Delimited Text|Delta|		
|CALENDAR|Dimension|Internal|internal|calendar.csv||manual setup.  Copy data||||
|CASHFLOW|Cashflow|DST|||N|||||
|CASHFLOW_ACTIVITY|Cashflow|DST||||Can generate records for this table from SDCM_CASHFLOW table after that table is populated||||
|CASHFLOW_ACTIVITY_HIST|Cashflow|DST||||Onetime cashflow load of historical data.||||
|CASHFLOW_DETAIL|Cashflow|DST|||N|||||
|CASHFLOW_HISTORY|Cashflow|Internal||||Onetime cashflow load of historical data.||||
|CASH_HOLDING|Fund|ssb|	ssb|full_prc\<dt\>.csv,PTA_SSB_POSITION\<dt\>.txt ||Cash_coltrl column values comes from PTA_SSB_POSITION file|Week days|Fixed with Text|Delta|
|CASH_REASON|Shareholder|Internal||||manual setup.  Copy data||||
|CCT_CITY_TAX_LIST|City Tax|Internal|tax-data|taxlist.txt||Corporate accounting users upload these files.  This is typically once a month , but could also happen multiple times a month|on demand|Delimited Text|Delta|
|CCT_HARBOR_GROUP|City Tax|Internal|tax-data|company.txt||Corporate accounting users upload these files.  This is typically once a month , but could also happen multiple times a month|on demand|Delimited Text|Delta|
|CCT_SPEND_CATEGORY|City Tax|Internal|tax-data|spendcategory.txt||Corporate accounting users upload these files.  This is typically once a month , but could also happen multiple times a month|on demand|Delimited Text|Delta|
|CCT_SUPPLIER|City Tax|Internal|tax-data|supplier.txt||Corporate accounting users upload these files.  This is typically once a month , but could also happen multiple times a month|on demand|Delimited Text|Delta|
|CCT_TAX_DETAIL|City Tax|Internal|tax-data|tax.txt||Corporate accounting users upload these files.  This is typically once a month , but could also happen multiple times a month|on demand|Delimited Text|Delta|
|CCT_TAX_SUMMARY|City Tax|Internal|tax-data|invoice.txt||Corporate accounting users upload these files.  This is typically once a month , but could also happen multiple times a month|on demand|Delimited Text|Delta|
|COST_TYPE|Shareholder|DST||||manual setup.  Copy data||||
|COUNTRY|Holdings|Internal||||manual setup.  Copy data||||
|COUNTRY_THRESHOLD|Foreign Ownership|Internal||||manual setup.  Copy data||||
|CURRENCY|Holdings|Internal||||manual setup.  Copy data||||
|DAILY_AVG_ACCT_POSITION|Fees|Internal||pkg_dealer_analysis||stored procedure||||
|DEALER|Shareholder|DST|dstprod	|DTO.HCA.FDP.TIP56.\*|||Business Days|COBOL|Full|
|DEALER_ASSET_SUMMARY|Fees|Internal||pkg_dealer_analysis||||||
|DEALER_BRANCH|Shareholder|DST|dstprod|DTO.HCA.FBP.TIP55.\*|||Business Days|COBOL|Full|
|DEALER_BRANCH_ASSOC|Shareholder|DST|||N|Unused||||
|DEALER_CHANGE_CONTROL|Fees|SQL Server DB||||sharepoint list data||||
|DEALER_CHANGE_LIST|Fees|SQL Server DB||||sharepoint list data||||
|DEALER_FEE_ANALYSIS|Fees|Internal||pkg_dealer_analysis||||||
|DEALER_FEE_RESEARCH|Fees|Internal||pkg_dealer_analysis||||||
|DEALER_FEE_SCHEDULE|Fees|SQL Server DB||||sharepoint list data||||
|DEALER_FEE_TYPE|Fees|Internal||||Manual setup.  Copy data||||
|DEALER_INVOICE|Fees|Internal|dealer-invoice|invoice.csv||||Delimited Text||
|DEALER_INVOICE_MODEL|Fees|Internal||pkg_dealer_asset_fee||||
|DEALER_INVOICE_RULE|Fees|Internal|||manual setup.  Copy data||||
|DEALER_INVOICE_TRIAL|Fees|Internal|dealer-invoice-trial|invoice_trial.csv||||Delimited Text||		
|DEALER_LEVEL_CONTROL|Shareholder|DST||||Onetime setup.  Copy data||||
|DEALER_PAYMENT_DETAIL|Shareholder|DST|dstprod|DTO.HCA.MTF.R00857.\*|||Monthly|COBOL|Delta|
|DEALER_PAYMENT_METHOD|Shareholder|DST||||Onetime setup.  Copy data||||
|DEALER_PAYMENT_SUMMARY|Fees|DST|dstprod|DTO.HCA.MTF.R00858.\*|||Monthly|COBOL|Delta|
|DEALER_PAYMENT_TRIAL|Fees|DST|dstprod|DTO.HCA.MTF.R00857.TRIAL.\*|||Monthly|COBOL|Delta|
|DEALER_PAYOUT_TYPE|Shareholder|DST||||Onetime setup.  Copy data||||
|DISTRIBUTION_CODE|Shareholder|DST||||Onetime setup.  Copy data	||||
|DISTRIBUTION_TYPE|Shareholder|DST||||Onetime setup.  Copy data	||||
|DIVIDEND_CASHFLOW|||||N|||||
|DIVIDEND_TAX_RATE|Fund|Internal||||Manual load.  Copy data|Annual|||
|ELIGIBLE_ASSET_LIST|Fees|Internal||||Onetime setup.  Copy data||||
|EXCHANGE_TYPE|	Holdings|ssb||||Onetime setup.  Copy data||||
|EXPENSE_RATIO_HISTORY|	Fund|Internal||||Onetime load of history data.  Copy data||||
|EXPENSE_RATIO_WAIVER_TYPE|Fund|Internal||||Onetime setup.  Copy data||||
|EXTERNAL_FUND_BALANCE|Fund|Internal||||Manual load.  Copy data|On demand|||
|EXTERNAL_FUND_LIST|Fund|Internal||||Manual load.  Copy data|On demand|||
|PERF_RUN_TYPE|Fund|Internal||||Manual load.  Copy data|On demand|||
|FACILITY|Power Select|DST||||||||
|FEE_SCHEDULE|Fees|SQL Server DB||||SQL Server DB||||
|FIDUCIARY_ASSET_CLASS|Fiduciary|Internal||||Onetime setup.  Copy data||||
|FIDUCIARY_ASSET_LINK|Fiduciary|Internal||||Onetime setup.  Copy data||||
|FIDUCIARY_CUSTOMER|Fiduciary|Internal||||Onetime setup.  Copy data||||
|FIDUCIARY_POSITION|Fiduciary|Internal|fiduciary-position|fund_position.csv||||Delimited Text||
|FID_BENCHMARK_PERFORMANCE|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FID_BENCHMARK_POSITION|Fiduciary|Internal|fiduciary-position|benchmark_position.csv||||Delimited Text||
|FID_BENCHMARK_WEIGHT|Fiduciary|Internal||||Manual load.  Copy data||||
|FID_BENCHMARK_WEIGHT_TEMP|Fiduciary|Internal||||Manual load.  Copy data||||
|FID_BM_LEVEL_PERFORMANCE|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FID_BM_LEVEL_WEALTH_INDEX|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FID_BM_WEALTH_INDEX|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FID_CLIENT_ASSET_MIX|Fiduciary|Internal||||Manual load.  Copy data||||
|FID_EXTERNAL_BENCHMARK|Fiduciary|Internal||||Manual load.  Copy data||||
|FID_EXTERNAL_FUND|Fiduciary|Internal||||Manual load.  Copy data||||
|FID_FUND_LEVEL_PERFORMANCE|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FID_FUND_LEVEL_WEALTH_INDEX|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FID_FUND_PERFORMANCE|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FID_FUND_WEALTH_INDEX|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FID_LETTER_RECIPIENT|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FID_WEALTH_INDEX_TEMP|Fiduciary|Internal||pkg_fiduciary_load||Package/stored procedure||||
|FI_CHARACTERISTICS|Fixed Income|IRM|irm|characteristics\<dt\>.csv||||Delimited Text|Delta|
|FI_CHARACTERISTIC_LIST|Fixed Income|IRM|irm|characteristics\<dt\>.csv||||Delimited Text|Delta|
|FI_COMPOSITION_MAP|||||N|||||
|FI_COUNTRY|Fixed Income|Shenkman||||New project||||
|FI_COUNTRY_ALLOCATION|Fixed Income|Shenkman||||New project||||
|FI_CREDIT_RATING|Fixed Income|IRM|irm|quality\<dt\>.csv||||Delimited Text|Delta|
|FI_CREDIT_RATING_LIST|Fixed Income|IRM|irm|quality\<dt\>.csv||||Delimited Text|Delta|
|FI_DURATION_MATURITY|Fixed Income|IRM|irm|duration\<dt\>.csv||||Delimited Text|Delta|
|FI_ISSUER|Fixed Income|IRM|irm|issuer\<dt\>.csv||||Delimited Text|Delta|
|FI_SECTOR|Fixed Income|IRM|irm|sector\<dt\>.csv||||Delimited Text|Delta|
|FI_SECTOR_ALLOCATION|Fixed Income|IRM|irm|sector\<dt\>.csv||||Delimited Text|Delta|
|FI_TIME_PERIOD|Fixed Income|IRM|irm|duration\<dt\>.csv||||Delimited Text|Delta|
|FI_TIME_PERIOD_MAPPING|Fixed Income|IRM|||N|||||||
|FI_TOP_ISSUERS|Fixed Income|IRM|irm|issuer\<dt\>.csv||||Delimited Text|Delta|
|FUND|Fund|hcprod|fund|fund.csv||Manual Load.  SQL Script. Used to add new or retire old funds|Week Days|Delimited Text|Full|
|FUND_ANNUAL_INVESTMENT|Fund|Internal||||Manual load.  Copy data||||
|FUND_COMPOSITE|Fund|hcprod|fund|fund_composite.csv||HCA will build a process to copy data from hcprod to s3|Week days|Delimited Text|Full|
|FUND_DIVIDEND|Fund|hcprod|fund|fund_dividend.csv||HCA will build a process to copy data from hcprod to s3|Monthly|Delimited Text|Delta|
|FUND_EXPENSE_RATIO|Fund|hcprod|fund|fund_expense_ratio.csv||HCA will build a process to copy data from hcprod to s3|Monthly|Delimited Text|Delta|
|FUND_HOLDINGS|	Holdings|SSB|ssb|PTA_SSB_SECURITY\*|||Daily|||
|FUND_INDEX_RLTN|Fund|hcprod|fund|fund_index_rltn.csv||HCA will build a process to copy data from hcprod to s3|Week days|Delimited Text|Full|
|FUND_RETURN|Fund|Internal||pkg_fund_perf|||Week days|||
|FUND_TRANS_DFA_HIST|||||N|||||
|FUND_TRANS_HIST|Shareholder|DST|dstprod|DTO.HCA.DFA.TIP02.\*|||Business Days|COBOL|Delta|
|FUND_TRANS_HIST_DEBUG|||||N|||||
|FUND_VALUATION|Fund|SSB|ssb|ssbnav\<dt\>.txt,BI_MONTHLY_AUM\<dt\>.csv||Wealth index needs to be calculated in the code.  Will supply the formula|Week days, Bi monthly|Delimited Text|Delta|
|FUND_YIELD|Fund|SSB|ssb|SEC_YLD\<dlt\>.CSV||All effective Yields needs to be calculated in the code.  Will supply the formula|Week days|Delimited Text|Delta|
|FUND_YIELD_ERROR_RECS|||||N|||||
|GENDER|Shareholder|DST||||Onetime setup.  Copy data||||
|GROSS_RETURN|Fund|Internal||pkg_fund_perf||||||
|GROSS_WEALTH_INDEX|Fund|Internal||pkg_fund_perf||||||
|GROUP_TRUST_ASSET|||||N|||||
|GROUP_TRUST_FUND|||||N|||||
|GROUP_TRUST_LEVELS|||||N|||||
|GROUP_TRUST_PLAN|||||N|||||
|HBR_DLR_UPDATE|||||N|||||
|HOLDINGS_FOREIGN_OWNERSHIP|Foreign Ownership|Internal||pkg_foreign_ownership||||||
|HOLDINGS_OPEN_POSITION|Foreign Ownership|ssb||	HARBCUST\<dt\>.CSV||||Delimited Text|Delta|
|HOLDINGS_POSITION_TEMP|Foreign Ownership|Internal||pkg_foreign_ownership||||||
|HOLDINGS_TRADE_HIST|Holdings|ssb||HARBORCP_TXN102\<dt\>.txt|||Daily|Fixed Width|Delta|
|HOLDINGS_TXN_CODES|Holdings|Internal||||Onetime setup.  Copy data||||
|INDEX_RETURN|Fund|Internal||pkg_fund_perf|||Daily|||
|INDEX_VALUATION|Fund|bloomberg|bloomberg|harbor-bb-idx_final,harbor-bb-idx-data-ml|||Daily|Fixed Width|Delta|
|INDUSTRY|Holdings|ssb|ssb|||||||	
|INTERMEDIARY_ISSUES|Fees|SQLServer DB|||||Sharepoint data||||
|INTERMEDIARY_ISSUE_TYPE|Fees|Internal||||Onetime setup.  Copy data||||
|INVOICE_TRACKING|Fees|SQL Server DB||||Sharepoint data||||
|INVOICE_TRACKING_SUMMARY|Fees|SQL Server DB||||Sharepoint data||||
|MANAGER_FINAL_SCORE|Manager|Internal|manager-info|||on demand load by IPM users|on demand|Delimited Text|Delta|
|MANAGER_INFORMATION|Manager|Internal|manager-info|||on demand load by IPM users|on demand|Delimited Text|Delta|
|MANAGER_LIST|Manager|Internal|manager-info|||on demand load by IPM users|on demand|Delimited Text|Delta|
|MANAGER_RANK_DATA|Manager|Internal|manager-info|||on demand load by IPM users|on demand|Delimited Text|Delta|
|MEASURE_CATEGORY|Manager|Internal|manager-info|||Onetime setup.  Copy data||||
|MEASURE_LIST|Manager|Internal|manager-info|||Onetime setup.  Copy data||||
|MONEY_MARKET_ACTIVITY|Fund|ssb|ssb|RATES\<dt\>.CSV,harbor_mm_web_disclosure\*,MMKT_info\*, GB05_Comparison_NAV_Fund_Summary_\<dt\>.csv||1.  mmkt_info + rates :  These files become available current day evening and starts the insert process.  Other 2 files are transmitted on next day morning and respective columns are updated in mart.|Week days|Delimited Text|Delta|
|MONTHLY_AVG_POSITION|	Fees|Internal||pkg_dealer_analysis||||||
|MONTHLY_AVG_POSN_DRAFT|Fees|Internal||pkg_dealer_analysis||||||		
|OPERATOR|Power Select|DST||||||||
|OPERATOR_FUNCTION|Power Select|DST||||||||
|PAYMENT_METHOD|Shareholder|DST||||Onetime setup.  Copy data||||
|PENDING_ALL_TRADE|Shareholder|DST|dstprod|DTO.HCA.SPOT.TXN.\*||Multiple times in a day|Business Days - Multiple times|COBOL||
|PENDING_TRADE|Shareholder|DST|dstprod|DTO.HCA.SPOT.TXN.\*||Multiple times in a day|Business Days - Multiple times|COBOL||
|PERF_RUN_TYPE|	Fund|||||Onetime setup.  Copy data||||
|PIMCO_ACCOUNT_REFERENCE|PIMCO|Internal||||Onetime setup.  Copy data|||
|PIMCO_COUNTRY_BREAKOUT|PIMCO|PIMCO|pimco|pimco_country_breakout\*.csv||files come suffixed with date||||
|PIMCO_DERIVATIVES|PIMCO|PIMCO|pimco|pimco_deriv_exposure\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_DERIVATIVE_BUCKET|PIMCO|PIMCO|pimco|pimco_deriv_exposure\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_DURATION_BREAKOUT|PIMCO|PIMCO|pimco|pimco_duration_breakout\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_DURATION_BUCKET|PIMCO|PIMCO|pimco|pimco_duration_breakout\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_MATURITY_BREAKOUT|PIMCO|PIMCO|pimco|pimco_maturity_breakout\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_MATURITY_BUCKET|	PIMCO|PIMCO|pimco|pimco_maturity_breakout\*.csv||( Li)files also include pimco_mkt_maturity\*.csv files|Monthly|Delimited Text|Delta|
|PIMCO_MKT_COMMODITY|PIMCO|PIMCO|pimco||N|||||
|PIMCO_MKT_COUNTRY|PIMCO|PIMCO|pimco|pimco_mkt_survey\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_MKT_MATURITY|PIMCO|PIMCO|pimco|pimco_mkt_maturity\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_MKT_QUALITY|PIMCO|PIMCO|pimco|pmco_mkt_quality\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_MKT_SURVEY|PIMCO|PIMCO|pimco|pimco_mkt_survey\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_QUALITY_BREAKOUT|PIMCO|PIMCO|pimco|pimco_quality_breakout\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_QUALITY_BUCKET|PIMCO|PIMCO|pimco|pimco_quality_breakout\*.csv||( Li)files also include pimco_mkt_quality\*.csv files|Monthly|Delimited Text|Delta|
|PIMCO_REGION|PIMCO|PIMCO|pimco|pimco_country_breakout\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_SECTOR|PIMCO|PIMCO|pimco|pimco_sector_breakout\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_SECTOR_BREAKOUT|PIMCO|PIMCO|pimco|pimco_sector_breakout\*.csv|||Monthly|Delimited Text|Delta|
|PIMCO_STATISTICS|PIMCO|PIMCO|pimco|pimco_statistics\*.csv,pimco_mkt_statistics\*.csv|||Monthly|Delimited Text|Delta|
|POSITION_RECALC_ACCTS|||||N|||||
|POSITION_TYPE|	Broker Confirm|	Internal||||||||	
|PROFILE_VALUES|reporting|Internal||||||||
|PRO_EXPENSE_RATIO|||||N|||||
|REASON_CODE|Shareholder|DST||||Onetime setup.  Copy data||||
|REF_MKT_NOMINEE_LIST||||||Onetime setup.  Copy data||||
|REGION|Shareholder|Internal||||Onetime setup.  Copy data||||
|REPORT_FILTERS|reporting|Internal||||Onetime setup.  Copy data||||
|RETURN_PERIODS|Fund|Internal||||Onetime setup.  Copy data||||
|RFS_REPORT_DATE||||||Onetime setup.  Copy data||||
|SDCM_CASHFLOW|Cashflow|DST|dstprod|DTO.HCA.SDCM.POSITION\*||Files come in 3 times a day.  Load them to staging and move to mart at 8.30 am|Business Days|COBOL|Delta|
|SDCM_FUND_BALANCE|Cashflow|DST|dstprod|DTO.HCA.SDCM.BALANCE\*||Files come in 3 times a day.  Load them to staging and move to mart at 8.30 am|Business Days|COBOL|Delta|
|SECURITY|Holdings|ssb|ssb|PTA_SSB_SECURITY\*|||Daily|||
|SECURITY_ATTRIBUTES|Holdings|ssb|ssb|PTA_SSB_SECURITY\*|||Daily|||
|SHAREHOLDER|Shareholder|DST|dstprod|DTO.HCA.CLO.TIP42.\*|||Business Days|COBOL|Full|
|SHAREHOLDER_DISTRIB|Shareholder|DST|dstprod|DTO.HCA.DVRA.TIP01.\*||Once a month file.  On quarter ends, the file comes 3 different files on 3 different days|Monthly|COBOL|Delta|
|SHDR_ACCT_FUND|Shareholder|DST|dstprod|DTO.HCA.AMP.TIP09.\*|||Business Days|COBOL|Full|
|SMIP_FUND_PARAMETERS|SMIP|Internal||||||||
|SMIP_FUND_PERFORMANCE|SMIP|Internal||||||||
|SMIP_FUND_PERF_LEVEL|SMIP|Internal||||||||
|SMIP_GROUP_TRUST_PERF|SMIP|Internal||||||||
|SMIP_METRICS|SMIP|Internal||||||||
|SMIP_METRICS_ORIGINAL|SMIP|Internal||||||||
|SMIP_PERFORMANCE|SMIP|Internal||||||||
|SMIP_QUINTILE_PERF|SMIP|Internal||||||||
|SOCIAL_SECURITY_STATUS|Shareholder|DST||||Onetime setup.  Copy data||||
|SPR_SHEET_CASH_ACT|||||N|||||
|TARGET_DAILY_WEIGHT|Target Fund|ssb|ssb|ssb_hlds\<dt\>.txt|||Daily|Fixed Width|Delta|
|TARGET_ORIG_GLIDEPATH|Target Fund|Internal||||||||
|TARGET_PERFORMANCE_TYPE|Target Fund|Internal||||||||
|TARGET_RETURN|Target Fund|Internal||||||||
|TARGET_WEALTH_INDEX|Target Fund|Internal||||||||
|TARGET_WEIGHTS|Target Fund|Internal||||||||
|TRADE_ENTRY_METHOD|Shareholder|Internal||||Onetime setup.  Copy data||||
|TRADE_ORIGIN|Shareholder|Internal||||Onetime setup.  Copy data||||
|TRANSACTION_TYPE|Shareholder|Internal||||Onetime setup.  Copy data||||
|UNIVERSE|Manager|Internal|||||||||
|YIELD_TYPES|Fund|Internal||||Onetime setup.  Copy data||||
