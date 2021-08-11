# Incoming File Catalog

## Description 
This document will describe the files received from FileProc into the HCA - Data Lake project scope.
The catalog lists the source files and describes the 

The following tables describe the incoming files:

1. [SSB Files Catalog](#SSB-Files-Catalog)
1. [DST Files Catalog ](#DST-Files-Catalog )
1. [Other Files Catalog](#Other-Files-Catalog)

## SSB Files Catalog 

| File name | Type | BUSINESS UNIT | FREQENCY | Tidal jobs/other jobs to download files | COMMENTS |Data Sensitivity|
|--- |--- |--- |--- |--- |--- |--- |
| ssb_hlds_mmddyy.txt | pip delimited | IPM | Business day | Load Daily Target Holdings\001_Get Target Weights | Daily Target Fund Holdings. Ftp script on Informatica server. Created tidal job to replace shell script |confidential|
| full_prc_mmddyy.CSV | CSV | HSG PRICING | Business day | NewNightOps-scheduled jobs\NewNightOps 0B- ftp SSB files to Informatica | file for nightly pricing. the file contains all composite level fund data. It includes fund number, fund name, cash totals, short term totals,interest rec, interest payable, net shares and total net assets. |confidential|
| MMKT_info_mmddyy.CSV | CSV | HSG PRICING | Business day | NewNightOps-scheduled jobs\NewNightOps 0B- ftp SSB files to Informatica | file for nightly pricing.This file contains Gb05 daily dollar distribution amounts plus daily gain loss totals. Also includes days to maturity and WAL |confidential|
| RATES_mmddyy.CSV | CSV | HSG PRICING | Business day | NewNightOps-scheduled jobs\NewNightOps 0B- ftp SSB files to Informatica | file for nightly pricing.This file contains GB05 1 ,7 and 30 day yield rates and aslo daily dist rate . For whatever reason it contains gb30 sec yield |confidential|
| SEC_YLD_mmddyy.CSV | CSV | HSG PRICING | Business day | NewNightOps-scheduled jobs\NewNightOps 0B- ftp SSB files to Informatica | file for nightly pricing. This file contains as bond funds rolling 30 day yield information |confidential|
| ssbnav_yyyymmdd.txt | fixed-length including header | HSG PRICING | Business day | NewNightOps-scheduled jobs\NewNightOps 0B- ftp SSB files to Informatica | file for nightly pricing. the file contains all NAV's data for all funds. |confidential|
| HCPTCG.TXT | fixed-length including header | | Business day | ? | HARB CAP DLY PORTFOLIO CHG RPT TO CLIENT VI |confidential|
| daily_account_summary.txt | fixed-length including header | | Business day | ? | HARBOR CAPITAL FTP DAILY SUMMARY HOLDINGS |confidential|
| HARBORCP_TXN102_MMDDYY.txt | pip delimited | | Business day | Holdings_Combined\Get_Holdings_Txn_File\030_Get Daily Txn Holdings File to informprod | a daily file that contains all security transaction based on post date |Restricted|
| CONFL_MEHOLDINGS_MMDDYY.TXT | pip delimited | Accounting | monthly | N/A | manually downloaded by accounting team, 3rd Business day of a month |confidential|
| CONFL_GLBALANCE_MMDDYY.TXT | pip delimited | Accounting | semi-annually | N/A | manually downloaded by accounting team, twice a year in May and Nov |confidential|
| PTA_TRANSACTIONS_MMDDYY.txt | pip delimited | Application Development | Business day | PTA Daily Jobs\501_download PTA_TRANSACTIONS | files for PTA load, downloaded by FtpFetch.bat script |Restricted|
| PTA_SSB_SECURITY_MMDDYY.txt | pip delimited | Application Development | Business day | PTA Daily Jobs\502_download PTA_SSB_SECURITY | files for PTA load, downloaded by FtpFetch.bat script |Restricted|
| PTA_SSB_POSITION_MMDDYY.txt | pip delimited | Application Development | Business day | PTA Daily Jobs\503_download PTA_SSB_POSITION, Holdings_Combined\Get_Holdings_File\030_Get Daily Holdings File to informprod | files for PTA load, downloaded by FtpFetch.bat script |Restricted|
| HARBORCP_LLH102_mmddyy.txt | pip delimited | Accounting | Daily | holdings combined run | Future Holding Datamart |Restricted|
| HARBORCP_SLH103_mmddyy.txt | pip delimited | Accounting | Daily | holdings combined run | Future Holding Datamart |Restricted|
| DividendDataFeed&lt;yesterday.52.mmddyy&gt;.txt | hasn't arrived yet | ACCOUNTING | Quarterly | Dividend Calculation\Dividend 1_ssb trial balance- run on demand\001_FTP_trial balance file from ssb | SSB trial balance file |confidential|
| GB05_Comparison_NAV_Fund Summary-&lt;last_business_day.mm-dd-yyyy&gt;.csv | CSV + XLSX | DSS | Business day | MM investment and MTM NAV file\001-download SSB MM_INVEST file from fileproc | Money Market investment file |confidential|
| GB05_Comparison_NAV_Portfolio Comparison_-&lt; last_business_day .mm-dd-yyyy&gt;. | CSV + XLSX | DSS | Business day | MM investment and MTM NAV file\011-download SSB MTM NAV from fileproc | Money Market MTM NAV file |confidential|
| Group_Trust_Daily_NAV.txt | pip delimited | DSS | Business day | N/A | Group Trust Daily NAV |confidential|
| Group_Trust_GL_Activity.txt | pip delimited | DSS | Business day | N/A | Group Trust GL Activity |confidential|
| HARBCUST_mmddyy.txt | pip delimited | DSS | Business day | Holdings_Combined\Get Holdings Open Position File\020_Get_Holdings Open Posn File to informprod | Open and Pending security positions for Foreign ownership reporting |Restricted|
| GBxx back test file YYYYMMDD.xls | CSV | Application Development | Business day | Fairvalue_Daily\010-scp download backtest fiels | Fair value files processed by App dev java code |confidential|
| harbor_mm_web_disclosure_YYYYMMDD.txt | pip delimited | MM investment and MTM NAV file\021-download harbor_mm_web_disclosure.txt from fileproc | - |confidential|

## DST Files Catalog 

| File name | Type | BUSINESS UNIT | FREQENCY | COMMENTS |Data Sensitivity |
|--- |--- |--- |--- |--- |--- |
| DTO.HCA.AMP.TIP09.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.APR.TIP03.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.BLUESKY.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.CLO.TIP39.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.CLO.TIP40.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.CLO.TIP41.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.CLO.TIP42.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.DFA.HIST.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.DFA.TIP02.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.DVRA.TIP01.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.FBP.TIP55.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |Restricted|
| DTO.HCA.FDP.TIP56.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |Restricted|
| DTO.HCA.MBP.TIP50.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.MTF.852.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Monthly | Restricted|
| DTO.HCA.MTF.857.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Monthly |Restricted|
| DTO.HCA.MTF.858.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Monthly |Restricted|
| DTO.HCA.NAA.TIP08.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.NFA.TIP07.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |pii|
| DTO.HCA.OPP.TIP11.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |Restricted|
| DTO.HCA.SDCM.BAL.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |Restricted|
| DTO.HCA.SDCM.mmddyyyy.hhmiss | Fixed length | HSG | Business day | Daily TIP Files |Restricted|
| ESCHEAT_CONTACT_ERRORS.TXT.&lt;SysDate.yyyymmdd&gt; | ; delimited | HSG | Business day | - |pii|
| DTO.HCA.UPP.YYYYMMDD.HHMISS | space delimited | HSG | Business day | pricing file |Restricted|
| DTO.HCA.UPR.YYYYMMDD.HHMISS | space delimited | HSG | Business day | pricing file,UPR or RUP |Restricted|

## Other Files Catalog 

| File name | Type | FROM | BUSINESS UNIT | FREQENCY | COMMENTS |
|--- |--- |--- |--- |--- |--- |
| harbor-bb-idx-data-ml.gz.&lt;SysDate.yyyymmdd&gt; | gzip, we should unzip | Bloomberg | IT | Business day | download Merrill Lynch file |
| CDLYbalancingreport.csv | CSV | FSSI | /home/fssi/prod/ | HSG | Daily |
| harbor-bb-idx-data-final.&lt;SysDate.yyyymmdd&gt; | Bloomberg format (need to extract data) | Bloomberg | IT | Business day | encrypted Bloomberg final price file |
| harbor-bb-idx-data-prelim.&lt;SysDate.yyyymmdd&gt; | Bloomberg format (need to extract data) | Bloomberg | IT | Business day | download prelim benchmark file |
| 783239.hpc.yyyymmdd | waiting to arrive | Bloomberg | DSS | business day | benchmark price correction file |
| BB_SECURITIES.out | waiting to arrive | Bloomberg | Accounting | monthly? | confluence output file from Bloomberg |
| bdgt_budget.csv                 | CSV             | HC                   | - | - | -|
| bdgt_account.csv                | CSV             | HC                   | - | - | -|
| bdgt_acct_accrual.csv           | CSV             | HC                   | - | - | -|
| bdgt_acct_adjustment.csv        | CSV             | HC                   | - | - | -|
|  bdgt_category_accrual.csv      | CSV             | HC                   | - | - | -|
| bdgt_acct_transfer.csv          | CSV             | HC                   | - | - | -|
| bdgt_assumption_change.csv      | CSV             | HC                   | - | - | -|
| bdgt_assumption_change_fund.csv | CSV             | HC                   | - | - | -|
| bdgt_data_source.csv            | CSV             | HC                   | - | - | -|
| bdgt_data_source_fund.csv       | CSV             | HC                   | - | - | -|
| bdgt_line_item.csv              | CSV             | HC                   | - | - | -|
| bdgt_fund.csv                   | CSV             | HC                   | - | - | -|
| bdgt_fund_multiplier.csv        | CSV             | HC                   | - | - | -|
| bdgt_invc_alloc.csv             | CSV             | HC                   | - | - | -|
| bdgt_invc_alloc_fund.csv        | CSV             | HC                   | - | - | -|
| bdgt_liability.csv              | CSV             | HC                   | - | - | -|
| bdgt_liability_fund.csv         | CSV             | HC                   | - | - | -|
| bdgt_line_item_alloc.csv        | CSV             | HC                   | - | - | -|
| bdgt_line_item_alloc_fund.csv   | CSV             | HC                   | - | - | -|
| bdgt_payment_diff.csv           | CSV             | HC                   | - | - | -|
| bdgt_payment_diff_fund.csv      | CSV             | HC                   | - | - | -|
| bdgt_trustee_election.csv       | CSV             | HC                   | - | - | -|
| invoice_trial.csv               | CSV             | dealer-invoice-trial | - | - | -|
| invoice.csv                     | CSV             | dealer-invoice       | - | - | -|
| taxlist.txt                     | space delimited | tax-data             | - | - | -|
| company.txt                     | single column   | tax-data             | - | - | only holds one company (1 row)|
| spendcategory.txt               | single column   | tax-data             | - | - | has 1 row|
| supplier.txt                    | single column   | tax-data             | - | - | has 1 row|
| tax.txt                         | space delimited | tax-data             | - | - | value that includes space character replace it with ~ |
| invoice.txt                     | space delimited | tax-data             | - | - | value that includes space character replace it with ~ |
| fund_position.csv               | CSV             | fiduciary-reporting  | - | - | -|
| benchmark_position.csv          | CSV             | fiduciary-reporting  | - | - | -|
| characteristics*.csv         | CSV             | IRM                  | - | - | -|
| characteristics*.csv         | CSV             | IRM                  | - | - | -|
| quality*.csv                 | CSV             | IRM                  | - | - | -|
| quality*.csv                 | CSV             | IRM                  | - | - | -|
| duration*.csv                | CSV             | IRM                  | - | - | -|
| issuer*.csv                  | CSV             | IRM                  | - | - | -|
| sector*.csv                  | CSV             | IRM                  | - | - | -|
| sector*.csv                  | CSV             | IRM                  | - | - | -|
| duration*.csv                | CSV             | IRM                  | - | - | -|
| issuer*.csv                  | CSV             | IRM                  | - | - | -|
| fund.csv                        | CSV             | fund                 | - | - | -|
| fund_composite.csv              | CSV             | fund                 | - | - | -|
| pimco_country_breakout*.csv     | CSV             | pimco                | - | - | -|
| pimco_deriv_exposure*.csv       | CSV             | pimco                | - | - | -|
| pimco_duration_breakout*.csv    | CSV             | pimco                | - | - | -|
| pimco_maturity_breakout*.csv    | CSV             | pimco                | - | - | -|
| pimco_mkt_survey*.csv           | CSV             | pimco                | - | - | -|
| pimco_mkt_maturity*.csv         | CSV             | pimco                | - | - | -|
| pmco_mkt_quality*.csv           | CSV             | pimco                | - | - | -|
| pimco_quality_breakout*.csv     | CSV             | pimco                | - | - | -|
| pimco_country_breakout*.csv     | CSV             | pimco                | - | - | -|
| pimco_sector_breakout*.csv      | CSV             | pimco                | - | - | -|
| pimco_statistics*.csv           | CSV             | pimco                | - | - | -|
| pimco_mkt_statistics*.csv       | CSV             | pimco                | - | - | -|
