# Input files

## harbor_mm_web_disclosure_{{yyyyMMdd}} - alias hwd
file location: /ssb

### Date columns
| Column | Type | Format Details |
| ------ | ------ | ------------ |
| business_date | date | yyyyMMdd |

### format
Delimited: |\
Extra header lines: 1\
Extra footer lines: 1

## gb05_comparison_nav_fund_summary_{{dd-MM-yyyy}}.csv - alias gb05
file location: /ssb

## mmkt_info_{{mmDDyy}}.csv - alias mmkt
file location: /ssb

### format
Delimited: ,\
Extra header lines: 0\
Extra footer lines: 0

## rates_{{mmDDyy}}.csv - alias rates
file location: /ssb

### Formatted columns:
| Column | Type | Format Details |
| ------ | ------ | ------------ |
| period end date | date | dd/MM/yyyy |

### format
Delimited: ,\
Extra header lines: 0\
Extra footer lines: 0

# Lookup tables

## hdm_fund - alias fund
## hdm_calendar - alias calendar

# Join conditions
## hwd, gb05
hwd.business_date == gb05."period end date" AND\
hwd.basis_ind == gb05."dual pricing basis"

## hwd, fund
hwd.FUND_CODE = fund.ST_STR_FUND_NBR AND\
???lower(df_joined.SHARECLASSNAME)== F.lower(df_fund.SHR_CLS_DESC)

## hwd, mmkt
hwd.FUND_NBR == mmkt["Alternate Fund ID"]

## fund, rates(alias as rates_with_gl_y)
hwd.FUND_NBR == rates["ssb_fund"] AND\
rates["with gl"]=="Y"

## fund, rates(alias as rates_with_gl_n)
hwd.FUND_NBR == rates["ssb_fund"] AND\
rates["with gl"]=="N"

# Keys
Business key: fund.FUND_KEY\
Primary key: PK

# Trigger rules
when to trigger a calculation / recalculation of this job
MMKT_info, Rates arrive in the evening (market close) - initial recalculation. The files are incremental.
wait for both.
gb05 and harbor_mm - next morning. Relies on the market info arriving the day before.

MM_INVEST is not needed?


# Job dependencies
Does this job depend on any other jobs?

