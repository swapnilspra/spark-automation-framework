# HOLDINGS_OPEN_POSITION

This table holds the Holdings OPen Position data.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[new_secr_flg](#new_secr_flg)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[breach_ind_cls](#breach_ind_cls)|character|1|YES||NO
|[breach_ind_acls](#breach_ind_acls)|character|1|YES||NO
|[breach_ind_voting](#breach_ind_voting)|character|1|YES||NO
|[breach_ind_nonvoting](#breach_ind_nonvoting)|character|1|YES||NO
|[new_br_ind_cls](#new_br_ind_cls)|character|1|YES||NO
|[new_br_ind_acls](#new_br_ind_acls)|character|1|YES||NO
|[new_br_ind_voting](#new_br_ind_voting)|character|1|YES||NO
|[new_br_ind_nonvoting](#new_br_ind_nonvoting)|character|1|YES||NO
|[spec_alert_cls](#spec_alert_cls)|character varying|200|YES||NO
|[spec_alert_acls](#spec_alert_acls)|character varying|200|YES||NO
|[spec_alert_voting](#spec_alert_voting)|character varying|200|YES||NO
|[spec_alert_nonvoting](#spec_alert_nonvoting)|character varying|200|YES||NO
|[breach_ind_int_cls](#breach_ind_int_cls)|character|1|YES||NO
|[breach_ind_int_acls](#breach_ind_int_acls)|character|1|YES||NO
|[breach_ind_int_voting](#breach_ind_int_voting)|character|1|YES||NO
|[breach_ind_int_nonvoting](#breach_ind_int_nonvoting)|character|1|YES||NO
|[issuer_nm](#issuer_nm)|character varying|200|YES||NO
|[adr_spsr_ship_type](#adr_spsr_ship_type)|character varying|25|YES||NO
|[holdg_open_posn_key](#holdg_open_posn_key)|integer|(32,0)|NO||YES
|[fund_compst_key](#fund_compst_key)|integer|(32,0)|YES||NO
|[secr_key](#secr_key)|integer|(32,0)|YES||NO
|[inc_crty_key](#inc_crty_key)|integer|(32,0)|YES||NO
|[iss_crty_key](#iss_crty_key)|integer|(32,0)|YES||NO
|[trde_crty_key](#trde_crty_key)|integer|(32,0)|YES||NO
|[thrs_crty_key](#thrs_crty_key)|integer|(32,0)|YES||NO
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[tot_long_posn](#tot_long_posn)|numeric|(38,15)|YES||NO
|[actl_cstdy_posn](#actl_cstdy_posn)|numeric|(38,15)|YES||NO
|[pend_sale_qty](#pend_sale_qty)|numeric|(38,15)|YES||NO
|[pend_buy_qty](#pend_buy_qty)|numeric|(38,15)|YES||NO
|[free_qty](#free_qty)|numeric|(38,15)|YES||NO
|[oshr_qty](#oshr_qty)|numeric|(38,15)|YES||NO
|[par_shr_qty](#par_shr_qty)|numeric|(38,15)|YES||NO
|[tot_shrt_posn](#tot_shrt_posn)|numeric|(38,15)|YES||NO
|[on_loan_qty](#on_loan_qty)|numeric|(38,15)|YES||NO
|[all_cls_oshr_qty](#all_cls_oshr_qty)|numeric|(38,15)|YES||NO
|[all_voting_oshr_qty](#all_voting_oshr_qty)|numeric|(38,15)|YES||NO
|[all_non_voting_oshr_qty](#all_non_voting_oshr_qty)|numeric|(38,15)|YES||NO
|[voting_shr_ind](#voting_shr_ind)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[prc_shrs_cls](#prc_shrs_cls)|numeric|(38,15)|YES||NO
|[prc_shrs_acls](#prc_shrs_acls)|numeric|(38,15)|YES||NO
|[prc_shrs_voting](#prc_shrs_voting)|numeric|(38,15)|YES||NO
|[prc_shrs_nonvoting](#prc_shrs_nonvoting)|numeric|(38,15)|YES||NO
|[thrsh_br_cls](#thrsh_br_cls)|numeric|(38,15)|YES||NO
|[thrsh_br_acls](#thrsh_br_acls)|numeric|(38,15)|YES||NO
|[thrsh_br_voting](#thrsh_br_voting)|numeric|(38,15)|YES||NO
|[thrsh_br_nonvoting](#thrsh_br_nonvoting)|numeric|(38,15)|YES||NO
|[prox_int_br_cls](#prox_int_br_cls)|numeric|(38,15)|YES||NO
|[prox_ext_br_cls](#prox_ext_br_cls)|numeric|(38,15)|YES||NO
|[prox_int_br_acls](#prox_int_br_acls)|numeric|(38,15)|YES||NO
|[prox_ext_br_acls](#prox_ext_br_acls)|numeric|(38,15)|YES||NO
|[prox_int_br_voting](#prox_int_br_voting)|numeric|(38,15)|YES||NO
|[prox_ext_br_voting](#prox_ext_br_voting)|numeric|(38,15)|YES||NO
|[prox_int_br_nonvoting](#prox_int_br_nonvoting)|numeric|(38,15)|YES||NO
|[prox_ext_br_nonvoting](#prox_ext_br_nonvoting)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### holdg_open_posn_key
#### Description

Holding Open Position Key

```
Autoincrement +1 for new inserts
```

### fund_compst_key
#### Description

Fund Composite Key

```
file.fund_id = fc.st_str_fund_nbr where fc.st_str_fund_nbr != 'GB35'
```

### secr_key
#### Description

Security Key

```
(
IF file.ALT_ASSET_ID_TYPE = 'SDL' then file.ASSET_ID = security.SEDOL_ID else
IF file.ALT_ASSET_ID_TYPE = 'ISN' then file.ALT_ASSET_ID1 = security.ISIN_ID else
IF file.ALT_ASSET_ID_TYPE IS NULL AND ALT_ASSET_ID1 IS NULL then file.ASSET_ID = security.ASSET_ID)
WHERE file.FUND_ID != 'GB35'

```



### inc_crty_key
#### Description

Incorporated Country Key

```
Match file.ASSET_ID with security.ASSET_ID and ASSET_TYPE SDL or ISN to get the CNTRY_OF_INCORPORATION from the Bloomberg file. If the bloomberg join returns NULL then join HARBCUST file with PTA_SSB_POSITION file based on ASSET_ID and take the PTA file.INCORP_CNTRY_CD and get the key from the country table. 

IF ISNULL(bloomberg.CNTRY_OF_INCORPORATION) where file.asset_id = bloomberg.asset_id then PTA_file.INCORP_CNTRY_CD = country.CTRY_CD
else bloomberg.CNTRY_OF_INCORPORATION = country.CTRY_CD
```



### iss_crty_key
#### Description

Issue Country Key


```
Match file.ASSET_ID with security.ASSET_ID and ASSET_TYPE SDL or ISN to get the CNTRY_ISSUE_ISO from the Bloomberg file. If the bloomberg join returns NULL then join HARBCUST file with PTA_SSB_POSITION file based on ASSET_ID and take the PTA file.issue_cntry_cd and get the key from the country table. 

IF ISNULL(bloomberg.CNTRY_ISSUE_ISO) where file.asset_id = bloomberg.asset_id then PTA_file.ISSUE_CNTRY_CD = country.CTRY_CD
else bloomberg.CNTRY_ISSUE_ISO = country.CTRY_CD
```

### trde_crty_key
#### Description

Trade Country Key

```
Match file.ASSET_ID with security.ASSET_ID and ASSET_TYPE SDL or ISN to get the SEDOL1_COUNTRY_ISO from the Bloomberg file. If the bloomberg join returns NULL then join HARBCUST file with PTA_SSB_POSITION file based on ASSET_ID and take the PTA file.trd_cntry_cd and get the key from the country table. 

IF ISNULL(bloomberg.SEDOL1_COUNTRY_ISO) where file.asset_id = bloomberg.asset_id then file.TRD_CNTRY_CD = country.CTRY_CD
else bloomberg.SEDOL1_COUNTRY_ISO = country.CTRY_CD
```

### thrs_crty_key
#### Description

Threshold Country Key


```
Unused
```



### day_key
#### Description

Day Key

```
Take the BUSINESS_DATE from the SSBCUST file header and get the key from Calendar
```

### tot_long_posn
#### Description

Total Long Position 

```
ssbcust.TRD_DT_LONG_QTY
```

### actl_cstdy_posn
#### Description

Actual Custody Long Position

```
ssbcust.SAFEKEEP_QTY
```

### pend_sale_qty
#### Description

Pending Sale Quantity

```
PEND_SALE_QTY
```

### pend_buy_qty
#### Description

Pending Buy Quantity

```
ssbcust.PND_BUY_QTY
```

### free_qty
#### Description

Free Quantity

#### Value Range

N/A

```
ssbcust.FREE_QTY
```

### oshr_qty
#### Description

Equity Share Out Quantity

#### Value Range

```
bloombergfile.EQY_SH_OUT_REAL
```

### par_shr_qty
#### Description

Share Par Quantity

```
ssbcust.SHRPAR_QTY
```

### tot_shrt_posn
#### Description

Total Short Position

```
ssbcust.TRD_DT_SHORT_QTY
```

### on_loan_qty
#### Description

On Loan Quantity

```
ssbcust.ON_LOAN_QTY
```

### new_secr_flg
#### Description

New Security Flag

```
join SSBCUST file with PTA_SSB_POSITION file based on the ASSET_ID. If there is a ASSET_ID in SSB_CUST and NOT IN PTA_SSB_POSITION, then mark the new_secr_flg = 'Y' else 'N'
```



### all_cls_oshr_qty
#### Description

All Closed Outstanding Share Quantity

```
bloombergfile.EQY_SH_OUT_TOT_MULT_SH
```

### all_voting_oshr_qty
#### Description

All Voting Outstanding Shares Quantity


```
bloomberg.TOTAL_VOTING_SHARES_VALUE
```

### all_non_voting_oshr_qty
#### Description

All Non-Voting Outstanding Shares Quantity

```
bloomberg.TOTAL_NON_VOTING_SHARES_VALUE
```

### voting_shr_ind
#### Description

Voting Shares Indicator

```
bloomberg.VOTING_RIGHTS
```



### curr_row_flg
#### Description

Current Row flag

#### Value Range

Y/N

### row_strt_dttm
#### Description

Row Start datetime

```
Row load date
```

### adr_spsr_ship_type
#### Description

ADR Sponsorship Type

```
bloomberg.ADR_SPONSORSHIP_TYP
```
###### Unused columns

prc_shrs_cls
prc_shrs_acls
prc_shrs_voting
prc_shrs_nonvoting
breach_ind_cls
breach_ind_acls
breach_ind_voting 
breach_ind_nonvoting
thrsh_br_cls
thrsh_br_acls
thrsh_br_voting
thrsh_br_nonvoting
new_br_ind_cls
new_br_ind_acls
new_br_ind_voting
new_br_ind_nonvoting
spec_alert_cls
spec_alert_acls
spec_alert_voting
spec_alert_nonvoting
prox_int_br_cls
prox_ext_br_cls
prox_int_br_acls
prox_ext_br_acls
prox_int_br_voting
prox_ext_br_voting
prox_int_br_nonvoting
prox_ext_br_nonvoting
breach_ind_int_cls
breach_ind_int_acls
breach_ind_int_voting
breach_ind_int_nonvoting
issuer_nm

######

====================================================================

Business Keys - 

FUND_COMPST_KEY,SECR_KEY,INC_CRTY_KEY,ISS_CRTY_KEY,TRDE_CRTY_KEY,DAY_KEY


Files Used - 

PTA_SSB_POSITION_<datetime>.txt (Also referred to as Summary Level Holdings)
Frequency - Harbor Business Days

HARBCUST.txt (Also called Daily Holdings file or Holdings Open Position file) - Referred in this doc as "file"

BB_COUNTRY_THRESHOLD Bloomberg file 

Joins - 

HARBCUST.ASSET_ID = BB_COUNTRY_THRESHOLD.ASSET_ID

Calendar.CAL_DAY = HARBCUST.BUSINESS_DATE (derived from the header row of the file)

Security.ASSET_ID = HARBCUST.ASSET_ID

