# FUND_HOLDINGS
# Adding today again
This table holds the data from SSB's Summary Level Holdings.

## DDL

| Column Name                                         | SQL Type                    | Length  | Nullable | Default Value | PK  |
| --------------------------------------------------- | --------------------------- | ------- | -------- | ------------- | --- |
| [curr_row_flg](#curr_row_flg)                       | character varying           | 1       | YES      |               | NO  |
| [fund_holdg_key](#fund_holdg_key)                   | integer                     | (32,0)  | NO       |               | YES |
| [day_key](#day_key)                                 | integer                     | (32,0)  | YES      |               | NO  |
| [fund_compst_key](#fund_compst_key)                 | integer                     | (32,0)  | YES      |               | NO  |
| [secr_key](#secr_key)                               | integer                     | (32,0)  | YES      |               | NO  |
| [secr_attr_key](#secr_attr_key)                     | integer                     | (32,0)  | YES      |               | NO  |
| [indtry_key](#indtry_key)                           | integer                     | (32,0)  | YES      |               | NO  |
| [inc_crty_key](#inc_crty_key)                       | integer                     | (32,0)  | YES      |               | NO  |
| [iss_crty_key](#iss_crty_key)                       | integer                     | (32,0)  | YES      |               | NO  |
| [trde_crty_key](#trde_crty_key)                     | integer                     | (32,0)  | YES      |               | NO  |
| [nra_tax_crty_key](#nra_tax_crty_key)               | integer                     | (32,0)  | YES      |               | NO  |
| [base_crty_key](#base_crty_key)                     | integer                     | (32,0)  | YES      |               | NO  |
| [base_currcy_key](#base_currcy_key)                 | integer                     | (32,0)  | YES      |               | NO  |
| [loc_currcy_key](#loc_currcy_key)                   | integer                     | (32,0)  | YES      |               | NO  |
| [mkt_pr_currcy_key](#mkt_pr_currcy_key)             | integer                     | (32,0)  | YES      |               | NO  |
| [incm_currcy_key](#incm_currcy_key)                 | integer                     | (32,0)  | YES      |               | NO  |
| [row_seq_nbr](#row_seq_nbr)                         | integer                     | (32,0)  | NO       |               | NO  |
| [days_to_mtry](#days_to_mtry)                       | numeric                     | (38,15) | YES      |               | NO  |
| [par_shrs](#par_shrs)                               | numeric                     | (38,15) | YES      |               | NO  |
| [orig_base_cost](#orig_base_cost)                   | numeric                     | (38,15) | YES      |               | NO  |
| [orig_loc_cost](#orig_loc_cost)                     | numeric                     | (38,15) | YES      |               | NO  |
| [idfd_base_cost](#idfd_base_cost)                   | numeric                     | (38,15) | YES      |               | NO  |
| [idfd_loc_cost](#idfd_loc_cost)                     | numeric                     | (38,15) | YES      |               | NO  |
| [base_mkt_val](#base_mkt_val)                       | numeric                     | (38,15) | YES      |               | NO  |
| [loc_mkt_val](#loc_mkt_val)                         | numeric                     | (38,15) | YES      |               | NO  |
| [curr_base_pr](#curr_base_pr)                       | numeric                     | (38,15) | YES      |               | NO  |
| [curr_loc_pr](#curr_loc_pr)                         | numeric                     | (38,15) | YES      |               | NO  |
| [urlz_gl_loc](#urlz_gl_loc)                         | numeric                     | (38,15) | YES      |               | NO  |
| [urlz_gl_base](#urlz_gl_base)                       | numeric                     | (38,15) | YES      |               | NO  |
| [avg_cost_base_par_shrs](#avg_cost_base_par_shrs)   | numeric                     | (38,15) | YES      |               | NO  |
| [avg_cost_loc_par_shrs](#avg_cost_loc_par_shrs)     | numeric                     | (38,15) | YES      |               | NO  |
| [orig_strk_pr](#orig_strk_pr)                       | numeric                     | (38,15) | YES      |               | NO  |
| [curr_strk_pr](#curr_strk_pr)                       | numeric                     | (38,15) | YES      |               | NO  |
| [vend_suppld_iss_curr_pr](#vend_suppld_iss_curr_pr) | numeric                     | (38,15) | YES      |               | NO  |
| [base_net_amort](#base_net_amort)                   | numeric                     | (38,15) | YES      |               | NO  |
| [loc_net_amort](#loc_net_amort)                     | numeric                     | (38,15) | YES      |               | NO  |
| [gr_base_amt_accrd](#gr_base_amt_accrd)             | numeric                     | (38,15) | YES      |               | NO  |
| [gr_loc_amt_accrd](#gr_loc_amt_accrd)               | numeric                     | (38,15) | YES      |               | NO  |
| [curr_varb_rt](#curr_varb_rt)                       | numeric                     | (38,15) | YES      |               | NO  |
| [partition_yr](#partition_yr)                       | numeric                     | (38,15) | YES      |               | NO  |
| [etl_load_cyc_key](#etl_load_cyc_key)               | integer                     | (32,0)  | YES      |               | NO  |
| [src_sys_id](#src_sys_id)                           | numeric                     | (38,15) | YES      |               | NO  |
| [row_strt_dttm](#row_strt_dttm)                     | timestamp without time zone | 6       | YES      |               | NO  |
| [row_stop_dttm](#row_stop_dttm)                     | timestamp without time zone | 6       | YES      |               | NO  |

### fund_holdg_key

#### Description

Generated value

#### Value Range

N/A

#### Logic

Sequence Generator

```
Autoincrement +1 for new inserts
```

### day_key

#### Description

Holding Position date

#### Value Range

N/A

#### Logic

Match the date to the mart calendar to get the date key

```
SSB_PTA_POSITION.CALEN_DT = calendar.cal_day

```

### fund_compst_key

#### Description

FUND Composite ID

#### Value Range

N/A

#### Logic

FUND Composite key matched on FUND_ID

```
SSB_PTA_POSITION.FUND_ID

```

### secr_key

#### Description

ASSET_ID

#### Value Range

N/A

#### Logic

Match the ASSET_ID with FUND_SECURITY table to get the key

```
SSB_PTA_POSITION.ASSET_ID

```

### secr_attr_key

#### Description

Security Attribute Key

#### Value Range

N/A

#### Logic

Match the above source fields for each record to get the key value from security_attributes

```
SSB_PTA_POSITION.ASSET_ID,FUND_ID,LOT_ACCT_NBR,POS_TYPE_CD

```

### indtry_key

#### Description

Industry Key

#### Value Range

N/A

#### Logic

Get the key from industry by matching MJR_INDSTY_CD=sctr_cd and MNR_INDSTY_CD=indtry_cd

```
PTA_SSB_POSITION.MJR_INDSTY_CD, MNR_INDSTY_CD.
MJR_INDSTY_CD = Sector Code
MNR_INDSTY_CD = Industry Code

```

### inc_crty_key

#### Description

Incorporated Country Key

#### Value Range

N/A

#### Logic

get the key from country by matching INCORP_CNTRY_CD=crty_cd

```
PTA_SSB_POSITION.INCORP_CNTRY_CD
```

### iss_crty_key

#### Description

Issue Country Key

#### Value Range

N/A

#### Logic

get the key from country by matching ISSUE_CNTRY_CD=crty_cd

```
PTA_SSB_POSITION.ISSUE_CNTRY_CD
```

### trde_crty_key

#### Description

Trade Country Key

#### Value Range

N/A

#### Logic

get the key from country by matching TRD_CNTRY_CD=crty_cd

```
PTA_SSB_POSITION.TRD_CNTRY_CD
```

### nra_tax_crty_key

#### Description

NRA Tax Country Key

#### Value Range

N/A

#### Logic

get the key from country by matching NRATAX_CNTRY_CD=crty_cd

```
PTA_SSB_POSITION.NRATAX_CNTRY_CD
```

### base_crty_key

#### Description

Base Country Key

#### Value Range

N/A

#### Logic

get the key from country by matching BASE_CNTRY_CD=crty_cd

```
PTA_SSB_POSITION.BASE_CNTRY_CD
```

### base_currcy_key

#### Description

Base Currency Key

#### Value Range

N/A

#### Logic

get the key from currency by matching CRNCY_CD_BASE=currcy_cd

```
PTA_SSB_POSITION.CRNCY_CD_BASE
```

### loc_currcy_key

#### Description

Local Currency Key

#### Value Range

N/A

#### Logic

get the key from currency by matching MKTPRC_CRNCY_CD=currcy_cd

```
PTA_SSB_POSITION.MKTPRC_CRNCY_CD
```

### mkt_pr_currcy_key

#### Description

Market Price Currency Key

#### Value Range

N/A

#### Logic

get the key from currency by matching MKTPRC_CRNCYCD_EUR=currcy_cd

```
PTA_SSB_POSITION.MKTPRC_CRNCYCD_EUR
```

### incm_currcy_key

#### Description

Income Currency Key

#### Value Range

N/A

#### Logic

get the key from currency by matching INCM_CRNCY_CD=currcy_cd.

```
PTA_SSB_POSITION.INCM_CRNCY_CD
```

### row_seq_nbr

#### Description

Original Coupon Rate

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.ORIG_CPN_RT
```

#### days_to_mtry

#### Description

Days to Maturity

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.DAYS_TOMAT_CNT
```

#### par_shrs

#### Description

Share Par Quantity

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.SHRPAR_QTY
```

#### orig_base_cost

#### Description

Base Cost

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.CST_ORG_BTL
```

### orig_loc_cost

#### Description

Original Loc Cost

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.CST_ORG_LTL
```

### idfd_base_cost

#### Description

Identified Base Cost

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.IDCST_BTL
```

### idfd_loc_cost

#### Description

Identified Loc Cost

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.IDCST_LTL
```

### base_mkt_val

#### Description

Base Market Value

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.MKTVAL_BTL
```

### loc_mkt_val

#### Description

Loc Market Value

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.MKTVAL_LTL
```

### curr_base_pr

#### Description

Current Base Price

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.MKTPRC_BAM
```

### curr_loc_pr

#### Description

Current Loc Price

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.MKTPRC_LAM
```

### urlz_gl_loc

#### Description

Unrealized GL Loc

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.UNRLZD_GNLS_LTL
```

### urlz_gl_base

#### Description

Unrealized GL Base

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.UNRLZD_GNLS_BTL
```

### avg_cost_base_par_shrs

#### Description

Average Cost Base Par Shares

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.UNTCST_BTL
```

### avg_cost_loc_par_shrs

#### Description

Average Cost Loc Par Shares

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.UNTCST_LTL
```

### orig_strk_pr

#### Description

Original Strike Price

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.ORG_STRIKE_PRC
```

### curr_strk_pr

#### Description

Current Strike Price

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.CRNT_STRIKE_PRC
```

### vend_suppld_iss_curr_pr

#### Description

Vendor Supplied Issue Current Price

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.MKTPRC_EAM
```

### base_net_amort

#### Description

Base net amortized

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.AMTZD_BTL
```

### loc_net_amort

#### Description

Loc Net Amortized

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.AMTZD_LTL
```

### gr_base_amt_accrd

#### Description

Gross Base Amount Accrued

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.ACCRD_BTL
```

### gr_loc_amt_accrd

#### Description

Gross Loc Amount Accrued

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.ACCRD_LTL
```

### curr_varb_rt

#### Description

Current Variable Rate

#### Value Range

N/A

#### Logic

```
PTA_SSB_POSITION.POS_INT_RT
```

### partition_yr

#### Description

Partition Year

#### Value Range

N/A

#### Logic

```
TO_INTEGER(TO_CHAR(CALEN_DT,'YYYY'))
```

### curr_row_flg

#### Description

Current Row Flag

#### Value Range

'Y'/'N'

#### Logic

Defaults to Y

```
Y
```

### row_strt_dttm

#### Description

Date row was inserted

#### Value Range

N/A

#### Logic

```
sysdate
```

### row_stop_dttm

#### Description

Date row marked as Not current

#### Value Range

N/A

#### Logic

```
sysdate
```

### etl_load_cyc_key

#### Description

Unused

#### Value Range

N/A

#### Logic

```
Unused
```

### src_sys_id

#### Description

unused

#### Value Range

N/A

#### Logic

```
unused
```
----------------------------------------------------------------

## Files Used
1. PTA_SSB_POSITION.mmddyy.txt -


File Type -- Pipe delimited file.
File Location --


## Business Key
FUND_ID, CALEN_DT, ASSET_ID, LOT_ACCT_NUM

## Logic
Only get records where FUND_ID != 'GB35'

## Joins used
day_key         - SSB_PTA_POSITION.CALEN_DT = calendar.cal_day

fund_compst_key - SSB_PTA_POSITION.FUND_ID = fund_composite.fund_id

secr_key        - SSB_PTA_POSITION.ASSET_ID = security.asset_id

secr_attr_key   - SSB_PTA_POSITION.ASSET_ID = security_attribute.asset_id and
                  SSB_PTA_POSITION.FUND_ID = security_attribute.ssb_fund_nbr and
                  SSB_PTA_POSITION.LOT_ACCT_NBR = security_attribute.lot_acct_nbr and
                  SSB_PTA_POSITION.POS_TYPE_CD = security_attribute.secr_type

indtry_key      - PTA_SSB_POSITION.MJR_INDSTY_CD = industry.sctr_cd and
                  PTA_SSB_POSITION.MNR_INDSTY_CD = industry.indtry_cd

inc_crty_key    - PTA_SSB_POSITION.INCORP_CNTRY_CD = country.crty_cd

iss_crty_key    - PTA_SSB_POSITION.ISSUE_CNTRY_CD = country.crty_cd

trde_crty_key   - PTA_SSB_POSITION.TRD_CNTRY_CD = country.crty_cd

nra_tax_crty_key- PTA_SSB_POSITION.NRATAX_CNTRY_CD = country.crty_cd

base_crty_key   - PTA_SSB_POSITION.BASE_CNTRY_CD = country.crty_cd

base_currcy_key - PTA_SSB_POSITION.CRNCY_CD_BASE = currency.currcy_cd

loc_currcy_key  - PTA_SSB_POSITION.MKTPRC_CRNCY_CD = currency.currcy_cd

loc_currcy_key  - PTA_SSB_POSITION.MKTPRC_CRNCY_CD = currency.currcy_cd

incm_currcy_key - PTA_SSB_POSITION.INCM_CRNCY_CD = currency.currcy_cd
