# SECURITY

This table holds the lorem ipsum dolor sit amet.

## DDL

| Column Name                             | SQL Type                    | Length  | Nullable | Default Value | PK  |
| --------------------------------------- | --------------------------- | ------- | -------- | ------------- | --- |
| [sedol_id](#sedol_id)                   | character varying           | 20      | YES      |               | NO  |
| [isin_id](#isin_id)                     | character varying           | 20      | YES      |               | NO  |
| [tckr_sym](#tckr_sym)                   | character varying           | 50      | YES      |               | NO  |
| [secr_nm](#secr_nm)                     | character varying           | 250     | YES      |               | NO  |
| [issuer_nm](#issuer_nm)                 | character varying           | 255     | YES      |               | NO  |
| [glbl_issuer_nm](#glbl_issuer_nm)       | character varying           | 255     | YES      |               | NO  |
| [invmt_type_cd](#invmt_type_cd)         | character varying           | 2       | YES      |               | NO  |
| [invmt_type_desc](#invmt_type_desc)     | character varying           | 20      | YES      |               | NO  |
| [dtc_cusip](#dtc_cusip)                 | character varying           | 50      | YES      |               | NO  |
| [asset_id](#asset_id)                   | character varying           | 20      | YES      |               | NO  |
| [muncpl_secr_st_cd](#muncpl_secr_st_cd) | character varying           | 2       | YES      |               | NO  |
| [muncpl_secr_st](#muncpl_secr_st)       | character varying           | 255     | YES      |               | NO  |
| [curr_row_flg](#curr_row_flg)           | character varying           | 1       | YES      |               | NO  |
| [secr_key](#secr_key)                   | integer                     | (32,0)  | NO       |               | YES |
| [etl_load_cyc_key](#etl_load_cyc_key)   | integer                     | (32,0)  | YES      |               | NO  |
| [src_sys_id](#src_sys_id)               | numeric                     | (38,15) | YES      |               | NO  |
| [row_strt_dttm](#row_strt_dttm)         | timestamp without time zone | 6       | YES      |               | NO  |
| [row_stop_dttm](#row_stop_dttm)         | timestamp without time zone | 6       | YES      |               | NO  |

### secr_key

#### Description

Security Key

#### Value Range

N/A

#### Logic

Sequence generator.

```
Autoincrement +1 for new inserts
```

### sedol_id

#### Description

ALT_ASSET_ID1/ALT_ASSET_ID2

N/A

#### Logic

```
IF(ALT_ASSET_ID_TYPE_CD1='SDL')
  THEN ALT_ASSET_ID1
ELSE
    IF(ALT_ASSET_ID_TYPE_CD2='SDL')
        THEN ALT_ASSET_ID2
ELSE
    NULL
```

### isin_id

#### Description

ALT_ASSET_ID1/ALT_ASSET_ID2

#### Value Range

N/A

#### Logic

```
IF(ALT_ASSET_ID_TYPE_CD1='ISN')
  THEN ALT_ASSET_ID1
ELSE
    IF(ALT_ASSET_ID_TYPE_CD2='ISN')
        THEN ALT_ASSET_ID2
ELSE
    NULL
```

### tckr_sym

#### Description

TICKER_SYMB

#### Value Range

N/A

#### Logic

```
If the TICKER_SYMBOL is NULL, then check if the existing Security exists. If yes then take the exising value; else use the TICKER_SYMB

IF(IN_TCKR_SYM = NULL
  THEN OLD_TCKR_SYM
ELSE
  IN_TCKR_SYM
```

### secr_nm

#### Description

Issue Long Name

#### Value Range

N/A

#### Logic

```
ISSUE_LONG_NM
```

### issuer_nm

#### Description

Issuer Name

#### Value Range

N/A

#### Logic

```
If the ISSUER_NM is NULL, then check if the existing Security exists. If yes then take the exising value; else use the ISSUER_NM

IF(IN_ISSUER_NM = NULL
  THEN OLD_ISSUER_NM
ELSE
  IN_ISSUER_NM
```

### glbl_issuer_nm

#### Description

Global Issuer Name

#### Value Range

N/A

#### Logic

```
If the GLBL_ISSUER_NM is NULL, then check if the existing Security exists. If yes then take the exising value; else use the GLBL_ISSUER_NM

IF(IN_GLBL_ISSUER_NM = NULL
  THEN OLD_GLBL_ISSUER_NM
ELSE
  IN_GLBL_ISSUER_NM
```

### invmt_type_cd

#### Description

Investment Type Code

#### Value Range

N/A

#### Logic

```
INVST_TYPE_CD
```

### invmt_type_desc

#### Description

Investment Type Description

#### Value Range

N/A

#### Logic

```
Lookup the Description from INVESTMENT_TYPE by matching
INVST_TYPE_CD
```

### dtc_cusip

#### Description

DTC CUSIP ID

#### Value Range

N/A

#### Logic

```
DTC_CUSIP
```

### asset_id

#### Description

Asset ID

#### Value Range

N/A

#### Logic

```
ASSET_ID
```

### muncpl_secr_st_cd

#### Description

Municipal Security State Code

#### Value Range

N/A

#### Logic

```
STATE_CD
```

### muncpl_secr_st

#### Description

Municipal Security State Name

#### Value Range

N/A

#### Logic

```
Get name from STATE based on STATE_CD
```

### curr_row_flg

#### Description

Current Row Flag

#### Value Range

N/A

#### Logic

```
"Y"
```

### row_strt_dttm

#### Description

Row inserted date

#### Value Range

N/A

#### Logic

```
sysdate
```

### row_stop_dttm

#### Description

Date when row marked obsolete

#### Value Range

N/A

#### Logic

```
Old ROW_STRT_DTTM
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

See Step 3 below

#### Logic

```
Null & PEND_TRADES (Logic in Step 3)
```
## Files Used
1. PTA_SSB_POSITION.mmddyy.txt
2. Confluence_Holding.csv
3. BB_COUNTRY_THRESHOLD.out (bbt)
4. SSBCUST.txt


File Type -- Pipe delimited
File Location --


## Business Key
CALEN_DT, ASSET_ID


## Logic
Select only DISTINCT key values WHERE FUND_ID != 'GB35'

## Joins
Step 1 - 

The PTA_SSB_POSITION file has to be first read and WHERE  ASSET_ID = CONFLUENCE.ASSET_ID update file attributes as
BASE_CNTRY_CD = CONFLUENCE.BASE_CNTRY_CD, TICKER_SYMB = CONFLUENCE.TICKER_SYMB, ISSUE_LONG_NM = CONFLUENCE.ISSUE_LONG_NM, INCORP_CNTRY_CD = CONFLUENCE.INCORP_CNTRY_CD, ISSUE_CNTRY_CD = CONFLUENCE.ISSUE_CNTRY_CD, MOODY_RTG = CONFLUENCE.MOODY_RTG, SNP_RTG = CONFLUENCE.SNP_RTG, MKTPRC_CRNCY_CD = CONFLUENCE.MKTPRC_CRNCY_CD  

Step 2

PTA_SSB_POSITION attributes needs updated as below where ASSET_ID = BB_COUNTRY_THRESHOLD.ASSET_ID
as ISSUER_NM = bbt.ISSUER_NM , GLBL_ISSUER_NM = bbt.GLBL_ISSUER_NM, CUR_MKT_CAP=:bbt.CUR_MKT_CAP  WHERE 

Step 3

read distinct SSBCUST.ASSET_ID, SSBCUST.ALT_ASSET_ID_TYPE_CD1, SSBCUST.ALT_ASSET_ID1, SSBCUST.INVEST_TYPE_CD from SSBCUST file and check if the record exists based on ASSET_ID. If record is found then check if they are different. If yes then update changed rows and update SRC_SYS_ID = "PEND_TRADES". If record found and there are no changes update SRC_SYS_ID = PEND_TRADES
