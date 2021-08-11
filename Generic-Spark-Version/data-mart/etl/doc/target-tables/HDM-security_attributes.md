# SECURITY_ATTRIBUTES

This table holds the lorem ipsum dolor sit amet.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[asset_id](#asset_id)|character varying|100|NO||NO
|[ssb_fund_nbr](#ssb_fund_nbr)|character varying|25|NO||NO
|[lot_acct_nbr](#lot_acct_nbr)|character varying|6|YES||NO
|[secr_type](#secr_type)|character varying|100|YES||NO
|[cusip_id](#cusip_id)|character varying|100|YES||NO
|[cash_id](#cash_id)|character varying|20|YES||NO
|[spot_id](#spot_id)|character varying|50|YES||NO
|[fwd_id](#fwd_id)|character varying|20|YES||NO
|[moody_rtng](#moody_rtng)|character varying|20|YES||NO
|[s_p_rtng](#s_p_rtng)|character varying|20|YES||NO
|[call_put_ind](#call_put_ind)|character varying|1|YES||NO
|[repo_nbr](#repo_nbr)|character varying|2|YES||NO
|[ssb_trde_id](#ssb_trde_id)|character varying|11|YES||NO
|[fx_brkr_cd](#fx_brkr_cd)|character varying|5|YES||NO
|[rpt_cls_cd](#rpt_cls_cd)|character varying|2|YES||NO
|[rpt_cls_desc](#rpt_cls_desc)|character varying|255|YES||NO
|[iss_cls_cd](#iss_cls_cd)|character varying|25|YES||NO
|[iss_cls_desc](#iss_cls_desc)|character varying|255|YES||NO
|[fx_closg_ind](#fx_closg_ind)|character varying|1|YES||NO
|[invmt_ind](#invmt_ind)|character varying|2|YES||NO
|[invmt_desc](#invmt_desc)|character varying|50|YES||NO
|[asset_grp_cd](#asset_grp_cd)|character varying|1|YES||NO
|[asset_grp_desc](#asset_grp_desc)|character varying|50|YES||NO
|[invmt_mkt_val_flg](#invmt_mkt_val_flg)|character varying|1|YES||NO
|[mkt_val_cd](#mkt_val_cd)|character varying|1|YES||NO
|[mkt_val_desc](#mkt_val_desc)|character varying|250|YES||NO
|[posn_type_cd](#posn_type_cd)|character varying|1|YES||NO
|[posn_type_desc](#posn_type_desc)|character varying|50|YES||NO
|[varb_rt_freq_cd](#varb_rt_freq_cd)|character varying|2|YES||NO
|[varb_rt_freq_desc](#varb_rt_freq_desc)|character varying|255|YES||NO
|[mkt_pr_src_cd](#mkt_pr_src_cd)|character varying|2|YES||NO
|[mkt_pr_src_vend](#mkt_pr_src_vend)|character varying|255|YES||NO
|[mkt_pr_src_desc](#mkt_pr_src_desc)|character varying|255|YES||NO
|[pool_type_cd](#pool_type_cd)|character varying|2|YES||NO
|[pool_type_desc](#pool_type_desc)|character varying|255|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[confl_asset_grp_cd](#confl_asset_grp_cd)|character varying|25|YES||NO
|[confl_asset_grp_nm](#confl_asset_grp_nm)|character varying|255|YES||NO
|[secr_attr_key](#secr_attr_key)|integer|(32,0)|NO||YES
|[nbr_of_contrct](#nbr_of_contrct)|numeric|(38,15)|YES||NO
|[orig_face_amt](#orig_face_amt)|numeric|(38,15)|YES||NO
|[orig_coupn_rt](#orig_coupn_rt)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[shrt_term_purch_dt](#shrt_term_purch_dt)|timestamp without time zone|6|YES||NO
|[shrt_term_sttl_dt](#shrt_term_sttl_dt)|timestamp without time zone|6|YES||NO
|[orig_iss_dt](#orig_iss_dt)|timestamp without time zone|6|YES||NO
|[mtry_dt](#mtry_dt)|timestamp without time zone|6|YES||NO
|[varb_rt_chg_dt](#varb_rt_chg_dt)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### secr_attr_key
#### Description

Security Attribute Key

#### Value Range

N/A

#### Logic

Sequence generator.

```
Autoincrement +1 for new inserts
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



### ssb_fund_nbr
#### Description

SSB Fund Number

#### Value Range

N/A

#### Logic



```
FUND_ID
```

### lot_acct_nbr
#### Description

Lot Account Number

#### Value Range

N/A

#### Logic


```
LOT_ACCT_NUM
```

### secr_type
#### Description

Security Type

#### Value Range

N/A

#### Logic


```
First derive the CUSIP, CASH, FWD & SPOT IDs :

Then the SECURITY_TYPE would be

    IF(NOT ISNULL(CUSIP_ID) THEN 1
    ELSE
        IF(NOT ISNULL(CASH_ID)) THEN 2
    ELSE
        IF(NOT ISNULL(SPOT_ID)) THEN 3
    ELSE
        IF(NOT ISNULL(FWD_ID)) THEN 4
    ELSE
        NULL
```

### cusip_id
#### Description

CUSIP ID

#### Value Range

N/A

#### Logic

```
CUSIP_ID =
            IF( ISNULL(CASH_ID) AND
                ISNULL(FWD_ID) AND
                ISNULL(SPOT_ID)
            THEN
                ASSET_ID
            ELSE
                NULL)
```

### cash_id
#### Description

Cash ID

#### Value Range

N/A

#### Logic

```
IF(ASSET_GRP_CD='M')
    THEN ASSET_ID
ELSE
    NULL

```

### spot_id
#### Description

SPOT ID

#### Value Range

N/A

#### Logic

```
IF(ASSET_GRP_CD='P'
    THEN ASSET_ID
ELSE
    NULL
```

### fwd_id
#### Description

Forward ID

#### Value Range

N/A

#### Logic

```
IF(ASSET_GRP_CD='W')
    THEN ASSET_ID
ELSE
    NULL
```

### shrt_term_purch_dt
#### Description

Short Term Purchase Date

#### Value Range

N/A

#### Logic

```
TRD_DT
```

### shrt_term_sttl_dt
#### Description

Short Term Settlement Date

#### Value Range

N/A

#### Logic


```
SETTLE_DT
```

### orig_iss_dt
#### Description

Original Issue Date

#### Value Range

N/A

#### Logic



```
ISSUE_DT
```

### mtry_dt
#### Description

Maturity Date

#### Value Range

N/A

#### Logic


```
POS_MTRTY_DT
```

### moody_rtng
#### Description

Moody Rating

#### Value Range

N/A

#### Logic



```
MOODY_RTG
```

### s_p_rtng
#### Description

S&P Rating

#### Value Range

N/A

#### Logic


```
SNP_RTG
```

### call_put_ind
#### Description

Call or Put Indicator

#### Value Range

N/A

#### Logic

```
CALL_PUT_IND
```

### repo_nbr
#### Description

Repo number

#### Value Range

N/A

#### Logic


```
REPO_NUM
```

### ssb_trde_id
#### Description

SSB Trade ID

#### Value Range

N/A

#### Logic


```
SSB_TRADE_ID
```

### fx_brkr_cd
#### Description

Fixed Broker ID

#### Value Range

N/A

#### Logic


```
BRKR_FINS
```

### rpt_cls_cd
#### Description

Report Classification Code

#### Value Range

N/A

#### Logic

```
RPT_CLS_CD
```

### rpt_cls_desc
#### Description

Report Classification Description

#### Value Range

N/A

#### Logic


```
Lookup Report Class Desc from REPORT_CLASS
where RPT_CLS_CD = RPT_CLS_CD
```

### iss_cls_cd
#### Description

Issue class code

#### Value Range

N/A

#### Logic

```
ISSUE_CLS_CD
```

### iss_cls_desc
#### Description

Issue Class description

#### Value Range

N/A

#### Logic

```
Lookup Issue Class Desc from ISSUE_CLASS
where ISS_CLS_CD = ISS_CLS_CD
```

### nbr_of_contrct
#### Description

Number of Contracts

#### Value Range

N/A

#### Logic


```
CONTRACTS_QTY
```



### orig_face_amt
#### Description

Original face amount

#### Value Range

N/A

#### Logic



```
ORG_FACE_POS_QTY
```



### fx_closg_ind
#### Description

FX Closing Indicator

#### Value Range

N/A

#### Logic



```
SW_CLOSING_FX
```



### invmt_ind
#### Description

Investment Indicator

#### Value Range

N/A

#### Logic



```
INVEST_IND
```



### invmt_desc
#### Description

Investement Indicator description

#### Value Range

N/A

#### Logic



```
Match INVEST_IND with INVESTMENT_INDICATOR
```



### asset_grp_cd
#### Description

Asset Group Code

#### Value Range

N/A

#### Logic



```
ASSET_GRP_CD
```



### asset_grp_desc
#### Description

Asset Group Description

#### Value Range

N/A

#### Logic



```
Match ASSET_GRP_CD with  ASSET_GROUP to get Description
```



### invmt_mkt_val_flg
#### Description

Investment Market Value Flag

#### Value Range

N/A

#### Logic


```
Match ASSET_GRP_CD with  ASSET_GROUP to get INVMT_MKT_VAL_FLG
```



### mkt_val_cd
#### Description

Market Value Code

#### Value Range

N/A

#### Logic


```
MKT_VAL_CD
```



### mkt_val_desc
#### Description

Market Value Description

#### Value Range

N/A

#### Logic


```
Match MKT_VAL_CD with MARKET_VALUE to get description
```

### posn_type_cd
#### Description

Position Type Code

#### Value Range

N/A

#### Logic

```
POSN_TYPE_CD
```

### posn_type_desc
#### Description

Postion Type Description

#### Value Range

N/A

#### Logic


```
Match POSN_TYPE_CD with POSITION_TYPE to get description
```



### orig_coupn_rt
#### Description

Original Coupon Rate

#### Value Range

N/A

#### Logic


```
ORIG_CPN_RT
```

### varb_rt_freq_cd
#### Description

Variable Rate Frequency Code

#### Value Range

N/A

#### Logic



```
VAR_RT_FREQ_CD
```



### varb_rt_freq_desc
#### Description

Variable Rate Frequency Description

#### Value Range

N/A

#### Logic



```
Match VAR_RT_FREQ_CD to VARIABLE_RATE_FREQ to get description
```



### varb_rt_chg_dt
#### Description

Variable Rate Change Date

#### Value Range

N/A

#### Logic



```
VAR_RT_CHG_DT
```



### mkt_pr_src_cd
#### Description

Market Price Source Code

#### Value Range

N/A

#### Logic



```
MKTPRC_SRC_CD
```



### mkt_pr_src_vend
#### Description

Vendor Market Price Source

#### Value Range

N/A

#### Logic


```
Match MKTPRC_SRC_CD to MARKET_PRICE_SOURCE to get the Vendor
```



### mkt_pr_src_desc
#### Description

Market Price Source Description

#### Value Range

N/A

#### Logic


```
Match MKTPRC_SRC_CD to MARKET_PRICE_SOURCE to get the description
```



### pool_type_cd
#### Description

Pool Type Code

#### Value Range

N/A

#### Logic


```
POOL_TYPE_CD
```



### pool_type_desc
#### Description

Pool Type Desc

#### Value Range

N/A

#### Logic



```
Match POOL_TYPE_CD with POOL_TYPE to get description
```



### curr_row_flg
#### Description

Current Row Flag

#### Value Range

Y/N

#### Logic



```
Defaults to Y
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

Date the row marked not-current

#### Value Range


#### Logic



```
sysdate
```



### etl_load_cyc_key
#### Description

unused


### src_sys_id
#### Description

Unused


### confl_asset_grp_cd
#### Description

Confluence Asset Group Code

#### Value Range

N/A

#### Logic



```
Match ASSET_GRP_CD with ASSET_GROUP get the corresponding code
```

### confl_asset_grp_nm
#### Description

Confluence Asset Group Name

#### Value Range

N/A

#### Logic


```
Match ASSET_GRP_CD with ASSET_GROUP get the corresponding name
```

## Files Used
1. PTA_SSB_POSITION.mmddyy.txt


File Type -- Pipe Delimited
File Location --


## Business Key
ASSET_ID, SSB_FUND_NBR, LOT_ACCT_NBR, POSN_TYPE_CD


## Logic
Select only DISTINCT key values WHERE FUND_ID != 'GB35'

## Joins
rpt_cls_desc    - PTA_SSB_POSITION.RPT_CLS_CD = report_class.rpt_cls_cd

iss_cls_desc    - PTA_SSB_POSITION.ISS_CLS_CD = issue_class.iss_cls_cd

invmt_desc      - PTA_SSB_POSITION.INVEST_IND = investment_indicator.invmt_ind_cd

asset_grp_desc  - PTA_SSB_POSITION.ASSET_GRP_CD = asset_group.asset_grp_cd

invmt_mkt_val_flg - PTA_SSB_POSITION.ASSET_GRP_CD = asset_group.asset_grp_cd

mkt_val_desc    - PTA_SSB_POSITION.MKT_VAL_CD = market_value.mkt_val_cd

posn_type_desc  - PTA_SSB_POSTION.POSN_TYPE_CD = position_type.posn_type_cd

varb_rt_freq_desc PTA_SSB_POSITION.VAR_RT_FREQ_CD=variable_rate_freq.varb_rt_freq_cd

mkt_pr_src_vend - PTA_SSB_POSITION.MKTPRC_SRC_CD=market_price_source.mkt_pr_src_cd

mkt_pr_src_desc - PTA_SSB_POSITION.MKTPRC_SRC_CD=market_price_source.mkt_pr_src_cd

pool_type_desc  - PTA_SSB_POSITION.POOL_TYPE_CD=pool_type.pool_type_cd

confl_asset_grp_cd PTA_SSB_POSITION.asset_grp_cd=asset_group.asset_grp_cd

confl_asset_grp_nm PTA_SSB_POSITION.asset_grp_cd=asset_group.asset_grp_cd