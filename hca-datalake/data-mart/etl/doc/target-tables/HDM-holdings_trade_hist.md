# HOLDINGS_TRADE_HIST

This table holds the lorem ipsum dolor sit amet.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[ssb_trde_id](#ssb_trde_id)|character varying|11|YES||NO
|[lot_acct_nbr](#lot_acct_nbr)|character varying|6|YES||NO
|[lot_nbr](#lot_nbr)|character varying|5|YES||NO
|[sttl_locn_cd](#sttl_locn_cd)|character varying|3|YES||NO
|[sttl_dlr_id](#sttl_dlr_id)|character varying|5|YES||NO
|[stk_xchg_city_cd](#stk_xchg_city_cd)|character varying|2|YES||NO
|[shrt_term_secr_type](#shrt_term_secr_type)|character varying|4|YES||NO
|[exectg_dlr_id](#exectg_dlr_id)|character varying|5|YES||NO
|[brkr_rsn_cd](#brkr_rsn_cd)|character varying|1|YES||NO
|[cncl_flg](#cncl_flg)|character varying|1|YES||NO
|[trde_flg](#trde_flg)|character varying|1|YES||NO
|[rstc_flg](#rstc_flg)|character varying|1|YES||NO
|[ssb_trade_cncl_id](#ssb_trade_cncl_id)|character varying|25|YES||NO
|[post_type_cd](#post_type_cd)|character varying|25|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[holdg_trde_hist_key](#holdg_trde_hist_key)|integer|(32,0)|NO||YES
|[fund_compst_key](#fund_compst_key)|integer|(32,0)|YES||NO
|[base_currcy_key](#base_currcy_key)|integer|(32,0)|YES||NO
|[secr_key](#secr_key)|integer|(32,0)|YES||NO
|[trde_day_key](#trde_day_key)|integer|(32,0)|YES||NO
|[holdg_txn_cd_key](#holdg_txn_cd_key)|integer|(32,0)|YES||NO
|[cash_rsn__key](#cash_rsn__key)|integer|(32,0)|YES||NO
|[cost_type_key](#cost_type_key)|integer|(32,0)|YES||NO
|[loc_currcy_key](#loc_currcy_key)|integer|(32,0)|YES||NO
|[sttl_currcy_key](#sttl_currcy_key)|integer|(32,0)|YES||NO
|[trde_crty_key](#trde_crty_key)|integer|(32,0)|YES||NO
|[xchg_type_key](#xchg_type_key)|integer|(32,0)|YES||NO
|[indtry_key](#indtry_key)|integer|(32,0)|YES||NO
|[post_day_key](#post_day_key)|integer|(32,0)|YES||NO
|[sttl_day_key](#sttl_day_key)|integer|(32,0)|YES||NO
|[inrt](#inrt)|numeric|(38,15)|YES||NO
|[shr_qty](#shr_qty)|numeric|(38,15)|YES||NO
|[tot_amort_sold](#tot_amort_sold)|numeric|(38,15)|YES||NO
|[orig_face_mbs_qty](#orig_face_mbs_qty)|numeric|(38,15)|YES||NO
|[base_intr_purch_sold](#base_intr_purch_sold)|numeric|(38,15)|YES||NO
|[base_nra_tax_whg](#base_nra_tax_whg)|numeric|(38,15)|YES||NO
|[base_nra_tax_refnd](#base_nra_tax_refnd)|numeric|(38,15)|YES||NO
|[opt_contrct_ut](#opt_contrct_ut)|numeric|(38,15)|YES||NO
|[strk_pr](#strk_pr)|numeric|(38,15)|YES||NO
|[base_net_amt](#base_net_amt)|numeric|(38,15)|YES||NO
|[base_ut_pr](#base_ut_pr)|numeric|(38,15)|YES||NO
|[gr_intr_btsld](#gr_intr_btsld)|numeric|(38,15)|YES||NO
|[base_tax](#base_tax)|numeric|(38,15)|YES||NO
|[base_commsn](#base_commsn)|numeric|(38,15)|YES||NO
|[base_fees](#base_fees)|numeric|(38,15)|YES||NO
|[base_othr_fees](#base_othr_fees)|numeric|(38,15)|YES||NO
|[base_id_cost](#base_id_cost)|numeric|(38,15)|YES||NO
|[base_curr_id_cost](#base_curr_id_cost)|numeric|(38,15)|YES||NO
|[base_amort_bkd](#base_amort_bkd)|numeric|(38,15)|YES||NO
|[xchgrt](#xchgrt)|numeric|(38,15)|YES||NO
|[sttl_dt_xchgrt](#sttl_dt_xchgrt)|numeric|(38,15)|YES||NO
|[base_avg_gls](#base_avg_gls)|numeric|(38,15)|YES||NO
|[base_lt_gls](#base_lt_gls)|numeric|(38,15)|YES||NO
|[base_shtm_gls](#base_shtm_gls)|numeric|(38,15)|YES||NO
|[base_avg_currcy_gls](#base_avg_currcy_gls)|numeric|(38,15)|YES||NO
|[base_tax_expns_gls](#base_tax_expns_gls)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[acct_end_dt](#acct_end_dt)|timestamp without time zone|6|YES||NO
|[contrctl_sttl_dt](#contrctl_sttl_dt)|timestamp without time zone|6|YES||NO
|[mtry_dt](#mtry_dt)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### holdg_trde_hist_key
#### Description

Primary Key 

#### Value Range

N/A

#### Logic


```
Autoincrement +1 for new inserts
```



### fund_compst_key
#### Description

Fund Composite Key

#### Logic
.
```
file.FUND_ID = fc.st_str_fund_nbr
```



### base_currcy_key
#### Description

Base Currency Key

#### Logic

```
file.CRNCY_CD_BASE = cur.CURRCY_C.
```
### secr_key
#### Description

Security Key

#### Logic

```
file.ASSET_ID = hs.asset_i.
```

### trde_day_key
#### Description

Trade Date Key

#### Logic

```
file.TRD_DT = cal.cal_day
```

### holdg_txn_cd_key
#### Description

Holdings Transaction Code Key

#### Logic

```
file.TXN_TYPE_CD = htc.txn_type_cd and 
file.TXN_CD = htc.txn_cd
```

### cash_rsn__key
#### Description

Cash Reason Key

#### Logic

```
file.csh_rsn_cd = hcr.cash_rsn_cd
```

### cost_type_key
#### Description

Cost Type Key

#### Logic

```
file.CSTNG_IND = hct.cost_type_cd
```

### loc_currcy_key
#### Description

Location Currency Key

#### Logic

```
file.CRNCY_CD_LOC = cur.CURRCY_CD
```

### sttl_currcy_key
#### Description

Settlement Currency Key

```
file.SETTLE_CRNCY_CD = cur.CURRCY_CD
```

### trde_crty_key
#### Description

Trade Country_key

#### Logic

```
file.TRD_CNTRY_CD = ctry.CRTY_ISO_CD
```

### xchg_type_key
#### Description

Exchange Type Key

#### Logic

```
file.FX_TYUPE_CD = ext.xchg_type_cd
```

### indtry_key
#### Description

Industry Key

#### Logic

```
match file.MJR_INDSTY_CD(Sector) and file.MNR_INDSTY_CD (Industry) to
Dimesion Industry to get the key

```

### post_day_key
#### Description

Post Day Key

#### Logic

```
file.POST_DT = cal.cal_day
```

### sttl_day_key
#### Description

Settlement Day Key

#### Logic

```
file.ACCT_STTL_DT = cal.cal_day
```

### ssb_trde_id
#### Description

SSB Trade ID

#### Logic

```
file.SSB_TRD_ID_NUM
```

### lot_acct_nbr
#### Description

Lot Account Number

#### Logic

```
file.LOT_ACCT_NUM
```

### lot_nbr
#### Description

Lot Number

#### Logic

```
file.LOT_NUM
```

### acct_end_dt
#### Description

Account End Date

#### Logic

```
file.ACCT_END_DT
```

### contrctl_sttl_dt
#### Description

Control Settlement Date

#### Logic

```
file.CNTRCTL_SETTLE_DT
```

### sttl_locn_cd
#### Description

Settlement Location Code

#### Logic

```
file.SETTLE_LOC_CD
```

### sttl_dlr_id
#### Description

Settlement Dealer ID

#### Logic

```
STLBRKR_FINS_NUM
```

### stk_xchg_city_cd
#### Description

Stock Exchange City Code

#### Logic

```
file.XCH_CITY
```

### shrt_term_secr_type
#### Description

Short Term Security Type

#### Logic

```
file.TXN_SEC_TYPE_EXPND_CD
```

### exectg_dlr_id
#### Description

Expecting Dealer ID

#### Logic

```
file.BRKR_FINS_NUM
```

### brkr_rsn_cd
#### Description

Broker Reason Code

#### Logic

```
file.BRKR_RSNCD
```

### inrt
#### Description

Interest Rate

#### Logic

```
file.INT_RT
```

### mtry_dt
#### Description

Maturity Date

#### Logic

```
file.MAT_DT
```

### cncl_flg
#### Description

Cancel Flag

#### Logic

```
file.CNCL_IND
```

### trde_flg
#### Description

Trade Flag

#### Logic

```
file.PRINCIPAL_AGNT_CD
```

### rstc_flg
#### Description

Restrict Lot Indicator

#### Logic

```
RSTRCT_LOT_IND
```

### ssb_trade_cncl_id
#### Description

SSB Trade Cancel ID

#### Logic

```
file.SSB_TRD_ID_NUM_CNC
```

### post_type_cd
#### Description

Post Type Code

#### Logic

```
file.POST_TYPE_CD
```

### shr_qty
#### Description

Share Quantity

#### Logic

```
file.SHRPAR_QTY. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### tot_amort_sold
#### Description

Total Amortization Sold

#### Logic

```
file.AMTZD_SOLD_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### orig_face_mbs_qty
#### Description

Original Face Position Quantity

#### Logic

```
file.ORG_FACE_POS_QTY. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_intr_purch_sold
#### Description

Base Interest Purchase Sold

#### Logic

```
file.INT_PURCH_SOLD_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_nra_tax_whg
#### Description

Base NRA TAX Withholding

#### Logic

```
file.NRATAX_WTHLD_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_nra_tax_refnd
#### Description

Base NRA Tax Refund

#### Logic

```
file.NRATAX_RFND_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### opt_contrct_ut
#### Description

Optional Contract Units

#### Logic

```
file.OPT_UNITS_QTY. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### strk_pr
#### Description

Strike Price

#### Logic

```
file.STRIKE_PRC
```

### base_net_amt
#### Description

Base Net Amount

#### Logic

```
file.NET_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_ut_pr
#### Description

Base Unit Price

#### Logic

```
file.UNTPRC_BAM. 
```

### gr_intr_btsld
#### Description

Gross Interest Bought/Sold

#### Logic

```
file.INT_BGHT_SOLD_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_tax
#### Description

Base Tax

#### Logic

```
file.TAXES_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_commsn
#### Description

Base Commission

#### Logic

```
file.COMSN_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_fees
#### Description

Base Fees

#### Logic

```
file.FEES_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_othr_fees
#### Description

Base Other Fees

#### Logic

```
file.FEES_OTHR_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_id_cost
#### Description

Base ID Cost/AVG Cost

#### Logic

```
file.ORG_IDCST_AVGCST_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_curr_id_cost
#### Description

Base Current ID Cost

#### Logic

```
file.IDCST_AVGCST_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_amort_bkd
#### Description

Base Amortization Booked

#### Logic

```
file.AMORT_SINCEBUY_BTL. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### xchgrt
#### Description

Exchange Rate

#### Logic

```
file.TRD_DT_XCHRT
```

### sttl_dt_xchgrt
#### Description

Settlement Date Exchange Rate

#### Logic

```
file.SETTLE_DT_XCHRT
```

### base_avg_gls
#### Description

Base Average Short Term Gain Loss

#### Logic

```
file.AVGCST_GNLS_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_lt_gls
#### Description

Base Security Longterm Gain Loss

#### Logic

```
file.SECGL_LT_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_shtm_gls
#### Description

Base Short Term Gain Loss

#### Logic

```
file.SECGL_ST_BAM. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_avg_currcy_gls
#### Description

Base Average Shortterm Gain Loss Currency

#### Logic

```
file.AVGCST_CRNCYGL_AMT. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### base_tax_expns_gls
#### Description

Base Tax Expenses Gain Loss

#### Logic

```
file.TAXEXP_GNLS_AMT. If file.SSB_TRD_ID_NUM_CNCL NOT NULL then multiply by -1
```

### curr_row_flg
#### Description

Current Row Flag

#### Value Range

```
'Y'
```

### row_strt_dttm
#### Description

Row Start Datetime

#### Logic

```
row load datetime
```

-------------------------------------
File dependencies- 

SSB - HARBORCP_TXN102_<mmddyy>.txt. Use only rows which of file.TYPE <> 'TT'

Frequency - Daily (HarborBusiness Days)

Dimensions

fund_composite (fc)
currency (cur)
security (hs) 
calendar (cal)
holdings_txn_cd (htc)
holdings_cash_reason_cd (hcr)
holdings_cost_type (hct)
country (ctry)
exchange_type (ext)

Business Keys 
				(
				FUND_COMPST_KEY
				STTL_CURRCY_KEY (Can be NULL)
				TRDE_CRTY_KEY
				XCHG_TYPE_KEY (Can be NULL)
				INDTRY_KEY
				POST_DAY_KEY
				STTL_DAY_KEY
				BASE_CURRCY_KEY
				SECR_KEY
				TRDE_DAY_KEY
				HOLDG_TXN_CD_KEY
				CASH_RSN__KEY
				COST_TYPE_KEY
				LOC_CURRCY_KEY
				HOLDG_TRDE_HIST_KEY
				)