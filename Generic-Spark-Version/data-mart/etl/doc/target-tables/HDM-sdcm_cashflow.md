# SDCM_CASHFLOW

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[cash_cntl_recon_flg](#cash_cntl_recon_flg)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[sdcm_cashflow_key](#sdcm_cashflow_key)|integer|(32,0)|NO||YES
|[reg_key](#reg_key)|integer|(32,0)|YES||NO
|[pmt_mthd_key](#pmt_mthd_key)|integer|(32,0)|YES||NO
|[dlr_key](#dlr_key)|integer|(32,0)|YES||NO
|[shrhldr_key](#shrhldr_key)|integer|(32,0)|YES||NO
|[acct_key](#acct_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[spr_sheet_dt_key](#spr_sheet_dt_key)|integer|(32,0)|YES||NO
|[trde_dt_key](#trde_dt_key)|integer|(32,0)|YES||NO
|[confirm_dt_key](#confirm_dt_key)|integer|(32,0)|YES||NO
|[shrhldr_role_key](#shrhldr_role_key)|integer|(32,0)|NO||NO
|[acct_nbr](#acct_nbr)|numeric|(38,15)|YES||NO
|[sub_shrs](#sub_shrs)|numeric|(38,15)|YES||NO
|[sub_amt](#sub_amt)|numeric|(38,15)|YES||NO
|[redmpn_shrs](#redmpn_shrs)|numeric|(38,15)|YES||NO
|[redmpn_amt](#redmpn_amt)|numeric|(38,15)|YES||NO
|[ofrg_pr](#ofrg_pr)|numeric|(38,15)|YES||NO
|[nav](#nav)|numeric|(38,15)|YES||NO
|[dlr_commsn_amt](#dlr_commsn_amt)|numeric|(38,15)|YES||NO
|[undr_wrtr_commsn_amt](#undr_wrtr_commsn_amt)|numeric|(38,15)|YES||NO
|[adv_commsn_amt](#adv_commsn_amt)|numeric|(38,15)|YES||NO
|[net_shrs](#net_shrs)|numeric|(38,15)|YES||NO
|[net_cashflow_amt](#net_cashflow_amt)|numeric|(38,15)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|NO||NO
|[hbr_dlr_key](#hbr_dlr_key)|integer|(32,0)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### sdcm_cashflow_key
#### Description



#### Value Range

N/A

#### Logic


Auto Increment Sequence Number



### reg_key
#### Description



#### Value Range

N/A

#### Logic

REGIONREG_KEY


Join on REGION Using below output


DST_ST_COUNTRY.ST_CD = REGION.ST_CD


Join on DST_ST_COUNTRY Table

Get DST_ST_COUNTRY.ST_CD 

DST_ST_COUNTRY.DST_ST_CD = CASHFLOW.RESIDENT-STATE-COUNTRY




### pmt_mthd_key
#### Description



#### Value Range

N/A

#### Logic

PAYMENT_METHOD.PMT_MTHD_KEY

Join on PAYMENT_METHOD Table

CASHFLOW.PAYMENT-METHOD-CDE = PAYMENT_METHOD.PMT_MTHD_CD



### dlr_key
#### Description



#### Value Range

N/A

#### Logic


DEALER.DLR_KEY

Join on DEALER Table

CASHFLOW.FINANCIAL-INST-ID = DEALER.DLR_ID


### shrhldr_key
#### Description



#### Value Range

N/A

#### Logic


NULL



### acct_key
#### Description



#### Value Range

N/A

#### Logic

ACCOUNT.ACCT_KEY

Join with ACCOUNT Table

 IF CASHFLOW.LOAD-NOLOAD-CODE = '1'
 THEN
 CASHFLOW.NOLOAD_ACCT_NBR = ACCOUNT.ACCT_NBR
 ELSE
 CASHFLOW.LOAD-ACCOUNT-NUMBER = ACCOUNT.ACCT_NBR


### fund_key
#### Description



#### Value Range

N/A

#### Logic


FUND.FUND_KEY


Join with FUND Table

 IF CASHFLOW.LOAD-NOLOAD-CODE = '1'
 THEN
 CASHFLOW.FUND-CODE = FUND.FUND_NBR
 ELSE
 CASHFLOW.FUND-CODE = FUND.FUND_NBR



### spr_sheet_dt_key
#### Description



#### Value Range

N/A

#### Logic





### trde_dt_key
#### Description



#### Value Range

N/A

#### Logic

CALENDAR.CAL_DAY_KEY

Join on CALENDAR Table

CASHFLOW.TRADE-DATE = CALENDAR.CAL_DAY



### confirm_dt_key
#### Description



#### Value Range

N/A

#### Logic


CALENDAR.CAL_DAY_KEY

Join on CALENDAR Table

CASHFLOW.CONFIRM-DT = CALENDAR.CAL_DAY





### shrhldr_role_key
#### Description



#### Value Range

N/A

#### Logic


NULL


### acct_nbr
#### Description



#### Value Range

N/A

#### Logic


CASHFLOW.LOAD-ACCOUNT-NUMBER                            



### sub_shrs
#### Description



#### Value Range

N/A

#### Logic



IIF (CASHFLOW.TRANSACTION-TYPE-CD=0, CASHFLOW.SHARES, 0	)




### sub_amt
#### Description



#### Value Range

N/A

#### Logic


IIF(CASHFLOW.TRANSACTION-TYPE-CD=0,CASHFLOW.GROSS-AMOUNT,0)



### redmpn_shrs
#### Description



#### Value Range

N/A

#### Logic



IIF(CASHFLOW.TRANSACTION-TYPE-CD=1,CASHFLOW.SHARES,0)


### redmpn_amt
#### Description



#### Value Range

N/A

#### Logic


IIF(CASHFLOW.TRANSACTION-TYPE-CD=1,CASHFLOW.GROSS-AMOUNT,0)



### ofrg_pr
#### Description



#### Value Range

N/A

#### Logic


CASHFLOW.OFFERING-PRICE


### nav
#### Description



#### Value Range

N/A

#### Logic


CASHFLOW.NET-ASSET-VALUE-AMT


### dlr_commsn_amt
#### Description



#### Value Range

N/A

#### Logic


CASHFLOW.DEALER-COMMISSION


### undr_wrtr_commsn_amt
#### Description



#### Value Range

N/A

#### Logic


CASHFLOW.UNDERWRITER-COMMISSION



### adv_commsn_amt
#### Description



#### Value Range

N/A

#### Logic


CASHFLOW.FIN-TR-ADVANCED-COMMISSION-AT



### net_shrs
#### Description



#### Value Range

N/A

#### Logic

SUM(SUB_SHARES) -SUM(ABS(REDMPN_SHRS))


### net_cashflow_amt
#### Description



#### Value Range

N/A

#### Logic


SUM(SUB_AMT) +SUM(REDMPN_AMT)-SUM(DLR_COMMSN_AMT+UNDR_WRTR_COMMSN_AMT+ADV_COMMSN_AMT)


### cash_cntl_recon_flg
#### Description



#### Value Range

N/A

#### Logic



CASHFLOW.CASH-CONTROL-RECONCILEMENT-CD


### row_strt_dttm
#### Description



#### Value Range

N/A

#### Logic




### row_stop_dttm
#### Description



#### Value Range

N/A

#### Logic




### curr_row_flg
#### Description



#### Value Range

N/A

#### Logic




### src_sys_id
#### Description



#### Value Range

N/A

#### Logic




### etl_load_cyc_key
#### Description



#### Value Range

N/A

#### Logic




### hbr_dlr_key
#### Description



#### Value Range

N/A

#### Logic



DEALER.DLR_KEY

Join on DEALER Table

CASHFLOW.FINANCIAL-INST-ID = DEALER.DLR_ID



### Files Used

1. CASHFLOW (DTO.HCA.SDCM.POSITION.YYYYMMDD HH24:MI:SS)



## File Location

dstprod/DTO.HCA.SDCM.POSITION.YYYYMMDD HH24:MI:SS.zip.pgp.txt


### Tables Used

2. DST_ST_COUNTRY
3. REGION
4. PAYMENT_METHOD
5. CALENDAR
6. ACCOUNT
7. DEALER

### Current SQL used to get roes from ADW.SDCM_CASHFLOW table
SELECT TRUNC (A.SPR_SHEET_DT) AS SPR_SHEET_DT,
       TRUNC (A.TRDE_DT) AS TRDE_DT,
       TRUNC (A.CONFIRM_DT) AS CONFIRM_DT,
       A.SHRS,
       A.OFRG_PR,
       A.GR_AMT,
       A.DLR_COMMSN_AMT,
       A.UNDR_WRTR_COMMSN_AMT,
       A.NAV_AMT,
       A.TXN_TYPE_CD,
       A.CASH_CNTL_RECON_FLG,
       A.ADV_COMMSN_AMT,
       B.FUND_NBR,
       C.ACCT_NBR,
       D.DLR_ID,
       F.SHRHLDR_ROLE_KEY,
       F.CUST_ID,
       F.CUST_ID_2,
       NVL (G.ST_CD, 'XX') AS ST_CD,
       NVL (I.REG_KEY, -1) AS REG_KEY,
       NVL (J.CRTY_ISO_CD, 'XX') AS CRTY_ISO_CD,
       NVL (K.PMT_MTHD_CD, '0') AS PMT_MTHD_CD
  FROM (SELECT SDC.*
          FROM ADW.SDCM_CASHFLOW SDC,
               ADW.AS_OF_REASON_CODES ARC,
               HDM.CALENDAR C
         WHERE     SDC.RSN_KEY = ARC.RSN_KEY
               AND SDC.SPR_SHEET_DT = C.CAL_DAY
               AND C.CAL_DAY IN (SELECT DISTINCT SPR_SHEET_DT
                                   FROM ADW.SDCM_CASHFLOW
                                  WHERE ROW_STRT_DTTM > TRUNC (SYSDATE))
               AND SDC.DLR_ROLE_KEY IS NOT NULL
               AND (SDC.TRDE_DT = SDC.SPR_SHEET_DT OR (ARC.ASOF_RSN_CD <> 0))) A,
       ADW.SHARE_CLASS_FUND B,
       (SELECT *
          FROM ADW.CUSTOMER_ACCOUNT
         WHERE curr_row_flg = 'Y') C,
       ADW.DEALER D,
       (  SELECT A.CUST_ACCT_KEY CUST_ACCT_KEY,
                 MAX (A.SHRHLDR_ROLE_KEY) AS SHRHLDR_ROLE_KEY
            FROM ADW.ACCOUNT_SHAREHOLDER A,
                 ADW.SHAREHOLDER B,
                 ADW.PARTY_ROLE C,
                 ADW.PARTY D
           WHERE     A.SHRHLDR_ROLE_KEY = B.SHRHLDR_ROLE_KEY
                 AND B.SHRHLDR_ROLE_KEY = C.PARTY_ROLE_KEY
                 AND C.PARTY_KEY = D.PARTY_KEY
                 AND D.PARTY_FRMTD_NM <> 'DO NOT USE'
                 AND A.CURR_ROW_FLG = 'Y'
                 AND B.CURR_ROW_FLG = 'Y'
                 AND C.CURR_ROW_FLG = 'Y'
                 AND D.CURR_ROW_FLG = 'Y'
                 AND A.TXBL_PARTY = 'Y'
        GROUP BY A.CUST_ACCT_KEY
        UNION
        SELECT A.CUST_ACCT_KEY CUST_ACCT_KEY,
               A.SHRHLDR_ROLE_KEY AS SHRHLDR_ROLE_KEY
          FROM ADW.ACCOUNT_SHAREHOLDER A,
               ADW.SHAREHOLDER B,
               ADW.PARTY_ROLE C,
               ADW.PARTY D
         WHERE     A.SHRHLDR_ROLE_KEY = B.SHRHLDR_ROLE_KEY
               AND B.SHRHLDR_ROLE_KEY = C.PARTY_ROLE_KEY
               AND C.PARTY_KEY = D.PARTY_KEY
               AND D.PARTY_FRMTD_NM = 'DO NOT USE'
               AND A.CURR_ROW_FLG = 'Y'
               AND B.CURR_ROW_FLG = 'Y'
               AND C.CURR_ROW_FLG = 'Y'
               AND D.CURR_ROW_FLG = 'Y'
               AND A.TXBL_PARTY = 'Y'
               AND A.CUST_ACCT_KEY IN (SELECT DISTINCT CUST_ACCT_KEY
                                         FROM (  SELECT A.CUST_ACCT_KEY
                                                           CUST_ACCT_KEY,
                                                        COUNT (1)
                                                   FROM ADW.ACCOUNT_SHAREHOLDER A
                                                  WHERE A.CURR_ROW_FLG = 'Y'
                                                        AND A.TXBL_PARTY = 'Y'
                                               GROUP BY A.CUST_ACCT_KEY
                                                 HAVING COUNT (1) = 1))) E,
       (SELECT *
          FROM ADW.SHAREHOLDER
         WHERE CURR_ROW_FLG = 'Y') F,
       (SELECT *
          FROM ADW.STATE
         WHERE CURR_ROW_FLG = 'Y') G,
       (SELECT *
          FROM ADW.STATE_REGION
         WHERE CURR_ROW_FLG = 'Y') H,
       (SELECT *
          FROM ADW.REGION
         WHERE CURR_ROW_FLG = 'Y') I,
       (SELECT *
          FROM ADW.COUNTRY
         WHERE CURR_ROW_FLG = 'Y') J,
       ADW.PAYMENT_METHOD K,
       (SELECT *
          FROM ADW.FUND_TRANSACTION_CODE
         WHERE CURR_ROW_FLG = 'Y') L,
       (SELECT * FROM ADW.AS_OF_REASON_CODES) M
 WHERE     A.SHR_CLS_FUND_KEY = B.SHR_CLS_FUND_KEY
       AND A.CUST_ACCT_KEY = C.CUST_ACCT_KEY(+)
       AND A.DLR_ROLE_KEY = D.DLR_ROLE_KEY(+)
       AND C.CUST_ACCT_KEY = E.CUST_ACCT_KEY(+)
       AND E.SHRHLDR_ROLE_KEY = F.SHRHLDR_ROLE_KEY(+)
       AND A.ST_KEY = G.ST_KEY(+)
       AND G.ST_KEY = H.ST_KEY(+)
       AND H.REG_KEY = I.REG_KEY(+)
       AND A.CRTY_KEY = J.CRTY_KEY(+)
       AND A.PMT_MTHD_KEY = K.PMT_MTHD_KEY(+)
       AND NVL (A.FUND_TXN_CD_KEY, 0) = L.FUND_TXN_CD_KEY(+)
       AND A.RSN_KEY = M.RSN_KEY(+)
       AND B.CURR_ROW_FLG = 'Y'
       AND D.CURR_ROW_FLG = 'Y'
       AND A.TXN_TYPE_CD IN (0, 1)
       AND NOT (NVL (L.FUND_TXN_CD, 0) = 900
                AND NVL (L.TXN_SFX, 0) IN
                       (11,
                        16,
                        17,
                        21,
                        29,
                        30,
                        31,
                        32,
                        33,
                        34,
                        35,
                        36,
                        37,
                        38,
                        161,
                        171))

### Business Keys

