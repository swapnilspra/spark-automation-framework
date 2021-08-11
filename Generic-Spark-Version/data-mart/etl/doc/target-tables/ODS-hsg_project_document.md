# HSG_PROJECT_DOCUMENT

This table holds the lorem ipsum dolor sit amet.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[proj_titl](#proj_titl)|character varying|255|YES||NO
|[proj_mgr](#proj_mgr)|character varying|255|YES||NO
|[proj_nm](#proj_nm)|character varying|255|YES||NO
|[doc_type](#doc_type)|character varying|255|YES||NO
|[agnd_item_flg](#agnd_item_flg)|character varying|1|YES||NO
|[objv](#objv)|character varying|4000|YES||NO
|[chg_rqst](#chg_rqst)|character varying|4000|YES||NO
|[proj_dlvbl](#proj_dlvbl)|character varying|4000|YES||NO
|[ms](#ms)|character varying|4000|YES||NO
|[rsrc_respy](#rsrc_respy)|character varying|4000|YES||NO
|[rsks](#rsks)|character varying|4000|YES||NO
|[proj_docn_apprvr](#proj_docn_apprvr)|character varying|60|YES||NO
|[proj_docn_prev_subm](#proj_docn_prev_subm)|character varying|255|YES||NO
|[proj_isss](#proj_isss)|character varying|4000|YES||NO
|[bus_chg_cntl](#bus_chg_cntl)|character varying|4000|YES||NO
|[bus_drvr](#bus_drvr)|character varying|255|YES||NO
|[prog_val](#prog_val)|character varying|255|YES||NO
|[proj_foldr_locn](#proj_foldr_locn)|character varying|4000|YES||NO
|[proj_stat](#proj_stat)|character varying|25|YES||NO
|[cmmts](#cmmts)|character varying|4000|YES||NO
|[proj_freq](#proj_freq)|character varying|60|YES||NO
|[rsrc_mgr](#rsrc_mgr)|character varying|60|YES||NO
|[lsn_lrnd](#lsn_lrnd)|character varying|4000|YES||NO
|[life_cyc_type](#life_cyc_type)|character varying|255|YES||NO
|[proj_mtg_freq](#proj_mtg_freq)|character varying|60|YES||NO
|[closout_mtg_flg](#closout_mtg_flg)|character varying|1|YES||NO
|[proj_doc_archv_flg](#proj_doc_archv_flg)|character varying|1|YES||NO
|[stat_mtg_flg](#stat_mtg_flg)|character varying|1|YES||NO
|[yr_qtr](#yr_qtr)|character varying|25|YES||NO
|[chg_rqst_1_rsn](#chg_rqst_1_rsn)|character varying|255|YES||NO
|[chg_rqst_2_rsn](#chg_rqst_2_rsn)|character varying|255|YES||NO
|[chg_rqst_3_rsn](#chg_rqst_3_rsn)|character varying|255|YES||NO
|[chg_rqst_4_rsn](#chg_rqst_4_rsn)|character varying|255|YES||NO
|[chg_rqst_5_rsn](#chg_rqst_5_rsn)|character varying|255|YES||NO
|[chg_rqst_6_rsn](#chg_rqst_6_rsn)|character varying|255|YES||NO
|[chg_rqst_7_rsn](#chg_rqst_7_rsn)|character varying|255|YES||NO
|[chg_rqst_8_rsn](#chg_rqst_8_rsn)|character varying|255|YES||NO
|[chg_rqst_9_rsn](#chg_rqst_9_rsn)|character varying|255|YES||NO
|[cost_chg_desc](#cost_chg_desc)|character varying|4000|YES||NO
|[cost_impct_desc](#cost_impct_desc)|character varying|4000|YES||NO
|[cost_impct_orig_est](#cost_impct_orig_est)|character varying|4000|YES||NO
|[creatr](#creatr)|character varying|60|YES||NO
|[modfr](#modfr)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[hsg_proj_doc_key](#hsg_proj_doc_key)|numeric|(10,0)|NO||YES
|[etl_load_cyc_key](#etl_load_cyc_key)|numeric|(10,0)|YES||NO
|[pres_dt](#pres_dt)|timestamp without time zone|0|YES||NO
|[proj_scp_apprvl_dt](#proj_scp_apprvl_dt)|timestamp without time zone|0|YES||NO
|[trgt_cpltn_dt](#trgt_cpltn_dt)|timestamp without time zone|0|YES||NO
|[proj_chg_apprvl_dt](#proj_chg_apprvl_dt)|timestamp without time zone|0|YES||NO
|[closout_apprvl_dt](#closout_apprvl_dt)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_cpltn_dt_1](#rvsd_trgt_cpltn_dt_1)|timestamp without time zone|0|YES||NO
|[actl_cpltn_dt](#actl_cpltn_dt)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_cpltn_dt_2](#rvsd_trgt_cpltn_dt_2)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_cpltn_dt_3](#rvsd_trgt_cpltn_dt_3)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_cpltn_dt_4](#rvsd_trgt_cpltn_dt_4)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_cpltn_dt_5](#rvsd_trgt_cpltn_dt_5)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_cpltn_dt_6](#rvsd_trgt_cpltn_dt_6)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_cpltn_dt_7](#rvsd_trgt_cpltn_dt_7)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_cpltn_dt_8](#rvsd_trgt_cpltn_dt_8)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_cpltn_dt_9](#rvsd_trgt_cpltn_dt_9)|timestamp without time zone|0|YES||NO
|[creatn_dt](#creatn_dt)|timestamp without time zone|0|YES||NO
|[chg_dt](#chg_dt)|timestamp without time zone|0|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|0|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|0|YES||NO
### hsg_proj_doc_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_doc_id
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_titl
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_mgr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### doc_type
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### agnd_item_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### objv
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_dlvbl
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### ms
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rsrc_respy
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rsks
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### est_hrs
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### actl_hrs
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_docn_apprvr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_docn_prev_subm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_isss
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### bus_chg_cntl
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### bus_drvr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prog_val
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### pres_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_scp_apprvl_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### trgt_cpltn_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_foldr_locn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_stat
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cmmts
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_freq
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rsrc_mgr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### lsn_lrnd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_chg_apprvl_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### closout_apprvl_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### life_cyc_type
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### initn_hrs_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### plng_hrs_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### exctn_hrs_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mntrg_hrs_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### closg_hrs_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### tot_hrs_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_mtg_freq
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### closout_mtg_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### proj_doc_archv_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### stat_mtg_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_1_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_2_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_3_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_4_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_5_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_6_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_7_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_8_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_9_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_tot_hrs_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_trgt_cpltn_dt_1
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### actl_cpltn_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### yr_qtr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_1_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_2_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_3_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_4_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_5_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_6_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_7_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_8_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_rqst_9_rsn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_trgt_cpltn_dt_2
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_trgt_cpltn_dt_3
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_trgt_cpltn_dt_4
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_trgt_cpltn_dt_5
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_trgt_cpltn_dt_6
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_trgt_cpltn_dt_7
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_trgt_cpltn_dt_8
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### rvsd_trgt_cpltn_dt_9
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cost_impct_ind
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cost_impct
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cost_chg_desc
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cost_impct_desc
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cost_impct_orig_est
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### creatr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### modfr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### creatn_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### chg_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_row_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### row_strt_dttm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### row_stop_dttm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### etl_load_cyc_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### src_sys_id
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



