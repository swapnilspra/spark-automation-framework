# HOLDINGS_FOREIGN_OWNERSHIP

This table holds the lorem ipsum dolor sit amet.
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
|[holdg_open_posn_key](#holdg_open_posn_key)|integer|(32,0)|NO||NO
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

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fund_compst_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### secr_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### inc_crty_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### iss_crty_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### trde_crty_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### thrs_crty_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### day_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### tot_long_posn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### actl_cstdy_posn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### pend_sale_qty
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### pend_buy_qty
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### free_qty
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### oshr_qty
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### par_shr_qty
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### tot_shrt_posn
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### on_loan_qty
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### new_secr_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### all_cls_oshr_qty
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### all_voting_oshr_qty
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### all_non_voting_oshr_qty
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### voting_shr_ind
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



### prc_shrs_cls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prc_shrs_acls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prc_shrs_voting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prc_shrs_nonvoting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### breach_ind_cls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### breach_ind_acls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### breach_ind_voting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### breach_ind_nonvoting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### thrsh_br_cls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### thrsh_br_acls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### thrsh_br_voting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### thrsh_br_nonvoting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### new_br_ind_cls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### new_br_ind_acls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### new_br_ind_voting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### new_br_ind_nonvoting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### spec_alert_cls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### spec_alert_acls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### spec_alert_voting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### spec_alert_nonvoting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prox_int_br_cls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prox_ext_br_cls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prox_int_br_acls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prox_ext_br_acls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prox_int_br_voting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prox_ext_br_voting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prox_int_br_nonvoting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prox_ext_br_nonvoting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### breach_ind_int_cls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### breach_ind_int_acls
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### breach_ind_int_voting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### breach_ind_int_nonvoting
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### issuer_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### adr_spsr_ship_type
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



