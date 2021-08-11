# DEALER_ASSET_SUMMARY

This table holds the lorem ipsum dolor sit amet.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[dlr_tpa_flg](#dlr_tpa_flg)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[dlr_fee_sum_key](#dlr_fee_sum_key)|integer|(32,0)|NO||YES
|[dlr_key](#dlr_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[prir_per_asset](#prir_per_asset)|numeric|(38,15)|YES||NO
|[curr_per_asset](#curr_per_asset)|numeric|(38,15)|YES||NO
|[prir_per_eligbl_asset](#prir_per_eligbl_asset)|numeric|(38,15)|YES||NO
|[curr_per_eligbl_asset](#curr_per_eligbl_asset)|numeric|(38,15)|YES||NO
|[prir_per_inelig_asset](#prir_per_inelig_asset)|numeric|(38,15)|YES||NO
|[curr_per_inelig_asset](#curr_per_inelig_asset)|numeric|(38,15)|YES||NO
|[prir_per_ta_asset](#prir_per_ta_asset)|numeric|(38,15)|YES||NO
|[curr_per_ta_asset](#curr_per_ta_asset)|numeric|(38,15)|YES||NO
|[prir_per_ta_eligbl_asset](#prir_per_ta_eligbl_asset)|numeric|(38,15)|YES||NO
|[curr_per_ta_eligbl_asset](#curr_per_ta_eligbl_asset)|numeric|(38,15)|YES||NO
|[prir_per_ta_inelig_asset](#prir_per_ta_inelig_asset)|numeric|(38,15)|YES||NO
|[curr_per_ta_inelig_asset](#curr_per_ta_inelig_asset)|numeric|(38,15)|YES||NO
|[prir_per_avg_asset](#prir_per_avg_asset)|numeric|(38,15)|YES||NO
|[curr_per_avg_asset](#curr_per_avg_asset)|numeric|(38,15)|YES||NO
|[prir_per_avg_eligbl_asset](#prir_per_avg_eligbl_asset)|numeric|(38,15)|YES||NO
|[curr_per_avg_eligbl_asset](#curr_per_avg_eligbl_asset)|numeric|(38,15)|YES||NO
|[prir_per_avg_inelig_asset](#prir_per_avg_inelig_asset)|numeric|(38,15)|YES||NO
|[curr_per_avg_inelig_asset](#curr_per_avg_inelig_asset)|numeric|(38,15)|YES||NO
|[prir_per_avg_ta_asset](#prir_per_avg_ta_asset)|numeric|(38,15)|YES||NO
|[curr_per_avg_ta_asset](#curr_per_avg_ta_asset)|numeric|(38,15)|YES||NO
|[prir_avg_ta_eligbl_asset](#prir_avg_ta_eligbl_asset)|numeric|(38,15)|YES||NO
|[curr_avg_ta_inelig_asset](#curr_avg_ta_inelig_asset)|numeric|(38,15)|YES||NO
|[prir_avg_ta_inelig_asset](#prir_avg_ta_inelig_asset)|numeric|(38,15)|YES||NO
|[curr_avg_ta_eligbl_asset](#curr_avg_ta_eligbl_asset)|numeric|(38,15)|YES||NO
|[sub_amt](#sub_amt)|numeric|(38,15)|YES||NO
|[redmpn_amt](#redmpn_amt)|numeric|(38,15)|YES||NO
|[net_cashflow_amt](#net_cashflow_amt)|numeric|(38,15)|YES||NO
|[mkt_val_chg](#mkt_val_chg)|numeric|(38,15)|YES||NO
|[avg_sub_amt](#avg_sub_amt)|numeric|(38,15)|YES||NO
|[avg_redmpn_amt](#avg_redmpn_amt)|numeric|(38,15)|YES||NO
|[avg_net_amt](#avg_net_amt)|numeric|(38,15)|YES||NO
|[avg_mkt_val_chg](#avg_mkt_val_chg)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### dlr_fee_sum_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### dlr_key
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fund_key
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



### prir_per_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_per_eligbl_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_eligbl_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_per_inelig_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_inelig_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_per_ta_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_ta_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_per_ta_eligbl_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_ta_eligbl_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_per_ta_inelig_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_ta_inelig_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_per_avg_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_avg_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_per_avg_eligbl_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_avg_eligbl_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_per_avg_inelig_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_avg_inelig_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_per_avg_ta_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_per_avg_ta_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_avg_ta_eligbl_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_avg_ta_inelig_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### prir_avg_ta_inelig_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### curr_avg_ta_eligbl_asset
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### sub_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### redmpn_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### net_cashflow_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mkt_val_chg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### avg_sub_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### avg_redmpn_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### avg_net_amt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### avg_mkt_val_chg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### dlr_tpa_flg
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



