# CALENDAR

This table holds the lorem ipsum dolor sit amet.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[cal_day_shrt_nm](#cal_day_shrt_nm)|character varying|3|YES||NO
|[cal_day_long_nm](#cal_day_long_nm)|character varying|20|YES||NO
|[bus_day_flg](#bus_day_flg)|character varying|1|YES||NO
|[mkt_hol_flg](#mkt_hol_flg)|character varying|1|YES||NO
|[wk_cd](#wk_cd)|character varying|20|YES||NO
|[wk_shrt_nm](#wk_shrt_nm)|character varying|20|YES||NO
|[wk_long_nm](#wk_long_nm)|character varying|60|YES||NO
|[mo_cd](#mo_cd)|character varying|20|YES||NO
|[mo_nm](#mo_nm)|character varying|25|YES||NO
|[mo_shrt_nm](#mo_shrt_nm)|character varying|3|YES||NO
|[mo_long_nm](#mo_long_nm)|character varying|20|YES||NO
|[cal_mo_end_flg](#cal_mo_end_flg)|character varying|1|YES||NO
|[bus_mo_end_flg](#bus_mo_end_flg)|character varying|1|YES||NO
|[qtr_cd](#qtr_cd)|character varying|10|YES||NO
|[qtr_shrt_nm](#qtr_shrt_nm)|character varying|20|YES||NO
|[qtr_long_nm](#qtr_long_nm)|character varying|60|YES||NO
|[cal_qtr_end_flg](#cal_qtr_end_flg)|character varying|1|YES||NO
|[bus_qtr_end_flg](#bus_qtr_end_flg)|character varying|1|YES||NO
|[yr_cd](#yr_cd)|character varying|20|YES||NO
|[yr_shrt_nm](#yr_shrt_nm)|character varying|20|YES||NO
|[yr_long_nm](#yr_long_nm)|character varying|60|YES||NO
|[ye_flg](#ye_flg)|character varying|1|YES||NO
|[yr_last_bus_day_flg](#yr_last_bus_day_flg)|character varying|1|YES||NO
|[fiscl_wk_cd](#fiscl_wk_cd)|character varying|20|YES||NO
|[fiscl_wk_shrt_nm](#fiscl_wk_shrt_nm)|character varying|20|YES||NO
|[fiscl_wk_long_nm](#fiscl_wk_long_nm)|character varying|60|YES||NO
|[fiscl_mo_cd](#fiscl_mo_cd)|character varying|20|YES||NO
|[fiscl_mo_shrt_nm](#fiscl_mo_shrt_nm)|character varying|20|YES||NO
|[fiscl_mo_long_nm](#fiscl_mo_long_nm)|character varying|60|YES||NO
|[fiscl_mo_end_flg](#fiscl_mo_end_flg)|character varying|1|YES||NO
|[fiscl_mo_last_bus_day_flg](#fiscl_mo_last_bus_day_flg)|character varying|1|YES||NO
|[fiscl_qtr_cd](#fiscl_qtr_cd)|character varying|20|YES||NO
|[fiscl_qtr_shrt_nm](#fiscl_qtr_shrt_nm)|character varying|20|YES||NO
|[fiscl_qtr_long_nm](#fiscl_qtr_long_nm)|character varying|60|YES||NO
|[fiscl_qtr_ending_flg](#fiscl_qtr_ending_flg)|character varying|1|YES||NO
|[fiscl_qtr_last_bus_day_flg](#fiscl_qtr_last_bus_day_flg)|character varying|1|YES||NO
|[fiscl_yr_cd](#fiscl_yr_cd)|character varying|20|YES||NO
|[fiscl_yr_shrt_nm](#fiscl_yr_shrt_nm)|character varying|20|YES||NO
|[fiscl_yr_long_nm](#fiscl_yr_long_nm)|character varying|60|YES||NO
|[fiscl_yr_ending_flg](#fiscl_yr_ending_flg)|character varying|1|YES||NO
|[fiscl_yr_last_bus_day_flg](#fiscl_yr_last_bus_day_flg)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[day_key](#day_key)|integer|(32,0)|NO||YES
|[wk_nbr](#wk_nbr)|integer|(32,0)|YES||NO
|[wk_days_cnt](#wk_days_cnt)|numeric|(38,15)|YES||NO
|[mo_nbr](#mo_nbr)|integer|(32,0)|YES||NO
|[mo_days_cnt](#mo_days_cnt)|numeric|(38,15)|YES||NO
|[qtr_nbr](#qtr_nbr)|integer|(32,0)|YES||NO
|[qtr_days_cnt](#qtr_days_cnt)|numeric|(38,15)|YES||NO
|[yr_nbr](#yr_nbr)|integer|(32,0)|YES||NO
|[yr_days_cnt](#yr_days_cnt)|numeric|(38,15)|YES||NO
|[fiscl_wk_nbr](#fiscl_wk_nbr)|integer|(32,0)|YES||NO
|[fiscl_wk_days_cnt](#fiscl_wk_days_cnt)|numeric|(38,15)|YES||NO
|[fiscl_mo_nbr](#fiscl_mo_nbr)|integer|(32,0)|YES||NO
|[fiscl_mo_days_cnt](#fiscl_mo_days_cnt)|numeric|(38,15)|YES||NO
|[fiscl_qtr_nbr](#fiscl_qtr_nbr)|integer|(32,0)|YES||NO
|[fiscl_qtr_days_cnt](#fiscl_qtr_days_cnt)|numeric|(38,15)|YES||NO
|[fiscl_yr_nbr](#fiscl_yr_nbr)|numeric|(38,15)|YES||NO
|[fiscl_yr_days_cnt](#fiscl_yr_days_cnt)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[cal_day](#cal_day)|timestamp without time zone|6|YES||NO
|[wk_ending_dt](#wk_ending_dt)|timestamp without time zone|6|YES||NO
|[mo_ending_dt](#mo_ending_dt)|timestamp without time zone|6|YES||NO
|[qtr_ending_dt](#qtr_ending_dt)|timestamp without time zone|6|YES||NO
|[yr_ending_dt](#yr_ending_dt)|timestamp without time zone|6|YES||NO
|[fiscl_wk_ending_dt](#fiscl_wk_ending_dt)|timestamp without time zone|6|YES||NO
|[fiscl_mo_ending_dt](#fiscl_mo_ending_dt)|timestamp without time zone|6|YES||NO
|[fiscl_qtr_ending_dt](#fiscl_qtr_ending_dt)|timestamp without time zone|6|YES||NO
|[fiscl_yr_ending_dt](#fiscl_yr_ending_dt)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
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



### cal_day
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cal_day_shrt_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cal_day_long_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### bus_day_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mkt_hol_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### wk_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### wk_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### wk_shrt_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### wk_long_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### wk_ending_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### wk_days_cnt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mo_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mo_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mo_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mo_shrt_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mo_long_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cal_mo_end_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### bus_mo_end_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mo_ending_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### mo_days_cnt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### qtr_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### qtr_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### qtr_shrt_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### qtr_long_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### cal_qtr_end_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### bus_qtr_end_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### qtr_ending_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### qtr_days_cnt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### yr_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### yr_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### yr_shrt_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### yr_long_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### yr_ending_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### ye_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### yr_last_bus_day_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### yr_days_cnt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_wk_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_wk_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_wk_shrt_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_wk_long_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_wk_ending_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_wk_days_cnt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_mo_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_mo_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_mo_shrt_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_mo_long_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_mo_ending_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_mo_end_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_mo_last_bus_day_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_mo_days_cnt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_qtr_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_qtr_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_qtr_shrt_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_qtr_long_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_qtr_ending_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_qtr_last_bus_day_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_qtr_ending_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_qtr_days_cnt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_yr_cd
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_yr_shrt_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_yr_long_nm
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_yr_ending_dt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_yr_ending_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_yr_last_bus_day_flg
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_yr_nbr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### fiscl_yr_days_cnt
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



