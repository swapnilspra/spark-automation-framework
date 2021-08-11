# COUNTRY_THRESHOLD

This table holds the Country Threshold limits.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[thrs_desc](#thrs_desc)|character varying|255|YES||NO
|[intrl_trig_displ](#intrl_trig_displ)|character varying|25|YES||NO
|[ofcl_trig_displ](#ofcl_trig_displ)|character varying|25|YES||NO
|[thrs_days_displ](#thrs_days_displ)|character varying|25|YES||NO
|[reglty_filng_displ_days](#reglty_filng_displ_days)|character varying|25|YES||NO
|[filng_displ_days](#filng_displ_days)|character varying|25|YES||NO
|[custom_ind](#custom_ind)|character varying|1|YES||NO
|[thrshld_mlstn](#thrshld_mlstn)|character varying|25|YES||NO
|[periodic_trshld](#periodic_trshld)|character varying|25|YES||NO
|[any_amt_ind](#any_amt_ind)|character varying|1|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[crty_thrs_key](#crty_thrs_key)|integer|(32,0)|NO||YES
|[crty_key](#crty_key)|integer|(32,0)|YES||NO
|[intrl_trig](#intrl_trig)|numeric|(38,15)|YES||NO
|[ofcl_trig](#ofcl_trig)|numeric|(38,15)|YES||NO
|[thrs_days](#thrs_days)|numeric|(38,15)|YES||NO
|[reglty_filng_days](#reglty_filng_days)|numeric|(38,15)|YES||NO
|[filng_days](#filng_days)|numeric|(38,15)|YES||NO
|[threshold_type](#threshold_type)|integer|(32,0)|YES||NO
|[thrshld_mlstn_displ](#thrshld_mlstn_displ)|numeric|(38,15)|YES||NO
|[periodic_thrsld_disp](#periodic_thrsld_disp)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### crty_thrs_key
#### Description

Country Threshold key

```
Autoincrement +1 for new inserts
```



### crty_key
#### Description

Country Key

```
file.Country_Code = country.crty_cd
```



### thrs_desc
#### Description

Threshold Desciption

```
IIF(ISNULL(LTRIM(RTRIM(Remarks))),'NA',LTRIM(RTRIM(Remarks)))
```

### intrl_trig
#### Description

Internal Trigger

```
LTRIM(RTRIM(Trigger))
```

### ofcl_trig
#### Description

Official Trigger

```
IIF(
IS_NUMBER(
LTRIM(RTRIM(OfficialTrigger))),TO_DECIMAL(LTRIM(RTRIM(OfficialTrigger))),NULL)
```

### thrs_days
#### Description

Threshold Days

```
IIF(IS_NUMBER(LTRIM(RTRIM(Timing))),TO_DECIMAL(LTRIM(RTRIM(Timing))),NULL)
```

### reglty_filng_days
#### Description

Regulatory Filing Days

```
Unused
```

### filng_days
#### Description

Filing Days


```
Ununsed
```

### intrl_trig_displ
#### Description

Internal Trigger Display

```
LTRIM(RTRIM(Trigger))
```

### ofcl_trig_displ
#### Description

Official Trigger Display

```
LTRIM(RTRIM(OfficialTrigger))
```

### thrs_days_displ
#### Description

Threshold Days DIsplay

```
LTRIM(RTRIM(Timing))
```

### reglty_filng_displ_days
#### Description

Regulatory Filing Dipslay (Days)

```
Ununsed
```

### filng_displ_days
#### Description

Filing Display (Days)

```
Unused
```

### threshold_type
#### Description

Threshold Type

```
LTRIM(RTRIM(Threashold_Type))
```

### custom_ind
#### Description

Custom Indicator

```
LTRIM(RTRIM(Custom))
```

### thrshld_mlstn
#### Description

Threshold Milestone


```
IIF(IS_NUMBER(LTRIM(RTRIM(Threshold_Milestone))),TO_DECIMAL(LTRIM(RTRIM(Threshold_Milestone))),NULL)
```

### thrshld_mlstn_displ
#### Description

Threshold Milestone Display

```
LTRIM(RTRIM(Threshold_Milestone))
```

### periodic_trshld
#### Description

Periodic Threshold

```
IIF(IS_NUMBER(LTRIM(RTRIM(Periodic_Threshold))),TO_DECIMAL(LTRIM(RTRIM(Periodic_Threshold))),NULL)
```

### periodic_thrsld_disp
#### Description

Periodic Threshold Display

```
LTRIM(RTRIM(Periodic_Threshold))
```

### any_amt_ind
#### Description

Any Amount Indicator


```
LTRIM(RTRIM(Any_Amount_Indicator))
```

### curr_row_flg
#### Description

Current Row Flag

#### Value Range

Y/N

### row_strt_dttm
#### Description

Row Start Datetime

```
Row load timestamp
```

Files - HFFOR.csv

Frequency - When received from Legal (Periodically)

Joins - 

 Country.CTRY_CD = file.country_code



