# FACILITY

This table holds the lorem ipsum dolor sit amet.

## DDL

| Column Name                           | SQL Type                    | Length  | Nullable | Default Value | PK  |
| ------------------------------------- | --------------------------- | ------- | -------- | ------------- | --- |
| [fac_id](#fac_id)                     | character varying           | 25      | YES      |               | NO  |
| [sub_fac_nm](#sub_fac_nm)             | character varying           | 50      | YES      |               | NO  |
| [curr_row_flg](#curr_row_flg)         | character varying           | 1       | YES      |               | NO  |
| [fac_key](#fac_key)                   | integer                     | (32,0)  | NO       |               | YES |
| [fac_func_id](#fac_func_id)           | numeric                     | (38,15) | NO       |               | NO  |
| [day_key](#day_key)                   | integer                     | (32,0)  | YES      |               | NO  |
| [etl_load_cyc_key](#etl_load_cyc_key) | integer                     | (32,0)  | YES      |               | NO  |
| [src_sys_id](#src_sys_id)             | numeric                     | (38,15) | YES      |               | NO  |
| [row_strt_dttm](#row_strt_dttm)       | timestamp without time zone | 6       | YES      |               | NO  |
| [row_stop_dttm](#row_stop_dttm)       | timestamp without time zone | 6       | YES      |               | NO  |

### fac_key

#### Description

Facility_key

#### Value Range

N/A

#### Logic

```
Autoincrement +1 for new inserts
```

### fac_id
#### Description

Facility ID

#### Value Range

N/A

#### Logic

```
oper_status.FACILITY_ID
```

### sub_fac_nm

#### Description

Facility Description Text

#### Value Range

N/A

#### Logic

```
oper_status.DESCRIPTION_TEXT
```

### fac_func_id

#### Description

Facility function ID

#### Value Range

N/A

#### Logic

```
oper_status.FACILITY_FUNCTION_ID
```

### day_key

#### Description

Day key

#### Value Range

N/A

#### Logic

```
Match the (oper_status.PROGRESS_DATE - 1) with hdm. calendar.cal_day and get the day_key
```

### curr_row_flg

#### Description

#### Value Range

Y/N

#### Logic

```
"Y"
```

### row_strt_dttm

#### Description

Row insert date

#### Value Range

N/A

#### Logic

```
sysdate
```

### row_stop_dttm

#### Description

Unused

#### Value Range

N/A

#### Logic

```
NULL
```

### etl_load_cyc_key

#### Description

Load Cycle Key

#### Value Range

N/A

#### Logic

Unused.

```
NULL
```

### src_sys_id

#### Description

Source System ID

#### Value Range

N/A

#### Logic

```
Default to 4
```
### Source File Name
```
oper_status.csv
```
### Tables used
hdm.calendar
hdm.facility
### Business Keys
```
    primary_key : "fac_key"
    business_key = "fac_id", "fac_func_id", "day_key"
```