# BUD_ACCOUNT

This table holds the Budget Accounts.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[acct_id](#acct_id)|character varying|30|YES||NO
|[acct_desc](#acct_desc)|character varying|200|YES||NO
|[acct_type](#acct_type)|character varying|30|YES||NO
|[actv_flg](#actv_flg)|character varying|1|YES||NO
|[cat_type](#cat_type)|character varying|30|YES||NO
|[expns_cat](#expns_cat)|character varying|30|YES||NO
|[prpd_expns_flg](#prpd_expns_flg)|character varying|1|YES||NO
|[acct_subt](#acct_subt)|character varying|30|YES||NO
|[payee](#payee)|character varying|100|YES||NO
|[insr_usr](#insr_usr)|character varying|30|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[bud_acct_key](#bud_acct_key)|integer|(32,0)|NO||YES
|[vrsn](#vrsn)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[insr_ts](#insr_ts)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### bud_acct_key
#### Description

Surrogate key for Budget Account
#### Value Range

Positive Integers

#### Logic
on insert 
```
increment by 1 for each new record
```
on update
```
lookup by ACCT_ID = ACCOUNT_ID from source
```
### acct_id
#### Description

Account ID

#### Value Range

any string value

#### Logic

```
Remove lead and trailing blanks from ACCOUNT_ID from SOURCE
```

### acct_desc
#### Description

Account Description

#### Value Range

any string value

#### Logic

```
copy as is from ACCOUNT_DESCRIPTION from source
```

### acct_type
#### Description

Account Type

#### Value Range


any string value

#### Logic

```
copy as is from ACCOUNT_TYPE from source
```

### actv_flg
#### Description

indicator is account is active

#### Value Range

Y, N

#### Logic

```
if ACTIVE from source is TRUE then Y otherwise N 
```

### cat_type
#### Description

Account Category Type

#### Value Range

any string value

#### Logic

```
copy as is from CATEGORY_TYPE from source
```

### expns_cat
#### Description

account expence category

#### Value Range

any string value

#### Logic 

Hybrid of sequence generator and source value from HDM.FUND id.

```
copy as is from EXPENSE_CATEGORY from source
```

### prpd_expns_flg
#### Description

indication if expense is prepaid

#### Value Range

Y, N

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
if PREPAID_EXPENSE from source is 	TRUE then Y otherwise N 
```

### acct_subt
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### payee
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### insr_usr
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### insr_ts
#### Description

Lorem ipsum dolor sit amet

#### Value Range

N/A

#### Logic

Hybrid of sequence generator and source value from HDM.FUND id.

```
Autoincrement +1 for new inserts
```



### vrsn
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

## Mappings used in documentation
### m_BUD_ACCOUNT
VW_BDGT_ACCOUNT is source


