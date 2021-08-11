# HOLDINGS_TXN_CODES

This table holds the lorem ipsum dolor sit amet.
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[txn_type_cd](#txn_type_cd)|character varying|5|YES||NO
|[txn_type_desc](#txn_type_desc)|character varying|255|YES||NO
|[txn_cd](#txn_cd)|character varying|5|YES||NO
|[txn_cd_desc](#txn_cd_desc)|character varying|255|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[holdg_txn_cd_key](#holdg_txn_cd_key)|integer|(32,0)|NO||YES
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
### holdg_txn_cd_key
#### Description

Generated value

#### Value Range

N/A

#### Logic

Sequence Generator

```
Autoincrement +1 for new inserts
```



### txn_type_cd
#### Description

Transaction Type Code

#### Value Range

N/A

#### Logic



```
HARBORCP_TXN102.TXN_TYPE_CD
```



### txn_type_desc
#### Description

Transaction Type Description

#### Value Range

N/A

#### Logic



```

```



### txn_cd
#### Description

Transaction Code

#### Value Range

N/A

#### Logic



```
HARBORCP_TXN102.TXN_CD
```



### txn_cd_desc
#### Description

Transaction Code Description

#### Value Range

N/A

#### Logic


```

```



### curr_row_flg
#### Description

Current Row Flag

#### Value Range

"Y"/"N"

#### Logic



```
Defaults to "Y"
```



### row_strt_dttm
#### Description

Row Insert Timestamp

#### Value Range

N/A

#### Logic



```
sysdate
```



### row_stop_dttm
#### Description

Date when row marked not current

#### Value Range

N/A

#### Logic



```
sysdate
```



### etl_load_cyc_key
#### Description

unused

#### Value Range

N/A

#### Logic



```
unused
```



### src_sys_id
#### Description

unused

#### Value Range

N/A

#### Logic

.

```
unused
```



