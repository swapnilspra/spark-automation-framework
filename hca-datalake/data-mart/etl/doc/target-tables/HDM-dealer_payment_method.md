# DEALER_PAYMENT_METHOD

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[pmt_mthd_cd](#pmt_mthd_cd)|character varying|25|YES||NO
|[pmt_mthd_desc](#pmt_mthd_desc)|character varying|50|YES||NO
|[dlr_pmt_mthd_key](#dlr_pmt_mthd_key)|integer|(32,0)|NO||YES
### dlr_pmt_mthd_key
#### Description



#### Value Range

NVARCHAR

#### Logic

Auto Increment Key


### pmt_mthd_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

PMT_MTHD_CD


### pmt_mthd_desc
#### Description

#### Value Range

NVARCHAR

#### Logic

PMT_MTHD_DESC

-------------------------------------------

#Files Used
1. dealer_payment_method.csv

File Type -- Comma Delimited
File Location -- /internal/dealer_payment_method.csv

## Business Key
PMT_MTHD_CD