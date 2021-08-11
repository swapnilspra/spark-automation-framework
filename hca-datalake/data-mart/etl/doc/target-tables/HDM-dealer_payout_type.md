# DEALER_PAYOUT_TYPE

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[payout_type_cd](#payout_type_cd)|character varying|1|YES||NO
|[payout_type_desc](#payout_type_desc)|character varying|255|YES||NO
|[payout_type_key](#payout_type_key)|integer|(32,0)|NO||YES
### payout_type_key
#### Description



#### Value Range

NVARCHAR

#### Logic

Auto Increment Key



### payout_type_cd
#### Description



#### Value Range

NVARCHAR

#### Logic


PAYOUT_TYPE_CD



### payout_type_desc
#### Description



#### Value Range

NVARCHAR

#### Logic

PAYOUT_TYPE_DESC

------------------------------------------------------

#Files Used
1. dealer_payout_type.csv

File Type -- Comma Delimited
File Location -- /internal/dealer_payout_type.csv

## Business Key
PAYOUT_TYPE_CD