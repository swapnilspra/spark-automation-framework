# ACCOUNT_CATEGORY

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[acct_cat_desc](#acct_cat_desc)|character varying|100|YES||NO
|[maj_cat_desc](#maj_cat_desc)|character varying|60|YES||NO
|[acct_cat_key](#acct_cat_key)|integer|(32,0)|NO||YES
|[maj_cat_cd](#maj_cat_cd)|integer|(32,0)|YES||NO
|[acct_cat_ord](#acct_cat_ord)|numeric|(38,15)|YES||NO
### acct_cat_key
#### Description



#### Value Range

NUMERIC

#### Logic

Auto Increment Key




### acct_cat_desc
#### Description



#### Value Range

NVARCHAR

#### Logic


ACCT_CAT_DESC



### maj_cat_cd
#### Description



#### Value Range

NVARCHAR

#### Logic

MAJ_CAT_CD




### maj_cat_desc
#### Description



#### Value Range

NVARCHAR

#### Logic


MAJ_CAT_DESC



### acct_cat_ord
#### Description



#### Value Range

NVARCHAR

#### Logic

ACCT_CAT_ORD



-----------------------------------------------------------------------------------------------

#Files Used
1. account_category.csv

File Type - Comma delimited
File Loation - internal/account_category.csv

## Business Key

ACCT_CAT_ORD




