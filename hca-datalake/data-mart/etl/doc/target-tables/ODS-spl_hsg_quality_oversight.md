# SPL_HSG_QUALITY_OVERSIGHT

This table holds the  .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[reviewer_name](#reviewer_name)|character varying|100|YES||NO
|[work_type](#work_type)|character varying|255|YES||NO
|[representative_name](#representative_name)|character varying|100|YES||NO
|[processor_qc](#processor_qc)|character varying|255|YES||NO
|[account_number](#account_number)|character varying|255|YES||NO
|[fund_number](#fund_number)|character varying|255|YES||NO
|[pass_fail](#pass_fail)|character varying|255|YES||NO
|[manager_name](#manager_name)|character varying|100|YES||NO
|[hsg_quality_oversight_workflow](#hsg_quality_oversight_workflow)|character varying|100|YES||NO
|[title](#title)|character varying|255|YES||NO
|[created_by](#created_by)|character varying|100|YES||NO
|[modified_by](#modified_by)|character varying|100|YES||NO
|[content_type](#content_type)|character varying|4000|YES||NO
|[type](#type)|character varying|4000|YES||NO
|[app_created_by](#app_created_by)|character varying|255|YES||NO
|[app_modified_by](#app_modified_by)|character varying|255|YES||NO
|[id](#id)|numeric|(38,0)|YES||NO
|[awd_item_review_date](#awd_item_review_date)|timestamp without time zone|0|YES||NO
|[modified](#modified)|timestamp without time zone|0|YES||NO
|[created](#created)|timestamp without time zone|0|YES||NO
|[reviewer_comments](#reviewer_comments)|text| 1GB |YES||NO
### id
#### Description

 

#### Value Range

ALL NUMBERS

#### Logic

Auto Increment sequence Generator

 



### reviewer_name
#### Description

 

#### Value Range

N/A

#### Logic

:LKP.LKP_NAME(ReviewerNameId_in)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person' and sipaddress is not null

Condition : Id = ReviewerNameId_in
Output Column : Name



 



### awd_item_review_date
#### Description

 

#### Value Range

DATE

#### Logic

HSG_Quality_Control_Oversight.AWDItemReviewDate

 



### work_type
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSG_Quality_Control_Oversight.WorkType

 



### representative_name
#### Description

 

#### Value Range

NVARCHAR

#### Logic

:LKP.LKP_NAME(RepresentativeNameId_in)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person' and sipaddress is not null

Condition : Id = ReviewerNameId_in
Output Column : Name

 



### processor_qc
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSG_Quality_Control_Oversight.ProcessorQCErValue

 



### account_number
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSG_Quality_Control_Oversight.AccountNumber

 



### fund_number
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSG_Quality_Control_Oversight.FundNumber

 



### pass_fail
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSG_Quality_Control_Oversight.PassFailValue

 



### reviewer_comments
#### Description

 

#### Value Range

NCLOB

#### Logic

HSG_Quality_Control_Oversight.ReviewerComments

 



### manager_name
#### Description

 

#### Value Range

NVARCHAR

#### Logic

:LKP.LKP_NAME(ManagerNameId_in)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person' and sipaddress is not null

Condition : Id = ManagerNameId_in
Output Column : Name

 



### hsg_quality_oversight_workflow
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSG_Quality_Control_Oversight.HSGQualityOversightWorkflow

 



### title
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSG_Quality_Control_Oversight.Title

 



### created_by
#### Description

 

#### Value Range

NVARCHAR

#### Logic

:LKP.LKP_NAME(CreatedById_in)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person' and sipaddress is not null

Condition : Id = CreatedById_in
Output Column : Name

 



### modified_by
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSG_Quality_Control_Oversight.ModifiedById

 



### modified
#### Description

 

#### Value Range

DATE

#### Logic

:LKP.LKP_NAME(ModifiedById_in)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person' and sipaddress is not null

Condition : Id = ModifiedById_in
Output Column : Name


 



### created
#### Description

 

#### Value Range

DATE

#### Logic

HSG_Quality_Control_Oversight.Created

 



### content_type
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 



### type
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 



### app_created_by
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 



### app_modified_by
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 



