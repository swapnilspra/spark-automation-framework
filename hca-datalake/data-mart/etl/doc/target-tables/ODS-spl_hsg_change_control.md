# SPL_HSG_CHANGE_CONTROL

This table holds the  .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[bus_area](#bus_area)|character varying|255|YES||NO
|[bus_drvr](#bus_drvr)|character varying|255|YES||NO
|[prog_nm](#prog_nm)|character varying|255|YES||NO
|[proj_nm](#proj_nm)|character varying|255|YES||NO
|[hsg_dept](#hsg_dept)|character varying|255|YES||NO
|[chg_cntl_titl](#chg_cntl_titl)|character varying|255|YES||NO
|[chg_stat](#chg_stat)|character varying|25|YES||NO
|[qtr_per](#qtr_per)|character varying|25|YES||NO
|[vldn_cplt_flg](#vldn_cplt_flg)|character varying|1|YES||NO
|[proj_mgr](#proj_mgr)|character varying|255|YES||NO
|[creatr](#creatr)|character varying|255|YES||NO
|[modfr](#modfr)|character varying|255|YES||NO
|[aml_flg](#aml_flg)|character varying|1|YES||NO
|[ye_reld_flg](#ye_reld_flg)|character varying|1|YES||NO
|[fld_cpltn_stat](#fld_cpltn_stat)|character varying|25|YES||NO
|[doc_attchmt_stat](#doc_attchmt_stat)|character varying|25|YES||NO
|[signoff_stat](#signoff_stat)|character varying|25|YES||NO
|[tmly_cpltn_stat](#tmly_cpltn_stat)|character varying|25|YES||NO
|[chg_cntl_updt_stat](#chg_cntl_updt_stat)|character varying|25|YES||NO
|[qa_item_flg](#qa_item_flg)|character varying|1|YES||NO
|[vrsn](#vrsn)|character varying|25|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[hi_lvl_pol_flg](#hi_lvl_pol_flg)|character varying|1|YES||NO
|[chg_cntl_key](#chg_cntl_key)|numeric|(10,0)|NO||YES
|[etl_load_cyc_key](#etl_load_cyc_key)|numeric|(10,0)|YES||NO
|[pres_dt](#pres_dt)|timestamp without time zone|0|YES||NO
|[apprvl_dt](#apprvl_dt)|timestamp without time zone|0|YES||NO
|[trgt_dt](#trgt_dt)|timestamp without time zone|0|YES||NO
|[creatn_dt](#creatn_dt)|timestamp without time zone|0|YES||NO
|[modfy_dt](#modfy_dt)|timestamp without time zone|0|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|0|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|0|YES||NO
|[chg_bus_rsn](#chg_bus_rsn)|text| 1GB |YES||NO
|[chg_desc](#chg_desc)|text| 1GB |YES||NO
|[chg_cmmt](#chg_cmmt)|text| 1GB |YES||NO
|[qa_cmmt_1](#qa_cmmt_1)|text| 1GB |YES||NO
|[qa_cmmt_2](#qa_cmmt_2)|text| 1GB |YES||NO
|[qa_cmmt_3](#qa_cmmt_3)|text| 1GB |YES||NO
|[qa_cmmt_4](#qa_cmmt_4)|text| 1GB |YES||NO
|[qa_cmmt_5](#qa_cmmt_5)|text| 1GB |YES||NO
### chg_cntl_key
#### Description

 

#### Value Range

ALL NUMBERS


#### Logic

Auto Increment sequence Generator

```	
Autoincrement +1 for new inserts
```



### bus_area
#### Description

 

#### Value Range

NVARCHAR

#### Logic

BusinessAreaValue
 
 cast (hsg_change_control.businessareavalue as varchar (4000))


### chg_cntl_id
#### Description

 

#### Value Range

NUMBER

#### Logic

Id
 


### bus_drvr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
BusinessDriverValue

cast (hsg_change_control.businessdrivervalue as varchar (4000))

### prog_nm
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
ProgramValue

cast (hsg_change_control.programvalue as varchar (4000))

### proj_nm
#### Description

 

#### Value Range

NVARCHAR

#### Logic

ProjectName
 
cast (hsg_change_control.projectname as varchar (4000))


### hsg_dept
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSGDepartmentValue
 
 cast (hsg_change_control.hsgdepartmentvalue as varchar (4000))


### chg_cntl_titl
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
Title

 cast (hsg_change_control.title as varchar (4000))

### chg_bus_rsn
#### Description

 

#### Value Range

nclob


#### Logic

BusinessReasonForChange
 
substring(cast (
          replace (
             replace (hsg_change_control.businessreasonforchange,
                      '&#160;',
                      ' '),
             '&#58;',
             ':') as varchar (4000)),1,3999)

### chg_desc
#### Description

 

#### Value Range

nclob


#### Logic

DescriptionOfChangeS
 
cast (
 replace (hsg_change_control.descriptionofchanges, '&#160;', ' ') as varchar (4000))

### chg_stat
#### Description

 

#### Value Range

NVARCHAR

#### Logic

StatusValue
 
cast (hsg_change_control.statusvalue as varchar (4000))


### chg_cmmt
#### Description

 

#### Value Range

nclob


#### Logic

Comments
 
 cast (hsg_change_control.comments as varchar (4000))


### pres_dt
#### Description

 

#### Value Range

DATE

#### Logic

hsg_change_control.DatePresented
 


### apprvl_dt
#### Description

 

#### Value Range

DATE`

#### Logic

 
hsg_change_control.DateApproved


### trgt_dt
#### Description

 

#### Value Range

DATE`

#### Logic

hsg_change_control.TargetDate
 


### tm_est
#### Description

 

#### Value Range

NUMBER

#### Logic

EstimatedTime
 
cast (hsg_change_control.estimatedtime as varchar (4000)

### rsrc_est
#### Description

 

#### Value Range

NUMBER

#### Logic

 
EstimatedResources

cast (hsg_change_control.estimatedresources as varchar (4000))

### qtr_per
#### Description

 

#### Value Range

NVARCHAR

#### Logic

YearQuarter
 
cast (hsg_change_control.yearquarter as varchar (4000))

### vldn_cplt_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
ValidatedComplete

cast (hsg_change_control.validatedcomplete as varchar (4000))

### chg_cntl_wkfl_nbr
#### Description

 

#### Value Range

NUMBER

#### Logic

ChangeControlWorkflow
 
cast (
          hsg_change_control.changecontrolworkflowpreviousversion626201295234am as varchar (4000))

### proj_mgr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
:LKP.LKP_USERNAME(ProjectManagerId)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name



### creatn_dt
#### Description

 

#### Value Range

DATE

#### Logic

hsg_change_control.Created
 


### modfy_dt
#### Description

 

#### Value Range

DATE`

#### Logic

hsg_change_control.Modified
 


### creatr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

:LKP.LKP_USERNAME(CreatedById)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name
 
 
 cast (hsg_change_control.createdbyid as varchar (4000))


### modfr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

:LKP.LKP_USERNAME(ModifiedById)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name
 

cast (hsg_change_control.modifiedbyid as varchar (4000))

### aml_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
AMLRelated

cast (hsg_change_control.amlrelated as varchar (4000))


### ye_reld_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

YearEndRelated
 
cast (hsg_change_control.yearendrelated as varchar (4000))

### fld_cpltn_stat
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
AppropriateFieldsCompletedValue

cast (
          hsg_change_control.appropriatefieldscompletedvalue as varchar (4000))
		  
### doc_attchmt_stat
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
AppropriateDocumentsAttachedValue

cast (
          hsg_change_control.appropriatedocumentsattachedvalue as varchar (4000))

### signoff_stat
#### Description

 

#### Value Range

NVARCHAR

#### Logic

AppropriateSignOffObtainedValue
 
cast (
          hsg_change_control.appropriatesignoffobtainedvalue as varchar (4000))

### tmly_cpltn_stat
#### Description

 

#### Value Range

NVARCHAR

#### Logic

CompletedTimelyValue

cast (hsg_change_control.completedtimelyvalue as varchar (4000))

### chg_cntl_updt_stat
#### Description

 

#### Value Range

NVARCHAR

#### Logic

ChangeControlUpdatesProperlyMadeValue
 
cast (
          hsg_change_control.changecontrolupdatesproperlymadevalue as varchar (4000))

### qa_cmmt_1
#### Description

 

#### Value Range

NCLOB

#### Logic

 
CommentsQA1

cast (hsg_change_control.commentsqa1 as varchar (4000))

### qa_cmmt_2
#### Description

 

#### Value Range

NCLOB

#### Logic

 
CommentsQA2

cast (hsg_change_control.commentsqa2 as varchar (4000))

### qa_cmmt_3
#### Description

 

#### Value Range

NCLOB

#### Logic

CommentsQA3
 
cast (hsg_change_control.commentsqa3 as varchar (4000))

### qa_cmmt_4
#### Description

 

#### Value Range

NCLOB

#### Logic

CommentsQA4
 
 cast (hsg_change_control.commentsqa4 as varchar (4000))


### qa_cmmt_5
#### Description

 

#### Value Range

NCLOB

#### Logic

 
CommentsQA5

cast (hsg_change_control.commentsqa5 as varchar (4000))

### qa_item_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

QAItem
 
cast (hsg_change_control.qaitem as varchar (4000))

### vrsn
#### Description

 

#### Value Range

NVARCHAR

#### Logic

Version
 
cast (hsg_change_control.version as varchar (4000))

### curr_row_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
'Y'


### row_strt_dttm
#### Description

 

#### Value Range

DATE`


#### Logic

SESSSTARTTIME
 


### row_stop_dttm
#### Description

 

#### Value Range

DATE`

#### Logic

 


### etl_load_cyc_key
#### Description

 

#### Value Range

NUMBER

#### Logic

 


### src_sys_id
#### Description

 

#### Value Range

NUMBER

#### Logic

6
 


### hi_lvl_pol_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
HighLevelPolicy

cast (hsg_change_control.HighLevelPolicy as varchar (4000))
