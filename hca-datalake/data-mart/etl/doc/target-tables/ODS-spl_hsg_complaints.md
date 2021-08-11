# SPL_HSG_COMPLAINTS

This table holds the  .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[cmplnt_titl](#cmplnt_titl)|character varying|255|YES||NO
|[stat](#stat)|character varying|60|YES||NO
|[assgne](#assgne)|character varying|60|YES||NO
|[rec_type](#rec_type)|character varying|25|YES||NO
|[cat_val](#cat_val)|character varying|255|YES||NO
|[orig_dept](#orig_dept)|character varying|255|YES||NO
|[cmplnt_type](#cmplnt_type)|character varying|25|YES||NO
|[fdbk_rep](#fdbk_rep)|character varying|60|YES||NO
|[acct_nbr](#acct_nbr)|character varying|60|YES||NO
|[fund_nbr](#fund_nbr)|character varying|255|YES||NO
|[iss_desc](#iss_desc)|character varying|4000|YES||NO
|[soln_desc](#soln_desc)|character varying|4000|YES||NO
|[addl_info](#addl_info)|character varying|4000|YES||NO
|[acct_comprmed_flg](#acct_comprmed_flg)|character varying|1|YES||NO
|[tckt_id](#tckt_id)|character varying|25|YES||NO
|[wrttn_cmplnt_id](#wrttn_cmplnt_id)|character varying|1000|YES||NO
|[lgl_stat](#lgl_stat)|character varying|25|YES||NO
|[finra_filng_flg](#finra_filng_flg)|character varying|1|YES||NO
|[bkup_docn_link](#bkup_docn_link)|character varying|4000|YES||NO
|[ceo_escl_flg](#ceo_escl_flg)|character varying|1|YES||NO
|[cco_escl_flg](#cco_escl_flg)|character varying|1|YES||NO
|[cmo_escl_flg](#cmo_escl_flg)|character varying|1|YES||NO
|[addr_line](#addr_line)|character varying|1000|YES||NO
|[city](#city)|character varying|60|YES||NO
|[st](#st)|character varying|25|YES||NO
|[zip_cd](#zip_cd)|character varying|25|YES||NO
|[push_ntfn_val](#push_ntfn_val)|character varying|4000|YES||NO
|[push_ntfn_id](#push_ntfn_id)|character varying|25|YES||NO
|[push_ntfn_email](#push_ntfn_email)|character varying|255|YES||NO
|[finra_rptg_per](#finra_rptg_per)|character varying|25|YES||NO
|[prnc_revwer](#prnc_revwer)|character varying|255|YES||NO
|[bmg_iss_link](#bmg_iss_link)|character varying|1000|YES||NO
|[url_path](#url_path)|character varying|4000|YES||NO
|[bus_area](#bus_area)|character varying|255|YES||NO
|[bus_drvr](#bus_drvr)|character varying|255|YES||NO
|[prog](#prog)|character varying|255|YES||NO
|[proj_nm](#proj_nm)|character varying|255|YES||NO
|[hsg_dept](#hsg_dept)|character varying|255|YES||NO
|[chg_bus_rsn](#chg_bus_rsn)|character varying|4000|YES||NO
|[chg_desc](#chg_desc)|character varying|4000|YES||NO
|[cmmt](#cmmt)|character varying|4000|YES||NO
|[link_1](#link_1)|character varying|4000|YES||NO
|[link_2](#link_2)|character varying|4000|YES||NO
|[tm_est](#tm_est)|character varying|255|YES||NO
|[rsrc_est](#rsrc_est)|character varying|255|YES||NO
|[qtr](#qtr)|character varying|25|YES||NO
|[proj_mgr](#proj_mgr)|character varying|255|YES||NO
|[creatr](#creatr)|character varying|60|YES||NO
|[modfr](#modfr)|character varying|60|YES||NO
|[cmplnt_mo](#cmplnt_mo)|character varying|25|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[cmplnt_key](#cmplnt_key)|numeric|(10,0)|NO||YES
|[etl_load_cyc_key](#etl_load_cyc_key)|numeric|(10,0)|YES||NO
|[recvd_dt](#recvd_dt)|timestamp without time zone|0|YES||NO
|[prnc_revw_dt](#prnc_revw_dt)|timestamp without time zone|0|YES||NO
|[finra_filng_dt](#finra_filng_dt)|timestamp without time zone|0|YES||NO
|[ceo_escl_dt](#ceo_escl_dt)|timestamp without time zone|0|YES||NO
|[cco_escl_dt](#cco_escl_dt)|timestamp without time zone|0|YES||NO
|[cmo_escl_dt](#cmo_escl_dt)|timestamp without time zone|0|YES||NO
|[hsg_rsln_dt](#hsg_rsln_dt)|timestamp without time zone|0|YES||NO
|[pres_dt](#pres_dt)|timestamp without time zone|0|YES||NO
|[apprvl_dt](#apprvl_dt)|timestamp without time zone|0|YES||NO
|[trgt_dt](#trgt_dt)|timestamp without time zone|0|YES||NO
|[dt_creatd](#dt_creatd)|timestamp without time zone|0|YES||NO
|[dt_modfd](#dt_modfd)|timestamp without time zone|0|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|0|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|0|YES||NO
### cmplnt_key
#### Description

 

#### Value Range

ALL NUMBERS

#### Logic

Auto Increment sequence Generator


### cmplnt_id
#### Description

 

#### Value Range

ALL NUMBERS

#### Logic

 
HSG_Complaints.Id


### cmplnt_titl
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
Title

LTRIM(RTRIM(HSG_Complaints.Title))


### stat
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
HSG_Complaints.StatusValue 



### assgne
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
LKP.LKP_USERS(AssignedToId)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name


HSG_Complaints.AssignedToId 


### rec_type
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 ComplaintComplimentValue

LTRIM(RTRIM(HSG_Complaints.ComplaintComplimentValue)) 


### cat_val
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 CategoryValue

LTRIM(RTRIM(HSG_Complaints.CategoryValue))


### recvd_dt
#### Description

 

#### Value Range

DATE

#### Logic

 HSG_Complaints.DateReceived




### orig_dept
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 OriginValue

LTRIM(RTRIM(HSG_Complaints.OriginValue))


### cmplnt_type
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
WrittenOrVerbalValue

LTRIM(RTRIM(HSG_Complaints.WrittenOrVerbalValue))

### fdbk_rep
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 :LKP.LKP_USERS(RepresentativeTakingFeedbackId)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name


HSG_Complaints.RepresentativeTakingFeedbackId RepresentativeTakingFeedbackId

### acct_nbr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
AccountNumber

HSG_Complaints.AccountNumber AccountNumber

### fund_nbr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
Fund

LTRIM(RTRIM(HSG_Complaints.Fund))


### iss_desc
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
DescriptionOfIssue

SUBSTRING(LTRIM(RTRIM(HSG_Complaints.DescriptionOfIssue)),1,3999)

### soln_desc
#### Description

 

#### Value Range

NVARCHAR

#### Logic

DescriptionOfResolution
 

SUBSTRING(LTRIM(RTRIM(HSG_Complaints.DescriptionOfResolution)),1,3999

### addl_info
#### Description

 

#### Value Range

NVARCHAR

#### Logic

AdditionalInformation
 
SUBSTRING(LTRIM(RTRIM(HSG_Complaints.AdditionalInformation)),1,3999)


### acct_comprmed_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

CompromisedAccountYNValue
 
LTRIM(RTRIM(HSG_Complaints.CompromisedAccountYNValue))


### tckt_id
#### Description

 

#### Value Range

NVARCHAR

#### Logic

RemedyTicket
 
LTRIM(RTRIM(HSG_Complaints.RemedyTicket))


### prnc_revw_dt
#### Description

 

#### Value Range

DATE

#### Logic

DatePrincipalReviewed
 

HSG_Complaints.DatePrincipalReviewed DatePrincipalReviewed

### wrttn_cmplnt_id
#### Description

 

#### Value Range

NVARCHAR

#### Logic

WrittenComplaintIdentifier
 
LTRIM(RTRIM(HSG_Complaints.WrittenComplaintIdentifier)) WrittenComplaintIdentifier


### lgl_stat
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
LegalStatusValue

LTRIM(RTRIM(HSG_Complaints.LegalStatusValue)) LegalStatusValue

### finra_filng_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
FiledWithFINRAValue
LTRIM(RTRIM(HSG_Complaints.FiledWithFINRAValue)) FiledWithFINRAValue


### finra_filng_dt
#### Description

 

#### Value Range

DATE

#### Logic

DateOfFINRAFiling
 

HSG_Complaints.DateOfFINRAFiling DateOfFINRAFiling

### aging
#### Description

 

#### Value Range

NUMBERS`

#### Logic

 
Aging



### bkup_docn_link
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
BackupDocumentation

LTRIM(RTRIM(HSG_Complaints.BackupDocumentation)) BackupDocumentation


### ceo_escl_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

EscalatedToDaveVanHooserValue
 
LTRIM(RTRIM(HSG_Complaints.EscalatedToDaveVanHooserValue))


### ceo_escl_dt
#### Description

 

#### Value Range

DATE

#### Logic

DateEscalatedToDaveVanHooser
 

HSG_Complaints.DateEscalatedToDaveVanHooser

### cco_escl_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

EscalatedToCharlieMcCainValue
 
LTRIM(RTRIM(HSG_Complaints.EscalatedToCharlieMcCainValue)) EscalatedToCharlieMcCainV


### cco_escl_dt
#### Description

 

#### Value Range

DATE

#### Logic

DateEscalatedToCharlieMcCain
 
HSG_Complaints.DateEscalatedToDaveVanHooser DateEscalatedToDaveVanHooser


### cmo_escl_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 

EscalatedToHughKellyValue

LTRIM(RTRIM(HSG_Complaints.EscalatedToHughKellyValue)) EscalatedToH

### cmo_escl_dt
#### Description

 

#### Value Range

DATE`

#### Logic

 
DateEscalatedToHughKelly

HSG_Complaints.DateEscalatedToHughKelly DateEscalatedToHughKelly


### addr_line
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
Address

LTRIM(RTRIM(HSG_Complaints.Address)) Address

### city
#### Description

 

#### Value Range

NVARCHAR

#### Logic

City
 LTRIM(RTRIM(HSG_Complaints.City)) City



### st
#### Description

 

#### Value Range

NVARCHAR

#### Logic

StateValue
 
LTRIM(RTRIM(HSG_Complaints.StateValue)) StateValue


### zip_cd
#### Description

 

#### Value Range

NVARCHAR

#### Logic

ZipCode
 
LTRIM(RTRIM(HSG_Complaints.ZipCode)) ZipCode


### push_ntfn_val
#### Description

 

#### Value Range

NVARCHAR

#### Logic

PushNotifValue
 
LTRIM(RTRIM(HSG_Complaints.PushNotifValue)) PushNotifValue


### push_ntfn_id
#### Description

 

#### Value Range

NVARCHAR

#### Logic

PushNotification
 
LTRIM(RTRIM(HSG_Complaints.PushNotification)) PushNotification


### push_ntfn_email
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
PushNotificationEmail

LTRIM(RTRIM(HSG_Complaints.PushNotificationEmail)) PushNotificationEmail

### finra_rptg_per
#### Description

 

#### Value Range

NVARCHAR

#### Logic

FINRAReportingPeriodValue
 
 LTRIM(RTRIM(HSG_Complaints.FINRAReportingPeriodValue)) FINRAReportingPeriodValue



### prnc_revwer
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
:LKP.LKP_USERS(PrincipalReviewId)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name


HSG_Complaints.PrincipalReviewId PrincipalReviewId

### bmg_iss_link
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 

BMGIssue

LTRIM(RTRIM(HSG_Complaints.BMGIssue)) BMGIssue

### hsg_rsln_dt
#### Description

 

#### Value Range

DATE

#### Logic

 
DateResolvedHSG
HSG_Complaints.DateResolvedHSG DateResolvedHSG


### list_hddn_vrsn
#### Description

 

#### Value Range

NUMBER

#### Logic

Owshiddenversion
 
HSG_Complaints.Owshiddenversion Owshiddenversion


### list_vrsn
#### Description

 

#### Value Range

NUMBER

#### Logic

Version
 
 HSG_Complaints.Version Version



### url_path
#### Description

 

#### Value Range

NVARCHAR

#### Logic

Path
 

LTRIM(RTRIM(HSG_Complaints.Path)) Path

### bus_area
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
BusinessAreaValue

LTRIM(RTRIM(HSG_Complaints.BusinessAreaValue)) BusinessAreaValue


### bus_drvr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

BusinessDriverValue
 
LTRIM(RTRIM(HSG_Complaints.BusinessDriverValue)) BusinessDriverValue


### prog
#### Description

 

#### Value Range

NVARCHAR

#### Logic

ProgramValue
 
 LTRIM(RTRIM(HSG_Complaints.ProgramValue)) ProgramValue



### proj_nm
#### Description

 

#### Value Range

NVARCHAR

#### Logic

ProjectName
 
LTRIM(RTRIM(HSG_Complaints.ProjectName)) ProjectName


### hsg_dept
#### Description

 

#### Value Range

NVARCHAR

#### Logic

HSGDepartmentValue
 

LTRIM(RTRIM(HSG_Complaints.HSGDepartmentValue)) HSGDepartmentValue

### chg_bus_rsn
#### Description

 

#### Value Range

NVARCHAR

#### Logic

BusinessReasonForChange
 
LTRIM(RTRIM(HSG_Complaints.BusinessReasonForChange)) BusinessReasonForChange


### chg_desc
#### Description

 

#### Value Range

NVARCHAR

#### Logic

DescriptionOfChangeS
 
 LTRIM(RTRIM(HSG_Complaints.DescriptionOfChangeS)) DescriptionOfCh
### cmmt
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
Comments

SUBSTRING(LTRIM(RTRIM(HSG_Complaints.Comments)),1,3000)

### pres_dt
#### Description

 

#### Value Range

DATE

#### Logic

 
HSG_Complaints.DatePresented



### apprvl_dt
#### Description

 

#### Value Range

DATE

#### Logic

HSG_Complaints.DateApproved
 



### trgt_dt
#### Description

 

#### Value Range

DATE

#### Logic

HSG_Complaints.TargetDate
 



### link_1
#### Description

 

#### Value Range

NVARCHAR

#### Logic

Link1
 
LTRIM(RTRIM(HSG_Complaints.Link1))


### link_2
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
Link2

LTRIM(RTRIM(HSG_Complaints.Link2))

### tm_est
#### Description

 

#### Value Range

NVARCHAR

#### Logic

EstimatedTime
 
LTRIM(RTRIM(HSG_Complaints.EstimatedTime)) EstimatedTime


### rsrc_est
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
EstimatedResources

LTRIM(RTRIM(HSG_Complaints.EstimatedResources))

### vldn_cpltn
#### Description

 

#### Value Range

NUMBER

#### Logic

ValidatedComplete
 
LTRIM(RTRIM(HSG_Complaints.ValidatedComplete))


### qtr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
YearQuarter

LTRIM(RTRIM(HSG_Complaints.YearQuarter)) YearQuarter

### chg_cntl_wkfl
#### Description

 

#### Value Range

NUMBER

#### Logic

 
ChangeControlWorkflow

LTRIM(RTRIM(HSG_Complaints.ChangeControlWorkflow)) ChangeControlWorkflow


### proj_mgr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

:LKP.LKP_USERS(ProjectManagerId)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name
 
LTRIM(RTRIM(HSG_Complaints.ProjectManagerId)) ProjectManagerId

### creatr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 

:LKP.LKP_USERS(CreatedById)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 
Return Column: Name

HSG_Complaints.CreatedById CreatedById

### modfr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
:LKP.LKP_USERS(ModifiedById)

Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name

HSG_Complaints.ModifiedById ModifiedById

### dt_creatd
#### Description

 

#### Value Range

DATE

#### Logic

Created
 

HSG_Complaints.Created Created

### dt_modfd
#### Description

 

#### Value Range

DATE

#### Logic

Modified
 

HSG_Complaints.Modified Modified

### cmplnt_yr
#### Description

 

#### Value Range

NUMBER

#### Logic

Year
 
HSG_Complaints.Year [Year]


### cmplnt_mo
#### Description

 

#### Value Range

NVARCHAR

#### Logic

 
MonthValue

HSG_Complaints.MonthValue MonthValue

### curr_row_flg
#### Description

 

#### Value Range

NVARCHAR

#### Logic

'Y'
 



### row_strt_dttm
#### Description

 

#### Value Range

DATE

#### Logic

SYSDATE
 



### row_stop_dttm
#### Description

 

#### Value Range

DATE

#### Logic

 



### etl_load_cyc_key
#### Description

 

#### Value Range

number(p,s)


#### Logic

 



### src_sys_id
#### Description

 

#### Value Range

number


#### Logic

 



