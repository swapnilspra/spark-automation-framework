# SPL_HSG_ISSUE_LIST

This table holds the  .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[iss_desc](#iss_desc)|character varying|4000|YES||NO
|[assgne](#assgne)|character varying|255|YES||NO
|[bmg_assgne](#bmg_assgne)|character varying|255|YES||NO
|[contnt_type](#contnt_type)|character varying|25|YES||NO
|[dlr_id](#dlr_id)|character varying|1000|YES||NO
|[dlr_iss_type](#dlr_iss_type)|character varying|1000|YES||NO
|[hsg_dept](#hsg_dept)|character varying|25|YES||NO
|[iss_orig](#iss_orig)|character varying|1000|YES||NO
|[ovst_rptb_flg](#ovst_rptb_flg)|character varying|1|YES||NO
|[sys_type](#sys_type)|character varying|1000|YES||NO
|[prio](#prio)|character varying|255|YES||NO
|[proj_nm](#proj_nm)|character varying|1000|YES||NO
|[proj_locn](#proj_locn)|character varying|4000|YES||NO
|[iss_rpter](#iss_rpter)|character varying|1000|YES||NO
|[rsrh_cmmt](#rsrh_cmmt)|character varying|4000|YES||NO
|[rsln](#rsln)|character varying|4000|YES||NO
|[awd_send_flg](#awd_send_flg)|character varying|1|YES||NO
|[tckt_nbr](#tckt_nbr)|character varying|255|YES||NO
|[iss_titl](#iss_titl)|character varying|1000|YES||NO
|[rqst_type](#rqst_type)|character varying|255|YES||NO
|[fan_usr_id](#fan_usr_id)|character varying|25|YES||NO
|[vend_impct](#vend_impct)|character varying|4000|YES||NO
|[fan_vrsn](#fan_vrsn)|character varying|25|YES||NO
|[brwsr_nm](#brwsr_nm)|character varying|1000|YES||NO
|[prog_nm](#prog_nm)|character varying|255|YES||NO
|[bus_drvr](#bus_drvr)|character varying|255|YES||NO
|[iss_stat](#iss_stat)|character varying|25|YES||NO
|[aml_recertfn](#aml_recertfn)|character varying|255|YES||NO
|[fincen_revw](#fincen_revw)|character varying|255|YES||NO
|[trgt_ddt_rvsr](#trgt_ddt_rvsr)|character varying|255|YES||NO
|[creatr](#creatr)|character varying|25|YES||NO
|[modfr](#modfr)|character varying|25|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[dlr_grp_id](#dlr_grp_id)|character varying|25|YES||NO
|[dlr_id_list](#dlr_id_list)|character varying|255|YES||NO
|[iss_list_key](#iss_list_key)|numeric|(10,0)|NO||YES
|[etl_load_cyc_key](#etl_load_cyc_key)|numeric|(10,0)|YES||NO
|[ddt](#ddt)|timestamp without time zone|0|YES||NO
|[orig_trgt_ddt](#orig_trgt_ddt)|timestamp without time zone|0|YES||NO
|[rvsd_trgt_ddt](#rvsd_trgt_ddt)|timestamp without time zone|0|YES||NO
|[creatn_dt](#creatn_dt)|timestamp without time zone|0|YES||NO
|[last_modfd_dt](#last_modfd_dt)|timestamp without time zone|0|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|0|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|0|YES||NO
|[iss_creatn_dt](#iss_creatn_dt)|timestamp without time zone|0|YES||NO
|[iss_closout_dt](#iss_closout_dt)|timestamp without time zone|0|YES||NO
### iss_list_key
#### Description



#### Value Range

ALL NUMBERS

#### Logic

Auto Increment sequence Generator 

#### iss_id
 ID

### Value Range

 ALL NUMBERS

#### Logic
 
hsg_intermediary_list.id


### iss_desc
#### Description



#### Value Range

ALL STRINGS

#### Logic

substr(Description,1,1999)

cast([dbo].[fn_cleanandtrim](ltrim(rtrim(replace (replace (replace ([dbo].[udf_striphtml]([hsg_intermediary_list].[description])        ,'?',''),'&#160;',' '),'&#58;',' - '))),' ','',0)  as varchar(2000)) as description



### acct_nbr


#### Value Range

ALL NUMBERS

#### Logic

 hsg_intermediary_list.accountnumber

### agng


#### Value Range

ALL NUMBERS

#### Logic

hsg_intermediary_list.aging

### assgne

#### Value Range

NVARCHAR

#### Logic
IIF(ISNULL(:LKP.LKP_USERID(AssignedToId)),TO_CHAR(AssignedToId),(:LKP.LKP_USERID(AssignedToId)) )

LOOK UP OVERRIDE- select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'
Condition: Id = Id1 
Return Column: Name

### bmg_assgne

#### Value Range

NVARCHAR

#### Logic

IIF(ISNULL(:LKP.LKP_USERID(BMGAssignmentId)),TO_CHAR(BMGAssignmentId),(:LKP.LKP_USERID(BMGAssignmentId)) )	

LOOK UP OVERRIDE : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'
Condition: Id = Id1 
Return Column: Name



### contnt_type


#### Value Range

NVARCHAR

#### Logic
 hsg_intermediary_list.contenttype

### dlr_id


#### Value Range

N/A

#### Logic

hsg_intermediary_list.dlr_id
IIF(NOT ISNULL(LTRIM(RTRIM(DLR_ID))), TO_INTEGER(LTRIM(RTRIM(DLR_ID))),NULL)


### dlr_iss_type


#### Value Range

VARCHAR

#### Logic

hsg_intermediary_list.dealerissuetypevalue

### hsg_dept

#### Value Range

NVARCHAR

#### Logic

hsg_intermediary_list.hsgdepartmentvalue

### ddt

#### Value Range
DATE

#### Logic


### fund_nbr


#### Value Range

ALL NUMBERS

#### Logic
hsg_intermediary_list.fundnumber


### iss_orig

#### Value Range

NVARCHAR

#### Logic

hsg_intermediary_list.issueoriginvalue


### ovst_rptb_flg
#### Description

#### Value Range

NVARCHAR

#### Logic

hsg_intermediary_list.oversightreportable


### sys_type
#### Description


#### Value Range

NVARCHAR

#### Logic

hsg_intermediary_list.typeofissuevalue


### prio
#### Description



#### Value Range

NVARCHAR

#### Logic

hsg_intermediary_list.priorityvalue

### proj_nm
#### Description

#### Value Range

N/A

#### Logic

substr(Project,1,1999)

cast([dbo].[fn_cleanandtrim](ltrim(rtrim(replace (replace (replace ([dbo].[udf_striphtml]([hsg_intermediary_list].[project])        ,'?',''),'&#160;',' '),'&#58;',' - '))),' ','',0) as varchar(2000)) as project,



### proj_locn
#### Description

#### Value Range

NVARCHAR

#### Logic

substr(ProjectDocumentation,1,1999)


### iss_rpter
#### Description

#### Value Range

NVARCHAR

#### Logic

IIF(ISNULL(:LKP.LKP_USERID(ReportedById)),TO_CHAR(ReportedById),(:LKP.LKP_USERID(ReportedById)) )

"
Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name"


### rsrh_cmmt
#### Description

#### Value Range

NVARCHAR

#### Logic

substr(ResearchCommentary,1,1999)

cast([dbo].[fn_cleanandtrim](ltrim(rtrim(replace (replace (replace ([dbo].[udf_striphtml]([hsg_intermediary_list].[researchcommentary] ),'?',''),'&#160;',' '),'&#58;',' - '))),' ','',0)  as varchar(2000)) as researchcommentary

### rsln
#### Description


#### Value Range

NVARCHAR

#### Logic

substr(Resolution,1.1999)

cast([dbo].[fn_cleanandtrim](ltrim(rtrim(replace (replace (replace ([dbo].[udf_striphtml]([hsg_intermediary_list].[resolution] )        ,'?',''),'&#160;',' '),'&#58;',' - '))),' ','',0)  as varchar(2000)) as resolution,

### awd_send_flg
#### Description



#### Value Range

NVARCHAR

#### Logic

hsg_intermediary_list.SendToAWD



### tckt_nbr
#### Description

#### Value Range

NVARCHAR

#### Logic

hsg_intermediary_list.Ticket

### iss_titl
#### Description

 

#### Value Range

NVARCHAR


#### Logic

hsg_intermediary_list.Title


### rqst_type
#### Description

 

#### Value Range

NVARCHAR


#### Logic


hsg_intermediary_list.TypeOfRequestValue

### fan_usr_id
#### Description

 

#### Value Range

NVARCHAR


#### Logic

hsg_intermediary_list.UserIDFANOnly





### vend_impct
#### Description

 

#### Value Range

NVARCHAR


#### Logic





### fan_vrsn
#### Description

 

#### Value Range

N/A

#### Logic

 



### brwsr_nm
#### Description

 

#### Value Range

NVARCHAR


#### Logic

hsg_intermediary_list.WebBrowserVersionFANOnly




### prog_nm
#### Description

 

#### Value Range

N/A

#### Logic

hsg_intermediary_list.ProgramValue




### bus_drvr
#### Description

 

#### Value Range

NVARCHAR


#### Logic

hsg_intermediary_list.BusinessDriverValue



### iss_stat
#### Description

 

#### Value Range

NVARCHAR


#### Logic

hsg_intermediary_list.StatusValue



### aml_recertfn
#### Description

 

#### Value Range

NVARCHAR


#### Logic

hsg_intermediary_list.AMLReCertification



### fincen_revw
#### Description

 

#### Value Range

NVARCHAR


#### Logic

hsg_intermediary_list.C_314a




### orig_trgt_ddt
#### Description

 

#### Value Range

date


#### Logic

hsg_intermediary_list.OriginalTargetDueDate




### rvsd_trgt_ddt
#### Description

 

#### Value Range

date


#### Logic

hsg_intermediary_list.RevisedTargetDueDate




### trgt_ddt_rvsr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

hsg_intermediary_list.TargetDueDateRevisedByValue





### creatr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

IIF(ISNULL(:LKP.LKP_USERID(CreatedById)),TO_CHAR(CreatedById),(:LKP.LKP_USERID(CreatedById)) )


Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name





### creatn_dt
#### Description

 

#### Value Range

DATE

#### Logic

Created





### modfr
#### Description

 

#### Value Range

NVARCHAR

#### Logic

IIF(ISNULL(:LKP.LKP_USERID(ModifiedById)),TO_CHAR(ModifiedById),(:LKP.LKP_USERID(ModifiedById)) )


Override Query : select dbo.itlistusers.name as name, dbo.itlistusers.id as id from dbo.itlistusers
where ltrim(rtrim(contenttype)) = 'Person'

Condition: Id = Id1 

Return Column: Name





### last_modfd_dt
#### Description

 

#### Value Range

DATE

#### Logic

Modified





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

SESSSTARTTIME





### row_stop_dttm
#### Description

 

#### Value Range

N/A

#### Logic





### etl_load_cyc_key
#### Description

 

#### Value Range

N/A

#### Logic




### src_sys_id
#### Description

 

#### Value Range

NUMBERS

#### Logic

'6'




### dlr_grp_id
#### Description

 

#### Value Range

N/A

#### Logic





### dlr_id_list
#### Description

 

#### Value Range

N/A

#### Logic





### iss_creatn_dt
#### Description

 

#### Value Range

N/A

#### Logic

 



### iss_closout_dt
#### Description

 

#### Value Range

N/A

#### Logic





