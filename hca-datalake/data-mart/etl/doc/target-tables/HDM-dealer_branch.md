 # DEALER_BRANCH

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[dlr_id](#dlr_id)|character varying|25|YES||NO
|[branch_id](#branch_id)|character varying|25|YES||NO
|[branch_nm](#branch_nm)|character varying|60|YES||NO
|[branch_addr_line_1](#branch_addr_line_1)|character varying|35|YES||NO
|[branch_addr_line_2](#branch_addr_line_2)|character varying|35|YES||NO
|[branch_addr_line_3](#branch_addr_line_3)|character varying|35|YES||NO
|[branch_addr_line_4](#branch_addr_line_4)|character varying|35|YES||NO
|[branch_city](#branch_city)|character varying|35|YES||NO
|[branch_st_cd](#branch_st_cd)|character varying|3|YES||NO
|[branch_st](#branch_st)|character varying|35|YES||NO
|[branch_crty](#branch_crty)|character varying|35|YES||NO
|[branch_zip_cd](#branch_zip_cd)|character varying|25|YES||NO
|[branch_pay_ofc_id](#branch_pay_ofc_id)|character varying|25|YES||NO
|[branch_func_type_id](#branch_func_type_id)|character varying|3|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[dlr_branch_key](#dlr_branch_key)|integer|(32,0)|NO||YES
|[branch_forgn_tax_cd](#branch_forgn_tax_cd)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|NO||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[branch_estb_dt](#branch_estb_dt)|timestamp without time zone|6|YES||NO
|[branch_closed_dt](#branch_closed_dt)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO


### dlr_branch_key
#### Description



#### Value Range

NUMBERS

#### Logic

Auto Increment sequence Generator 



### dlr_id
#### Description



#### Value Range

VARCHAR2

#### Logic


DEALER_BRANCH.FINANCIAL-INST-ID



### branch_id
#### Description



#### Value Range

VARCHAR2

#### Logic


DEALER_BRANCH.FINCL-INST-BRCH-ID 



### branch_nm
#### Description



#### Value Range

VARCHAR2

#### Logic


DEALER_BRANCH.FI-BRCH-NAME



### branch_addr_line_1
#### Description



#### Value Range

VARCHAR2

#### Logic


DEALER_BRANCH.FI-BRCH-ADDR-1-TXT


### branch_addr_line_2
#### Description



#### Value Range

VARCHAR2

#### Logic



DEALER_BRANCH.FI-BRCH-ADDR-2-TXT 

### branch_addr_line_3
#### Description



#### Value Range

VARCHAR2

#### Logic

DEALER_BRANCH.FI-BRCH-ADDR-3-TXT 



### branch_addr_line_4
#### Description



#### Value Range

VARCHAR2

#### Logic

DEALER_BRANCH.FI-BRCH-ADDR-4-TXT 



### branch_city
#### Description



#### Value Range

VARCHAR2

#### Logic


DEALER_BRANCH.FI-BRCH-CITY-NME


### branch_st_cd
#### Description



#### Value Range

VARCHAR2

#### Logic

STATE.ST_CD


DEALER_BRANCH.FI-BRCH-ST-CO-CDE = STATE.DST_ST_CD




### branch_st
#### Description



#### Value Range

VARCHAR2

#### Logic


STATE.ST_NM


DEALER_BRANCH.FI-BRCH-ST-CO-CDE = STATE.DST_ST_CD


### branch_crty
#### Description



#### Value Range

VARCHAR2

#### Logic



DEALER_BRANCH.FI-BRCH-ST-CO-CDE


### branch_zip_cd
#### Description



#### Value Range

VARCHAR2

#### Logic


DEALER_BRANCH.FI-BRCH-ZIP-CDE


### branch_estb_dt
#### Description



#### Value Range

DATE

#### Logic

 DEALER_BRANCH.FI-BRCH-ESTB-DTE



### branch_closed_dt
#### Description



#### Value Range

DATE

#### Logic


DEALER_BRANCH.FI-BRCH-CLOSED-DTE


### branch_pay_ofc_id
#### Description



#### Value Range

VARCHAR2

#### Logic


DEALER_BRANCH.FI-BRCH-PAY-OFC-ID


### branch_func_type_id
#### Description



#### Value Range

VARCHAR2

#### Logic


DEALER_BRANCH.FI-FG-BR-FN-TYP-ID


### branch_forgn_tax_cd
#### Description



#### Value Range

NUMBER

#### Logic


DEALER_BRANCH.FI-BR-FOREIGN-TAX



### curr_row_flg
#### Description



#### Value Range

VARCHAR2

#### Logic





### row_strt_dttm
#### Description



#### Value Range

DATE

#### Logic





### row_stop_dttm
#### Description



#### Value Range

DATE

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






### Files used 

1. DEALER_BRANCH (DTO.HCA.FBP.TIP55.YYYYMMDD.HHMISS.zip.pgp.txt)


### File Location

dstprod/DTO.HCA.FBP.TIP55.20190719.231343.zip.pgp.txt



### Tables Used

1. STATE



### Business Key

FINANCIAL-INST-ID  
FINCL-INST-BRCH-ID 


### COBOL Files

FBP.TIP55.CBL

