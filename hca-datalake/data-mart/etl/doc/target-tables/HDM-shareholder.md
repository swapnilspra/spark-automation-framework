# SHAREHOLDER

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[shrhldr_top_lvl_cd](#shrhldr_top_lvl_cd)|character varying|3|YES||NO
|[shrhldr_top_lvl_desc](#shrhldr_top_lvl_desc)|character varying|60|YES||NO
|[shrhldr_nm](#shrhldr_nm)|character varying|60|YES||NO
|[ssn_stat_desc](#ssn_stat_desc)|character varying|25|YES||NO
|[tax_idnfcn_type](#tax_idnfcn_type)|character varying|25|YES||NO
|[vip_cust_flg](#vip_cust_flg)|character varying|1|YES||NO
|[send_mktg_info_flg](#send_mktg_info_flg)|character varying|1|YES||NO
|[informed_cnsnt_flg](#informed_cnsnt_flg)|character varying|1|YES||NO
|[party_type](#party_type)|character varying|60|YES||NO
|[cust_empl_flg](#cust_empl_flg)|character varying|1|YES||NO
|[mgmt_co_empl_flg](#mgmt_co_empl_flg)|character varying|1|YES||NO
|[lgl_stat](#lgl_stat)|character varying|60|YES||NO
|[gend](#gend)|character varying|60|YES||NO
|[emplmt_stat](#emplmt_stat)|character varying|25|YES||NO
|[mrtl_stat](#mrtl_stat)|character varying|25|YES||NO
|[crty_of_citzn_ship](#crty_of_citzn_ship)|character varying|60|YES||NO
|[addr_type](#addr_type)|character varying|60|YES||NO
|[addr_line_1](#addr_line_1)|character varying|60|YES||NO
|[addr_line_2](#addr_line_2)|character varying|60|YES||NO
|[addr_line_3](#addr_line_3)|character varying|60|YES||NO
|[addr_line_4](#addr_line_4)|character varying|60|YES||NO
|[city](#city)|character varying|60|YES||NO
|[st](#st)|character varying|2|YES||NO
|[prov](#prov)|character varying|60|YES||NO
|[crty](#crty)|character varying|60|YES||NO
|[reg](#reg)|character varying|60|YES||NO
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[purg_prcsd_flg](#purg_prcsd_flg)|character varying|1|YES||NO
|[shrhldr_key](#shrhldr_key)|integer|(32,0)|NO||YES
|[shrhldr_role_key](#shrhldr_role_key)|integer|(32,0)|NO||NO
|[tax_idnfcn_nbr](#tax_idnfcn_nbr)|numeric|(38,15)|YES||NO
|[cust_idnfcn_nbr](#cust_idnfcn_nbr)|numeric|(38,15)|NO||NO
|[cust_idnfcn_nbr_2](#cust_idnfcn_nbr_2)|numeric|(38,15)|NO||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[cust_lgl_ownr_regstn_dt](#cust_lgl_ownr_regstn_dt)|timestamp without time zone|6|YES||NO
|[brth_dt](#brth_dt)|timestamp without time zone|6|YES||NO
|[dcsd_dt](#dcsd_dt)|timestamp without time zone|6|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO
|[purg_prcsd_dt](#purg_prcsd_dt)|timestamp without time zone|6|YES||NO


### shrhldr_key
#### Description



#### Value Range

N/A

#### Logic


Auto Increment Sequence generator


### shrhldr_role_key
#### Description



#### Value Range

N/A

#### Logic


NULL



### shrhldr_top_lvl_cd
#### Description



#### Value Range

N/A

#### Logic


'All'


### shrhldr_top_lvl_desc
#### Description



#### Value Range

N/A

#### Logic


'All Shareholders'


### shrhldr_nm
#### Description



#### Value Range

N/A

#### Logic


IIF(SHAREHOLDER.CMR-TYP-CDE = 'I',
IIF(
ISNULL(SHAREHOLDER.CMR-FRST-NME ) AND
ISNULL(SHAREHOLDER.CMR-MDL-NME) AND
ISNULL(SHAREHOLDER.CMR-CRP-LST-NME) AND
ISNULL(SHAREHOLDER.CMR-NME-SFX-TXT),
'Name withheld. Maybe Omnibus Account',LTRIM(RTRIM(
SHAREHOLDER.CMR-NME-PRE-TXT || ' ' || SHAREHOLDER.CMR-FRST-NME || ' ' || SHAREHOLDER.CMR-MDL-NME || ' ' || SHAREHOLDER.CMR-CRP-LST-NME || SHAREHOLDER.CMR-NME-SFX-TXT))),SHAREHOLDER.CUSTOMER-CORPORATE-NM )


### tax_idnfcn_nbr
#### Description



#### Value Range

N/A

#### Logic

SHAREHOLDER.CMR-TAX-ID



### ssn_stat_desc
#### Description



#### Value Range

N/A

#### Logic

SOCIAL_SECURITY_STATUS.SSN_STAT_DESC

-- Join on SOCIAL_SECURITY_STATUS table 

-- SOCIAL_SECURITY_STATUS.SSN_STAT_CD = SHAREHOLDER.CMR-TAX-ID-TYP-CDE

This column is not required as DST does not send a source value for thius attribute


### tax_idnfcn_type
#### Description



#### Value Range

N/A

#### Logic


SHAREHOLDER.CMR-TAX-ID-TYP-CDE


### cust_lgl_ownr_regstn_dt
#### Description



#### Value Range

N/A

#### Logic


SHAREHOLDER.CMR-LST-LNK-LOR


### cust_idnfcn_nbr
#### Description



#### Value Range

N/A

#### Logic


SHAREHOLDER.CMR-BPR-ID1


### cust_idnfcn_nbr_2
#### Description



#### Value Range

N/A

#### Logic

SHAREHOLDER.CMR-BPR-ID2



### vip_cust_flg
#### Description



#### Value Range

N/A

#### Logic

SHAREHOLDER.CMR-MNTR-VIP-CDE 



### send_mktg_info_flg
#### Description



#### Value Range

N/A

#### Logic


SHAREHOLDER.CMR-SEN-MKT-ML-CDE


### informed_cnsnt_flg
#### Description



#### Value Range

N/A

#### Logic


SHAREHOLDER.CMR-INFD-CSNT-CD


### party_type
#### Description



#### Value Range

N/A

#### Logic

PARTY_TYPE.PARTY_TYPE_NM

Join on PARTY_TYPE Table

SHAREHOLDER.CMR-TYP-CDE = PARTY_TYPE.PARTY_TYPE_CD



### cust_empl_flg
#### Description



#### Value Range

N/A

#### Logic


SHAREHOLDER.CMR-FS-EEE-CDE


### mgmt_co_empl_flg
#### Description



#### Value Range

N/A

#### Logic


'N'


### lgl_stat
#### Description



#### Value Range

N/A

#### Logic

CITIZENSHIP_STATUS.CITIZENSHIP_STAT_NM

Join on CITIZENSHIP_STATUS Table

CITIZENSHIP_STATUS.CITIZENSHIP_STAT_CD = SHAREHOLDER.CMR-CTZ-CDE



### gend
#### Description



#### Value Range

N/A

#### Logic

GENDER.GEND_NM

Join on GENDER Table

GENDER.GEND_CD = SHAREHOLDER.CMR-GND-CDE


### brth_dt
#### Description



#### Value Range

N/A

#### Logic

SHAREHOLDER.CMR-BTH-DTE 



### dcsd_dt
#### Description



#### Value Range

N/A

#### Logic


SHAREHOLDER.CMR-DCS-DTE


### emplmt_stat
#### Description



#### Value Range

N/A

#### Logic


NULL

### mrtl_stat
#### Description



#### Value Range

N/A

#### Logic

SHAREHOLDER.CMR-MRTL-STS-CDE



### crty_of_citzn_ship
#### Description



#### Value Range

N/A

#### Logic

SHAREHOLDER.CMR-CTZ-CDE



### addr_type
#### Description



#### Value Range

N/A

#### Logic


'Work'


### addr_line_1
#### Description



#### Value Range

N/A

#### Logic

SHAREHOLDER.LABEL-PTL-ADR-LIN-TXT1                



### addr_line_2
#### Description



#### Value Range

N/A

#### Logic

SHAREHOLDER.LABEL-PTL-ADR-LIN-TXT2




### addr_line_3
#### Description



#### Value Range

N/A

#### Logic



SHAREHOLDER.LABEL-PTL-ADR-LIN-TXT3



### addr_line_4
#### Description



#### Value Range

N/A

#### Logic



SHAREHOLDER.LABEL-PTL-ADR-LIN-TXT4



### city
#### Description



#### Value Range

N/A

#### Logic


SHAREHOLDER.PTL-ADR-CTY-NME


### st
#### Description



#### Value Range

N/A

#### Logic

STATE.ST_NM

Join with Region on REGION.REG_KEY = SHAREHOLDER.STATE-COUNTRY-CDE WHERE CTRY_CD = 'US'




### prov
#### Description



#### Value Range

N/A

#### Logic



NULL


### crty
#### Description



#### Value Range

N/A

#### Logic


NULL



### reg
#### Description



#### Value Range

N/A

#### Logic


NULL



### row_strt_dttm
#### Description



#### Value Range

N/A

#### Logic





### row_stop_dttm
#### Description



#### Value Range

N/A

#### Logic





### curr_row_flg
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

N/A

#### Logic





### purg_prcsd_flg
#### Description



#### Value Range

N/A

#### Logic


NULL


### purg_prcsd_dt
#### Description



#### Value Range

N/A

#### Logic

NULL


## Files Used

1. SHAREHOLDER (DTO.HCA.CLO.TIP42.YYYYMMDD.HHMISS.zip.pgp.txt)



###File Location

dstprod/DTO.HCA.CLO.TIP42.20191022.000309.zip.pgp.txt



## Tables used

3. REGION
4. GENDER
5. SOCIAL_SECURITY_STATUS
6. PARTY_TYPE
7. CITIZENSHIP_STATUS


##Business Keys

1. CUST_IDNFCN_NBR_1 
2. CUST_IDNFCN_NBR_2


#### COBOL File

CLO.TIP42.CBL