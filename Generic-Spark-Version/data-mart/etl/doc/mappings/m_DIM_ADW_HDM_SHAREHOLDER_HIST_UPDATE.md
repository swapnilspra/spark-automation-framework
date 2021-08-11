|	Source File Name	|	Column Name	|	Data Type	|	Length	|	Precision	|	Nullable	|	PK	|	BK	|		|		|		|		|	Target Table Name	|	Column Name	|	Data Type	|	Length	|	Nullable	|	PK	|
|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|	---	|
|	Refer SQ Query	|	SHRHLDR_ROLE_KEY	|	decimal	|	10	|	9	|		|		|	Seq Number	|		|		|		|	//If record already exisit update the record<br>Table : HDM.SHAREHOLDER<br>Condition: CUST_IDNFCN_NBR = IN_CUST_IDNFCN_NBR AND CUST_IDNFCN_NBR_2 = IN_CUST_IDNFCN_NBR_2	|	SHAREHOLDER	|	SHRHLDR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	CUST_ID	|	decimal	|	11	|		|		|		|	SHRHLDR_ROLE_KEY	|		|		|		|		|	SHAREHOLDER	|	SHRHLDR_ROLE_KEY	|	"number(p,s)"	|	10	|		|		|
|		|	CUST_ID_2	|	decimal	|	11	|		|		|		|		|	'All'	|		|		|		|	SHAREHOLDER	|	SHRHLDR_TOP_LVL_CD	|	varchar2	|	3	|		|		|
|		|	TAX_ID	|	decimal	|	11	|		|		|		|		|	'All Shareholders'	|		|		|		|	SHAREHOLDER	|	SHRHLDR_TOP_LVL_DESC	|	varchar2	|	60	|		|		|
|		|	CUST_TAX_IDNFCN_TYPE	|	string	|	25	|		|		|		|	PARTY_FRMTD_NM	|		|		|		|		|	SHAREHOLDER	|	SHRHLDR_NM	|	varchar2	|	60	|		|		|
|		|	LGL_OWNRP_REGSTN_DT	|	date/time	|	29	|		|		|		|	TAX_ID	|		|		|		|		|	SHAREHOLDER	|	TAX_IDNFCN_NBR	|	"number(p,s)"	|	9	|		|		|
|		|	CUST_VIP_FLG	|	string	|	1	|		|		|		|	STAT_DESC	|		|		|		|		|	SHAREHOLDER	|	SSN_STAT_DESC	|	varchar2	|	25	|		|		|
|		|	SEND_MKTG_INFO_FLG	|	string	|	1	|		|		|		|	CUST_TAX_IDNFCN_TYPE	|		|		|		|		|	SHAREHOLDER	|	TAX_IDNFCN_TYPE	|	varchar2	|	25	|		|		|
|		|	INFORMED_CNSNT_FLG	|	string	|	1	|		|		|		|	LGL_OWNRP_REGSTN_DT	|		|		|		|		|	SHAREHOLDER	|	CUST_LGL_OWNR_REGSTN_DT	|	date	|	19	|		|		|
|		|	PARTY_ROLE_KEY	|	decimal	|	10	|		|		|		|	CUST_ID	|		|		|		|		|	SHAREHOLDER	|	CUST_IDNFCN_NBR	|	number	|	15	|		|		|
|		|	PARTY_KEY	|	decimal	|	10	|		|		|		|	CUST_ID_2	|		|		|		|		|	SHAREHOLDER	|	CUST_IDNFCN_NBR_2	|	number	|	15	|		|		|
|		|	PARTY_FRMTD_NM	|	string	|	60	|		|		|		|	CUST_VIP_FLG	|		|		|		|		|	SHAREHOLDER	|	VIP_CUST_FLG	|	varchar2	|	1	|		|		|
|		|	ACCT_MGMT_CO_EMPL_FLG	|	string	|	1	|		|		|		|	SEND_MKTG_INFO_FLG	|		|		|		|		|	SHAREHOLDER	|	SEND_MKTG_INFO_FLG	|	varchar2	|	1	|		|		|
|		|	CUST_EMPL_FLG	|	string	|	1	|		|		|		|	SEND_MKTG_INFO_FLG	|		|		|		|		|	SHAREHOLDER	|	INFORMED_CNSNT_FLG	|	varchar2	|	1	|		|		|
|		|	PARTY_TYPE_KEY	|	decimal	|	10	|		|		|		|	PARTY_TYPE_NM	|		|		|		|		|	SHAREHOLDER	|	PARTY_TYPE	|	varchar2	|	60	|		|		|
|		|	PARTY_TYPE_NM	|	string	|	60	|		|		|		|	CUST_EMPL_FLG	|		|		|		|		|	SHAREHOLDER	|	CUST_EMPL_FLG	|	varchar2	|	1	|		|		|
|		|	MRTL_STAT_CD	|	string	|	3	|		|		|		|	ACCT_MGMT_CO_EMPL_FLG	|		|		|		|		|	SHAREHOLDER	|	MGMT_CO_EMPL_FLG	|	varchar2	|	1	|		|		|
|		|	BRTH_DT	|	date/time	|	29	|		|		|		|	CITIZENSHIP_STAT_NM	|		|		|		|		|	SHAREHOLDER	|	LGL_STAT	|	varchar2	|	60	|		|		|
|		|	DCSD_DT	|	date/time	|	29	|		|		|		|	GEND_NM	|		|		|		|		|	SHAREHOLDER	|	GEND	|	varchar2	|	60	|		|		|
|		|	GEND_NM	|	string	|	60	|		|		|		|	BRTH_DT	|		|		|		|		|	SHAREHOLDER	|	BRTH_DT	|	date	|	19	|		|		|
|		|	CRTY_KEY	|	decimal	|	10	|		|		|		|	DCSD_DT	|		|		|		|		|	SHAREHOLDER	|	DCSD_DT	|	date	|	19	|		|		|
|		|	CITIZENSHIP_STAT_NM	|	string	|	60	|		|		|		|		|		|		|		|		|	SHAREHOLDER	|	EMPLMT_STAT	|	varchar2	|	25	|		|		|
|		|	LOCN_LINE_1_ADDR	|	string	|	60	|		|		|		|	MRTL_STAT_CD	|		|		|		|		|	SHAREHOLDER	|	MRTL_STAT	|	varchar2	|	25	|		|		|
|		|	LOCN_LINE_2_ADDR	|	string	|	60	|		|		|		|		|		|	Table Name: ADW.COUNTRY<br>Condition: CRTY_KEY = in_CRTY_KEY<br>Output Column: CRTY_FULL_NM	|		|		|	SHAREHOLDER	|	CRTY_OF_CITZN_SHIP	|	varchar2	|	60	|		|		|
|		|	LOCN_LINE_3_ADDR	|	string	|	60	|		|		|		|	LOCN_TYPE_NM	|		|		|		|		|	SHAREHOLDER	|	ADDR_TYPE	|	varchar2	|	60	|		|		|
|		|	LOCN_LINE_4_ADDR	|	string	|	60	|		|		|		|	LOCN_LINE_1_ADDR	|		|		|		|		|	SHAREHOLDER	|	ADDR_LINE_1	|	varchar2	|	60	|		|		|
|		|	CITY_NM	|	string	|	60	|		|		|		|	LOCN_LINE_2_ADDR	|		|		|		|		|	SHAREHOLDER	|	ADDR_LINE_2	|	varchar2	|	60	|		|		|
|		|	LOCN_TYPE_NM	|	string	|	60	|		|		|		|	LOCN_LINE_3_ADDR	|		|		|		|		|	SHAREHOLDER	|	ADDR_LINE_3	|	varchar2	|	60	|		|		|
|		|	CRTY_FULL_NM	|	string	|	60	|		|		|		|	LOCN_LINE_4_ADDR	|		|		|		|		|	SHAREHOLDER	|	ADDR_LINE_4	|	varchar2	|	60	|		|		|
|		|	ST_CD	|	string	|	3	|		|		|		|	CITY_NM	|		|		|		|		|	SHAREHOLDER	|	CITY	|	varchar2	|	60	|		|		|
|		|	STAT_DESC	|	string	|	255	|		|		|		|	ST_CD	|		|		|		|		|	SHAREHOLDER	|	ST	|	varchar2	|	2	|		|		|
|		|	EMAIL_ADDR	|	string	|	60	|		|		|		|		|		|		|		|		|	SHAREHOLDER	|	PROV	|	varchar2	|	60	|		|		|
|		|	EMAIL_CONSNT_FLG	|	string	|	1	|		|		|		|	CRTY_FULL_NM	|		|		|		|		|	SHAREHOLDER	|	CRTY	|	varchar2	|	60	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	SHAREHOLDER	|	REG	|	varchar2	|	60	|		|		|
|		|		|		|		|		|		|		|	Current Date	|	SYSDATE	|		|		|		|	SHAREHOLDER	|	ROW_STRT_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|	SHAREHOLDER	|	ROW_STOP_DTTM	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|	Y'	|		|		|		|	SHAREHOLDER	|	CURR_ROW_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|	$$etlcyckey	|		|		|		|	SHAREHOLDER	|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	4	|		|		|		|	SHAREHOLDER	|	SRC_SYS_ID	|	number	|	15	|		|		|
|		|		|		|		|		|		|		|	EMAIL_ADDR	|		|		|		|		|	SHAREHOLDER	|	EMAIL_ADDR	|	varchar2	|	255	|		|		|
|		|		|		|		|		|		|		|	EMAIL_CONSNT_FLG	|		|		|		|		|	SHAREHOLDER	|	EMAIL_CONSNT_FLG	|	varchar2	|	1	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|	"When Error_Flag ='Y'<br><br>//Error Flag<br>IIF(NOT ISNULL(v_ERR_MSG),'Y','N')"	|	ETL_LOAD_ERROR	|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|	ERR_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	$$etlcyckey.	|		|		|		|		|	ETL_LOAD_CYC_KEY	|	"number(p,s)"	|	10	|		|		|
|		|		|		|		|		|		|		|		|	"IIF(<br>      (NOT ISNULL(v_SHRHLDR_ROLE_KEY_ERR_MSG)<br>                                                       OR<br>       NOT ISNULL(v_CUST_ID_ERR_MSG)<br>                                                       OR<br>       NOT ISNULL(v_CUST_ID_2_ERR_MSG)),<br>(v_SHRHLDR_ROLE_KEY_ERR_MSG&vert;&vert;v_CUST_ID_ERR_MSG&vert;&vert;v_CUST_ID_2_ERR_MSG&vert;&vert;' FOR PARTY_ROLE_KEY:'&vert;&vert;TO_CHAR(PARTY_ROLE_KEY)&vert;&vert;' AND PARTY_KEY:'&vert;&vert;TO_CHAR(PARTY_KEY)&vert;&vert;' AND IN_PARTY_TYPE_KEY:'&vert;&vert;TO_CHAR(PARTY_TYPE_KEY)),NULL)"	|		|		|		|		|	ERR_MSG	|	varchar2	|	4000	|		|		|
|		|		|		|		|		|		|		|		|	SYSDATE	|		|		|		|		|	RUN_DT_TS	|	date	|	19	|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|		|
