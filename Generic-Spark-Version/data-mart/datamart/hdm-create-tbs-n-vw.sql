
-- DROP TABLE hdm.account;

CREATE TABLE hdm.account (
	acct_key int4 NOT NULL,
	acct_top_lvl_cd varchar(3) NULL,
	acct_top_lvl_desc varchar(60) NULL,
	acct_type_cd numeric(38,15) NULL,
	acct_type varchar(60) NULL,
	acct_nbr numeric(38,15) NULL,
	fund_nbr int4 NULL,
	tax_id numeric(38,15) NULL,
	acct_stat_cd int4 NULL,
	acct_stat varchar(60) NULL,
	acct_strt_dt timestamp NULL,
	last_mntn_dt timestamp NULL,
	dlr_lvl_cntl_cd int4 NOT NULL,
	dlr_lvl_cntl_desc varchar(60) NULL,
	pre_authzd_chk_acct_flg int4 NULL,
	swp_acct_flg int4 NULL,
	ach_acct_flg int4 NULL,
	re_invst_to_another_acct_flg int4 NULL,
	monitored_vip_acct int4 NULL,
	fiduciary_acct_flg int4 NULL,
	mailg_flg int4 NULL,
	intr_party_flg varchar(1) NULL,
	mgmt_co_empl_flg varchar(1) NULL,
	trst_co_nbr numeric(38,15) NULL,
	tpa_nbr numeric(38,15) NULL,
	custodian_id numeric(38,15) NULL,
	tpa_id numeric(38,15) NULL,
	ext_pln_id varchar(25) NULL,
	tin_cd int4 NULL,
	tin_desc varchar(60) NULL,
	alpha_cd varchar(10) NULL,
	cap_gains_dstrbtn_flg int4 NOT NULL,
	chk_writing_acct_flg int4 NULL,
	divd_dstrbtn_opt_flg int4 NULL,
	expedited_redmpn_acct_flg int4 NULL,
	sub_acct_flg int4 NULL,
	forgn_tax_rt numeric(38,15) NULL,
	cum_discnt_nbr numeric(38,15) NULL,
	ltr_of_intend_nbr numeric(38,15) NULL,
	timer_flg int4 NULL,
	gpurch_acct_flg int4 NULL,
	expdt_xchg_acct_flg int4 NULL,
	pnlty_whg_acct_flg int4 NULL,
	certfc_issuance_flg int4 NULL,
	stop_xfer_flg int4 NULL,
	bluesky_xmptn_flg int4 NULL,
	bnk_card_issd_flg int4 NULL,
	nav_acct_flg varchar(1) NULL,
	phon_chk_redmpn_flg varchar(1) NULL,
	house_acct_flg varchar(1) NULL,
	divd_mail_acct_flg int4 NULL,
	stop_purch_acct_flg int4 NULL,
	stop_mail_acct_flg int4 NULL,
	fractional_chk_flg int4 NULL,
	acct_pr_sched_flg int4 NULL,
	cap_gains_dstrbtn_cd bpchar(18) NULL,
	cap_gains_dstrbtn_type bpchar(18) NULL,
	divd_dstrbtn_type_cd bpchar(18) NULL,
	divd_dstrbtn_type bpchar(18) NULL,
	cust_bin varchar(20) NULL,
	acct_origination_id numeric(38,15) NULL,
	regstn_line_1 varchar(50) NULL,
	regstn_line_2 varchar(50) NULL,
	regstn_line_3 varchar(50) NULL,
	regstn_line_4 varchar(50) NULL,
	regstn_line_5 varchar(50) NULL,
	regstn_line_6 varchar(50) NULL,
	regstn_line_7 varchar(50) NULL,
	st_nm varchar(60) NULL,
	crty_nm varchar(60) NULL,
	st_cd varchar(3) NOT NULL,
	crty_cd varchar(3) NOT NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	legacy_type_desc varchar(100) NULL,
	fund_strt_dt timestamp NULL,
	stop_mail_dt timestamp NULL,
	acct_setup_dt timestamp NULL,
	internet_acct_flg varchar(1) NULL,
	grnd_father_acct_flg varchar(1) NULL,
	pln_clsfcn_flg varchar(1) NULL,
	mktg_clsfcn_flg varchar(1) NULL,
	mktg_clsfcn_desc varchar(255) NULL,
	rep_cd varchar(25) NULL,
	rep_nm varchar(255) NULL,
	dlr_id numeric(38,15) NULL,
	dlr_type varchar(20) NULL,
	hbr_dlr_id numeric(38,15) NULL,
	hbr_dlr_nm varchar(60) NULL,
	trst_nm varchar(60) NULL,
	acct_clsd_dt timestamp NULL,
	redmpn_fee_xmpt_flg varchar(1) NULL,
	pay_to_play_flg varchar(1) NULL,
	citzn_st_crty_cd int4 NULL,
	purg_prcsd_flg varchar(1) NULL,
	purg_prcsd_dt timestamp NULL
);
CREATE INDEX account_ix_ie1 ON hdm.account USING btree (dlr_lvl_cntl_cd);
CREATE INDEX account_ix_ie2 ON hdm.account USING btree (acct_type_cd);
CREATE INDEX account_ix_ie3 ON hdm.account USING btree (tpa_id);
CREATE INDEX account_ix_ie4 ON hdm.account USING btree (hbr_dlr_id);

-- Drop table

-- DROP TABLE hdm.account_category;

CREATE TABLE hdm.account_category (
	acct_cat_key int4 NOT NULL,
	acct_cat_desc varchar(100) NULL,
	maj_cat_cd int4 NULL,
	maj_cat_desc varchar(60) NULL,
	acct_cat_ord numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.account_position;

CREATE TABLE hdm.account_position (
	acct_posn_key int4 NOT NULL,
	acct_key int4 NULL,
	fund_key int4 NULL,
	shrhldr_key int4 NULL,
	dlr_key int4 NULL,
	day_key int4 NULL,
	acct_nbr numeric(38,15) NULL,
	shrhldr_role_key int4 NOT NULL,
	tot_shrs numeric(38,15) NULL,
	un_issd_shrs numeric(38,15) NULL,
	issd_shrs numeric(38,15) NULL,
	nav numeric(38,15) NULL,
	rsrvd_shrs numeric(38,15) NULL,
	accrd_divd_amt numeric(38,15) NULL,
	escrw_shrs numeric(38,15) NULL,
	collctd_shrs numeric(38,15) NULL,
	collctd_bal_amt numeric(38,15) NULL,
	curr_incm_divd numeric(38,15) NULL,
	curr_long_term_cap_gain numeric(38,15) NULL,
	curr_shrt_term_cap_gain numeric(38,15) NULL,
	curr_forgn_w_holdg numeric(38,15) NULL,
	curr_divd_w_holdg numeric(38,15) NULL,
	curr_forgn_tax_paid_amt numeric(38,15) NULL,
	curr_yr_1099b_gr_redmpn_amt numeric(38,15) NULL,
	curr_yr_1099b_gr_w_holdg_amt numeric(38,15) NULL,
	collctd_shrs_net_chg_cnt numeric(38,15) NULL,
	tot_shrs_net_chg_cnt numeric(38,15) NULL,
	dsgntd_w_holdg_amt numeric(38,15) NULL,
	dsgntd_w_holdg_rt numeric(38,15) NULL,
	divd_w_holdg_amt numeric(38,15) NULL,
	divd_w_holdg_rt numeric(38,15) NULL,
	shrt_term_cap_gain_w_holdg_amt numeric(38,15) NULL,
	shrt_term_cap_gain_w_holdg_rt numeric(38,15) NULL,
	long_term_cap_gain_w_holdg_amt numeric(38,15) NULL,
	long_term_cap_gain_w_holdg_rt numeric(38,15) NULL,
	partition_yr int4 NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	dlr_branch_key int4 NULL,
	tot_bal_amt numeric(38,15) NULL,
	shrhldr_copy_key numeric(38,15) NULL,
	hbr_dlr_key int4 NULL,
	acct_stat_cd numeric(38,15) NULL
);
CREATE INDEX account_position_ix_ie1 ON hdm.account_position USING btree (fund_key);
CREATE INDEX account_position_ix_ie3 ON hdm.account_position USING btree (day_key);
CREATE INDEX account_position_ix_ie4 ON hdm.account_position USING btree (partition_yr);
CREATE INDEX account_position_ix_ie5 ON hdm.account_position USING btree (dlr_key);
CREATE INDEX account_position_ix_ie7 ON hdm.account_position USING btree (acct_stat_cd);
CREATE INDEX account_position_ix_if1 ON hdm.account_position USING btree (shrhldr_key);
CREATE INDEX account_position_ix_if10 ON hdm.account_position USING btree (hbr_dlr_key);
CREATE INDEX account_position_ix_if2 ON hdm.account_position USING btree (acct_key);
CREATE INDEX account_position_ix_if7 ON hdm.account_position USING btree (dlr_branch_key);

-- Drop table

-- DROP TABLE hdm.account_position_hist;

CREATE TABLE hdm.account_position_hist (
	acct_posn_key int4 NOT NULL,
	acct_key int4 NULL,
	fund_key int4 NULL,
	shrhldr_key int4 NULL,
	dlr_key int4 NULL,
	day_key int4 NULL,
	acct_nbr numeric(38,15) NULL,
	shrhldr_role_key int4 NOT NULL,
	tot_shrs numeric(38,15) NULL,
	un_issd_shrs numeric(38,15) NULL,
	issd_shrs numeric(38,15) NULL,
	nav numeric(38,15) NULL,
	rsrvd_shrs numeric(38,15) NULL,
	accrd_divd_amt numeric(38,15) NULL,
	escrw_shrs numeric(38,15) NULL,
	collctd_shrs numeric(38,15) NULL,
	collctd_bal_amt numeric(38,15) NULL,
	curr_incm_divd numeric(38,15) NULL,
	curr_long_term_cap_gain numeric(38,15) NULL,
	curr_shrt_term_cap_gain numeric(38,15) NULL,
	curr_forgn_w_holdg numeric(38,15) NULL,
	curr_divd_w_holdg numeric(38,15) NULL,
	curr_forgn_tax_paid_amt numeric(38,15) NULL,
	curr_yr_1099b_gr_redmpn_amt numeric(38,15) NULL,
	curr_yr_1099b_gr_w_holdg_amt numeric(38,15) NULL,
	collctd_shrs_net_chg_cnt numeric(38,15) NULL,
	tot_shrs_net_chg_cnt numeric(38,15) NULL,
	dsgntd_w_holdg_amt numeric(38,15) NULL,
	dsgntd_w_holdg_rt numeric(38,15) NULL,
	divd_w_holdg_amt numeric(38,15) NULL,
	divd_w_holdg_rt numeric(38,15) NULL,
	shrt_term_cap_gain_w_holdg_amt numeric(38,15) NULL,
	shrt_term_cap_gain_w_holdg_rt numeric(38,15) NULL,
	long_term_cap_gain_w_holdg_amt numeric(38,15) NULL,
	long_term_cap_gain_w_holdg_rt numeric(38,15) NULL,
	partition_yr int4 NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	dlr_branch_key int4 NULL,
	tot_bal_amt numeric(38,15) NULL,
	shrhldr_copy_key numeric(38,15) NULL,
	hbr_dlr_key int4 NULL,
	acct_stat_cd numeric(38,15) NULL
);
CREATE INDEX account_position_hist_ix_ie1 ON hdm.account_position_hist USING btree (fund_key);
CREATE INDEX account_position_hist_ix_ie3 ON hdm.account_position_hist USING btree (day_key);
CREATE INDEX account_position_hist_ix_ie4 ON hdm.account_position_hist USING btree (partition_yr);
CREATE INDEX account_position_hist_ix_ie5 ON hdm.account_position_hist USING btree (dlr_key);
CREATE INDEX account_position_hist_ix_ie7 ON hdm.account_position_hist USING btree (acct_stat_cd);
CREATE INDEX account_position_hist_ix_if1 ON hdm.account_position_hist USING btree (shrhldr_key);
CREATE INDEX account_position_hist_ix_if10 ON hdm.account_position_hist USING btree (hbr_dlr_key);
CREATE INDEX account_position_hist_ix_if2 ON hdm.account_position_hist USING btree (acct_key);
CREATE INDEX account_position_hist_ix_if7 ON hdm.account_position_hist USING btree (dlr_branch_key);
CREATE UNIQUE INDEX account_position_hist_ix_pk ON hdm.account_position_hist USING btree (acct_posn_key);

-- Drop table

-- DROP TABLE hdm.account_setup_year;

CREATE TABLE hdm.account_setup_year (
	acct_setup_yr_key int4 NOT NULL,
	acct_setup_yr numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.account_statistics;

CREATE TABLE hdm.account_statistics (
	acct_stats_key int4 NOT NULL,
	gend_key int4 NULL,
	acct_cat_key int4 NULL,
	bal_cat_key int4 NULL,
	fund_key int4 NULL,
	day_key int4 NULL,
	acct_setup_yr_key int4 NULL,
	acct_bal_amt numeric(38,15) NULL,
	tot_shrs bpchar(18) NULL,
	acct_cnt numeric(38,15) NULL,
	shrhldr_cnt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX account_statitics_ix_if1 ON hdm.account_statistics USING btree (gend_key);
CREATE INDEX account_statitics_ix_if2 ON hdm.account_statistics USING btree (acct_cat_key);
CREATE INDEX account_statitics_ix_if3 ON hdm.account_statistics USING btree (bal_cat_key);
CREATE INDEX account_statitics_ix_if4 ON hdm.account_statistics USING btree (fund_key);
CREATE INDEX account_statitics_ix_if5 ON hdm.account_statistics USING btree (day_key);
CREATE INDEX account_statitics_ix_if6 ON hdm.account_statistics USING btree (acct_setup_yr_key);

-- Drop table

-- DROP TABLE hdm.after_tax_calculation;

CREATE TABLE hdm.after_tax_calculation (
	atax_calc_key int4 NOT NULL,
	reinv_day_key int4 NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	per_key int4 NULL,
	tax_yr numeric(38,15) NULL,
	cb_amt numeric(38,15) NULL,
	shrs_calc numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX after_tax_calculation_ix_if1 ON hdm.after_tax_calculation USING btree (reinv_day_key);
CREATE INDEX after_tax_calculation_ix_if2 ON hdm.after_tax_calculation USING btree (day_key);
CREATE INDEX after_tax_calculation_ix_if3 ON hdm.after_tax_calculation USING btree (fund_key);
CREATE INDEX after_tax_calculation_ix_if4 ON hdm.after_tax_calculation USING btree (per_key);

-- Drop table

-- DROP TABLE hdm.after_tax_return;

CREATE TABLE hdm.after_tax_return (
	atax_ret_key int4 NOT NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	per_key int4 NULL,
	sold_amt numeric(38,15) NULL,
	cb_amt numeric(38,15) NULL,
	gls_amt numeric(38,15) NULL,
	st_tax_gls numeric(38,15) NULL,
	lt_tax_gls numeric(38,15) NULL,
	tax_amt numeric(38,15) NULL,
	net_prcd_atax numeric(38,15) NULL,
	strt_per timestamp NULL,
	annlzn_fctr numeric(38,15) NULL,
	atax_ret_wo_sale numeric(38,15) NULL,
	atax_ret numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX after_tax_return_ix_if1 ON hdm.after_tax_return USING btree (day_key);
CREATE INDEX after_tax_return_ix_if2 ON hdm.after_tax_return USING btree (fund_key);
CREATE INDEX after_tax_return_ix_if3 ON hdm.after_tax_return USING btree (per_key);

-- Drop table

-- DROP TABLE hdm.after_tax_wealth_index;

CREATE TABLE hdm.after_tax_wealth_index (
	atax_wlth_idx_key int4 NOT NULL,
	day_key int4 NULL,
	reinv_day_key int4 NULL,
	fund_key int4 NULL,
	atax_ordn_incm numeric(38,15) NULL,
	atax_ltcg numeric(38,15) NULL,
	atax_stcg numeric(38,15) NULL,
	atax_forgn_tax_cr numeric(38,15) NULL,
	atax_tot_divd numeric(38,15) NULL,
	prir_mo_pr numeric(38,15) NULL,
	curr_mo_pr numeric(38,15) NULL,
	reinv_pr numeric(38,15) NULL,
	btax_ret numeric(38,15) NULL,
	atax_ret numeric(38,15) NULL,
	atax_reinv_per_shr numeric(38,15) NULL,
	cb_amt numeric(38,15) NULL,
	shrs_calc numeric(38,15) NULL,
	sold_amt numeric(38,15) NULL,
	gls_amt numeric(38,15) NULL,
	st_tax_gls numeric(38,15) NULL,
	lt_tax_gls numeric(38,15) NULL,
	tax_amt numeric(38,15) NULL,
	net_prcd_atax numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX after_tax_wealth_index_ix_if1 ON hdm.after_tax_wealth_index USING btree (reinv_day_key);
CREATE INDEX after_tax_wealth_index_ix_if2 ON hdm.after_tax_wealth_index USING btree (fund_key);
CREATE INDEX after_tax_wealth_index_ix_if4 ON hdm.after_tax_wealth_index USING btree (day_key);


-- Drop table

-- DROP TABLE hdm.awd_privilege_type;

CREATE TABLE hdm.awd_privilege_type (
	prvl_type_key int4 NOT NULL,
	prvl_type varchar(60) NULL,
	prvl_type_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.awd_queue;

CREATE TABLE hdm.awd_queue (
	q_key int4 NOT NULL,
	q_cd varchar(25) NULL,
	q_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.awd_record_type;

CREATE TABLE hdm.awd_record_type (
	rec_type_key int4 NOT NULL,
	rec_cd varchar(25) NULL,
	rec_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.awd_role;

CREATE TABLE hdm.awd_role (
	role_key int4 NOT NULL,
	role_nm varchar(60) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.awd_role_privilege;

CREATE TABLE hdm.awd_role_privilege (
	role_prvl_key int4 NOT NULL,
	rec_type_key int4 NOT NULL,
	role_key int4 NOT NULL,
	prvl_type_key int4 NOT NULL,
	day_key int4 NOT NULL,
	ut_cd varchar(25) NULL,
	exist_flg varchar(1) NULL,
	view_flg varchar(1) NULL,
	updt_flg varchar(1) NULL,
	wk_slctn_flg varchar(1) NULL,
	last_maint_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX awd_role_privilege_ix_if1 ON hdm.awd_role_privilege USING btree (rec_type_key);
CREATE INDEX awd_role_privilege_ix_if2 ON hdm.awd_role_privilege USING btree (role_key);
CREATE INDEX awd_role_privilege_ix_if3 ON hdm.awd_role_privilege USING btree (prvl_type_key);
CREATE INDEX awd_role_privilege_ix_if4 ON hdm.awd_role_privilege USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.awd_user;

CREATE TABLE hdm.awd_user (
	usr_key int4 NOT NULL,
	grp_nm varchar(255) NULL,
	usr_stat_cd varchar(25) NULL,
	usr_stat_desc varchar(50) NULL,
	usr_id varchar(25) NULL,
	first_nm varchar(60) NULL,
	mint varchar(50) NULL,
	last_nm varchar(60) NULL,
	tax_id numeric(38,15) NOT NULL,
	clsfcn varchar(25) NULL,
	lck_stat varchar(1) NULL,
	q_flg varchar(1) NULL,
	phon_nbr varchar(25) NULL,
	fax_nbr varchar(25) NULL,
	wk_slctn_cd numeric(38,15) NULL,
	fax_dept varchar(60) NULL,
	rout_usr_id varchar(25) NULL,
	sec_lvl_cd numeric(38,15) NULL,
	ut_cd varchar(25) NULL,
	last_maint_dt timestamp NULL,
	last_dt timestamp NULL,
	last_pswd_maint_dt timestamp NULL,
	awd_desktop_vrsn varchar(25) NULL,
	data_revs numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.awd_user_queue;

CREATE TABLE hdm.awd_user_queue (
	usr_q_key int4 NOT NULL,
	q_key int4 NOT NULL,
	usr_key int4 NOT NULL,
	day_key int4 NOT NULL,
	ut_cd varchar(25) NULL,
	exist_flg varchar(1) NULL,
	view_flg varchar(1) NULL,
	updt_flg varchar(1) NULL,
	wk_slctn_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX awd_user_queue_ix_if1 ON hdm.awd_user_queue USING btree (q_key);
CREATE INDEX awd_user_queue_ix_if2 ON hdm.awd_user_queue USING btree (usr_key);
CREATE INDEX awd_user_queue_ix_if3 ON hdm.awd_user_queue USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.awd_user_role;

CREATE TABLE hdm.awd_user_role (
	usr_role_key int4 NOT NULL,
	role_key int4 NULL,
	usr_key int4 NULL,
	day_key int4 NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX awd_user_role_ix_if1 ON hdm.awd_user_role USING btree (role_key);
CREATE INDEX awd_user_role_ix_if2 ON hdm.awd_user_role USING btree (usr_key);
CREATE INDEX awd_user_role_ix_if3 ON hdm.awd_user_role USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.awd_user_work_type;

CREATE TABLE hdm.awd_user_work_type (
	usr_wk_type_key int4 NOT NULL,
	rec_type_key int4 NOT NULL,
	wk_type_key int4 NOT NULL,
	usr_key int4 NOT NULL,
	day_key int4 NOT NULL,
	ut_cd varchar(25) NULL,
	exist_flg varchar(1) NULL,
	view_flg varchar(1) NULL,
	updt_flg varchar(1) NULL,
	wk_slctn_flg varchar(1) NULL,
	last_maint_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX awd_user_work_type_ix_if1 ON hdm.awd_user_work_type USING btree (rec_type_key);
CREATE INDEX awd_user_work_type_ix_if2 ON hdm.awd_user_work_type USING btree (wk_type_key);
CREATE INDEX awd_user_work_type_ix_if3 ON hdm.awd_user_work_type USING btree (usr_key);
CREATE INDEX awd_user_work_type_ix_if4 ON hdm.awd_user_work_type USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.awd_work_type;

CREATE TABLE hdm.awd_work_type (
	wk_type_key int4 NOT NULL,
	wk_type_nm varchar(60) NULL,
	wk_type_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.balance_category;

CREATE TABLE hdm.balance_category (
	bal_cat_key int4 NOT NULL,
	cat_desc varchar(100) NULL,
	cat_min numeric(38,15) NULL,
	cat_max numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.benchmark_index;

CREATE TABLE hdm.benchmark_index (
	bmk_idx_key int4 NOT NULL,
	idx_id varchar(25) NULL,
	idx_nm varchar(150) NULL,
	idx_shrt_nm varchar(15) NULL,
	robeco_idx_id numeric(38,15) NULL,
	bloomberg_sym varchar(25) NULL,
	idx_type numeric(38,15) NULL,
	idx_type_desc varchar(60) NULL,
	bmk_legacy_id numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bluesky_transactions;

CREATE TABLE hdm.bluesky_transactions (
	blue_sky_txn_key bpchar(18) NOT NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	reg_key int4 NULL,
	shr_qty numeric(38,15) NULL,
	txn_amt numeric(38,15) NULL,
	mtd_rept_amt numeric(38,15) NULL,
	mtd_re_open_amt numeric(38,15) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL
);
CREATE INDEX bluesky_transactions_ix_if1 ON hdm.bluesky_transactions USING btree (day_key);
CREATE INDEX bluesky_transactions_ix_if2 ON hdm.bluesky_transactions USING btree (fund_key);
CREATE INDEX bluesky_transactions_ix_if3 ON hdm.bluesky_transactions USING btree (reg_key);

-- Drop table

-- DROP TABLE hdm.broker;

CREATE TABLE hdm.broker (
	brkr_key int4 NOT NULL,
	posn_type_key int4 NULL,
	brkr_nm varchar(100) NULL,
	st_str_nbr varchar(25) NULL,
	compst_nm varchar(100) NULL,
	brkr_acct_nbr varchar(100) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX broker_ix_if2 ON hdm.broker USING btree (posn_type_key);


-- Drop table

-- DROP TABLE hdm.broker_position;

CREATE TABLE hdm.broker_position (
	brkr_posn_key int4 NOT NULL,
	brkr_key int4 NULL,
	day_key int4 NULL,
	ser_nbr numeric(38,15) NULL,
	sub_advsr_fund_id numeric(38,15) NULL,
	asset_id varchar(255) NULL,
	thrd_party_id varchar(255) NULL,
	othr_id varchar(255) NULL,
	trde_id varchar(255) NULL,
	asset_desc varchar(255) NULL,
	prod_nm varchar(255) NULL,
	asset_qty numeric(38,15) NULL,
	trde_dt timestamp NULL,
	sttl_dt timestamp NULL,
	txn_type varchar(255) NULL,
	purch_pr numeric(38,15) NULL,
	vend_pr numeric(38,15) NULL,
	mkt_val numeric(38,15) NULL,
	coupn numeric(38,15) NULL,
	mtry_dt timestamp NULL,
	trde_amt numeric(38,15) NULL,
	sttl_amt numeric(38,15) NULL,
	pldg_amt numeric(38,15) NULL,
	un_sttld_amt numeric(38,15) NULL,
	contct_nm varchar(255) NULL,
	phon_det varchar(255) NULL,
	fax_det varchar(255) NULL,
	addr varchar(255) NULL,
	email_id varchar(255) NULL,
	notes varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX broker_position_ix_if2 ON hdm.broker_position USING btree (brkr_key);
CREATE INDEX broker_position_ix_if3 ON hdm.broker_position USING btree (day_key);


-- Drop table

-- DROP TABLE hdm.bud_account;

CREATE TABLE hdm.bud_account (
	bud_acct_key int4 NOT NULL,
	acct_id varchar(30) NULL,
	acct_desc varchar(200) NULL,
	acct_type varchar(30) NULL,
	actv_flg varchar(1) NULL,
	cat_type varchar(30) NULL,
	expns_cat varchar(30) NULL,
	prpd_expns_flg varchar(1) NULL,
	acct_subt varchar(30) NULL,
	payee varchar(100) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_acct_accrual;

CREATE TABLE hdm.bud_acct_accrual (
	acct_accrl_key int4 NOT NULL,
	accrl_id varchar(100) NULL,
	accrl_src varchar(60) NULL,
	fiscl_yr numeric(38,15) NULL,
	accrl_stat varchar(60) NULL,
	ntes varchar(4000) NULL,
	insr_usr varchar(25) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_acct_accrual_fnd;

CREATE TABLE hdm.bud_acct_accrual_fnd (
	acct_accural_fund_key int4 NOT NULL,
	acct_accrl_key int4 NULL,
	src_bud_key int4 NULL,
	src_assmpn_chg_key int4 NULL,
	expns_line_item_key int4 NULL,
	bud_fund_compst_key int4 NULL,
	curr_acct_accrl_fund_key int4 NULL,
	accrl_id varchar(255) NULL,
	daily_accrl numeric(38,15) NULL,
	orig_bud numeric(38,15) NULL,
	rvsd_bud numeric(38,15) NULL,
	remng_to_accrl numeric(38,15) NULL,
	fytd_expns numeric(38,15) NULL,
	efftv_dt timestamp NULL,
	accrl_strt_dt timestamp NULL,
	accrl_by_dt timestamp NULL,
	ssb_efftv_dt timestamp NULL,
	nbr_accrl_days numeric(38,15) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_acct_accrual_fnd_ix_if1 ON hdm.bud_acct_accrual_fnd USING btree (expns_line_item_key);
CREATE INDEX bud_acct_accrual_fnd_ix_if11 ON hdm.bud_acct_accrual_fnd USING btree (bud_fund_compst_key);
CREATE INDEX bud_acct_accrual_fnd_ix_if12 ON hdm.bud_acct_accrual_fnd USING btree (acct_accrl_key);
CREATE INDEX bud_acct_accrual_fnd_ix_if2 ON hdm.bud_acct_accrual_fnd USING btree (src_bud_key);
CREATE INDEX bud_acct_accrual_fnd_ix_if5 ON hdm.bud_acct_accrual_fnd USING btree (src_assmpn_chg_key);
CREATE INDEX bud_acct_accrual_fnd_ix_if9 ON hdm.bud_acct_accrual_fnd USING btree (curr_acct_accrl_fund_key);

-- Drop table

-- DROP TABLE hdm.bud_acct_adjust_fnd;

CREATE TABLE hdm.bud_acct_adjust_fnd (
	acct_adjmt_key int4 NOT NULL,
	bud_fund_compst_key int4 NULL,
	bud_key int4 NULL,
	expns_line_item_key int4 NULL,
	assmpn_chg_key int4 NULL,
	expns_acct_key int4 NULL,
	paybl_acct_key int4 NULL,
	dbt_acct_key int4 NULL,
	cr_acct_key int4 NULL,
	cat_accrl_dim_key int4 NULL,
	adjmt_id varchar(100) NULL,
	fiscl_yr numeric(38,15) NULL,
	adjmt_amt numeric(38,15) NULL,
	act_stat varchar(30) NULL,
	ntes varchar(4000) NULL,
	efftv_dt timestamp NULL,
	ssb_efftv_dt timestamp NULL,
	adjmt_type varchar(30) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_acct_adjust_fnd_ix_if1 ON hdm.bud_acct_adjust_fnd USING btree (bud_fund_compst_key);
CREATE INDEX bud_acct_adjust_fnd_ix_if10 ON hdm.bud_acct_adjust_fnd USING btree (dbt_acct_key);
CREATE INDEX bud_acct_adjust_fnd_ix_if11 ON hdm.bud_acct_adjust_fnd USING btree (cr_acct_key);
CREATE INDEX bud_acct_adjust_fnd_ix_if12 ON hdm.bud_acct_adjust_fnd USING btree (cat_accrl_dim_key);
CREATE INDEX bud_acct_adjust_fnd_ix_if2 ON hdm.bud_acct_adjust_fnd USING btree (bud_key);
CREATE INDEX bud_acct_adjust_fnd_ix_if3 ON hdm.bud_acct_adjust_fnd USING btree (expns_line_item_key);
CREATE INDEX bud_acct_adjust_fnd_ix_if4 ON hdm.bud_acct_adjust_fnd USING btree (assmpn_chg_key);
CREATE INDEX bud_acct_adjust_fnd_ix_if8 ON hdm.bud_acct_adjust_fnd USING btree (expns_acct_key);
CREATE INDEX bud_acct_adjust_fnd_ix_if9 ON hdm.bud_acct_adjust_fnd USING btree (paybl_acct_key);

-- Drop table

-- DROP TABLE hdm.bud_acct_transfer;

CREATE TABLE hdm.bud_acct_transfer (
	acct_xfer_key numeric(38,15) NOT NULL,
	xfer_id varchar(100) NULL,
	xfer_grp_nm varchar(100) NULL,
	entry_nm varchar(100) NULL,
	efftv_dt timestamp NULL,
	ssb_efftv_dt timestamp NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	editbl_flg varchar(1) NULL,
	xfer_stat varchar(30) NULL,
	ntes varchar(4000) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_acct_transfer_fnd;

CREATE TABLE hdm.bud_acct_transfer_fnd (
	acct_xfer_fund_key int4 NOT NULL,
	acct_xfer_key numeric(38,15) NULL,
	src_bud_acct_key int4 NULL,
	dest_bud_acct_key int4 NULL,
	src_expns_line_item_key int4 NULL,
	dest_expns_line_item_key int4 NULL,
	bud_fund_compst_key int4 NULL,
	dbt_acct_key int4 NULL,
	cr_acct_key int4 NULL,
	xfer_id varchar(100) NULL,
	xfer_val numeric(38,15) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_acct_transfer_fnd_ix_if1 ON hdm.bud_acct_transfer_fnd USING btree (acct_xfer_key);
CREATE INDEX bud_acct_transfer_fnd_ix_if2 ON hdm.bud_acct_transfer_fnd USING btree (src_bud_acct_key);
CREATE INDEX bud_acct_transfer_fnd_ix_if3 ON hdm.bud_acct_transfer_fnd USING btree (dest_bud_acct_key);
CREATE INDEX bud_acct_transfer_fnd_ix_if4 ON hdm.bud_acct_transfer_fnd USING btree (src_expns_line_item_key);
CREATE INDEX bud_acct_transfer_fnd_ix_if5 ON hdm.bud_acct_transfer_fnd USING btree (dest_expns_line_item_key);
CREATE INDEX bud_acct_transfer_fnd_ix_if6 ON hdm.bud_acct_transfer_fnd USING btree (bud_fund_compst_key);
CREATE INDEX bud_acct_transfer_fnd_ix_if8 ON hdm.bud_acct_transfer_fnd USING btree (dbt_acct_key);
CREATE INDEX bud_acct_transfer_fnd_ix_if9 ON hdm.bud_acct_transfer_fnd USING btree (cr_acct_key);

-- Drop table

-- DROP TABLE hdm.bud_asmpt_change;

CREATE TABLE hdm.bud_asmpt_change (
	assmpn_chg_key int4 NOT NULL,
	adjmt_type varchar(25) NULL,
	chg_amt numeric(38,15) NULL,
	efftv_dt timestamp NULL,
	fiscl_yr numeric(38,15) NULL,
	editbl_flg int4 NULL,
	assmpn_chg_ntes varchar(255) NULL,
	assmpn_id varchar(255) NULL,
	liab_adj_bud_id varchar(30) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_asmpt_change_fund;

CREATE TABLE hdm.bud_asmpt_change_fund (
	assmpn_chg_fund_key int4 NOT NULL,
	bud_fund_compst_key int4 NULL,
	bud_key int4 NULL,
	assmpn_chg_key int4 NULL,
	expns_line_item_key int4 NULL,
	data_src_dim_key int4 NULL,
	rt numeric(38,15) NULL,
	allocn numeric(38,15) NULL,
	data_src_val numeric(38,15) NULL,
	assmpn_chg_per_fund_ntes varchar(255) NULL,
	adjmt numeric(38,15) NULL,
	rsdl_val numeric(38,15) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_asmpt_change_fund_ix_if4 ON hdm.bud_asmpt_change_fund USING btree (assmpn_chg_key);
CREATE INDEX bud_asmpt_change_fund_ix_if5 ON hdm.bud_asmpt_change_fund USING btree (bud_fund_compst_key);
CREATE INDEX bud_asmpt_change_fund_ix_if6 ON hdm.bud_asmpt_change_fund USING btree (bud_key);
CREATE INDEX bud_asmpt_change_fund_ix_if7 ON hdm.bud_asmpt_change_fund USING btree (expns_line_item_key);
CREATE INDEX bud_asmpt_change_fund_ix_if9 ON hdm.bud_asmpt_change_fund USING btree (data_src_dim_key);

-- Drop table

-- DROP TABLE hdm.bud_cat_accrual_dim;

CREATE TABLE hdm.bud_cat_accrual_dim (
	cat_accrl_dim_key int4 NOT NULL,
	cat_accrl_id varchar(100) NULL,
	ntes varchar(4000) NULL,
	accrl_type varchar(25) NULL,
	insr_dt timestamp NULL,
	insr_usr varchar(25) NULL,
	rndedup_to_1penny_flg int4 NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_cat_accrual_fact;

CREATE TABLE hdm.bud_cat_accrual_fact (
	cat_accrl_fact_key int4 NOT NULL,
	bud_fund_compst_key int4 NULL,
	expns_acct_key int4 NULL,
	paybl_acct_key int4 NULL,
	dbt_acct_key int4 NULL,
	cr_acct_key int4 NULL,
	cat_accrl_dim_key int4 NULL,
	cat_accrl_id varchar(100) NULL,
	daily_accrl numeric(38,15) NULL,
	adjmt_val numeric(38,15) NULL,
	fiscl_yr numeric(38,15) NULL,
	accrl_stat varchar(25) NULL,
	efftv_dt timestamp NULL,
	ssb_efftv_dt timestamp NULL,
	expn_dt timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_cat_accrual_fact_ix_if10 ON hdm.bud_cat_accrual_fact USING btree (cat_accrl_dim_key);
CREATE INDEX bud_cat_accrual_fact_ix_if2 ON hdm.bud_cat_accrual_fact USING btree (bud_fund_compst_key);
CREATE INDEX bud_cat_accrual_fact_ix_if6 ON hdm.bud_cat_accrual_fact USING btree (expns_acct_key);
CREATE INDEX bud_cat_accrual_fact_ix_if7 ON hdm.bud_cat_accrual_fact USING btree (paybl_acct_key);
CREATE INDEX bud_cat_accrual_fact_ix_if8 ON hdm.bud_cat_accrual_fact USING btree (dbt_acct_key);
CREATE INDEX bud_cat_accrual_fact_ix_if9 ON hdm.bud_cat_accrual_fact USING btree (cr_acct_key);

-- Drop table

-- DROP TABLE hdm.bud_cat_acct_accrual;

CREATE TABLE hdm.bud_cat_acct_accrual (
	cat_acct_accrl_key int4 NOT NULL,
	cat_accrl_dim_key int4 NULL,
	acct_accrl_key int4 NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_cat_acct_accrual_ix_if1 ON hdm.bud_cat_acct_accrual USING btree (cat_accrl_dim_key);
CREATE INDEX bud_cat_acct_accrual_ix_if2 ON hdm.bud_cat_acct_accrual USING btree (acct_accrl_key);

-- Drop table

-- DROP TABLE hdm.bud_data_source_dim;

CREATE TABLE hdm.bud_data_source_dim (
	data_src_dim_key int4 NOT NULL,
	data_src_id varchar(60) NULL,
	fiscl_yr numeric(38,15) NULL,
	src_type varchar(60) NULL,
	shrt_nm varchar(60) NULL,
	src_nm varchar(100) NULL,
	tna_dt timestamp NULL,
	tna_end_dt timestamp NULL,
	tna_strt_dt timestamp NULL,
	nbr_accts_dt timestamp NULL,
	reg24f2_dt timestamp NULL,
	data_src_ntes varchar(2000) NULL,
	invc_strt_dt timestamp NULL,
	invc_end_dt timestamp NULL,
	invc_fiscl_yr numeric(38,15) NULL,
	invc_fee_type varchar(30) NULL,
	reg24f2_strt_dt varchar(30) NULL,
	reg24f2_end_dt varchar(30) NULL,
	calc_mthd varchar(30) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn int4 NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_data_source_fact;

CREATE TABLE hdm.bud_data_source_fact (
	data_src_fact_key int4 NOT NULL,
	data_src_dim_key int4 NULL,
	trste_elctn_key int4 NULL,
	qtrly_sal numeric(38,15) NULL,
	neg_carrovr_bal_yr numeric(38,15) NULL,
	neg_carrovr_bal_amt numeric(38,15) NULL,
	fee_per_mil numeric(38,15) NULL,
	fee_rqrd_min numeric(38,15) NULL,
	ric_pct numeric(38,15) NULL,
	tot_fee numeric(38,15) NULL,
	tot_issr_mkt_cap numeric(38,15) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_data_source_fact_ix_if2 ON hdm.bud_data_source_fact USING btree (data_src_dim_key);
CREATE INDEX bud_data_source_fact_ix_if3 ON hdm.bud_data_source_fact USING btree (trste_elctn_key);

-- Drop table

-- DROP TABLE hdm.bud_data_source_fund;

CREATE TABLE hdm.bud_data_source_fund (
	bud_data_src_fund_key int4 NOT NULL,
	data_src_dim_key int4 NULL,
	bud_fund_compst_key int4 NULL,
	avg_mthly_tna numeric(38,15) NULL,
	issr_pct numeric(38,15) NULL,
	rt numeric(38,15) NULL,
	reqs_fee_flg int4 NULL,
	src_val numeric(38,15) NULL,
	tot_fee numeric(38,15) NULL,
	otpt_val numeric(38,15) NULL,
	adjmt numeric(38,15) NULL,
	data_src_fund_nte varchar(2000) NULL,
	sby_multr numeric(38,15) NULL,
	hbr_nbr_accts numeric(38,15) NULL,
	broadridge_nbr_accts numeric(38,15) NULL,
	reinvs numeric(38,15) NULL,
	bllbl_act numeric(38,15) NULL,
	capstk_adjmts numeric(38,15) NULL,
	tna_val numeric(38,15) NULL,
	rsdl_val numeric(38,15) NULL,
	subs numeric(38,15) NULL,
	redmpns numeric(38,15) NULL,
	net_sales_act numeric(38,15) NULL,
	annl_net_sales_act numeric(38,15) NULL,
	subs_including_adjmts numeric(38,15) NULL,
	avg_daily_tna numeric(38,15) NULL,
	adj_avg_daily_tna numeric(38,15) NULL,
	allocn_adjmt numeric(38,15) NULL,
	orig_allocn numeric(38,15) NULL,
	greatest_commn_denomr numeric(38,15) NULL,
	tot_avg_daily_tna numeric(38,15) NULL,
	tot_compnsn numeric(38,15) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_data_source_fund_ix_if1 ON hdm.bud_data_source_fund USING btree (data_src_dim_key);
CREATE INDEX bud_data_source_fund_ix_if2 ON hdm.bud_data_source_fund USING btree (bud_fund_compst_key);

-- Drop table

-- DROP TABLE hdm.bud_expense_line_item;

CREATE TABLE hdm.bud_expense_line_item (
	expns_line_item_key int4 NOT NULL,
	expns_line_item_nm varchar(255) NULL,
	expns_line_item_id varchar(255) NULL,
	line_item_ntes varchar(1000) NULL,
	actv_flg int4 NULL,
	allocn_type varchar(60) NULL,
	expns_item_nm varchar(255) NULL,
	expns_cat_nm varchar(255) NULL,
	invc_vends varchar(255) NULL,
	expns_acct_id varchar(25) NULL,
	paybl_acct_id varchar(25) NULL,
	prapaid_expns_flg int4 NULL,
	hddn_flg int4 NULL,
	hddn_rsn varchar(1000) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_fund_composite;

CREATE TABLE hdm.bud_fund_composite (
	bud_fund_compst_key int4 NOT NULL,
	asset_type varchar(25) NULL,
	asset_type_desc varchar(255) NULL,
	fund_nm varchar(100) NULL,
	fund_ord numeric(38,15) NULL,
	ssb_cd varchar(25) NULL,
	ssb_shrt_cd varchar(25) NULL,
	parn_compst_id varchar(25) NULL,
	fund_compst_id varchar(25) NULL,
	incpn_dt timestamp NULL,
	closd_dt timestamp NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_fund_multiplier;

CREATE TABLE hdm.bud_fund_multiplier (
	fund_multr_key int4 NOT NULL,
	bud_fund_compst_key int4 NULL,
	fiscl_yr numeric(38,15) NULL,
	multr numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_fund_multiplier_ix_if1 ON hdm.bud_fund_multiplier USING btree (bud_fund_compst_key);

-- Drop table

-- DROP TABLE hdm.bud_invoice;

CREATE TABLE hdm.bud_invoice (
	bud_invc_key int4 NOT NULL,
	invc_id varchar(100) NULL,
	allocn_id varchar(100) NULL,
	invc_line_id varchar(100) NULL,
	fiscl_yr numeric(38,15) NULL,
	invc_allocn_type varchar(30) NULL,
	invc_nbr varchar(100) NULL,
	invc_vend varchar(100) NULL,
	ssb_efftv_dt timestamp NULL,
	pend_flg varchar(1) NULL,
	invc_src varchar(30) NULL,
	invc_desc varchar(4000) NULL,
	acctg_dt timestamp NULL,
	del_flg varchar(1) NULL,
	buded_flg varchar(1) NULL,
	rptb1099_flg varchar(1) NULL,
	acctg_per varchar(20) NULL,
	ntes varchar(2000) NULL,
	insr_ts timestamp NULL,
	insr_usr varchar(30) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_invoice_alloc;

CREATE TABLE hdm.bud_invoice_alloc (
	bud_invc_alloc_key int4 NOT NULL,
	bud_invc_key int4 NULL,
	data_src_dim_key int4 NULL,
	expns_line_item_key int4 NULL,
	bud_key int4 NULL,
	tax_amt numeric(38,15) NULL,
	invc_amt numeric(38,15) NULL,
	invc_line_amt numeric(38,15) NULL,
	vrsn numeric(38,15) NULL,
	allocn_amt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_invoice_alloc_ix_if1 ON hdm.bud_invoice_alloc USING btree (bud_invc_key);
CREATE INDEX bud_invoice_alloc_ix_if2 ON hdm.bud_invoice_alloc USING btree (data_src_dim_key);
CREATE INDEX bud_invoice_alloc_ix_if3 ON hdm.bud_invoice_alloc USING btree (expns_line_item_key);
CREATE INDEX bud_invoice_alloc_ix_if5 ON hdm.bud_invoice_alloc USING btree (bud_key);

-- Drop table

-- DROP TABLE hdm.bud_invoice_alloc_fund;

CREATE TABLE hdm.bud_invoice_alloc_fund (
	bud_invc_alloc_fund_key int4 NOT NULL,
	bud_invc_key int4 NULL,
	data_src_dim_key int4 NULL,
	bud_fund_compst_key int4 NULL,
	expns_line_item_key int4 NULL,
	fund_adjmt numeric(38,15) NULL,
	fund_rsdl_val numeric(38,15) NULL,
	fund_rt numeric(38,15) NULL,
	tax_rt numeric(38,15) NULL,
	fund_data_src_val numeric(38,15) NULL,
	fund_allocn numeric(38,15) NULL,
	fund_tax_allocn numeric(38,15) NULL,
	fund_invc_allocn numeric(38,15) NULL,
	ntes varchar(300) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_invoice_alloc_fund_ix_if1 ON hdm.bud_invoice_alloc_fund USING btree (bud_invc_key);
CREATE INDEX bud_invoice_alloc_fund_ix_if2 ON hdm.bud_invoice_alloc_fund USING btree (data_src_dim_key);
CREATE INDEX bud_invoice_alloc_fund_ix_if4 ON hdm.bud_invoice_alloc_fund USING btree (bud_fund_compst_key);
CREATE INDEX bud_invoice_alloc_fund_ix_if6 ON hdm.bud_invoice_alloc_fund USING btree (expns_line_item_key);

-- Drop table

-- DROP TABLE hdm.bud_liability;

CREATE TABLE hdm.bud_liability (
	liab_key int4 NOT NULL,
	bud_id varchar(100) NULL,
	line_item_id varchar(100) NULL,
	paybl_acct_id varchar(30) NULL,
	data_src_id varchar(100) NULL,
	editbl_flg varchar(1) NULL,
	ntes varchar(4000) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.bud_liability_fund;

CREATE TABLE hdm.bud_liability_fund (
	liab_fund_key int4 NOT NULL,
	bud_key int4 NULL,
	expns_line_item_key int4 NULL,
	bud_acct_key int4 NULL,
	data_src_dim_key int4 NULL,
	bud_fund_compst_key int4 NULL,
	liab_key int4 NULL,
	prir_yr_pmt_diff_bud_key int4 NULL,
	allocn numeric(38,15) NULL,
	data_src_val numeric(38,15) NULL,
	fytd_liab numeric(38,15) NULL,
	fytd_expns numeric(38,15) NULL,
	prir_yr_fye_liab numeric(38,15) NULL,
	prir_yr_pmt_diff numeric(38,15) NULL,
	pend_invc_tot numeric(38,15) NULL,
	prcsd_invc_tot numeric(38,15) NULL,
	remng_to_accrue numeric(38,15) NULL,
	fye_liab numeric(38,15) NULL,
	needed_fye_liab numeric(38,15) NULL,
	suggested_liab_adjmt numeric(38,15) NULL,
	actl_liab_adjmt numeric(38,15) NULL,
	one_tm_adjmts numeric(38,15) NULL,
	line_item_bud numeric(38,15) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_liability_fund_ix_if1 ON hdm.bud_liability_fund USING btree (bud_key);
CREATE INDEX bud_liability_fund_ix_if2 ON hdm.bud_liability_fund USING btree (expns_line_item_key);
CREATE INDEX bud_liability_fund_ix_if3 ON hdm.bud_liability_fund USING btree (bud_acct_key);
CREATE INDEX bud_liability_fund_ix_if4 ON hdm.bud_liability_fund USING btree (data_src_dim_key);
CREATE INDEX bud_liability_fund_ix_if5 ON hdm.bud_liability_fund USING btree (bud_fund_compst_key);
CREATE INDEX bud_liability_fund_ix_if6 ON hdm.bud_liability_fund USING btree (liab_key);
CREATE INDEX bud_liability_fund_ix_if7 ON hdm.bud_liability_fund USING btree (prir_yr_pmt_diff_bud_key);

-- Drop table

-- DROP TABLE hdm.bud_lineitem_alloc;

CREATE TABLE hdm.bud_lineitem_alloc (
	lineitem_allocn_key int4 NOT NULL,
	bud_key int4 NULL,
	expns_line_item_key int4 NULL,
	data_src_dim_key int4 NULL,
	allocn_amt numeric(38,15) NULL,
	line_item_to_be_reallocd numeric(38,15) NULL,
	ntes varchar(4000) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_lineitem_alloc_ix_if2 ON hdm.bud_lineitem_alloc USING btree (bud_key);
CREATE INDEX bud_lineitem_alloc_ix_if3 ON hdm.bud_lineitem_alloc USING btree (expns_line_item_key);
CREATE INDEX bud_lineitem_alloc_ix_if4 ON hdm.bud_lineitem_alloc USING btree (data_src_dim_key);

-- Drop table

-- DROP TABLE hdm.bud_lineitem_alloc_fund;

CREATE TABLE hdm.bud_lineitem_alloc_fund (
	line_itm_allc_fund_key int4 NOT NULL,
	bud_strt_dt_key int4 NULL,
	bud_fund_compst_key int4 NULL,
	bud_key int4 NULL,
	expns_line_item_key int4 NULL,
	data_src_dim_key int4 NULL,
	rt numeric(38,15) NULL,
	data_src_val numeric(38,15) NULL,
	allocn_amt numeric(38,15) NULL,
	allocn_per_fund_ntes varchar(1000) NULL,
	adjmt numeric(38,15) NULL,
	rsdl_val numeric(38,15) NULL,
	orig_allocn numeric(38,15) NULL,
	reallocd_remng numeric(38,15) NULL,
	fytd_expns numeric(38,15) NULL,
	assmpn_chg numeric(38,15) NULL,
	to_be_reallocd numeric(38,15) NULL,
	src_bud_key numeric(38,15) NULL,
	pmt_difftl numeric(38,15) NULL,
	allocn_ntes varchar(1000) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_lineitem_alloc_fund_ix_if1 ON hdm.bud_lineitem_alloc_fund USING btree (bud_fund_compst_key);
CREATE INDEX bud_lineitem_alloc_fund_ix_if2 ON hdm.bud_lineitem_alloc_fund USING btree (bud_key);
CREATE INDEX bud_lineitem_alloc_fund_ix_if3 ON hdm.bud_lineitem_alloc_fund USING btree (expns_line_item_key);
CREATE INDEX bud_lineitem_alloc_fund_ix_if5 ON hdm.bud_lineitem_alloc_fund USING btree (bud_strt_dt_key);
CREATE INDEX bud_lineitem_alloc_fund_ix_if7 ON hdm.bud_lineitem_alloc_fund USING btree (data_src_dim_key);

-- Drop table

-- DROP TABLE hdm.bud_pmt_diff;

CREATE TABLE hdm.bud_pmt_diff (
	pmt_diff_key int4 NOT NULL,
	bud_key int4 NULL,
	expns_line_item_key int4 NULL,
	fiscl_yr numeric(38,15) NULL,
	invc_expctation varchar(30) NULL,
	pmt_difftl numeric(38,15) NULL,
	remng_liab numeric(38,15) NULL,
	adjmt_type varchar(30) NULL,
	reshuffle_pmt_diff_flg varchar(1) NULL,
	editbl_flg varchar(1) NULL,
	prir_yr_flg varchar(1) NULL,
	ntes varchar(4000) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_pmt_diff_ix_if1 ON hdm.bud_pmt_diff USING btree (bud_key);
CREATE INDEX bud_pmt_diff_ix_if2 ON hdm.bud_pmt_diff USING btree (expns_line_item_key);

-- Drop table

-- DROP TABLE hdm.bud_pmt_diff_fnd_invc;

CREATE TABLE hdm.bud_pmt_diff_fnd_invc (
	pmt_diff_fund_invc_key int4 NOT NULL,
	bud_key int4 NULL,
	expns_line_item_key int4 NULL,
	bud_fund_compst_key int4 NULL,
	fund_lvl_bud_key int4 NULL,
	bud_invc_key int4 NULL,
	fiscl_yr numeric(38,15) NULL,
	allocn_val numeric(38,15) NULL,
	stat varchar(30) NULL,
	prir_yr_flg varchar(1) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_pmt_diff_fnd_invc_ix_if1 ON hdm.bud_pmt_diff_fnd_invc USING btree (fund_lvl_bud_key);
CREATE INDEX bud_pmt_diff_fnd_invc_ix_if2 ON hdm.bud_pmt_diff_fnd_invc USING btree (bud_key);
CREATE INDEX bud_pmt_diff_fnd_invc_ix_if3 ON hdm.bud_pmt_diff_fnd_invc USING btree (expns_line_item_key);
CREATE INDEX bud_pmt_diff_fnd_invc_ix_if4 ON hdm.bud_pmt_diff_fnd_invc USING btree (bud_fund_compst_key);
CREATE INDEX bud_pmt_diff_fnd_invc_ix_if5 ON hdm.bud_pmt_diff_fnd_invc USING btree (bud_invc_key);

-- Drop table

-- DROP TABLE hdm.bud_pmt_diff_fund;

CREATE TABLE hdm.bud_pmt_diff_fund (
	pmt_diff_fund_key int4 NOT NULL,
	bud_key int4 NULL,
	expns_line_item_key int4 NULL,
	bud_fund_compst_key int4 NULL,
	fund_lvl_bud_key int4 NULL,
	fiscl_yr numeric(38,15) NULL,
	orig_allocn numeric(38,15) NULL,
	tot_invcs_paid numeric(38,15) NULL,
	tot_invcs_pend numeric(38,15) NULL,
	acct_xfers numeric(38,15) NULL,
	pmt_difftl numeric(38,15) NULL,
	remng_liab numeric(38,15) NULL,
	assmpn_chg_allocn numeric(38,15) NULL,
	prir_yr_flg varchar(1) NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_pmt_diff_fund_ix_if2 ON hdm.bud_pmt_diff_fund USING btree (bud_key);
CREATE INDEX bud_pmt_diff_fund_ix_if3 ON hdm.bud_pmt_diff_fund USING btree (expns_line_item_key);
CREATE INDEX bud_pmt_diff_fund_ix_if4 ON hdm.bud_pmt_diff_fund USING btree (bud_fund_compst_key);
CREATE INDEX bud_pmt_diff_fund_ix_if6 ON hdm.bud_pmt_diff_fund USING btree (fund_lvl_bud_key);

-- Drop table

-- DROP TABLE hdm.bud_trustee_election;

CREATE TABLE hdm.bud_trustee_election (
	trste_elctn_key int4 NOT NULL,
	bud_fund_compst_key int4 NULL,
	trste_elctn_id varchar(100) NULL,
	trste_nm varchar(100) NULL,
	fiscl_yr numeric(38,15) NULL,
	data_src_val numeric(38,15) NULL,
	ntes varchar(4000) NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX bud_trustee_election_ix_if1 ON hdm.bud_trustee_election USING btree (bud_fund_compst_key);

-- Drop table

-- DROP TABLE hdm.budget;

CREATE TABLE hdm.budget (
	bud_key int4 NOT NULL,
	bud_id varchar(60) NULL,
	bud_type varchar(25) NULL,
	end_dt timestamp NULL,
	fiscl_yr numeric(38,15) NULL,
	init_bud_w varchar(60) NULL,
	strt_dt timestamp NULL,
	bud_desc varchar(255) NULL,
	bud_src varchar(30) NULL,
	fiscl_ye_flg int4 NULL,
	insr_usr varchar(30) NULL,
	insr_ts timestamp NULL,
	vrsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.business_line;

CREATE TABLE hdm.business_line (
	bus_line_key int4 NOT NULL,
	bus_line varchar(255) NULL,
	invc_fee_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-

-- Drop table

-- DROP TABLE hdm.calendar;

CREATE TABLE hdm.calendar (
	day_key int4 NOT NULL,
	cal_day timestamp NULL,
	cal_day_shrt_nm varchar(3) NULL,
	cal_day_long_nm varchar(20) NULL,
	bus_day_flg varchar(1) NULL,
	mkt_hol_flg varchar(1) NULL,
	wk_cd varchar(20) NULL,
	wk_nbr int4 NULL,
	wk_shrt_nm varchar(20) NULL,
	wk_long_nm varchar(60) NULL,
	wk_ending_dt timestamp NULL,
	wk_days_cnt numeric(38,15) NULL,
	mo_cd varchar(20) NULL,
	mo_nbr int4 NULL,
	mo_nm varchar(25) NULL,
	mo_shrt_nm varchar(3) NULL,
	mo_long_nm varchar(20) NULL,
	cal_mo_end_flg varchar(1) NULL,
	bus_mo_end_flg varchar(1) NULL,
	mo_ending_dt timestamp NULL,
	mo_days_cnt numeric(38,15) NULL,
	qtr_cd varchar(10) NULL,
	qtr_nbr int4 NULL,
	qtr_shrt_nm varchar(20) NULL,
	qtr_long_nm varchar(60) NULL,
	cal_qtr_end_flg varchar(1) NULL,
	bus_qtr_end_flg varchar(1) NULL,
	qtr_ending_dt timestamp NULL,
	qtr_days_cnt numeric(38,15) NULL,
	yr_cd varchar(20) NULL,
	yr_nbr int4 NULL,
	yr_shrt_nm varchar(20) NULL,
	yr_long_nm varchar(60) NULL,
	yr_ending_dt timestamp NULL,
	ye_flg varchar(1) NULL,
	yr_last_bus_day_flg varchar(1) NULL,
	yr_days_cnt numeric(38,15) NULL,
	fiscl_wk_cd varchar(20) NULL,
	fiscl_wk_nbr int4 NULL,
	fiscl_wk_shrt_nm varchar(20) NULL,
	fiscl_wk_long_nm varchar(60) NULL,
	fiscl_wk_ending_dt timestamp NULL,
	fiscl_wk_days_cnt numeric(38,15) NULL,
	fiscl_mo_cd varchar(20) NULL,
	fiscl_mo_nbr int4 NULL,
	fiscl_mo_shrt_nm varchar(20) NULL,
	fiscl_mo_long_nm varchar(60) NULL,
	fiscl_mo_ending_dt timestamp NULL,
	fiscl_mo_end_flg varchar(1) NULL,
	fiscl_mo_last_bus_day_flg varchar(1) NULL,
	fiscl_mo_days_cnt numeric(38,15) NULL,
	fiscl_qtr_cd varchar(20) NULL,
	fiscl_qtr_nbr int4 NULL,
	fiscl_qtr_shrt_nm varchar(20) NULL,
	fiscl_qtr_long_nm varchar(60) NULL,
	fiscl_qtr_ending_flg varchar(1) NULL,
	fiscl_qtr_last_bus_day_flg varchar(1) NULL,
	fiscl_qtr_ending_dt timestamp NULL,
	fiscl_qtr_days_cnt numeric(38,15) NULL,
	fiscl_yr_cd varchar(20) NULL,
	fiscl_yr_shrt_nm varchar(20) NULL,
	fiscl_yr_long_nm varchar(60) NULL,
	fiscl_yr_ending_dt timestamp NULL,
	fiscl_yr_ending_flg varchar(1) NULL,
	fiscl_yr_last_bus_day_flg varchar(1) NULL,
	fiscl_yr_nbr numeric(38,15) NULL,
	fiscl_yr_days_cnt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX calendar_hcf1 ON hdm.calendar USING btree (bus_day_flg, cal_mo_end_flg, cal_day);
CREATE INDEX calendar_hcf2 ON hdm.calendar USING btree (bus_day_flg, cal_day, cal_mo_end_flg);
CREATE INDEX calendar_ix_ie1 ON hdm.calendar USING btree (bus_day_flg);
CREATE INDEX calendar_ix_ie2 ON hdm.calendar USING btree (mo_cd);
CREATE INDEX calendar_ix_ie3 ON hdm.calendar USING btree (qtr_cd);
CREATE INDEX calendar_ix_ie4 ON hdm.calendar USING btree (yr_cd);

-- Drop table

-- DROP TABLE hdm.cash_holding;

CREATE TABLE hdm.cash_holding (
	cash_holdg_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	cash numeric(38,15) NULL,
	shrt_term_invmt numeric(38,15) NULL,
	recbl numeric(38,15) NULL,
	paybl numeric(38,15) NULL,
	net_shrs numeric(38,15) NULL,
	net_asset numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	cash_coltrl numeric(38,15) NULL
);
CREATE INDEX cash_holding_ix_if1 ON hdm.cash_holding USING btree (fund_compst_key);
CREATE INDEX cash_holding_ix_if2 ON hdm.cash_holding USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.cash_reason;

CREATE TABLE hdm.cash_reason (
	cash_rsn__key int4 NOT NULL,
	cash_rsn_cd varchar(3) NULL,
	cash_rsn_desc varchar(255) NULL
);

-- Drop table

-- DROP TABLE hdm.cashflow;

CREATE TABLE hdm.cashflow (
	cashflow_key int4 NOT NULL,
	reg_key int4 NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	acct_key int4 NULL,
	pmt_mthd_key int4 NULL,
	trde_dt_key int4 NULL,
	shrhldr_key int4 NULL,
	confirm_dt_key int4 NULL,
	shrhldr_role_key int4 NOT NULL,
	acct_nbr numeric(38,15) NULL,
	sub_shrs numeric(38,15) NULL,
	sub_amt numeric(38,15) NULL,
	redmpn_shrs numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	ofrg_pr numeric(38,15) NULL,
	nav numeric(38,15) NULL,
	dlr_commsn_amt numeric(38,15) NULL,
	undr_wrtr_commsn_amt numeric(38,15) NULL,
	adv_commsn_amt numeric(38,15) NULL,
	net_cashflow_amt numeric(38,15) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NULL,
	net_shrs numeric(38,15) NULL,
	cash_cntl_recon_flg varchar(1) NULL,
	shrhldr_copy_key numeric(38,15) NULL
);
CREATE INDEX cashflow_ix_ie1 ON hdm.cashflow USING btree (shrhldr_role_key);
CREATE INDEX cashflow_ix_ie2 ON hdm.cashflow USING btree (acct_nbr);
CREATE INDEX cashflow_ix_if2 ON hdm.cashflow USING btree (dlr_key);
CREATE INDEX cashflow_ix_if3 ON hdm.cashflow USING btree (fund_key);
CREATE INDEX cashflow_ix_if4 ON hdm.cashflow USING btree (acct_key);
CREATE INDEX cashflow_ix_if5 ON hdm.cashflow USING btree (pmt_mthd_key);
CREATE INDEX cashflow_ix_if6 ON hdm.cashflow USING btree (trde_dt_key);
CREATE INDEX cashflow_ix_if7 ON hdm.cashflow USING btree (shrhldr_key);
CREATE INDEX cashflow_ix_if8 ON hdm.cashflow USING btree (confirm_dt_key);
CREATE INDEX cashflow_ix_if9 ON hdm.cashflow USING btree (reg_key);

-- Drop table

-- DROP TABLE hdm.cashflow_activity;

CREATE TABLE hdm.cashflow_activity (
	cashflow_act_hist_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	day_key int4 NOT NULL,
	sub_shrs numeric(38,15) NULL,
	sub_amt numeric(38,15) NULL,
	redmpn_shrs numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	net_shrs numeric(38,15) NULL,
	net_cashflow_amt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX cashflow_activity_ix_if1 ON hdm.cashflow_activity USING btree (fund_key);
CREATE INDEX cashflow_activity_ix_if2 ON hdm.cashflow_activity USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.cashflow_activity_hist;

CREATE TABLE hdm.cashflow_activity_hist (
	cashflow_act_hist_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	day_key int4 NOT NULL,
	sub_shrs numeric(38,15) NULL,
	sub_amt numeric(38,15) NULL,
	redmpn_shrs numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	net_shrs numeric(38,15) NULL,
	net_cashflow_amt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX cashflow_activity_hist_ix_if1 ON hdm.cashflow_activity_hist USING btree (fund_key);
CREATE INDEX cashflow_activity_hist_ix_if2 ON hdm.cashflow_activity_hist USING btree (day_key);
CREATE UNIQUE INDEX cashflow_activity_hist_ix_pk ON hdm.cashflow_activity_hist USING btree (cashflow_act_hist_key);

-- Drop table

-- DROP TABLE hdm.cashflow_detail;

CREATE TABLE hdm.cashflow_detail (
	sdcm_cashflow_key int4 NOT NULL,
	reg_key int4 NULL,
	pmt_mthd_key int4 NULL,
	dlr_key int4 NULL,
	shrhldr_key int4 NULL,
	acct_key int4 NULL,
	fund_key int4 NULL,
	spr_sheet_dt_key int4 NULL,
	trde_dt_key int4 NULL,
	confirm_dt_key int4 NULL,
	shrhldr_role_key int4 NOT NULL,
	acct_nbr numeric(38,15) NULL,
	sub_shrs numeric(38,15) NULL,
	sub_amt numeric(38,15) NULL,
	redmpn_shrs numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	ofrg_pr numeric(38,15) NULL,
	nav numeric(38,15) NULL,
	dlr_commsn_amt numeric(38,15) NULL,
	undr_wrtr_commsn_amt numeric(38,15) NULL,
	adv_commsn_amt numeric(38,15) NULL,
	net_shrs numeric(38,15) NULL,
	net_cashflow_amt numeric(38,15) NULL,
	cash_cntl_recon_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NOT NULL,
	hbr_dlr_key int4 NULL
);
CREATE INDEX cashflow_detail_ix_ie1 ON hdm.cashflow_detail USING btree (shrhldr_role_key);
CREATE INDEX cashflow_detail_ix_ie2 ON hdm.cashflow_detail USING btree (acct_nbr);
CREATE INDEX cashflow_detail_ix_if1 ON hdm.cashflow_detail USING btree (reg_key);
CREATE INDEX cashflow_detail_ix_if2 ON hdm.cashflow_detail USING btree (pmt_mthd_key);
CREATE INDEX cashflow_detail_ix_if3 ON hdm.cashflow_detail USING btree (spr_sheet_dt_key);
CREATE INDEX cashflow_detail_ix_if4 ON hdm.cashflow_detail USING btree (dlr_key);
CREATE INDEX cashflow_detail_ix_if5 ON hdm.cashflow_detail USING btree (fund_key);
CREATE INDEX cashflow_detail_ix_if6 ON hdm.cashflow_detail USING btree (shrhldr_key);
CREATE INDEX cashflow_detail_ix_if7 ON hdm.cashflow_detail USING btree (acct_key);
CREATE INDEX cashflow_detail_ix_if8 ON hdm.cashflow_detail USING btree (trde_dt_key);
CREATE INDEX cashflow_detail_ix_if9 ON hdm.cashflow_detail USING btree (confirm_dt_key);

-- Drop table

-- DROP TABLE hdm.cashflow_history;

CREATE TABLE hdm.cashflow_history (
	cashflow_hist_key int4 NOT NULL,
	reg_key int4 NULL,
	dlr_key int4 NULL,
	shrhldr_key int4 NULL,
	acct_key int4 NULL,
	fund_key int4 NULL,
	pmt_mthd_key int4 NULL,
	trde_dt_key int4 NULL,
	confirm_dt_key int4 NULL,
	acct_nbr numeric(38,15) NULL,
	sub_shrs numeric(38,15) NULL,
	sub_amt numeric(38,15) NULL,
	redmpn_shrs numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	ofrg_pr numeric(38,15) NULL,
	nav numeric(38,15) NULL,
	dlr_commsn_amt numeric(38,15) NULL,
	undr_wrtr_commsn_amt numeric(38,15) NULL,
	adv_commsn_amt numeric(38,15) NULL,
	net_shrs numeric(38,15) NULL,
	net_cashflow_amt numeric(38,15) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	hbr_dlr_key int4 NULL
);
CREATE INDEX cashflow_history_ix_ie2 ON hdm.cashflow_history USING btree (acct_nbr);
CREATE INDEX cashflow_history_ix_if1 ON hdm.cashflow_history USING btree (reg_key);
CREATE INDEX cashflow_history_ix_if2 ON hdm.cashflow_history USING btree (trde_dt_key);
CREATE INDEX cashflow_history_ix_if3 ON hdm.cashflow_history USING btree (confirm_dt_key);
CREATE INDEX cashflow_history_ix_if4 ON hdm.cashflow_history USING btree (fund_key);
CREATE INDEX cashflow_history_ix_if5 ON hdm.cashflow_history USING btree (dlr_key);
CREATE INDEX cashflow_history_ix_if6 ON hdm.cashflow_history USING btree (shrhldr_key);
CREATE INDEX cashflow_history_ix_if7 ON hdm.cashflow_history USING btree (acct_key);
CREATE INDEX cashflow_history_ix_if8 ON hdm.cashflow_history USING btree (pmt_mthd_key);
CREATE INDEX cashflow_history_ix_if9 ON hdm.cashflow_history USING btree (hbr_dlr_key);

-- Drop table

-- DROP TABLE hdm.cct_city_tax_list;

CREATE TABLE hdm.cct_city_tax_list (
	city_tax_list_key int4 NOT NULL,
	city_tax_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.cct_harbor_group;

CREATE TABLE hdm.cct_harbor_group (
	hbr_grp_key int4 NOT NULL,
	co_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.cct_spend_category;

CREATE TABLE hdm.cct_spend_category (
	spend_cat_key int4 NOT NULL,
	spend_cat varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.cct_supplier;

CREATE TABLE hdm.cct_supplier (
	suppl_key int4 NOT NULL,
	suppl_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.cct_tax_detail;

CREATE TABLE hdm.cct_tax_detail (
	tax_det_key int4 NOT NULL,
	tax_sum_key int4 NULL,
	city_tax_list_key int4 NULL,
	city_tax_amt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX cct_tax_detail_ix_if1 ON hdm.cct_tax_detail USING btree (tax_sum_key);
CREATE INDEX cct_tax_detail_ix_if2 ON hdm.cct_tax_detail USING btree (city_tax_list_key);

-- Drop table

-- DROP TABLE hdm.cct_tax_summary;

CREATE TABLE hdm.cct_tax_summary (
	tax_sum_key int4 NOT NULL,
	hbr_grp_key int4 NULL,
	spend_cat_key int4 NULL,
	suppl_key int4 NULL,
	day_key int4 NULL,
	key_ref varchar(25) NULL,
	line_memo varchar(1000) NULL,
	suppl_ref_nbr varchar(60) NULL,
	invc_line_desc varchar(4000) NULL,
	tax_anlys_conclsn varchar(4000) NULL,
	invc_amt numeric(38,15) NULL,
	tot_tax_amt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX cct_tax_summary_ix_if1 ON hdm.cct_tax_summary USING btree (hbr_grp_key);
CREATE INDEX cct_tax_summary_ix_if2 ON hdm.cct_tax_summary USING btree (spend_cat_key);
CREATE INDEX cct_tax_summary_ix_if3 ON hdm.cct_tax_summary USING btree (suppl_key);
CREATE INDEX cct_tax_summary_ix_if4 ON hdm.cct_tax_summary USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.cost_type;

CREATE TABLE hdm.cost_type (
	cost_type_key int4 NOT NULL,
	cost_type_id varchar(25) NULL,
	cost_type_desc varchar(255) NULL
);

-- Drop table

-- DROP TABLE hdm.country;

CREATE TABLE hdm.country (
	crty_key int4 NOT NULL,
	crty_nm varchar(250) NULL,
	crty_reg varchar(150) NULL,
	crty_cd varchar(5) NULL,
	crty_factset_reg varchar(250) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.country_threshold;

CREATE TABLE hdm.country_threshold (
	crty_thrs_key int4 NOT NULL,
	crty_key int4 NULL,
	thrs_desc varchar(255) NULL,
	intrl_trig numeric(38,15) NULL,
	ofcl_trig numeric(38,15) NULL,
	thrs_days numeric(38,15) NULL,
	reglty_filng_days numeric(38,15) NULL,
	filng_days numeric(38,15) NULL,
	intrl_trig_displ varchar(25) NULL,
	ofcl_trig_displ varchar(25) NULL,
	thrs_days_displ varchar(25) NULL,
	reglty_filng_displ_days varchar(25) NULL,
	filng_displ_days varchar(25) NULL,
	threshold_type int4 NULL,
	custom_ind varchar(1) NULL,
	thrshld_mlstn varchar(25) NULL,
	thrshld_mlstn_displ numeric(38,15) NULL,
	periodic_trshld varchar(25) NULL,
	periodic_thrsld_disp numeric(38,15) NULL,
	any_amt_ind varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX country_threshold_ix_if1 ON hdm.country_threshold USING btree (crty_key);

-- Drop table

-- DROP TABLE hdm.currency;

CREATE TABLE hdm.currency (
	currcy_key int4 NOT NULL,
	currcy_cd varchar(20) NULL,
	currcy_nm varchar(255) NULL,
	currcy_desc varchar(250) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.daily_avg_acct_position;

CREATE TABLE hdm.daily_avg_acct_position (
	daily_avg_acct_posn_key int4 NOT NULL,
	dlr_key int4 NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	acct_key int4 NULL,
	avg_daily_shrs numeric(38,15) NULL,
	avg_daily_bal numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	tpa_key int4 NULL
);
CREATE INDEX daily_avg_acct_position_ix_if1 ON hdm.daily_avg_acct_position USING btree (dlr_key);
CREATE INDEX daily_avg_acct_position_ix_if2 ON hdm.daily_avg_acct_position USING btree (day_key);
CREATE INDEX daily_avg_acct_position_ix_if3 ON hdm.daily_avg_acct_position USING btree (fund_key);
CREATE INDEX daily_avg_acct_position_ix_if4 ON hdm.daily_avg_acct_position USING btree (acct_key);
CREATE INDEX daily_avg_acct_position_ix_if6 ON hdm.daily_avg_acct_position USING btree (tpa_key);

-- Drop table

-- DROP TABLE hdm.dealer;

CREATE TABLE hdm.dealer (
	dlr_key int4 NOT NULL,
	dlr_top_lvl varchar(3) NULL,
	dlr_top_lvl_desc varchar(60) NULL,
	dlr_grp_id varchar(25) NULL,
	dlr_grp_nm varchar(255) NULL,
	dlr_id varchar(25) NULL,
	branch_estb_dt timestamp NULL,
	branch_closed_dt timestamp NULL,
	sellg_agrmt_rqrd_flg varchar(1) NULL,
	sellg_agrmt_12b1_flg varchar(1) NULL,
	payout_freq_cd varchar(1) NULL,
	payout_freq_desc varchar(60) NULL,
	stmt_freq_cd varchar(2) NULL,
	stmt_freq_desc varchar(60) NULL,
	recv_12b1_tape_flg varchar(1) NULL,
	auto_divd_chk_flg varchar(1) NULL,
	wire_incl_accrl_flg varchar(1) NULL,
	sipc_insd_flg varchar(1) NULL,
	dlr_fdic_sell_flg varchar(1) NULL,
	sec_id numeric(38,15) NULL,
	mbr_ship_type_cd varchar(2) NULL,
	mbr_ship_type varchar(60) NULL,
	inval_rep_hndlg_cd varchar(1) NULL,
	inval_rep_hndlg_desc varchar(60) NULL,
	invmt_slip_addr_cd varchar(1) NULL,
	invmt_slip_addr varchar(60) NULL,
	trste_dflt_dlr_id numeric(38,15) NULL,
	grp_sales_chnl_id varchar(25) NULL,
	grp_fincl_inst_id numeric(38,15) NULL,
	divd_confirm_sprsn_flg varchar(1) NULL,
	daily_confirm_sprsn_flg varchar(1) NULL,
	onln_inval_rep_hndlg_flg varchar(1) NULL,
	dlr_rep_st_lic_vldn_flg varchar(1) NULL,
	dlr_inval_rep_st_lic_actn_flg varchar(1) NULL,
	nasd_regstn_vldn_lic_flg varchar(1) NULL,
	inval_rep_nasd_regstr_flg varchar(1) NULL,
	dlr_nm varchar(60) NULL,
	addr_line_1 varchar(60) NULL,
	addr_line_2 varchar(60) NULL,
	addr_line_3 varchar(60) NULL,
	addr_line_4 varchar(60) NULL,
	city_nm varchar(60) NULL,
	st varchar(2) NULL,
	postl_zip_cd int4 NULL,
	crty varchar(60) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	spr_onbs_flg varchar(1) NULL,
	onbs_flg varchar(1) NULL,
	intrl_cntl_doc_dt timestamp NULL,
	ltst_cntl_audt_recpt_dt timestamp NULL,
	inactv_dt timestamp NULL,
	actv_flg varchar(1) NULL DEFAULT 'Y'::character varying
);

-- Drop table

-- DROP TABLE hdm.dealer_asset_summary;

CREATE TABLE hdm.dealer_asset_summary (
	dlr_fee_sum_key int4 NOT NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	day_key int4 NULL,
	prir_per_asset numeric(38,15) NULL,
	curr_per_asset numeric(38,15) NULL,
	prir_per_eligbl_asset numeric(38,15) NULL,
	curr_per_eligbl_asset numeric(38,15) NULL,
	prir_per_inelig_asset numeric(38,15) NULL,
	curr_per_inelig_asset numeric(38,15) NULL,
	prir_per_ta_asset numeric(38,15) NULL,
	curr_per_ta_asset numeric(38,15) NULL,
	prir_per_ta_eligbl_asset numeric(38,15) NULL,
	curr_per_ta_eligbl_asset numeric(38,15) NULL,
	prir_per_ta_inelig_asset numeric(38,15) NULL,
	curr_per_ta_inelig_asset numeric(38,15) NULL,
	prir_per_avg_asset numeric(38,15) NULL,
	curr_per_avg_asset numeric(38,15) NULL,
	prir_per_avg_eligbl_asset numeric(38,15) NULL,
	curr_per_avg_eligbl_asset numeric(38,15) NULL,
	prir_per_avg_inelig_asset numeric(38,15) NULL,
	curr_per_avg_inelig_asset numeric(38,15) NULL,
	prir_per_avg_ta_asset numeric(38,15) NULL,
	curr_per_avg_ta_asset numeric(38,15) NULL,
	prir_avg_ta_eligbl_asset numeric(38,15) NULL,
	curr_avg_ta_inelig_asset numeric(38,15) NULL,
	prir_avg_ta_inelig_asset numeric(38,15) NULL,
	curr_avg_ta_eligbl_asset numeric(38,15) NULL,
	sub_amt numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	net_cashflow_amt numeric(38,15) NULL,
	mkt_val_chg numeric(38,15) NULL,
	avg_sub_amt numeric(38,15) NULL,
	avg_redmpn_amt numeric(38,15) NULL,
	avg_net_amt numeric(38,15) NULL,
	avg_mkt_val_chg numeric(38,15) NULL,
	dlr_tpa_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dealer_asset_summary_ix_if1 ON hdm.dealer_asset_summary USING btree (dlr_key);
CREATE INDEX dealer_asset_summary_ix_if2 ON hdm.dealer_asset_summary USING btree (fund_key);
CREATE INDEX dealer_asset_summary_ix_if3 ON hdm.dealer_asset_summary USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.dealer_branch;

CREATE TABLE hdm.dealer_branch (
	dlr_branch_key int4 NOT NULL,
	dlr_id varchar(25) NULL,
	branch_id varchar(25) NULL,
	branch_nm varchar(60) NULL,
	branch_addr_line_1 varchar(35) NULL,
	branch_addr_line_2 varchar(35) NULL,
	branch_addr_line_3 varchar(35) NULL,
	branch_addr_line_4 varchar(35) NULL,
	branch_city varchar(35) NULL,
	branch_st_cd varchar(3) NULL,
	branch_st varchar(35) NULL,
	branch_crty varchar(35) NULL,
	branch_zip_cd varchar(25) NULL,
	branch_estb_dt timestamp NULL,
	branch_closed_dt timestamp NULL,
	branch_pay_ofc_id varchar(25) NULL,
	branch_func_type_id varchar(3) NULL,
	branch_forgn_tax_cd numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NOT NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.dealer_branch_assoc;

CREATE TABLE hdm.dealer_branch_assoc (
	dlr_branch_assn_key int4 NOT NULL,
	row_strt_dttm timestamp NOT NULL,
	branch_key int4 NOT NULL,
	dlr_role_key int4 NOT NULL,
	branch_pay_ofc_id varchar(25) NULL,
	row_stop_dttm timestamp NULL,
	src_sys_del_flg varchar(1) NULL,
	src_sys_userid varchar(12) NULL,
	curr_row_flg varchar(1) NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id varchar(25) NULL,
	dq_lvl_cd varchar(3) NULL
);
CREATE INDEX dealer_branch_assoc_ix_if2 ON hdm.dealer_branch_assoc USING btree (dlr_role_key);
CREATE UNIQUE INDEX dealer_branch_assoc_ix_if3 ON hdm.dealer_branch_assoc USING btree (branch_key);

-- Drop table

-- DROP TABLE hdm.dealer_change_control;

CREATE TABLE hdm.dealer_change_control (
	dlr_chg_cntl_key int4 NOT NULL,
	dlr_key int4 NULL,
	day_key int4 NULL,
	chg_cntl_id numeric(38,15) NULL,
	chg_type varchar(255) NULL,
	chg_desc varchar(4000) NULL,
	recvg_dlr_nm varchar(255) NULL,
	recvg_dlr_acct_type varchar(60) NULL,
	acct_move_cnt numeric(38,15) NULL,
	bsky_ntfn_flg varchar(1) NULL,
	hbr_impct varchar(1000) NULL,
	cusip_list varchar(255) NULL,
	delvy_dlr_mtrx_lvl varchar(60) NULL,
	delvy_dlr_acct_type varchar(60) NULL,
	asset_move_val numeric(38,15) NULL,
	delvy_dlr_plfm_nm varchar(255) NULL,
	delvy_dlr_fees numeric(38,15) NULL,
	recvg_dlr_id varchar(25) NULL,
	recvg_dlr_mtrx_lvl varchar(60) NULL,
	recvg_dlr_fees numeric(38,15) NULL,
	cnvrsn_strt_dt timestamp NULL,
	cnvrsn_end_dt timestamp NULL,
	chg_rsn varchar(4000) NULL,
	chg_contnt_type varchar(60) NULL,
	contct_nm varchar(255) NULL,
	ufid_stat varchar(60) NULL,
	dlr_grping_stat varchar(60) NULL,
	dlr_prof_stat varchar(60) NULL,
	mtf_stat varchar(60) NULL,
	live_link_stat varchar(60) NULL,
	rfrser_rqst varchar(4000) NULL,
	commfee_cmmt varchar(4000) NULL,
	cumdisc_cd varchar(25) NULL,
	mtf_inlsn_flg varchar(1) NULL,
	awd_send_val varchar(25) NULL,
	mtf_excln_flg varchar(1) NULL,
	excln_cmmts varchar(4000) NULL,
	inlsn_cmmts varchar(4000) NULL,
	cnvrsn_stat varchar(60) NULL,
	dlr_tot_asset_pct numeric(38,15) NULL,
	chg_cntl_wkfl varchar(25) NULL,
	fee_impct_flg varchar(1) NULL,
	creatr varchar(255) NULL,
	modfr varchar(255) NULL,
	creatn_dt timestamp NULL,
	chg_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dealer_change_control_ix_if1 ON hdm.dealer_change_control USING btree (dlr_key);
CREATE INDEX dealer_change_control_ix_if2 ON hdm.dealer_change_control USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.dealer_change_list;

CREATE TABLE hdm.dealer_change_list (
	dlr_chg_list_key int4 NOT NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	acct_key int4 NULL,
	dlr_chg_type varchar(60) NULL,
	acct_maint_seq_id int4 NULL,
	dlr_orig_val varchar(60) NULL,
	dlr_new_val varchar(60) NULL,
	batch_nbr varchar(25) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dealer_change_list_ix_if1 ON hdm.dealer_change_list USING btree (day_key);
CREATE INDEX dealer_change_list_ix_if2 ON hdm.dealer_change_list USING btree (fund_key);
CREATE INDEX dealer_change_list_ix_if3 ON hdm.dealer_change_list USING btree (acct_key);

-- Drop table

-- DROP TABLE hdm.dealer_fee_analysis;

CREATE TABLE hdm.dealer_fee_analysis (
	dlr_fee_anlys_key int4 NOT NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	day_key int4 NULL,
	dlr_fee_type_key int4 NULL,
	contrctl_rt numeric(38,15) NULL,
	actl_fee numeric(38,15) NULL,
	actl_rt numeric(38,15) NULL,
	avg_daily_asset numeric(38,15) NULL,
	avg_daily_asset_prir_per numeric(38,15) NULL,
	avg_eligbl_daily_asset numeric(38,15) NULL,
	avg_eligbl_asset_prir_per numeric(38,15) NULL,
	drvvd_rt numeric(38,15) NULL,
	prir_per_fee numeric(38,15) NULL,
	prir_per_rt numeric(38,15) NULL,
	fee_diff numeric(38,15) NULL,
	asset_diff numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dealer_fee_analysis_ix_if1 ON hdm.dealer_fee_analysis USING btree (dlr_key);
CREATE INDEX dealer_fee_analysis_ix_if2 ON hdm.dealer_fee_analysis USING btree (fund_key);
CREATE INDEX dealer_fee_analysis_ix_if3 ON hdm.dealer_fee_analysis USING btree (day_key);
CREATE INDEX dealer_fee_analysis_ix_if4 ON hdm.dealer_fee_analysis USING btree (dlr_fee_type_key);

-- Drop table

-- DROP TABLE hdm.dealer_fee_research;

CREATE TABLE hdm.dealer_fee_research (
	dlr_fee_rsrh_key int4 NOT NULL,
	dlr_key int4 NULL,
	invc_day_key int4 NULL,
	pmt_day_key int4 NULL,
	fund_key int4 NULL,
	acct_key int4 NULL,
	dlr_fee_type_key int4 NULL,
	hbr_avg_aum numeric(38,15) NULL,
	hbr_avg_shrs numeric(38,15) NULL,
	fee_bp numeric(38,15) NULL,
	fee_calc numeric(38,15) NULL,
	hbr_avg_aum_prev numeric(38,15) NULL,
	hbr_avg_shrs_prev numeric(38,15) NULL,
	invc_freq_flg varchar(1) NULL,
	billg_type varchar(25) NULL,
	fee_chrgd numeric(38,15) NULL,
	avg_aum numeric(38,15) NULL,
	avg_bp numeric(38,15) NULL,
	fee_chrgd_prev numeric(38,15) NULL,
	avg_aum_prev numeric(38,15) NULL,
	avg_bp_prev numeric(38,15) NULL,
	fee_chrgd_flg varchar(1) NULL,
	cusip_lvl_aum_diff_flg varchar(1) NULL,
	cusip_lvl_fee_diff_flg varchar(1) NULL,
	cusip_curr_aum_diff_flg varchar(1) NULL,
	acct_curr_aum_diff_flg varchar(1) NULL,
	fee_diff_flg varchar(1) NULL,
	aum_diff_flg varchar(1) NULL,
	nav_mo_avg numeric(38,15) NULL,
	nav_mo_avg_prev numeric(38,15) NULL,
	curr_per_days numeric(38,15) NULL,
	prir_per_days numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dealer_fee_research_ix_ie1 ON hdm.dealer_fee_research USING btree (fee_chrgd_flg);
CREATE INDEX dealer_fee_research_ix_ie2 ON hdm.dealer_fee_research USING btree (invc_freq_flg);
CREATE INDEX dealer_fee_research_ix_ie3 ON hdm.dealer_fee_research USING btree (cusip_lvl_aum_diff_flg);
CREATE INDEX dealer_fee_research_ix_ie4 ON hdm.dealer_fee_research USING btree (cusip_lvl_fee_diff_flg);
CREATE INDEX dealer_fee_research_ix_if1 ON hdm.dealer_fee_research USING btree (dlr_key);
CREATE INDEX dealer_fee_research_ix_if2 ON hdm.dealer_fee_research USING btree (invc_day_key);
CREATE INDEX dealer_fee_research_ix_if3 ON hdm.dealer_fee_research USING btree (fund_key);
CREATE INDEX dealer_fee_research_ix_if4 ON hdm.dealer_fee_research USING btree (acct_key);
CREATE INDEX dealer_fee_research_ix_if5 ON hdm.dealer_fee_research USING btree (dlr_fee_type_key);
CREATE INDEX dealer_fee_research_ix_if7 ON hdm.dealer_fee_research USING btree (pmt_day_key);

-- Drop table

-- DROP TABLE hdm.dealer_fee_schedule;

CREATE TABLE hdm.dealer_fee_schedule (
	dlr_fee_sched_key int4 NOT NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	dlr_fee_type_key int4 NULL,
	day_key int4 NULL,
	fee_rt numeric(38,15) NULL,
	bus_line varchar(255) NULL,
	invc_fee_desc varchar(255) NULL,
	cmmts varchar(4000) NULL,
	efftv_strt_dt timestamp NULL,
	efftv_end_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dealer_fee_schedule_ix_if2 ON hdm.dealer_fee_schedule USING btree (dlr_key);
CREATE INDEX dealer_fee_schedule_ix_if3 ON hdm.dealer_fee_schedule USING btree (fund_key);
CREATE INDEX dealer_fee_schedule_ix_if4 ON hdm.dealer_fee_schedule USING btree (dlr_fee_type_key);
CREATE INDEX dealer_fee_schedule_ix_if5 ON hdm.dealer_fee_schedule USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.dealer_fee_type;

CREATE TABLE hdm.dealer_fee_type (
	dlr_fee_type_key int4 NOT NULL,
	dlr_fee_type_cd varchar(4) NULL,
	dlr_fee_type_desc varchar(50) NULL
);

-- Drop table

-- DROP TABLE hdm.dealer_invoice;

CREATE TABLE hdm.dealer_invoice (
	dlr_invc_key int4 NOT NULL,
	dlr_fee_type_key int4 NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	invc_day_key int4 NULL,
	pmt_day_key int4 NULL,
	acct_key int4 NULL,
	bus_line varchar(255) NULL,
	invc_avg_asset numeric(38,15) NULL,
	fee_rt numeric(38,15) NULL,
	tot_fee_amt numeric(38,15) NULL,
	posn_cnt numeric(38,15) NULL,
	per_acct_fee numeric(38,15) NULL,
	dlr_portal_asset numeric(38,15) NULL,
	portal_posn_cnt numeric(38,15) NULL,
	invc_freq_flg varchar(1) NULL,
	invc_nbr varchar(25) NULL,
	invc_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	rec_splt_flg varchar(1) NULL,
	dlr_cat varchar(255) NULL,
	invc_cat varchar(255) NULL
);
CREATE INDEX dealer_invoice_ix_if1 ON hdm.dealer_invoice USING btree (dlr_fee_type_key);
CREATE INDEX dealer_invoice_ix_if2 ON hdm.dealer_invoice USING btree (dlr_key);
CREATE INDEX dealer_invoice_ix_if3 ON hdm.dealer_invoice USING btree (fund_key);
CREATE INDEX dealer_invoice_ix_if4 ON hdm.dealer_invoice USING btree (invc_day_key);
CREATE INDEX dealer_invoice_ix_if6 ON hdm.dealer_invoice USING btree (pmt_day_key);
CREATE INDEX dealer_invoice_ix_if7 ON hdm.dealer_invoice USING btree (acct_key);

-- Drop table

-- DROP TABLE hdm.dealer_invoice_model;

CREATE TABLE hdm.dealer_invoice_model (
	dlr_invc_mdl_key int4 NOT NULL,
	dlr_fee_type_key int4 NULL,
	dlr_key int4 NULL,
	invc_day_key int4 NULL,
	pmt_day_key int4 NULL,
	fund_key int4 NULL,
	bus_line varchar(255) NULL,
	invc_avg_asset numeric(38,15) NULL,
	tot_fee_amt numeric(38,15) NULL,
	posn_cnt numeric(38,15) NULL,
	per_acct_fee numeric(38,15) NULL,
	dlr_portal_asset numeric(38,15) NULL,
	portal_posn_cnt numeric(38,15) NULL,
	invc_freq_flg varchar(1) NULL,
	rec_splt_flg varchar(1) NULL,
	invc_nbr varchar(20) NULL,
	invc_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dealer_invoice_model_ix_if1 ON hdm.dealer_invoice_model USING btree (dlr_fee_type_key);
CREATE INDEX dealer_invoice_model_ix_if2 ON hdm.dealer_invoice_model USING btree (dlr_key);
CREATE INDEX dealer_invoice_model_ix_if3 ON hdm.dealer_invoice_model USING btree (invc_day_key);
CREATE INDEX dealer_invoice_model_ix_if4 ON hdm.dealer_invoice_model USING btree (pmt_day_key);
CREATE INDEX dealer_invoice_model_ix_if5 ON hdm.dealer_invoice_model USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.dealer_invoice_rule;

CREATE TABLE hdm.dealer_invoice_rule (
	invc_dlr_rule_key int4 NOT NULL,
	dlr_key int4 NULL,
	shr_cls_cd numeric(38,15) NOT NULL,
	trgt_bus_line varchar(255) NULL,
	src_bus_line varchar(255) NULL,
	calc_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	mul_src_bl_flg bpchar(1) NULL,
	fee_used_in_fee_flg bpchar(1) NULL,
	bp_value numeric(38,15) NULL,
	cnt_me_only_me_flg numeric(38,15) NULL,
	efftv_strt_dt timestamp NULL,
	efftv_end_dt timestamp NULL
);
CREATE INDEX dealer_invoice_rule_ix_if1 ON hdm.dealer_invoice_rule USING btree (dlr_key);

-- Drop table

-- DROP TABLE hdm.dealer_invoice_trial;

CREATE TABLE hdm.dealer_invoice_trial (
	dlr_invc_trl_key int4 NOT NULL,
	dlr_key int4 NULL,
	acct_key int4 NULL,
	fund_key int4 NULL,
	invc_day_key int4 NULL,
	dlr_fee_type_key int4 NULL,
	pmt_day_key int4 NULL,
	fee_rt numeric(38,15) NULL,
	tot_fee_amt numeric(38,15) NULL,
	invc_nbr varchar(25) NULL,
	invc_dt timestamp NULL,
	bus_line varchar(255) NULL,
	invc_avg_asset numeric(38,15) NULL,
	posn_cnt numeric(38,15) NULL,
	per_acct_fee numeric(38,15) NULL,
	dlr_portal_asset numeric(38,15) NULL,
	portal_posn_cnt numeric(38,15) NULL,
	invc_freq_flg varchar(1) NULL,
	rec_splt_flg varchar(1) NULL,
	dlr_cat varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dealer_invoice_trial_ix_if1 ON hdm.dealer_invoice_trial USING btree (dlr_key);
CREATE INDEX dealer_invoice_trial_ix_if2 ON hdm.dealer_invoice_trial USING btree (acct_key);
CREATE INDEX dealer_invoice_trial_ix_if3 ON hdm.dealer_invoice_trial USING btree (fund_key);
CREATE INDEX dealer_invoice_trial_ix_if4 ON hdm.dealer_invoice_trial USING btree (invc_day_key);
CREATE INDEX dealer_invoice_trial_ix_if5 ON hdm.dealer_invoice_trial USING btree (dlr_fee_type_key);
CREATE INDEX dealer_invoice_trial_ix_if6 ON hdm.dealer_invoice_trial USING btree (pmt_day_key);

-- Drop table

-- DROP TABLE hdm.dealer_level_control;

CREATE TABLE hdm.dealer_level_control (
	dlr_lvl_cntl_key int4 NOT NULL,
	dlr_lvl_cntl_cd varchar(3) NOT NULL,
	dlr_lvl_cntl_desc varchar(60) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NOT NULL
);

-- Drop table

-- DROP TABLE hdm.dealer_payment_detail;

CREATE TABLE hdm.dealer_payment_detail (
	dlr_pmt_det_key int4 NOT NULL,
	day_key int4 NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	dlr_branch_key int4 NULL,
	dlr_fee_type_key int4 NULL,
	dlr_pmt_mthd_key int4 NULL,
	acct_key int4 NULL,
	wk_ord_id varchar(25) NULL,
	nscc_flg varchar(1) NULL,
	nscc_nm_use_flg varchar(1) NULL,
	pmt_ofst_cd varchar(25) NULL,
	payee_type varchar(3) NULL,
	splt_compnsn_flg varchar(1) NULL,
	prev_compnsn_ovrd_type varchar(4) NULL,
	eligbl_compnsn_ovrd_type varchar(4) NULL,
	payout_type_key int4 NULL,
	pre_agreed_shr_rt numeric(38,15) NULL,
	pre_agreed_asset numeric(38,15) NULL,
	pre_agreed_fee numeric(38,15) NULL,
	prev_splt_fee numeric(38,15) NULL,
	tot_eligbl_rt numeric(38,15) NULL,
	tot_eligbl_asset numeric(38,15) NULL,
	tot_eligbl_fee numeric(38,15) NULL,
	tot_compnsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	avg_eligbl_bp numeric(38,15) NULL,
	avg_eligbl_asset numeric(38,15) NULL,
	avg_eligbl_fees numeric(38,15) NULL,
	pmt_day_key int4 NULL
);
CREATE INDEX dealer_payment_detail_ix_if1 ON hdm.dealer_payment_detail USING btree (day_key);
CREATE INDEX dealer_payment_detail_ix_if10 ON hdm.dealer_payment_detail USING btree (pmt_day_key);
CREATE INDEX dealer_payment_detail_ix_if2 ON hdm.dealer_payment_detail USING btree (dlr_key);
CREATE INDEX dealer_payment_detail_ix_if3 ON hdm.dealer_payment_detail USING btree (fund_key);
CREATE INDEX dealer_payment_detail_ix_if4 ON hdm.dealer_payment_detail USING btree (dlr_branch_key);
CREATE INDEX dealer_payment_detail_ix_if5 ON hdm.dealer_payment_detail USING btree (dlr_fee_type_key);
CREATE INDEX dealer_payment_detail_ix_if6 ON hdm.dealer_payment_detail USING btree (dlr_pmt_mthd_key);
CREATE INDEX dealer_payment_detail_ix_if7 ON hdm.dealer_payment_detail USING btree (acct_key);
CREATE INDEX dealer_payment_detail_ix_if8 ON hdm.dealer_payment_detail USING btree (payout_type_key);

-- Drop table

-- DROP TABLE hdm.dealer_payment_method;

CREATE TABLE hdm.dealer_payment_method (
	dlr_pmt_mthd_key int4 NOT NULL,
	pmt_mthd_cd varchar(25) NULL,
	pmt_mthd_desc varchar(50) NULL
);

-- Drop table

-- DROP TABLE hdm.dealer_payment_summary;

CREATE TABLE hdm.dealer_payment_summary (
	dlr_pmt_sum_key int4 NOT NULL,
	dlr_fee_type_key int4 NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	dlr_key int4 NULL,
	dlr_pmt_mthd_key int4 NULL,
	wk_ord_nbr varchar(25) NULL,
	compnsn_pyee_type_cd varchar(25) NULL,
	pre_agreed_sched_ovrd_type varchar(25) NULL,
	pre_agree_shr_rt numeric(38,15) NULL,
	pre_agree_avg_asset numeric(38,15) NULL,
	pre_agree_compnsn numeric(38,15) NULL,
	eligbl_compnsn_ovrd_type varchar(25) NULL,
	eligbl_shr_rt numeric(38,15) NULL,
	eligbl_avg_asset numeric(38,15) NULL,
	eligbl_compnsn numeric(38,15) NULL,
	tot_compnsn numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	compnsn_diff numeric(38,15) NULL,
	prev_per_compnsn numeric(38,15) NULL,
	avg_asset_diff numeric(38,15) NULL,
	prev_per_asset numeric(38,15) NULL,
	trl_mode_flg varchar(1) NULL,
	pmt_day_key int4 NULL,
	bus_line varchar(25) NULL
);
CREATE INDEX dealer_payment_summary_ix_if1 ON hdm.dealer_payment_summary USING btree (dlr_fee_type_key);
CREATE INDEX dealer_payment_summary_ix_if2 ON hdm.dealer_payment_summary USING btree (day_key);
CREATE INDEX dealer_payment_summary_ix_if3 ON hdm.dealer_payment_summary USING btree (fund_key);
CREATE INDEX dealer_payment_summary_ix_if4 ON hdm.dealer_payment_summary USING btree (dlr_key);
CREATE INDEX dealer_payment_summary_ix_if5 ON hdm.dealer_payment_summary USING btree (dlr_pmt_mthd_key);
CREATE INDEX dealer_payment_summary_ix_if7 ON hdm.dealer_payment_summary USING btree (pmt_day_key);

-- Drop table

-- DROP TABLE hdm.dealer_payment_trial;

CREATE TABLE hdm.dealer_payment_trial (
	dlr_pmt_trl_key int4 NOT NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	dlr_branch_key int4 NULL,
	dlr_fee_type_key int4 NULL,
	dlr_pmt_mthd_key int4 NULL,
	acct_key int4 NULL,
	payout_type_key int4 NULL,
	pmt_day_key int4 NULL,
	invc_day_key int4 NULL,
	wk_ord_id varchar(25) NULL,
	nscc_flg varchar(1) NULL,
	nscc_nm_use_flg varchar(1) NULL,
	pmt_ofst_cd varchar(25) NULL,
	payee_type varchar(3) NULL,
	splt_compnsn_flg varchar(1) NULL,
	prev_compnsn_ovrd_type varchar(4) NULL,
	eligbl_compnsn_ovrd_type varchar(4) NULL,
	pre_agrd_shr_rt numeric(38,15) NULL,
	pre_agrd_asset numeric(38,15) NULL,
	pre_agrd_fee numeric(38,15) NULL,
	prev_splt_fee numeric(38,15) NULL,
	tot_eligbl_rt numeric(38,15) NULL,
	tot_eligbl_asset numeric(38,15) NULL,
	tot_eligbl_fee numeric(38,15) NULL,
	tot_compnsn numeric(38,15) NULL,
	avg_eligbl_bp numeric(38,15) NULL,
	avg_eligbl_asset numeric(38,15) NULL,
	avg_eligbl_fees numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NOT NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dealer_payment_trial_ix_if1 ON hdm.dealer_payment_trial USING btree (dlr_key);
CREATE INDEX dealer_payment_trial_ix_if2 ON hdm.dealer_payment_trial USING btree (fund_key);
CREATE INDEX dealer_payment_trial_ix_if3 ON hdm.dealer_payment_trial USING btree (dlr_branch_key);
CREATE INDEX dealer_payment_trial_ix_if4 ON hdm.dealer_payment_trial USING btree (dlr_fee_type_key);
CREATE INDEX dealer_payment_trial_ix_if5 ON hdm.dealer_payment_trial USING btree (dlr_pmt_mthd_key);
CREATE INDEX dealer_payment_trial_ix_if6 ON hdm.dealer_payment_trial USING btree (acct_key);
CREATE INDEX dealer_payment_trial_ix_if7 ON hdm.dealer_payment_trial USING btree (payout_type_key);
CREATE INDEX dealer_payment_trial_ix_if8 ON hdm.dealer_payment_trial USING btree (pmt_day_key);
CREATE INDEX dealer_payment_trial_ix_if9 ON hdm.dealer_payment_trial USING btree (invc_day_key);

-- Drop table

-- DROP TABLE hdm.dealer_payout_type;

CREATE TABLE hdm.dealer_payout_type (
	payout_type_key int4 NOT NULL,
	payout_type_cd varchar(1) NULL,
	payout_type_desc varchar(255) NULL
);

-- Drop table

-- DROP TABLE hdm.dealer_position;

CREATE TABLE hdm.dealer_position (
	dlr_key int4 NULL,
	fund_key int4 NULL,
	day_key int4 NULL,
	mo_cd varchar(20) NULL,
	dealer_balance numeric(38,15) NULL
);
CREATE INDEX dlr_pos_day_ix ON hdm.dealer_position USING btree (day_key);
CREATE INDEX dlr_pos_ix ON hdm.dealer_position USING btree (dlr_key);
CREATE UNIQUE INDEX dlr_pos_uix ON hdm.dealer_position USING btree (dlr_key, day_key, fund_key);

-- Drop table

-- DROP TABLE hdm.dev_idx_rltn;

CREATE TABLE hdm.dev_idx_rltn (
	fund_idx_key numeric(38,15) NULL,
	fund_key numeric(38,15) NULL,
	bmk_idx_key numeric(38,15) NULL,
	perf_run_type_key int4 NULL,
	link_prio numeric(38,15) NULL,
	rpt_sort_ord numeric(38,15) NULL,
	prim_bmk_flg varchar(1) NULL,
	gr_ret_inlsn_flg varchar(1) NULL,
	curr_row_flg bpchar(1) NULL,
	row_strt_dttm timestamp NULL
);

-- Drop table

-- DROP TABLE hdm.distribution_code;

CREATE TABLE hdm.distribution_code (
	dstrbtn_cd_key int4 NOT NULL,
	dstrbtn_cd numeric(38,15) NULL,
	dstrbtn_desc varchar(100) NULL
);

-- Drop table

-- DROP TABLE hdm.distribution_type;

CREATE TABLE hdm.distribution_type (
	dstrbtn_type_key int4 NOT NULL,
	dstrbtn_type_cd numeric(38,15) NULL,
	dstrbtn_type_desc varchar(100) NULL
);

-- Drop table

-- DROP TABLE hdm.dividend_cashflow;

CREATE TABLE hdm.dividend_cashflow (
	divd_cashflow_key int4 NOT NULL,
	reg_key int4 NULL,
	pmt_mthd_key int4 NULL,
	dlr_key int4 NULL,
	shrhldr_key int4 NULL,
	acct_key int4 NULL,
	fund_key int4 NULL,
	paybl_day_key int4 NULL,
	re_invst_day_key int4 NULL,
	shrhldr_role_key int4 NOT NULL,
	acct_nbr numeric(38,15) NULL,
	sub_shrs numeric(38,15) NULL,
	sub_amt numeric(38,15) NULL,
	redmpn_shrs numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	ofrg_pr numeric(38,15) NULL,
	nav numeric(38,15) NULL,
	dlr_commsn_amt numeric(38,15) NULL,
	undr_wrtr_commsn_amt numeric(38,15) NULL,
	adv_commsn_amt numeric(38,15) NULL,
	net_cashflow_amt numeric(38,15) NULL,
	net_shrs numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX dividend_cashflow_ix_if1 ON hdm.dividend_cashflow USING btree (reg_key);
CREATE INDEX dividend_cashflow_ix_if2 ON hdm.dividend_cashflow USING btree (pmt_mthd_key);
CREATE INDEX dividend_cashflow_ix_if3 ON hdm.dividend_cashflow USING btree (dlr_key);
CREATE INDEX dividend_cashflow_ix_if4 ON hdm.dividend_cashflow USING btree (shrhldr_key);
CREATE INDEX dividend_cashflow_ix_if5 ON hdm.dividend_cashflow USING btree (acct_key);
CREATE INDEX dividend_cashflow_ix_if6 ON hdm.dividend_cashflow USING btree (fund_key);
CREATE INDEX dividend_cashflow_ix_if7 ON hdm.dividend_cashflow USING btree (paybl_day_key);
CREATE INDEX dividend_cashflow_ix_if8 ON hdm.dividend_cashflow USING btree (re_invst_day_key);

-- Drop table

-- DROP TABLE hdm.dividend_tax_rate;

CREATE TABLE hdm.dividend_tax_rate (
	divd_tax_rt_key int4 NOT NULL,
	tax_yr numeric(38,15) NULL,
	ordn_incm_rt numeric(38,15) NULL,
	ltcg_rt numeric(38,15) NULL,
	stcg_rt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.eligible_asset_list;

CREATE TABLE hdm.eligible_asset_list (
	eligbl_asset_list_key int4 NOT NULL,
	dlr_key int4 NULL,
	inlsn_excln_flg varchar(1) NULL,
	cum_discnt_nbr numeric(38,15) NULL,
	shr_cls_cd numeric(38,15) NULL,
	qlfyd_rtrmt_flg varchar(1) NULL,
	efftv_strt_dt timestamp NULL,
	efftv_end_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX eligible_asset_list_ix_if1 ON hdm.eligible_asset_list USING btree (dlr_key);



-- Drop table

-- DROP TABLE hdm.exchange_type;

CREATE TABLE hdm.exchange_type (
	xchg_type_key int4 NOT NULL,
	xchg_type_cd varchar(25) NULL,
	xchg_type_desc varchar(255) NULL
);

-- Drop table

-- DROP TABLE hdm.expense_ratio_history;

CREATE TABLE hdm.expense_ratio_history (
	expns_ratio_hist_key int4 NOT NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	efftv_strt_dt timestamp NULL,
	efftv_end_dt timestamp NULL,
	net_expns_ratio numeric(38,15) NULL
);
CREATE INDEX expense_ratio_history_ix_if1 ON hdm.expense_ratio_history USING btree (day_key);
CREATE INDEX expense_ratio_history_ix_if2 ON hdm.expense_ratio_history USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.expense_ratio_waiver_type;

CREATE TABLE hdm.expense_ratio_waiver_type (
	expns_ratio_wavr_type_key int4 NOT NULL,
	expns_ratio_wavr_type_cd varchar(3) NOT NULL,
	wavr_type_desc varchar(255) NULL
);

-- Drop table

-- DROP TABLE hdm.external_fund_balance;

CREATE TABLE hdm.external_fund_balance (
	ext_fund_bal_key int4 NOT NULL,
	fund_key int4 NULL,
	day_key int4 NULL,
	ext_src varchar(50) NULL,
	shrs numeric(38,15) NULL,
	bal_amt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	offr_id bytea NULL,
	offr_nm bytea NULL
);
CREATE INDEX external_fund_balance_ix_if1 ON hdm.external_fund_balance USING btree (fund_key);
CREATE INDEX external_fund_balance_ix_if2 ON hdm.external_fund_balance USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.external_fund_list;

CREATE TABLE hdm.external_fund_list (
	ext_fund_list_key int4 NOT NULL,
	ext_fund_id varchar(25) NULL,
	ext_fund_desc varchar(255) NULL,
	fund_key int4 NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	fund_tickr_sym varchar(25) NULL
);
CREATE INDEX external_fund_list_ix_if1 ON hdm.external_fund_list USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.facility;

CREATE TABLE hdm.facility (
	fac_key int4 NOT NULL,
	fac_id varchar(25) NULL,
	sub_fac_nm varchar(50) NULL,
	fac_func_id numeric(38,15) NOT NULL,
	day_key int4 NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX facility_ix_if1 ON hdm.facility USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.fee_schedule;

CREATE TABLE hdm.fee_schedule (
	fee_sched_key int4 NOT NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	dlr_fee_type_key int4 NULL,
	contrct_strt_day_key int4 NULL,
	contrct_end_day_key int4 NULL,
	dlr_billg_type_flg varchar(1) NULL,
	fee_rt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fee_schedule_ix_if1 ON hdm.fee_schedule USING btree (dlr_key);
CREATE INDEX fee_schedule_ix_if2 ON hdm.fee_schedule USING btree (fund_key);
CREATE INDEX fee_schedule_ix_if3 ON hdm.fee_schedule USING btree (dlr_fee_type_key);
CREATE INDEX fee_schedule_ix_if5 ON hdm.fee_schedule USING btree (contrct_strt_day_key);
CREATE INDEX fee_schedule_ix_if6 ON hdm.fee_schedule USING btree (contrct_end_day_key);

CREATE TABLE hdm.fi_derivative_list
(
	fi_dervtv_list_key integer,
	dervtv_nm varchar(255),
	curr_row_flg varchar(1),
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL	
);

create table hdm.fi_derivatives
(
	fi_dervtv_key integer,
	fi_dervtv_list_key integer,
	day_key integer,
	fund_compst_key integer,
	ntional_amt numeric(38,15) NULL,
	mkt_val  numeric(38,15) NULL,
	mkt_val_rt  numeric(38,15) NULL,
	dur_rt  numeric(38,15) NULL,
	dwe  numeric(38,15) NULL,
	curr_row_flg varchar(1),
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL		
);

CREATE INDEX fi_derivatives_ix_if1 ON hdm.fi_derivatives USING btree (fi_dervtv_list_key);
CREATE INDEX fi_derivatives_ix_if2 ON hdm.fi_derivatives USING btree (day_key);
CREATE INDEX fi_derivatives_ix_if3 ON hdm.fi_derivatives USING btree (fund_compst_key);

-- Drop table

-- DROP TABLE hdm.fi_characteristic_list;

CREATE TABLE hdm.fi_characteristic_list (
	fi_charctc_list_key int4 NOT NULL,
	charctc_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.fi_characteristics;

CREATE TABLE hdm.fi_characteristics (
	fi_charctc_fact_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	fi_charctc_list_key int4 NULL,
	fund_charctc_val numeric(38,15) NULL,
	prim_bmk_charctc numeric(38,15) NULL,
	secy_bmk_charctc numeric(38,15) NULL,
	fund_charctc_strg_val varchar(255),
	bmk_charctc_strg_val varchar(255),
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fi_characteristics_ix_if1 ON hdm.fi_characteristics USING btree (fund_compst_key);
CREATE INDEX fi_characteristics_ix_if2 ON hdm.fi_characteristics USING btree (day_key);
CREATE INDEX fi_characteristics_ix_if3 ON hdm.fi_characteristics USING btree (fi_charctc_list_key);

-- Drop table

-- DROP TABLE hdm.fi_composition_map;

CREATE TABLE hdm.fi_composition_map (
	fi_compsn_map_key int4 NOT NULL,
	src_type varchar(60) NULL,
	compst_nm varchar(255) NULL,
	src_compsn_nm varchar(60) NULL,
	trgt_compsn_nm varchar(60) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	trgt_parn_compsn_nm varchar(60) NULL
);

-- Drop table

-- DROP TABLE hdm.fi_country;

CREATE TABLE hdm.fi_country (
	fi_crty_key int4 NOT NULL,
	reg varchar(60) NULL,
	crty_type varchar(60) NULL,
	crty varchar(60) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.fi_country_allocation;

CREATE TABLE hdm.fi_country_allocation (
	fi_crty_allocn_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	fi_crty_key int4 NULL,
	fund_allocn numeric(38,15) NULL,
	prim_bmk_allocn numeric(38,15) NULL,
	secy_bmk_allocn numeric(38,15) NULL,
	fund_ext__dur_yr numeric(38,15) NULL,
	fund_ext_em_sprd numeric(38,15) NULL,
	fund_loc_dur_yr numeric(38,15) NULL,
	fund_curr_expsr numeric(38,15) NULL,
	bmk_ext_dur_yr numeric(38,15) NULL,
	bmk_ext_em_sprd numeric(38,15) NULL,
	bmk_loc_dur_yr numeric(38,15) NULL,
	bmk_curr_expsr numeric(38,15) NULL,
	fund_currcy_rt numeric(38,15) NULL,
	bmk_currcy_rt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fi_country_allocation_ix_if1 ON hdm.fi_country_allocation USING btree (fund_compst_key);
CREATE INDEX fi_country_allocation_ix_if2 ON hdm.fi_country_allocation USING btree (day_key);
CREATE INDEX fi_country_allocation_ix_if3 ON hdm.fi_country_allocation USING btree (fi_crty_key);

-- Drop table

-- DROP TABLE hdm.fi_credit_rating;

CREATE TABLE hdm.fi_credit_rating (
	fi_cr_rtngs_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	fi_cr_rtng_list_key int4 NULL,
	fund_rtng_exclsv_cash numeric(38,15) NULL,
	fund_rtng_inclv_cash numeric(38,15) NULL,
	prim_bmk_rtng numeric(38,15) NULL,
	secy_bmk_rtng numeric(38,15) NULL,
	fund_dwe_exclsv_cash numeric(38,15) NULL,
	fund_dwe_inclv_cash numeric(38,15) NULL,
	prim_bmk_dwe numeric(38,15) NULL,
	secy_bmk_dwe numeric(38,15) NULL,
	fund_expsr_amt numeric(38,15) NULL,
	fund_expsr_rt numeric(38,15) NULL,
	fund_dur_rt numeric(38,15) NULL,
	fund_dwe numeric(38,15) NULL,
	bmk_expsr_rt numeric(38,15) NULL,
	bmk_dur_rt numeric(38,15) NULL,
	bmk_dwe numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fi_credit_rating_ix_if1 ON hdm.fi_credit_rating USING btree (fund_compst_key);
CREATE INDEX fi_credit_rating_ix_if2 ON hdm.fi_credit_rating USING btree (day_key);
CREATE INDEX fi_credit_rating_ix_if3 ON hdm.fi_credit_rating USING btree (fi_cr_rtng_list_key);

-- Drop table

-- DROP TABLE hdm.fi_credit_rating_list;

CREATE TABLE hdm.fi_credit_rating_list (
	fi_cr_rtng_list_key int4 NOT NULL,
	cr_rtng_nm varchar(60) NULL,
	rtng_sort_ord numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.fi_duration_maturity;

CREATE TABLE hdm.fi_duration_maturity (
	fi_dur_mtry_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	fi_tm_per_key int4 NULL,
	fund_yca_dur_inclv_cash numeric(38,15) NULL,
	fund_yca_dur_exclsv_cash numeric(38,15) NULL,
	prim_bmk_dur numeric(38,15) NULL,
	secy_bmk_dur numeric(38,15) NULL,
	fund_yca_mtry_inclv_cash numeric(38,15) NULL,
	fund_yca_mtry_exclsv_cash numeric(38,15) NULL,
	prim_bmk_mtry numeric(38,15) NULL,
	secy_bmk_mtry numeric(38,15) NULL,
	fund_dwe_inclv_cash numeric(38,15) NULL,
	fund_dwe_exclsv_cash numeric(38,15) NULL,
	prim_bmk_dwe numeric(38,15) NULL,
	secy_bmk_dwe numeric(38,15) NULL,
	mkt_val_amt numeric(38,15),
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fi_duration_maturity_ix_if1 ON hdm.fi_duration_maturity USING btree (fund_compst_key);
CREATE INDEX fi_duration_maturity_ix_if2 ON hdm.fi_duration_maturity USING btree (day_key);
CREATE INDEX fi_duration_maturity_ix_if3 ON hdm.fi_duration_maturity USING btree (fi_tm_per_key);

-- Drop table

-- DROP TABLE hdm.fi_issuer;

CREATE TABLE hdm.fund (
	fund_key int4 NOT NULL,
	all_fund_cd varchar(10) NULL,
	all_fund_desc varchar(25) NULL,
	fund_compst_key int4 NULL,
	shr_cls_cd numeric(38,15) NULL,
	shr_cls_desc varchar(100) NULL,
	fund_nm varchar(50) NULL,
	fund_long_desc varchar(255) NULL,
	fund_abrv varchar(50) NULL,
	fund_stat_cd varchar(1) NULL,
	fund_stat_desc varchar(50) NULL,
	fund_incpn_dt timestamp NULL,
	fund_clos_dt timestamp NULL,
	fund_nbr numeric(38,15) NULL,
	fund_sort_ord numeric(38,15) NULL,
	addr_id numeric(38,15) NULL,
	cusip_id varchar(9) NULL,
	quot_sym varchar(8) NULL,
	st_str_fund_nbr varchar(4) NULL,
	tax_id varchar(9) NULL,
	risk_lvl numeric(38,15) NULL,
	divd_freq_cd varchar(1) NULL,
	divd_freq_desc varchar(100) NULL,
	cap_gain_freq_cd varchar(1) NULL,
	cap_gain_freq_desc varchar(100) NULL,
	reg_acct_init_invmt numeric(38,15) NULL,
	ira_acct_init_invmt numeric(38,15) NULL,
	ugma_acct_init_invmt numeric(38,15) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	perf_incpn_dt timestamp NULL
);

-- Drop table

-- DROP TABLE hdm.fi_sector;

CREATE TABLE hdm.fi_sector (
	fi_sctr_key int4 NOT NULL,
	sctr_type varchar(25) NULL,
	sctr_nm varchar(255) NULL,
	sub_sctr_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.fi_sector_allocation;

CREATE TABLE hdm.fi_sector_allocation (
	fi_sctr_allocn_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	fi_sctr_key int4 NULL,
	fund_wgt_exclsv_cash numeric(38,15) NULL,
	fund_wgt_inclv_cash numeric(38,15) NULL,
	prim_bmk_wgt numeric(38,15) NULL,
	scnday_bmk_wgt numeric(38,15) NULL,
	fund_dwe_exclsv_cash numeric(38,15) NULL,
	fund_dwe_inclv_cash numeric(38,15) NULL,
	prim_bmk_dwe numeric(38,15) NULL,
    fund_mkt_val numeric(38,15) NULL,
	fund_mkt_val_rt numeric(38,15) NULL,
	bmk_mkt_val_rt numeric(38,15) NULL,
	fund_dur_rt numeric(38,15) NULL,
	bmk_dur_rt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	compst_src_id int4 NULL,
	fund_styl varchar(100) NULL,
	risk_ret_ord numeric(38,15) NULL,
	compst_incpn_dt timestamp NULL,
	sales_cat varchar(60) NULL,
	st_str_fund_nbr varchar(25) NULL
);
CREATE INDEX fi_sector_allocation_ix_if1 ON hdm.fi_sector_allocation USING btree (fund_compst_key);
CREATE INDEX fi_sector_allocation_ix_if2 ON hdm.fi_sector_allocation USING btree (day_key);
CREATE INDEX fi_sector_allocation_ix_if3 ON hdm.fi_sector_allocation USING btree (fi_sctr_key);

-- Drop table

-- DROP TABLE hdm.fi_time_period;

CREATE TABLE hdm.fi_time_period (
	fi_tm_per_key int4 NOT NULL,
	tm_per varchar(60) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.fi_time_period_mapping;

CREATE TABLE hdm.fi_time_period_mapping (
	fi_tm_per_mpng_key int4 NOT NULL,
	src_per varchar(60) NULL,
	trgt_per varchar(60) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.fi_top_issuers;

CREATE TABLE hdm.fi_top_issuers (
	fi_top_issr_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	fi_issr_key int4 NULL,
	issr_pct numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fi_top_issuers_ix_if1 ON hdm.fi_top_issuers USING btree (fund_compst_key);
CREATE INDEX fi_top_issuers_ix_if2 ON hdm.fi_top_issuers USING btree (day_key);
CREATE INDEX fi_top_issuers_ix_if3 ON hdm.fi_top_issuers USING btree (fi_issr_key);

-- Drop table

-- DROP TABLE hdm.fid_benchmark_performance;

CREATE TABLE hdm.fid_benchmark_performance (
	fid_bmk_perf_key int4 NOT NULL,
	day_key int4 NULL,
	fid_asset_cls_key int4 NULL,
	fid_cust_key int4 NULL,
	per_key int4 NULL,
	bmk_ret numeric(38,15) NULL,
	annlzn_fctr numeric(38,15) NULL,
	strt_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_benchmark_perf_ix_if1 ON hdm.fid_benchmark_performance USING btree (day_key);
CREATE INDEX fid_benchmark_perf_ix_if2 ON hdm.fid_benchmark_performance USING btree (fid_asset_cls_key);
CREATE INDEX fid_benchmark_perf_ix_if3 ON hdm.fid_benchmark_performance USING btree (fid_cust_key);
CREATE INDEX fid_benchmark_perf_ix_if4 ON hdm.fid_benchmark_performance USING btree (per_key);

-- Drop table

-- DROP TABLE hdm.fid_benchmark_position;

CREATE TABLE hdm.fid_benchmark_position (
	bmk_posn_key int4 NOT NULL,
	fid_cust_key int4 NULL,
	day_key int4 NULL,
	fund_key int4 NOT NULL,
	bmk_idx_key int4 NOT NULL,
	mo_ret numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_benchmark_position_ix_ie1 ON hdm.fid_benchmark_position USING btree (fund_key);
CREATE INDEX fid_benchmark_position_ix_if1 ON hdm.fid_benchmark_position USING btree (fid_cust_key);
CREATE INDEX fid_benchmark_position_ix_if2 ON hdm.fid_benchmark_position USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.fid_benchmark_weight;

CREATE TABLE hdm.fid_benchmark_weight (
	bmk_wgt_key int4 NOT NULL,
	bmk_idx_key int4 NOT NULL,
	fid_cust_key int4 NULL,
	fid_asset_cls_key int4 NULL,
	fund_key int4 NOT NULL,
	efftv_strt_dt timestamp NULL,
	efftv_end_dt timestamp NULL,
	bmk_wgt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_benchmark_weight_ix_ie1 ON hdm.fid_benchmark_weight USING btree (fund_key);
CREATE INDEX fid_benchmark_weight_ix_ie2 ON hdm.fid_benchmark_weight USING btree (bmk_idx_key);
CREATE INDEX fid_benchmark_weight_ix_if2 ON hdm.fid_benchmark_weight USING btree (fid_cust_key);
CREATE INDEX fid_benchmark_weight_ix_if4 ON hdm.fid_benchmark_weight USING btree (fid_asset_cls_key);

-- Drop table

-- DROP TABLE hdm.fid_benchmark_weight_temp;

CREATE TABLE hdm.fid_benchmark_weight_temp (
	bmk_wgt_key int4 NOT NULL,
	bmk_idx_key int4 NOT NULL,
	fid_cust_key int4 NULL,
	fid_asset_cls_key int4 NULL,
	fund_key int4 NOT NULL,
	efftv_strt_dt timestamp NULL,
	efftv_end_dt timestamp NULL,
	bmk_wgt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.fid_bm_level_performance;

CREATE TABLE hdm.fid_bm_level_performance (
	bm_lvl_perf_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	bmk_idx_key int4 NOT NULL,
	day_key int4 NULL,
	fid_cust_key int4 NULL,
	per_key int4 NULL,
	bmk_ret numeric(38,15) NULL,
	annlzn_fctr numeric(38,15) NULL,
	strt_dt timestamp NULL,
	incpn_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_bm_level_perf_ix_if1 ON hdm.fid_bm_level_performance USING btree (day_key);
CREATE INDEX fid_bm_level_perf_ix_if2 ON hdm.fid_bm_level_performance USING btree (fid_cust_key);
CREATE INDEX fid_bm_level_perf_ix_if3 ON hdm.fid_bm_level_performance USING btree (per_key);

-- Drop table

-- DROP TABLE hdm.fid_bm_level_wealth_index;

CREATE TABLE hdm.fid_bm_level_wealth_index (
	bm_lvl_wlth_idx_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	bmk_idx_key int4 NOT NULL,
	day_key int4 NULL,
	fid_cust_key int4 NULL,
	wlth_idx numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_bm_level_wi_ix_if1 ON hdm.fid_bm_level_wealth_index USING btree (day_key);
CREATE INDEX fid_bm_level_wi_ix_if2 ON hdm.fid_bm_level_wealth_index USING btree (fid_cust_key);

-- Drop table

-- DROP TABLE hdm.fid_bm_wealth_index;

CREATE TABLE hdm.fid_bm_wealth_index (
	bmk_wlth_idx_key int4 NOT NULL,
	day_key int4 NULL,
	fid_asset_cls_key int4 NULL,
	fid_cust_key int4 NULL,
	asset_ret numeric(38,15) NULL,
	wlth_idx numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_bm_wealth_index_ix_if1 ON hdm.fid_bm_wealth_index USING btree (day_key);
CREATE INDEX fid_bm_wealth_index_ix_if2 ON hdm.fid_bm_wealth_index USING btree (fid_asset_cls_key);
CREATE INDEX fid_bm_wealth_index_ix_if3 ON hdm.fid_bm_wealth_index USING btree (fid_cust_key);

-- Drop table

-- DROP TABLE hdm.fid_client_asset_mix;

CREATE TABLE hdm.fid_client_asset_mix (
	fid_client_asset_mix_key int4 NOT NULL,
	fid_asset_cls_key int4 NULL,
	fid_cust_key int4 NULL,
	incpn_dt timestamp NULL
);
CREATE INDEX fid_client_asset_mix_ix_if1 ON hdm.fid_client_asset_mix USING btree (fid_asset_cls_key);
CREATE INDEX fid_client_asset_mix_ix_if2 ON hdm.fid_client_asset_mix USING btree (fid_cust_key);

-- Drop table

-- DROP TABLE hdm.fid_external_benchmark;

CREATE TABLE hdm.fid_external_benchmark (
	ext_bmk_key int4 NOT NULL,
	idx_nm varchar(255) NULL
);

-- Drop table

-- DROP TABLE hdm.fid_external_fund;

CREATE TABLE hdm.fid_external_fund (
	ext_fund_key int4 NOT NULL,
	ext_fund_nm varchar(255) NULL,
	asset_cls_nm varchar(25) NULL
);

-- Drop table

-- DROP TABLE hdm.fid_fund_level_performance;

CREATE TABLE hdm.fid_fund_level_performance (
	fund_lvl_perf_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	day_key int4 NULL,
	fid_cust_key int4 NULL,
	per_key int4 NULL,
	fund_ret numeric(38,15) NULL,
	annlzn_fctr numeric(38,15) NULL,
	strt_dt timestamp NULL,
	incpn_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_fund_level_perf_ix_if1 ON hdm.fid_fund_level_performance USING btree (day_key);
CREATE INDEX fid_fund_level_perf_ix_if2 ON hdm.fid_fund_level_performance USING btree (fid_cust_key);
CREATE INDEX fid_fund_level_perf_ix_if3 ON hdm.fid_fund_level_performance USING btree (per_key);

-- Drop table

-- DROP TABLE hdm.fid_fund_level_wealth_index;

CREATE TABLE hdm.fid_fund_level_wealth_index (
	fund_lvl_wlth_idx_key int4 NOT NULL,
	day_key int4 NULL,
	fund_key int4 NOT NULL,
	fid_cust_key int4 NULL,
	wlth_idx numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_fund_level_wi_ix_if1 ON hdm.fid_fund_level_wealth_index USING btree (day_key);
CREATE INDEX fid_fund_level_wi_ix_if2 ON hdm.fid_fund_level_wealth_index USING btree (fid_cust_key);

-- Drop table

-- DROP TABLE hdm.fid_fund_performance;

CREATE TABLE hdm.fid_fund_performance (
	fid_fund_perf_key int4 NOT NULL,
	day_key int4 NULL,
	fid_asset_cls_key int4 NULL,
	fid_cust_key int4 NULL,
	per_key int4 NULL,
	fund_ret numeric(38,15) NULL,
	annlzn_fctr numeric(38,15) NULL,
	strt_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_fund_performance_ix_if1 ON hdm.fid_fund_performance USING btree (day_key);
CREATE INDEX fid_fund_performance_ix_if2 ON hdm.fid_fund_performance USING btree (fid_asset_cls_key);
CREATE INDEX fid_fund_performance_ix_if3 ON hdm.fid_fund_performance USING btree (fid_cust_key);
CREATE INDEX fid_fund_performance_ix_if4 ON hdm.fid_fund_performance USING btree (per_key);

-- Drop table

-- DROP TABLE hdm.fid_fund_wealth_index;

CREATE TABLE hdm.fid_fund_wealth_index (
	fid_wlth_idx_key int4 NOT NULL,
	day_key int4 NULL,
	fid_asset_cls_key int4 NULL,
	fid_cust_key int4 NULL,
	mkt_val numeric(38,15) NULL,
	asset_ret numeric(38,15) NULL,
	wlth_idx numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	mkt_val_adj numeric(38,15) NULL
);
CREATE INDEX fid_fund_wealth_index_ix_if1 ON hdm.fid_fund_wealth_index USING btree (day_key);
CREATE INDEX fid_fund_wealth_index_ix_if2 ON hdm.fid_fund_wealth_index USING btree (fid_asset_cls_key);
CREATE INDEX fid_fund_wealth_index_ix_if3 ON hdm.fid_fund_wealth_index USING btree (fid_cust_key);

-- Drop table

-- DROP TABLE hdm.fid_letter_recipient;

CREATE TABLE hdm.fid_letter_recipient (
	ltr_recpnt_key int4 NOT NULL,
	fid_cust_key int4 NULL,
	recpnt_nm varchar(255) NULL,
	addr_line_1 varchar(255) NULL,
	addr_line_2 varchar(255) NULL,
	addr_line_3 varchar(255) NULL,
	city varchar(60) NULL,
	st varchar(100) NULL,
	zip_cd varchar(25) NULL,
	actv_flg varchar(1) NULL DEFAULT 'Y'::character varying,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fid_letter_recipient_ix_if1 ON hdm.fid_letter_recipient USING btree (fid_cust_key);

-- Drop table

-- DROP TABLE hdm.fid_wealth_index_temp;

CREATE TABLE hdm.fid_wealth_index_temp (
	fid_asset_cls_key int4 NULL,
	fid_cust_key int4 NULL,
	day_key int4 NULL,
	asset_ret numeric(38,15) NULL,
	wlth_idx numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.fiduciary_asset_class;

CREATE TABLE hdm.fiduciary_asset_class (
	fid_asset_cls_key int4 NOT NULL,
	fid_asset_cls_desc varchar(255) NULL
);

-- Drop table

-- DROP TABLE hdm.fiduciary_asset_link;

CREATE TABLE hdm.fiduciary_asset_link (
	fid_asset_link_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	fid_asset_cls_key int4 NULL
);
CREATE INDEX fiduciary_asset_link_ix_if1 ON hdm.fiduciary_asset_link USING btree (fid_asset_cls_key);

-- Drop table

-- DROP TABLE hdm.fiduciary_customer;

CREATE TABLE hdm.fiduciary_customer (
	fid_cust_key int4 NOT NULL,
	fid_cust_nm varchar(255) NULL,
	fid_cust_acct_nbr varchar(50) NULL
);

-- Drop table

-- DROP TABLE hdm.fiduciary_position;

CREATE TABLE hdm.fiduciary_position (
	fid_posn_key int4 NOT NULL,
	fid_cust_key int4 NULL,
	day_key int4 NULL,
	fund_key int4 NOT NULL,
	nav numeric(38,15) NULL,
	shrs numeric(38,15) NULL,
	mkt_val numeric(38,15) NULL,
	mo_ret numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fiduciary_position_ix_if1 ON hdm.fiduciary_position USING btree (fid_cust_key);
CREATE INDEX fiduciary_position_ix_if2 ON hdm.fiduciary_position USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.fund;

CREATE TABLE hdm.fund (
	fund_key int4 NOT NULL,
	all_fund_cd varchar(10) NULL,
	all_fund_desc varchar(25) NULL,
	fund_compst_key int4 NULL,
	shr_cls_cd numeric(38,15) NULL,
	shr_cls_desc varchar(100) NULL,
	fund_nm varchar(255) NULL,
	fund_long_desc varchar(255) NULL,
	fund_abrv varchar(50) NULL,
	fund_stat_cd varchar(1) NULL,
	fund_stat_desc varchar(50) NULL,
	fund_incpn_dt timestamp NULL,
	fund_clos_dt timestamp NULL,
	fund_nbr numeric(38,15) NULL,
	fund_sort_ord numeric(38,15) NULL,
	addr_id numeric(38,15) NULL,
	cusip_id varchar(9) NULL,
	quot_sym varchar(8) NULL,
	st_str_fund_nbr varchar(4) NULL,
	tax_id varchar(9) NULL,
	risk_lvl numeric(38,15) NULL,
	divd_freq_cd varchar(1) NULL,
	divd_freq_desc varchar(100) NULL,
	cap_gain_freq_cd varchar(1) NULL,
	cap_gain_freq_desc varchar(100) NULL,
	reg_acct_init_invmt numeric(38,15) NULL,
	ira_acct_init_invmt numeric(38,15) NULL,
	ugma_acct_init_invmt numeric(38,15) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NULL,
	perf_incpn_dt timestamp NULL,
	perf_calc_flg varchar(1) NULL
);
CREATE INDEX fund_ix_if1 ON hdm.fund USING btree (fund_compst_key);

-- Drop table

-- DROP TABLE hdm.fund_annual_investment;

CREATE TABLE hdm.fund_annual_investment (
	fund_key int4 NOT NULL,
	annl_invmt_amt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.fund_composite;

CREATE TABLE hdm.fund_composite (
	fund_compst_key int4 NOT NULL,
	all_compst_cd varchar(10) NULL,
	all_compst_desc varchar(20) NULL,
	asset_cls_nm varchar(40) NULL,
	asset_cls_desc varchar(255) NULL,
	sub_cls_nm varchar(40) NULL,
	sub_cls_desc varchar(255) NULL,
	styl_nm varchar(40) NULL,
	styl_desc varchar(255) NULL,
	compst_cd int4 NOT NULL,
	compst_nm varchar(255) NULL,
	perf_cd varchar(20) NULL,
	perf_nm varchar(255) NULL,
	compst_ord numeric(38,15) NULL,
	compst_stat_cd varchar(1) NULL,
	compst_stat_desc varchar(50) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	compst_src_id int4 NULL,
	fund_styl varchar(100) NULL,
	risk_ret_ord numeric(38,15) NULL,
	compst_incpn_dt timestamp NULL,
	sales_cat varchar(60) NULL,
	st_str_fund_nbr varchar(25) NULL,
	invmt_veh varchar(100) null,
	invmt_advsr  varchar(100) null,
	invmt_rltn_type  varchar(25) null,
	prod_nm  varchar(100) null,
	invmt_cat  varchar(100) null
);

-- Drop table

-- DROP TABLE hdm.fund_dividend;

CREATE TABLE hdm.fund_dividend (
	fund_divd_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	rec_day_key int4 NULL,
	reinvest_day_key int4 NULL,
	paybl_day_key int4 NULL,
	tot_divd numeric(38,15) NULL,
	net_invmt_incm numeric(38,15) NULL,
	shrs_of_rec numeric(38,15) NULL,
	re_invst_pr numeric(38,15) NULL,
	ordn_incm numeric(38,15) NULL,
	tot_shrt_term_gains numeric(38,15) NULL,
	tot_mid_term_gains numeric(38,15) NULL,
	tot_long_term_gains numeric(38,15) NULL,
	shrt_term_cap_gains numeric(38,15) NULL,
	mid_term_cap_gains numeric(38,15) NULL,
	long_term_cap_gains numeric(38,15) NULL,
	non_txbl_discnt numeric(38,15) NULL,
	forgn_tax_cr numeric(38,15) NULL,
	re_invst_idx numeric(38,15) NULL,
	est_divd_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	qdi numeric(38,15) NULL
);
CREATE INDEX fund_dividend_ix_if1 ON hdm.fund_dividend USING btree (fund_key);
CREATE INDEX fund_dividend_ix_if2 ON hdm.fund_dividend USING btree (rec_day_key);
CREATE INDEX fund_dividend_ix_if3 ON hdm.fund_dividend USING btree (reinvest_day_key);
CREATE INDEX fund_dividend_ix_if4 ON hdm.fund_dividend USING btree (paybl_day_key);

-- Drop table

-- DROP TABLE hdm.fund_expense_ratio;

CREATE TABLE hdm.fund_expense_ratio (
	fund_expns_ratio_key int4 NOT NULL,
	expns_ratio_wavr_type_key int4 NULL,
	fund_key int4 NULL,
	semi_annl_rpt_dt timestamp NULL,
	efftv_strt_dt timestamp NULL,
	efftv_end_dt timestamp NULL,
	gr_expns_ratio numeric(38,15) NULL,
	net_expns_ratio numeric(38,15) NULL,
	expns_wavr_ratio numeric(38,15) NULL,
	cayman_ratio numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	manual_entry_flg bpchar(1) NULL DEFAULT 'N'::bpchar,
	per_flg varchar(1) NULL,
	pass_thru_mgmt_fee_wavr numeric(38,15) NULL
);
CREATE INDEX fund_expense_ratio_ix_if1 ON hdm.fund_expense_ratio USING btree (expns_ratio_wavr_type_key);
CREATE INDEX fund_expense_ratio_ix_if2 ON hdm.fund_expense_ratio USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.fund_holdings;

CREATE TABLE hdm.fund_holdings (
	fund_holdg_key int4 NOT NULL,
	day_key int4 NULL,
	fund_compst_key int4 NULL,
	secr_key int4 NULL,
	secr_attr_key int4 NULL,
	indtry_key int4 NULL,
	inc_crty_key int4 NULL,
	iss_crty_key int4 NULL,
	trde_crty_key int4 NULL,
	nra_tax_crty_key int4 NULL,
	base_crty_key int4 NULL,
	base_currcy_key int4 NULL,
	loc_currcy_key int4 NULL,
	mkt_pr_currcy_key int4 NULL,
	incm_currcy_key int4 NULL,
	row_seq_nbr int4 NOT NULL,
	days_to_mtry numeric(38,15) NULL,
	par_shrs numeric(38,15) NULL,
	orig_base_cost numeric(38,15) NULL,
	orig_loc_cost numeric(38,15) NULL,
	idfd_base_cost numeric(38,15) NULL,
	idfd_loc_cost numeric(38,15) NULL,
	base_mkt_val numeric(38,15) NULL,
	loc_mkt_val numeric(38,15) NULL,
	curr_base_pr numeric(38,15) NULL,
	curr_loc_pr numeric(38,15) NULL,
	urlz_gl_loc numeric(38,15) NULL,
	urlz_gl_base numeric(38,15) NULL,
	avg_cost_base_par_shrs numeric(38,15) NULL,
	avg_cost_loc_par_shrs numeric(38,15) NULL,
	orig_strk_pr numeric(38,15) NULL,
	curr_strk_pr numeric(38,15) NULL,
	vend_suppld_iss_curr_pr numeric(38,15) NULL,
	base_net_amort numeric(38,15) NULL,
	loc_net_amort numeric(38,15) NULL,
	gr_base_amt_accrd numeric(38,15) NULL,
	gr_loc_amt_accrd numeric(38,15) NULL,
	curr_varb_rt numeric(38,15) NULL,
	partition_yr numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fund_holding_ix_ie1 ON hdm.fund_holdings USING btree (partition_yr);
CREATE INDEX fund_holding_ix_if1 ON hdm.fund_holdings USING btree (day_key);
CREATE INDEX fund_holding_ix_if10 ON hdm.fund_holdings USING btree (mkt_pr_currcy_key);
CREATE INDEX fund_holding_ix_if11 ON hdm.fund_holdings USING btree (incm_currcy_key);
CREATE INDEX fund_holding_ix_if12 ON hdm.fund_holdings USING btree (secr_key);
CREATE INDEX fund_holding_ix_if13 ON hdm.fund_holdings USING btree (secr_attr_key);
CREATE INDEX fund_holding_ix_if15 ON hdm.fund_holdings USING btree (base_crty_key);
CREATE INDEX fund_holding_ix_if2 ON hdm.fund_holdings USING btree (fund_compst_key);
CREATE INDEX fund_holding_ix_if3 ON hdm.fund_holdings USING btree (indtry_key);
CREATE INDEX fund_holding_ix_if4 ON hdm.fund_holdings USING btree (inc_crty_key);
CREATE INDEX fund_holding_ix_if5 ON hdm.fund_holdings USING btree (iss_crty_key);
CREATE INDEX fund_holding_ix_if6 ON hdm.fund_holdings USING btree (trde_crty_key);
CREATE INDEX fund_holding_ix_if7 ON hdm.fund_holdings USING btree (base_currcy_key);
CREATE INDEX fund_holding_ix_if8 ON hdm.fund_holdings USING btree (loc_currcy_key);
CREATE INDEX fund_holdings_ix_if9 ON hdm.fund_holdings USING btree (nra_tax_crty_key);

-- Drop table

-- DROP TABLE hdm.fund_index_rltn;

CREATE TABLE hdm.fund_index_rltn (
	fund_idx_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	bmk_idx_key int4 NULL,
	perf_run_type_key int4 NULL,
	link_prio numeric(38,15) NULL,
	rpt_sort_ord numeric(38,15) NULL,
	prim_bmk_flg varchar(1) NULL,
	gr_ret_inlsn_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fund_index_rltn_ix_if2 ON hdm.fund_index_rltn USING btree (fund_compst_key);
CREATE INDEX fund_index_rltn_ix_if4 ON hdm.fund_index_rltn USING btree (bmk_idx_key);
CREATE INDEX fund_index_rltn_ix_if5 ON hdm.fund_index_rltn USING btree (perf_run_type_key);

-- Drop table

-- DROP TABLE hdm.fund_return;

CREATE TABLE hdm.fund_return (
	fund_ret_key int4 NOT NULL,
	fund_key int4 NULL,
	per_key int4 NULL,
	day_key int4 NULL,
	strt_per timestamp NULL,
	fund_ret numeric(38,15) NULL,
	annlzn_fctr numeric(38,15) NULL,
	asof_fund_ret numeric(38,15) NULL,
	ret_per_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	ret_vrfyn_flg varchar(1) NULL
);
CREATE INDEX fund_return_ix_if1 ON hdm.fund_return USING btree (fund_key);
CREATE INDEX fund_return_ix_if2 ON hdm.fund_return USING btree (per_key);
CREATE INDEX fund_return_ix_if4 ON hdm.fund_return USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.fund_trans_dfa_hist;

CREATE TABLE hdm.fund_trans_dfa_hist (
	txn_hist_key int4 NOT NULL,
	txn_type_key int4 NULL,
	reg_key int4 NULL,
	dlr_key int4 NULL,
	shrhldr_key int4 NULL,
	acct_key int4 NULL,
	fund_key int4 NULL,
	efftv_day_key int4 NULL,
	sttl_day_key int4 NULL,
	pmt_day_key int4 NULL,
	batch_day_key int4 NULL,
	dlr_branch_key int4 NULL,
	trde_orig_key int4 NULL,
	rsn_key int4 NULL,
	ssn_stat_key int4 NULL,
	trde_entry_mthd_key int4 NULL,
	dlr_lvl_cntl_key int4 NULL,
	pmt_mthd_key int4 NULL,
	shrhldr_role_key int4 NOT NULL,
	acct_nbr numeric(38,15) NULL,
	fund_txn_id numeric(38,15) NULL,
	fund_txn_id_2 numeric(38,15) NULL,
	ltr_of_intend_nbr numeric(38,15) NULL,
	batch_nbr numeric(38,15) NULL,
	discnt_cat numeric(38,15) NULL,
	fund_ord_nbr numeric(38,15) NULL,
	nav_cd varchar(1) NULL,
	acct_pr_sched numeric(38,15) NULL,
	undr_wrtr_commsn_cd varchar(1) NULL,
	cdsc_wavr_rsn_cd varchar(4) NULL,
	nscc_shrt_term_trader_wavr_rsn varchar(1) NULL,
	contrbn_tax_yr int4 NULL,
	shrs numeric(38,15) NULL,
	shr_pr numeric(38,15) NULL,
	gr_amt numeric(38,15) NULL,
	sales_pct numeric(38,15) NULL,
	dlr_commsn_amt numeric(38,15) NULL,
	undr_wrtr_commsn_amt numeric(38,15) NULL,
	net_amt numeric(38,15) NULL,
	bluesky_txn_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NOT NULL,
	txn_desc varchar(255) NULL,
	shr_bal_effct_flg varchar(1) NULL,
	bluesky_reopen_acct_flg varchar(1) NULL
);
CREATE INDEX fund_trans_dfa_hist_ix_ie1 ON hdm.fund_trans_dfa_hist USING btree (acct_nbr);
CREATE INDEX fund_trans_dfa_hist_ix_ie2 ON hdm.fund_trans_dfa_hist USING btree (shrhldr_key);
CREATE INDEX fund_trans_dfa_hist_ix_if1 ON hdm.fund_trans_dfa_hist USING btree (dlr_key);
CREATE INDEX fund_trans_dfa_hist_ix_if10 ON hdm.fund_trans_dfa_hist USING btree (rsn_key);
CREATE INDEX fund_trans_dfa_hist_ix_if11 ON hdm.fund_trans_dfa_hist USING btree (ssn_stat_key);
CREATE INDEX fund_trans_dfa_hist_ix_if12 ON hdm.fund_trans_dfa_hist USING btree (sttl_day_key);
CREATE INDEX fund_trans_dfa_hist_ix_if13 ON hdm.fund_trans_dfa_hist USING btree (pmt_day_key);
CREATE INDEX fund_trans_dfa_hist_ix_if14 ON hdm.fund_trans_dfa_hist USING btree (trde_entry_mthd_key);
CREATE INDEX fund_trans_dfa_hist_ix_if15 ON hdm.fund_trans_dfa_hist USING btree (dlr_lvl_cntl_key);
CREATE INDEX fund_trans_dfa_hist_ix_if16 ON hdm.fund_trans_dfa_hist USING btree (pmt_mthd_key);
CREATE INDEX fund_trans_dfa_hist_ix_if17 ON hdm.fund_trans_dfa_hist USING btree (batch_day_key);
CREATE INDEX fund_trans_dfa_hist_ix_if3 ON hdm.fund_trans_dfa_hist USING btree (acct_key);
CREATE INDEX fund_trans_dfa_hist_ix_if4 ON hdm.fund_trans_dfa_hist USING btree (fund_key);
CREATE INDEX fund_trans_dfa_hist_ix_if5 ON hdm.fund_trans_dfa_hist USING btree (efftv_day_key);
CREATE INDEX fund_trans_dfa_hist_ix_if6 ON hdm.fund_trans_dfa_hist USING btree (reg_key);
CREATE INDEX fund_trans_dfa_hist_ix_if7 ON hdm.fund_trans_dfa_hist USING btree (txn_type_key);
CREATE INDEX fund_trans_dfa_hist_ix_if8 ON hdm.fund_trans_dfa_hist USING btree (dlr_branch_key);
CREATE INDEX fund_trans_dfa_hist_ix_if9 ON hdm.fund_trans_dfa_hist USING btree (trde_orig_key);

-- Drop table

-- DROP TABLE hdm.fund_trans_hist;

CREATE TABLE hdm.fund_trans_hist (
	txn_hist_key int4 NOT NULL,
	txn_type_key int4 NULL,
	reg_key int4 NULL,
	dlr_key int4 NULL,
	shrhldr_key int4 NULL,
	acct_key int4 NULL,
	fund_key int4 NULL,
	efftv_day_key int4 NULL,
	sttl_day_key int4 NULL,
	pmt_day_key int4 NULL,
	batch_day_key int4 NULL,
	dlr_branch_key int4 NULL,
	trde_orig_key int4 NULL,
	rsn_key int4 NULL,
	ssn_stat_key int4 NULL,
	trde_entry_mthd_key int4 NULL,
	dlr_lvl_cntl_key int4 NULL,
	pmt_mthd_key int4 NULL,
	shrhldr_role_key int4 NOT NULL,
	acct_nbr numeric(38,15) NULL,
	fund_txn_id numeric(38,15) NULL,
	fund_txn_id_2 numeric(38,15) NULL,
	ltr_of_intend_nbr numeric(38,15) NULL,
	batch_nbr numeric(38,15) NULL,
	discnt_cat numeric(38,15) NULL,
	fund_ord_nbr numeric(38,15) NULL,
	nav_cd varchar(1) NULL,
	acct_pr_sched numeric(38,15) NULL,
	undr_wrtr_commsn_cd varchar(1) NULL,
	cdsc_wavr_rsn_cd varchar(4) NULL,
	nscc_shrt_term_trader_wavr_rsn varchar(1) NULL,
	contrbn_tax_yr int4 NULL,
	shrs numeric(38,15) NULL,
	shr_pr numeric(38,15) NULL,
	gr_amt numeric(38,15) NULL,
	sales_pct numeric(38,15) NULL,
	dlr_commsn_amt numeric(38,15) NULL,
	undr_wrtr_commsn_amt numeric(38,15) NULL,
	net_amt numeric(38,15) NULL,
	bluesky_txn_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NOT NULL,
	txn_desc varchar(255) NULL,
	shr_bal_effct_flg varchar(1) NULL,
	bluesky_reopen_acct_flg varchar(1) NULL,
	frto_fund_key int4 NULL,
	spr_sheet_day_key numeric(38,15) NULL,
	opr_id varchar(25) NULL,
	reg_of_sale_key int4 NULL,
	nscc_cntl_nbr varchar(15) NULL,
	tpa_dlr_key int4 NULL
);
CREATE INDEX fund_trans_hist_ix_ie1 ON hdm.fund_trans_hist USING btree (acct_nbr);
CREATE INDEX fund_trans_hist_ix_ie2 ON hdm.fund_trans_hist USING btree (shrhldr_key);
CREATE INDEX fund_trans_hist_ix_ie3 ON hdm.fund_trans_hist USING btree (row_strt_dttm);
CREATE INDEX fund_trans_hist_ix_if1 ON hdm.fund_trans_hist USING btree (dlr_key);
CREATE INDEX fund_trans_hist_ix_if10 ON hdm.fund_trans_hist USING btree (rsn_key);
CREATE INDEX fund_trans_hist_ix_if11 ON hdm.fund_trans_hist USING btree (ssn_stat_key);
CREATE INDEX fund_trans_hist_ix_if12 ON hdm.fund_trans_hist USING btree (sttl_day_key);
CREATE INDEX fund_trans_hist_ix_if13 ON hdm.fund_trans_hist USING btree (pmt_day_key);
CREATE INDEX fund_trans_hist_ix_if14 ON hdm.fund_trans_hist USING btree (trde_entry_mthd_key);
CREATE INDEX fund_trans_hist_ix_if15 ON hdm.fund_trans_hist USING btree (dlr_lvl_cntl_key);
CREATE INDEX fund_trans_hist_ix_if16 ON hdm.fund_trans_hist USING btree (pmt_mthd_key);
CREATE INDEX fund_trans_hist_ix_if17 ON hdm.fund_trans_hist USING btree (batch_day_key);
CREATE INDEX fund_trans_hist_ix_if18 ON hdm.fund_trans_hist USING btree (frto_fund_key);
CREATE INDEX fund_trans_hist_ix_if19 ON hdm.fund_trans_hist USING btree (spr_sheet_day_key);
CREATE INDEX fund_trans_hist_ix_if21 ON hdm.fund_trans_hist USING btree (tpa_dlr_key);
CREATE INDEX fund_trans_hist_ix_if3 ON hdm.fund_trans_hist USING btree (acct_key);
CREATE INDEX fund_trans_hist_ix_if4 ON hdm.fund_trans_hist USING btree (fund_key);
CREATE INDEX fund_trans_hist_ix_if5 ON hdm.fund_trans_hist USING btree (efftv_day_key);
CREATE INDEX fund_trans_hist_ix_if6 ON hdm.fund_trans_hist USING btree (reg_key);
CREATE INDEX fund_trans_hist_ix_if7 ON hdm.fund_trans_hist USING btree (txn_type_key);
CREATE INDEX fund_trans_hist_ix_if8 ON hdm.fund_trans_hist USING btree (dlr_branch_key);
CREATE INDEX fund_trans_hist_ix_if9 ON hdm.fund_trans_hist USING btree (trde_orig_key);



-- Drop table

-- DROP TABLE hdm.fund_valuation;

CREATE TABLE hdm.fund_valuation (
	fund_vltn_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	day_key int4 NOT NULL,
	nav_amt numeric(38,15) NULL,
	tot_shrs numeric(38,15) NULL,
	pr_per_shr numeric(38,15) NULL,
	wlth_idx int4 NULL,
	asof_pr numeric(38,15) NULL,
	daily_pr_diff numeric(38,15) NULL,
	asof_wlth_idx int4 NULL,
	hsg_pr_revs numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fund_valuation_hcf1 ON hdm.fund_valuation USING btree (fund_key, day_key);
CREATE INDEX fund_valuation_ix_if1 ON hdm.fund_valuation USING btree (fund_key);
CREATE INDEX fund_valuation_ix_if2 ON hdm.fund_valuation USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.fund_yield;

CREATE TABLE hdm.fund_yield (
	fund_yld_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	day_key int4 NOT NULL,
	yld_type_key int4 NOT NULL,
	fund_yld numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX fund_yield_ix_if1 ON hdm.fund_yield USING btree (fund_key);
CREATE INDEX fund_yield_ix_if2 ON hdm.fund_yield USING btree (day_key);
CREATE INDEX fund_yield_ix_if3 ON hdm.fund_yield USING btree (yld_type_key);

-- Drop table

-- DROP TABLE hdm.gender;

CREATE TABLE hdm.gender (
	gend_key int4 NOT NULL,
	gend_nm varchar(20) NULL,
	gend_cd varchar(1) NULL
);

-- Drop table

-- DROP TABLE hdm.gross_return;

CREATE TABLE hdm.gross_return (
	gr_ret_key int4 NOT NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	bmk_idx_key int4 NULL,
	per_key int4 NULL,
	gr_ret numeric(38,15) NULL,
	info_ratio numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	idx_ret numeric(38,15) NULL
);
CREATE INDEX gross_return_ix_if1 ON hdm.gross_return USING btree (day_key);
CREATE INDEX gross_return_ix_if2 ON hdm.gross_return USING btree (fund_key);
CREATE INDEX gross_return_ix_if3 ON hdm.gross_return USING btree (bmk_idx_key);
CREATE INDEX gross_return_ix_if4 ON hdm.gross_return USING btree (per_key);

-- Drop table

-- DROP TABLE hdm.gross_wealth_index;

CREATE TABLE hdm.gross_wealth_index (
	gr_wlth_idx_key int4 NOT NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	gr_wlth_idx numeric(38,15) NULL,
	net_wlth_idx numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX gross_wealth_index_ix_if1 ON hdm.gross_wealth_index USING btree (day_key);
CREATE INDEX gross_wealth_index_ix_if2 ON hdm.gross_wealth_index USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.group_trust_asset;

CREATE TABLE hdm.group_trust_asset (
	grp_trst_asset_key int4 NOT NULL,
	grp_trst_pln_key int4 NULL,
	grp_trst_fund_key int4 NULL,
	day_key int4 NULL,
	tot_net_asset numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX group_trust_asset_ix_if1 ON hdm.group_trust_asset USING btree (grp_trst_fund_key);
CREATE INDEX group_trust_asset_ix_if2 ON hdm.group_trust_asset USING btree (day_key);
CREATE INDEX group_trust_asset_ix_if4 ON hdm.group_trust_asset USING btree (grp_trst_pln_key);

-- Drop table

-- DROP TABLE hdm.group_trust_fund;

CREATE TABLE hdm.group_trust_fund (
	grp_trst_fund_key int4 NOT NULL,
	div_nm varchar(255) NULL,
	div_extd_nm varchar(255) NULL,
	div_type_cd varchar(25) NULL,
	div_type_nm varchar(255) NULL,
	acct_nbr varchar(25) NULL,
	div_sort_ord numeric(38,15) NULL,
	hdg_fund_flg varchar(1) NULL,
	mgr_nm varchar(255) NULL,
	fund_id varchar(25) NULL,
	fund_sort_ord numeric(38,15) NULL,
	fund_stat varchar(25) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX group_trust_fund_ix_if1 ON hdm.group_trust_fund USING btree (curr_row_flg, row_strt_dttm, row_stop_dttm, etl_load_cyc_key, src_sys_id);

-- Drop table

-- DROP TABLE hdm.group_trust_levels;

CREATE TABLE hdm.group_trust_levels (
	grp_trst_lvl_key int4 NOT NULL,
	asset_type varchar(20) NULL,
	perf_thrs numeric(38,15) NULL,
	asset_allocn numeric(38,15) NULL,
	asset_allocn_prxy numeric(38,15) NULL,
	accptbl_allocn_prxy numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX group_trust_levels_ix_if1 ON hdm.group_trust_levels USING btree (curr_row_flg, row_strt_dttm, row_stop_dttm, etl_load_cyc_key, src_sys_id);

-- Drop table

-- DROP TABLE hdm.group_trust_plan;

CREATE TABLE hdm.group_trust_plan (
	grp_trst_pln_key int4 NOT NULL,
	client_nm varchar(60) NULL,
	pln_nm varchar(60) NULL,
	current_row_flag varchar(1) NULL,
	row_start_date_time timestamp NULL,
	row_stop_date_time timestamp NULL,
	etl_load_cycle_key int4 NOT NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.hbr_dlr_update;

CREATE TABLE hdm.hbr_dlr_update (
	acct_posn_key int4 NOT NULL,
	hbr_dlr_key int4 NOT NULL
);

-- Drop table

-- DROP TABLE hdm.holdings_foreign_ownership;

CREATE TABLE hdm.holdings_foreign_ownership (
	holdg_open_posn_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	secr_key int4 NULL,
	inc_crty_key int4 NULL,
	iss_crty_key int4 NULL,
	trde_crty_key int4 NULL,
	thrs_crty_key int4 NULL,
	day_key int4 NULL,
	tot_long_posn numeric(38,15) NULL,
	actl_cstdy_posn numeric(38,15) NULL,
	pend_sale_qty numeric(38,15) NULL,
	pend_buy_qty numeric(38,15) NULL,
	free_qty numeric(38,15) NULL,
	oshr_qty numeric(38,15) NULL,
	par_shr_qty numeric(38,15) NULL,
	tot_shrt_posn numeric(38,15) NULL,
	on_loan_qty numeric(38,15) NULL,
	new_secr_flg varchar(1) NULL,
	all_cls_oshr_qty numeric(38,15) NULL,
	all_voting_oshr_qty numeric(38,15) NULL,
	all_non_voting_oshr_qty numeric(38,15) NULL,
	voting_shr_ind numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	prc_shrs_cls numeric(38,15) NULL,
	prc_shrs_acls numeric(38,15) NULL,
	prc_shrs_voting numeric(38,15) NULL,
	prc_shrs_nonvoting numeric(38,15) NULL,
	breach_ind_cls bpchar(1) NULL,
	breach_ind_acls bpchar(1) NULL,
	breach_ind_voting bpchar(1) NULL,
	breach_ind_nonvoting bpchar(1) NULL,
	thrsh_br_cls numeric(38,15) NULL,
	thrsh_br_acls numeric(38,15) NULL,
	thrsh_br_voting numeric(38,15) NULL,
	thrsh_br_nonvoting numeric(38,15) NULL,
	new_br_ind_cls bpchar(1) NULL,
	new_br_ind_acls bpchar(1) NULL,
	new_br_ind_voting bpchar(1) NULL,
	new_br_ind_nonvoting bpchar(1) NULL,
	spec_alert_cls varchar(200) NULL,
	spec_alert_acls varchar(200) NULL,
	spec_alert_voting varchar(200) NULL,
	spec_alert_nonvoting varchar(200) NULL,
	prox_int_br_cls numeric(38,15) NULL,
	prox_ext_br_cls numeric(38,15) NULL,
	prox_int_br_acls numeric(38,15) NULL,
	prox_ext_br_acls numeric(38,15) NULL,
	prox_int_br_voting numeric(38,15) NULL,
	prox_ext_br_voting numeric(38,15) NULL,
	prox_int_br_nonvoting numeric(38,15) NULL,
	prox_ext_br_nonvoting numeric(38,15) NULL,
	breach_ind_int_cls bpchar(1) NULL,
	breach_ind_int_acls bpchar(1) NULL,
	breach_ind_int_voting bpchar(1) NULL,
	breach_ind_int_nonvoting bpchar(1) NULL,
	issuer_nm varchar(200) NULL,
	adr_spsr_ship_type varchar(25) NULL
);
CREATE INDEX holdings_foreign_ownershi_idx1 ON hdm.holdings_foreign_ownership USING btree (day_key, breach_ind_nonvoting);

-- Drop table

-- DROP TABLE hdm.holdings_open_position;

CREATE TABLE hdm.holdings_open_position (
	holdg_open_posn_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	secr_key int4 NULL,
	inc_crty_key int4 NULL,
	iss_crty_key int4 NULL,
	trde_crty_key int4 NULL,
	thrs_crty_key int4 NULL,
	day_key int4 NULL,
	tot_long_posn numeric(38,15) NULL,
	actl_cstdy_posn numeric(38,15) NULL,
	pend_sale_qty numeric(38,15) NULL,
	pend_buy_qty numeric(38,15) NULL,
	free_qty numeric(38,15) NULL,
	oshr_qty numeric(38,15) NULL,
	par_shr_qty numeric(38,15) NULL,
	tot_shrt_posn numeric(38,15) NULL,
	on_loan_qty numeric(38,15) NULL,
	new_secr_flg varchar(1) NULL,
	all_cls_oshr_qty numeric(38,15) NULL,
	all_voting_oshr_qty numeric(38,15) NULL,
	all_non_voting_oshr_qty numeric(38,15) NULL,
	voting_shr_ind numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	prc_shrs_cls numeric(38,15) NULL,
	prc_shrs_acls numeric(38,15) NULL,
	prc_shrs_voting numeric(38,15) NULL,
	prc_shrs_nonvoting numeric(38,15) NULL,
	breach_ind_cls bpchar(1) NULL,
	breach_ind_acls bpchar(1) NULL,
	breach_ind_voting bpchar(1) NULL,
	breach_ind_nonvoting bpchar(1) NULL,
	thrsh_br_cls numeric(38,15) NULL,
	thrsh_br_acls numeric(38,15) NULL,
	thrsh_br_voting numeric(38,15) NULL,
	thrsh_br_nonvoting numeric(38,15) NULL,
	new_br_ind_cls bpchar(1) NULL,
	new_br_ind_acls bpchar(1) NULL,
	new_br_ind_voting bpchar(1) NULL,
	new_br_ind_nonvoting bpchar(1) NULL,
	spec_alert_cls varchar(200) NULL,
	spec_alert_acls varchar(200) NULL,
	spec_alert_voting varchar(200) NULL,
	spec_alert_nonvoting varchar(200) NULL,
	prox_int_br_cls numeric(38,15) NULL,
	prox_ext_br_cls numeric(38,15) NULL,
	prox_int_br_acls numeric(38,15) NULL,
	prox_ext_br_acls numeric(38,15) NULL,
	prox_int_br_voting numeric(38,15) NULL,
	prox_ext_br_voting numeric(38,15) NULL,
	prox_int_br_nonvoting numeric(38,15) NULL,
	prox_ext_br_nonvoting numeric(38,15) NULL,
	breach_ind_int_cls bpchar(1) NULL,
	breach_ind_int_acls bpchar(1) NULL,
	breach_ind_int_voting bpchar(1) NULL,
	breach_ind_int_nonvoting bpchar(1) NULL,
	issuer_nm varchar(200) NULL,
	adr_spsr_ship_type varchar(25) NULL
);
CREATE INDEX holdings_open_position_ix_if1 ON hdm.holdings_open_position USING btree (fund_compst_key);
CREATE INDEX holdings_open_position_ix_if2 ON hdm.holdings_open_position USING btree (secr_key);
CREATE INDEX holdings_open_position_ix_if3 ON hdm.holdings_open_position USING btree (inc_crty_key);
CREATE INDEX holdings_open_position_ix_if4 ON hdm.holdings_open_position USING btree (iss_crty_key);
CREATE INDEX holdings_open_position_ix_if5 ON hdm.holdings_open_position USING btree (trde_crty_key);
CREATE INDEX holdings_open_position_ix_if6 ON hdm.holdings_open_position USING btree (day_key);
CREATE INDEX holdings_open_position_ix_if8 ON hdm.holdings_open_position USING btree (thrs_crty_key);

-- Drop table

-- DROP TABLE hdm.holdings_position_temp;

CREATE TABLE hdm.holdings_position_temp (
	holdg_open_posn_key int4 NULL,
	fund_compst_key int4 NULL,
	secr_key int4 NULL,
	inc_crty_key int4 NULL,
	iss_crty_key int4 NULL,
	trde_crty_key int4 NULL,
	thrs_crty_key int4 NULL,
	day_key int4 NULL,
	tot_long_posn numeric(38,15) NULL,
	actl_cstdy_posn numeric(38,15) NULL,
	pend_sale_qty numeric(38,15) NULL,
	pend_buy_qty numeric(38,15) NULL,
	free_qty numeric(38,15) NULL,
	oshr_qty numeric(38,15) NULL,
	par_shr_qty numeric(38,15) NULL,
	tot_shrt_posn numeric(38,15) NULL,
	on_loan_qty numeric(38,15) NULL,
	new_secr_flg varchar(1) NULL,
	all_cls_oshr_qty numeric(38,15) NULL,
	all_voting_oshr_qty numeric(38,15) NULL,
	all_non_voting_oshr_qty numeric(38,15) NULL,
	voting_shr_ind numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	prc_shrs_cls numeric(38,15) NULL,
	prc_shrs_acls numeric(38,15) NULL,
	prc_shrs_voting numeric(38,15) NULL,
	prc_shrs_nonvoting numeric(38,15) NULL,
	breach_ind_cls bpchar(1) NULL,
	breach_ind_acls bpchar(1) NULL,
	breach_ind_voting bpchar(1) NULL,
	breach_ind_nonvoting bpchar(1) NULL,
	thrsh_br_cls numeric(38,15) NULL,
	thrsh_br_acls numeric(38,15) NULL,
	thrsh_br_voting numeric(38,15) NULL,
	thrsh_br_nonvoting numeric(38,15) NULL,
	new_br_ind_cls bpchar(1) NULL,
	new_br_ind_acls bpchar(1) NULL,
	new_br_ind_voting bpchar(1) NULL,
	new_br_ind_nonvoting bpchar(1) NULL,
	spec_alert_cls varchar(200) NULL,
	spec_alert_acls varchar(200) NULL,
	spec_alert_voting varchar(200) NULL,
	spec_alert_nonvoting varchar(200) NULL,
	prox_int_br_cls numeric(38,15) NULL,
	prox_ext_br_cls numeric(38,15) NULL,
	prox_int_br_acls numeric(38,15) NULL,
	prox_ext_br_acls numeric(38,15) NULL,
	prox_int_br_voting numeric(38,15) NULL,
	prox_ext_br_voting numeric(38,15) NULL,
	prox_int_br_nonvoting numeric(38,15) NULL,
	prox_ext_br_nonvoting numeric(38,15) NULL,
	breach_ind_int_cls bpchar(1) NULL,
	breach_ind_int_acls bpchar(1) NULL,
	breach_ind_int_voting bpchar(1) NULL,
	breach_ind_int_nonvoting bpchar(1) NULL,
	issuer_nm varchar(200) NULL,
	adr_spsr_ship_type varchar(25) NULL
);

-- Drop table

-- DROP TABLE hdm.holdings_trade_hist;

CREATE TABLE hdm.holdings_trade_hist (
	holdg_trde_hist_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	base_currcy_key int4 NULL,
	secr_key int4 NULL,
	trde_day_key int4 NULL,
	holdg_txn_cd_key int4 NULL,
	cash_rsn__key int4 NULL,
	cost_type_key int4 NULL,
	loc_currcy_key int4 NULL,
	sttl_currcy_key int4 NULL,
	trde_crty_key int4 NULL,
	xchg_type_key int4 NULL,
	indtry_key int4 NULL,
	post_day_key int4 NULL,
	sttl_day_key int4 NULL,
	ssb_trde_id varchar(11) NULL,
	lot_acct_nbr varchar(6) NULL,
	lot_nbr varchar(5) NULL,
	acct_end_dt timestamp NULL,
	contrctl_sttl_dt timestamp NULL,
	sttl_locn_cd varchar(3) NULL,
	sttl_dlr_id varchar(5) NULL,
	stk_xchg_city_cd varchar(2) NULL,
	shrt_term_secr_type varchar(4) NULL,
	exectg_dlr_id varchar(5) NULL,
	brkr_rsn_cd varchar(1) NULL,
	inrt numeric(38,15) NULL,
	mtry_dt timestamp NULL,
	cncl_flg varchar(1) NULL,
	trde_flg varchar(1) NULL,
	rstc_flg varchar(1) NULL,
	ssb_trade_cncl_id varchar(25) NULL,
	post_type_cd varchar(25) NULL,
	shr_qty numeric(38,15) NULL,
	tot_amort_sold numeric(38,15) NULL,
	orig_face_mbs_qty numeric(38,15) NULL,
	base_intr_purch_sold numeric(38,15) NULL,
	base_nra_tax_whg numeric(38,15) NULL,
	base_nra_tax_refnd numeric(38,15) NULL,
	opt_contrct_ut numeric(38,15) NULL,
	strk_pr numeric(38,15) NULL,
	base_net_amt numeric(38,15) NULL,
	base_ut_pr numeric(38,15) NULL,
	gr_intr_btsld numeric(38,15) NULL,
	base_tax numeric(38,15) NULL,
	base_commsn numeric(38,15) NULL,
	base_fees numeric(38,15) NULL,
	base_othr_fees numeric(38,15) NULL,
	base_id_cost numeric(38,15) NULL,
	base_curr_id_cost numeric(38,15) NULL,
	base_amort_bkd numeric(38,15) NULL,
	xchgrt numeric(38,15) NULL,
	sttl_dt_xchgrt numeric(38,15) NULL,
	base_avg_gls numeric(38,15) NULL,
	base_lt_gls numeric(38,15) NULL,
	base_shtm_gls numeric(38,15) NULL,
	base_avg_currcy_gls numeric(38,15) NULL,
	base_tax_expns_gls numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX holdings_trade_hist_ix_if1 ON hdm.holdings_trade_hist USING btree (fund_compst_key);
CREATE INDEX holdings_trade_hist_ix_if10 ON hdm.holdings_trade_hist USING btree (sttl_currcy_key);
CREATE INDEX holdings_trade_hist_ix_if11 ON hdm.holdings_trade_hist USING btree (trde_crty_key);
CREATE INDEX holdings_trade_hist_ix_if12 ON hdm.holdings_trade_hist USING btree (xchg_type_key);
CREATE INDEX holdings_trade_hist_ix_if13 ON hdm.holdings_trade_hist USING btree (indtry_key);
CREATE INDEX holdings_trade_hist_ix_if14 ON hdm.holdings_trade_hist USING btree (post_day_key);
CREATE INDEX holdings_trade_hist_ix_if15 ON hdm.holdings_trade_hist USING btree (sttl_day_key);
CREATE INDEX holdings_trade_hist_ix_if2 ON hdm.holdings_trade_hist USING btree (base_currcy_key);
CREATE INDEX holdings_trade_hist_ix_if3 ON hdm.holdings_trade_hist USING btree (secr_key);
CREATE INDEX holdings_trade_hist_ix_if5 ON hdm.holdings_trade_hist USING btree (trde_day_key);
CREATE INDEX holdings_trade_hist_ix_if6 ON hdm.holdings_trade_hist USING btree (holdg_txn_cd_key);
CREATE INDEX holdings_trade_hist_ix_if7 ON hdm.holdings_trade_hist USING btree (cash_rsn__key);
CREATE INDEX holdings_trade_hist_ix_if8 ON hdm.holdings_trade_hist USING btree (cost_type_key);
CREATE INDEX holdings_trade_hist_ix_if9 ON hdm.holdings_trade_hist USING btree (loc_currcy_key);

-- Drop table

-- DROP TABLE hdm.holdings_txn_codes;

CREATE TABLE hdm.holdings_txn_codes (
	holdg_txn_cd_key int4 NOT NULL,
	txn_type_cd varchar(5) NULL,
	txn_type_desc varchar(255) NULL,
	txn_cd varchar(5) NULL,
	txn_cd_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX holdings_txn_codes_ix_if1 ON hdm.holdings_txn_codes USING btree (curr_row_flg, row_strt_dttm, row_stop_dttm, etl_load_cyc_key, src_sys_id);

-- Drop table

-- DROP TABLE hdm.index_return;

CREATE TABLE hdm.index_return (
	idx_ret_key int4 NOT NULL,
	per_key int4 NULL,
	bmk_idx_key int4 NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	idx_ret numeric(38,15) NULL,
	strt_per timestamp NULL,
	annlzn_fctr numeric(38,15) NULL,
	ret_per_flg varchar(1) NULL,
	perf_displ_ord numeric(38,15) NULL,
	gen_rptg_flg varchar(1) NULL,
	daily_rptg_flg varchar(1) NULL,
	trgt_rptg_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX index_return_ix_if1 ON hdm.index_return USING btree (per_key);
CREATE INDEX index_return_ix_if2 ON hdm.index_return USING btree (bmk_idx_key);
CREATE INDEX index_return_ix_if4 ON hdm.index_return USING btree (day_key);
CREATE INDEX index_return_ix_if7 ON hdm.index_return USING btree (fund_compst_key);



-- Drop table

-- DROP TABLE hdm.index_valuation;

CREATE TABLE hdm.index_valuation (
	idx_vltn_key int4 NOT NULL,
	bmk_idx_key int4 NULL,
	day_key int4 NULL,
	idx_vltn_amt numeric(38,15) NULL,
	wlth_idx numeric(38,15) NULL,
	daily_ret numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX index_valuation_ix_if1 ON hdm.index_valuation USING btree (bmk_idx_key);
CREATE INDEX index_valuation_ix_if2 ON hdm.index_valuation USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.industry;

CREATE TABLE hdm.industry (
	indtry_key int4 NOT NULL,
	sctr_cd varchar(10) NULL,
	sctr_desc varchar(250) NULL,
	indtry_cd varchar(10) NULL,
	indtry_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.intermediary_issue_type;

CREATE TABLE hdm.intermediary_issue_type (
	intrm_iss_type_key int4 NOT NULL,
	intrm_iss_type varchar(255) NULL
);

-- Drop table

-- DROP TABLE hdm.intermediary_issues;

CREATE TABLE hdm.intermediary_issues (
	intrm_iss_key int4 NOT NULL,
	day_key int4 NULL,
	dlr_key int4 NULL,
	intrm_iss_type_key int4 NULL,
	iss_cnt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX intermediary_issues_ix_if1 ON hdm.intermediary_issues USING btree (day_key);
CREATE INDEX intermediary_issues_ix_if2 ON hdm.intermediary_issues USING btree (dlr_key);
CREATE INDEX intermediary_issues_ix_if3 ON hdm.intermediary_issues USING btree (intrm_iss_type_key);

-- Drop table

-- DROP TABLE hdm.invoice_tracking;

CREATE TABLE hdm.invoice_tracking (
	invc_trckg_key int4 NOT NULL,
	invc_trckg_sum_key int4 NULL,
	dlr_fee_type_key int4 NULL,
	shr_cls_cd int4 NULL,
	shr_cls varchar(60) NULL,
	fees numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX invoice_tracking_ix_if1 ON hdm.invoice_tracking USING btree (invc_trckg_sum_key);
CREATE INDEX invoice_tracking_ix_if2 ON hdm.invoice_tracking USING btree (dlr_fee_type_key);

-- Drop table

-- DROP TABLE hdm.invoice_tracking_summary;

CREATE TABLE hdm.invoice_tracking_summary (
	invc_trckg_sum_key int4 NOT NULL,
	dlr_key int4 NULL,
	pmt_day_key int4 NULL,
	invc_trckg_id numeric(38,15) NULL,
	invc_yr numeric(38,15) NULL,
	invc_per varchar(255) NULL,
	invc_recpt_dt timestamp NULL,
	invc_tot_amt numeric(38,15) NULL,
	pmt_tot_amt numeric(38,15) NULL,
	anlyst_cmmts varchar(4000) NULL,
	invc_anlyst varchar(255) NULL,
	invc_qc1 varchar(255) NULL,
	invc_qc2 varchar(255) NULL,
	accptbl_var varchar(255) NULL,
	pmt_freq varchar(60) NULL,
	invc_per_strt_dt timestamp NULL,
	invc_per_end_dt timestamp NULL,
	modfr varchar(255) NULL,
	creatn_dt timestamp NULL,
	creatr varchar(255) NULL,
	chg_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX invoice_track_summary_ix_if1 ON hdm.invoice_tracking_summary USING btree (dlr_key);
CREATE INDEX invoice_track_summary_ix_if2 ON hdm.invoice_tracking_summary USING btree (pmt_day_key);


-- Drop table

-- DROP TABLE hdm.lkp_morningstar_peers;

CREATE TABLE hdm.lkp_morningstar_peers (
	fund_id varchar(50) NULL,
	fund_nbr numeric(38,15) NULL,
	pbl_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.manager_final_score;

CREATE TABLE hdm.manager_final_score (
	fnl_scor_key int4 NOT NULL,
	mgr_list_key int4 NULL,
	msr_cat_key int4 NULL,
	day_key int4 NULL,
	univ_key int4 NULL,
	yr3_scor numeric(38,15) NULL,
	yr5_scor numeric(38,15) NULL,
	yr10_scor numeric(38,15) NULL,
	yr3_rank numeric(38,15) NULL,
	yr5_rank numeric(38,15) NULL,
	yr10_rank numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX manager_final_score_ix_if1 ON hdm.manager_final_score USING btree (msr_cat_key);
CREATE INDEX manager_final_score_ix_if2 ON hdm.manager_final_score USING btree (day_key);
CREATE INDEX manager_final_score_ix_if4 ON hdm.manager_final_score USING btree (mgr_list_key);
CREATE INDEX manager_final_score_ix_if5 ON hdm.manager_final_score USING btree (univ_key);

-- Drop table

-- DROP TABLE hdm.manager_information;

CREATE TABLE hdm.manager_information (
	mgr_info_key int4 NOT NULL,
	day_key numeric(38,15) NULL,
	mgr_list_key int4 NULL,
	univ_key int4 NULL,
	aum_tot numeric(38,15) NULL,
	firm_incpn_yr numeric(38,15) NULL,
	prod_tot numeric(38,15) NULL,
	prfrd_bmk varchar(100) NULL,
	prim_univ varchar(100) NULL,
	prod_stat varchar(25) NULL,
	curr_holdg_cnt numeric(38,15) NULL,
	pct_holdg_top_ten numeric(38,15) NULL,
	annl_ltm numeric(38,15) NULL,
	curr_cash_posn numeric(38,15) NULL,
	wgta_mkt_cap numeric(38,15) NULL,
	mdn_mkt_cap numeric(38,15) NULL,
	prim_eqty_cap varchar(255) NULL,
	prim_eqty_styl varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX manager_information_ix_if1 ON hdm.manager_information USING btree (day_key);
CREATE INDEX manager_information_ix_if2 ON hdm.manager_information USING btree (mgr_list_key);
CREATE INDEX manager_information_ix_if4 ON hdm.manager_information USING btree (univ_key);

-- Drop table

-- DROP TABLE hdm.manager_list;

CREATE TABLE hdm.manager_list (
	mgr_list_key int4 NOT NULL,
	mgr_nm varchar(255) NULL,
	prod_nm varchar(255) NULL,
	idx_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.manager_rank_data;

CREATE TABLE hdm.manager_rank_data (
	mgr_rank_data_key int4 NOT NULL,
	mgr_list_key int4 NULL,
	msr_list_key int4 NULL,
	day_key int4 NULL,
	univ_key int4 NULL,
	ret_set varchar(20) NULL,
	ret_val numeric(38,15) NULL,
	ret_rank numeric(38,15) NULL,
	ret_dt timestamp NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX manager_rank_data_ix_if1 ON hdm.manager_rank_data USING btree (msr_list_key);
CREATE INDEX manager_rank_data_ix_if2 ON hdm.manager_rank_data USING btree (day_key);
CREATE INDEX manager_rank_data_ix_if4 ON hdm.manager_rank_data USING btree (mgr_list_key);
CREATE INDEX manager_rank_data_ix_if5 ON hdm.manager_rank_data USING btree (univ_key);

-- Drop table

-- DROP TABLE hdm.mdd_cashflow;

CREATE TABLE hdm.mdd_cashflow (
	mdd_cashflow_key int4 NOT NULL,
	reg_key int4 NOT NULL,
	dlr_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	acct_key int4 NOT NULL,
	trde_dt_key int4 NOT NULL,
	confirm_dt_key int4 NOT NULL,
	spr_sheet_day_key int4 NOT NULL,
	sub_shrs numeric(38,15) NULL,
	sub_amt numeric(38,15) NULL,
	redmpn_shrs numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	ofrg_pr numeric(38,15) NULL,
	net_cashflow_amt numeric(38,15) NULL,
	net_shrs numeric(38,15) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NULL
);
CREATE INDEX mdd_cashflow_ix_if10 ON hdm.mdd_cashflow USING btree (spr_sheet_day_key);
CREATE INDEX mdd_cashflow_ix_if2 ON hdm.mdd_cashflow USING btree (dlr_key);
CREATE INDEX mdd_cashflow_ix_if3 ON hdm.mdd_cashflow USING btree (fund_key);
CREATE INDEX mdd_cashflow_ix_if4 ON hdm.mdd_cashflow USING btree (acct_key);
CREATE INDEX mdd_cashflow_ix_if6 ON hdm.mdd_cashflow USING btree (trde_dt_key);
CREATE INDEX mdd_cashflow_ix_if8 ON hdm.mdd_cashflow USING btree (confirm_dt_key);
CREATE INDEX mdd_cashflow_ix_if9 ON hdm.mdd_cashflow USING btree (reg_key);

-- Drop table

-- DROP TABLE hdm.measure_category;

CREATE TABLE hdm.measure_category (
	msr_cat_key int4 NOT NULL,
	msr_cat_desc varchar(50) NULL,
	sub_cat_desc varchar(50) NULL,
	cat_ord numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.measure_list;

CREATE TABLE hdm.measure_list (
	msr_list_key int4 NOT NULL,
	msr_cat_key int4 NULL,
	msr_id numeric(38,15) NULL,
	msr_nm varchar(50) NULL,
	cat_wgt numeric(38,15) NULL,
	yr3_wgt_flg varchar(1) NULL,
	yr5_wgt_flg varchar(1) NULL,
	yr10_wgt_flg varchar(1) NULL,
	rank_ord varchar(1) NULL,
	msr_grp numeric(38,15) NULL,
	msr_ord numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX measure_list_ix_if2 ON hdm.measure_list USING btree (msr_cat_key);



-- DROP TABLE hdm.money_market_activity;

CREATE TABLE hdm.money_market_activity (
	mony_mkt_key int4 NOT NULL,
	fund_key int4 NULL,
	day_key int4 NULL,
	gain_loss_amt numeric(38,15) NULL,
	dstrbtn_amt numeric(38,15) NULL,
	divd_paybl_amt numeric(38,15) NULL,
	basis_points numeric(38,15) NULL,
	days_to_mtry numeric(38,15) NULL,
	intr_w_gain_loss numeric(38,15) NULL,
	intr_wo_gain_loss numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	weighted_avg_life numeric(38,15) NULL,
	net_asset_at_cost numeric(38,15) NULL,
	net_asset_at_mkt numeric(38,15) NULL,
	invmt_at_cost numeric(38,15) NULL,
	invmt_at_val numeric(38,15) NULL,
	mkt_based_pr numeric(38,15) NULL,
	sub_w_drv_in_amt numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	net_shrhldr_cash_flow_amt numeric(38,15) NULL,
	daily_lqd_asset_amt numeric(38,15) NULL,
	daily_lqd_asset_rt numeric(38,15) NULL,
	wkl_lqd_asset_amt numeric(38,15) NULL,
	wkl_lqd_asset_rt numeric(38,15) NULL,
	shdw_nav numeric(38,15) NULL,
	cnst_nav numeric(38,15) NULL
);
CREATE INDEX money_market_activity_ix_if1 ON hdm.money_market_activity USING btree (fund_key);
CREATE INDEX money_market_activity_ix_if2 ON hdm.money_market_activity USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.monthly_avg_position;

CREATE TABLE hdm.monthly_avg_position (
	mthly_avg_posn_key int4 NOT NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	day_key int4 NULL,
	tot_shrs numeric(38,15) NULL,
	tot_bal_amt numeric(38,15) NULL,
	tot_amt_excl_tpa numeric(38,15) NULL,
	tot_eligbl_bal numeric(38,15) NULL,
	tot_eligbl_excl_tpa numeric(38,15) NULL,
	dlr_tpa_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX monthly_avg_position_ix_if3 ON hdm.monthly_avg_position USING btree (fund_key);
CREATE INDEX monthly_avg_position_ix_if4 ON hdm.monthly_avg_position USING btree (dlr_key);
CREATE INDEX monthly_avg_position_ix_if5 ON hdm.monthly_avg_position USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.monthly_avg_posn_draft;

CREATE TABLE hdm.monthly_avg_posn_draft (
	mthly_avg_posn_drft_key int4 NOT NULL,
	dlr_key int4 NOT NULL,
	day_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	tot_shrs numeric(38,15) NULL,
	tot_bal_amt numeric(38,15) NULL,
	tot_amt_excl_tpa numeric(38,15) NULL,
	tot_eligbl_bal numeric(38,15) NULL,
	tot_eligbl_excl_tpa numeric(38,15) NULL,
	dlr_tpa_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX monthly_avg_posn_draft_ix_if1 ON hdm.monthly_avg_posn_draft USING btree (dlr_key);
CREATE INDEX monthly_avg_posn_draft_ix_if2 ON hdm.monthly_avg_posn_draft USING btree (day_key);
CREATE INDEX monthly_avg_posn_draft_ix_if3 ON hdm.monthly_avg_posn_draft USING btree (fund_key);


-- Drop table

-- DROP TABLE hdm."operator";

CREATE TABLE hdm.operator (
	opr_key int4 NOT NULL,
	day_key int4 NOT NULL,
	opr_id varchar(25) NULL,
	first_logon_dt timestamp NULL,
	iws_access_cd varchar(1) NULL,
	last_logon_dt timestamp NULL,
	opr_secr_last_maint timestamp NULL,
	last_maint_opr varchar(25) NULL,
	logon_cnt numeric(38,15) NULL,
	opr_expn_flg varchar(1) NULL,
	opr_nm varchar(50) NULL,
	expctd_strt_dt timestamp NULL,
	last_pswd_rst_dt timestamp NULL,
	pswd_chg_dt timestamp NULL,
	pswd_try_cnt numeric(38,15) NULL,
	ta3270_access_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	parn_opr_key int4 NULL
);
CREATE INDEX operator_ix_if1 ON hdm.operator USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.operator_function;

CREATE TABLE hdm.operator_function (
	opr_func_key int4 NOT NULL,
	day_key int4 NULL,
	fac_key int4 NULL,
	opr_key int4 NULL,
	authzn_flg varchar(1) NULL,
	opr_func_last_mntn_dt timestamp NULL,
	add_func_flg varchar(1) NULL,
	brws_func_flg varchar(1) NULL,
	del_func_flg varchar(1) NULL,
	updt_func_flg varchar(1) NULL,
	minr_add_flg varchar(1) NULL,
	minr_brws_flg varchar(1) NULL,
	minr_del_flg varchar(1) NULL,
	minr_updt_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX operator_function_ix_if1 ON hdm.operator_function USING btree (day_key);
CREATE INDEX operator_function_ix_if2 ON hdm.operator_function USING btree (fac_key);
CREATE INDEX operator_function_ix_if3 ON hdm.operator_function USING btree (opr_key);

-- Drop table

-- DROP TABLE hdm.payment_method;

CREATE TABLE hdm.payment_method (
	pmt_mthd_key int4 NOT NULL,
	pmt_mthd_top_lvl_cd varchar(3) NULL,
	pmt_mthd_top_lvl_desc varchar(60) NULL,
	pmt_mthd_cd varchar(1) NULL,
	pmt_mthd_desc varchar(60) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NULL
);

-- Drop table

-- DROP TABLE hdm.pending_all_trade;

CREATE TABLE hdm.pending_all_trade (
	pend_all_trde_key int4 NOT NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	acct_key int4 NULL,
	txn_type_key int4 NULL,
	trde_day_key int4 NULL,
	sttl_day_key int4 NULL,
	log_day_key int4 NULL,
	txn_ref_id varchar(100) NULL,
	grp_txn_ref varchar(100) NULL,
	entry_type varchar(60) NULL,
	nca_flg varchar(1) NULL,
	rvrs_flg varchar(1) NULL,
	trde_delvy_flg varchar(1) NULL,
	rep_cd varchar(60) NULL,
	aft_hrs_flg varchar(1) NULL,
	nscc_confrm_id varchar(100) NULL,
	trde_src varchar(60) NULL,
	dln_levy varchar(255) NULL,
	pln_id varchar(25) NULL,
	txn_pr numeric(38,15) NULL,
	gr_cash_val numeric(38,15) NULL,
	cb_val numeric(38,15) NULL,
	txn_tax_amt numeric(38,15) NULL,
	shr_val numeric(38,15) NULL,
	net_cash_val numeric(38,15) NULL,
	ltst_batch_flg varchar(1) NULL,
	load_seq_nbr numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NOT NULL,
	src_sys_id numeric(38,15) NULL,
	row_updt_dttm timestamp NULL
);
CREATE INDEX pending_all_trade_ix_if1 ON hdm.pending_all_trade USING btree (dlr_key);
CREATE INDEX pending_all_trade_ix_if2 ON hdm.pending_all_trade USING btree (fund_key);
CREATE INDEX pending_all_trade_ix_if3 ON hdm.pending_all_trade USING btree (acct_key);
CREATE INDEX pending_all_trade_ix_if4 ON hdm.pending_all_trade USING btree (trde_day_key);
CREATE INDEX pending_all_trade_ix_if5 ON hdm.pending_all_trade USING btree (sttl_day_key);
CREATE INDEX pending_all_trade_ix_if6 ON hdm.pending_all_trade USING btree (log_day_key);
CREATE INDEX pending_all_trade_ix_if7 ON hdm.pending_all_trade USING btree (txn_type_key);

-- Drop table

-- DROP TABLE hdm.pending_trade;

CREATE TABLE hdm.pending_trade (
	pend_trde_key int4 NOT NULL,
	dlr_key int4 NULL,
	fund_key int4 NULL,
	acct_key int4 NULL,
	txn_type_key int4 NULL,
	trde_day_key int4 NULL,
	sttl_day_key int4 NULL,
	log_day_key int4 NULL,
	txn_ref_id varchar(100) NULL,
	grp_txn_ref varchar(100) NULL,
	entry_type varchar(60) NULL,
	nca_flg varchar(1) NULL,
	rvrs_flg varchar(1) NULL,
	trde_delvy_flg varchar(1) NULL,
	rep_cd varchar(60) NULL,
	aft_hrs_flg varchar(1) NULL,
	nscc_confrm_id varchar(100) NULL,
	trde_src varchar(60) NULL,
	dln_levy varchar(255) NULL,
	pln_id varchar(25) NULL,
	txn_pr numeric(38,15) NULL,
	gr_cash_val numeric(38,15) NULL,
	cb_val numeric(38,15) NULL,
	txn_tax_amt numeric(38,15) NULL,
	shr_val numeric(38,15) NULL,
	net_cash_val numeric(38,15) NULL,
	ltst_batch_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	load_seq_nbr numeric(38,15) NULL
);
CREATE INDEX pending_trade_ix_if1 ON hdm.pending_trade USING btree (dlr_key);
CREATE INDEX pending_trade_ix_if2 ON hdm.pending_trade USING btree (fund_key);
CREATE INDEX pending_trade_ix_if3 ON hdm.pending_trade USING btree (acct_key);
CREATE INDEX pending_trade_ix_if4 ON hdm.pending_trade USING btree (trde_day_key);
CREATE INDEX pending_trade_ix_if5 ON hdm.pending_trade USING btree (txn_type_key);
CREATE INDEX pending_trade_ix_if6 ON hdm.pending_trade USING btree (sttl_day_key);
CREATE INDEX pending_trade_ix_if7 ON hdm.pending_trade USING btree (log_day_key);

-- Drop table

-- DROP TABLE hdm.perf_run_type;

CREATE TABLE hdm.perf_run_type (
	perf_run_type_key int4 NOT NULL,
	run_type_cd varchar(25) NULL,
	run_type_desc varchar(100) NULL
);

-- Drop table

-- DROP TABLE hdm.pimco_account_reference;

CREATE TABLE hdm.pimco_account_reference (
	pmc_acct_ref_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	pmc_acct_nbr numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_account_reference_ix_if1 ON hdm.pimco_account_reference USING btree (fund_compst_key);

-- Drop table

-- DROP TABLE hdm.pimco_country_breakout;

CREATE TABLE hdm.pimco_country_breakout (
	crty_brk_out_key int4 NOT NULL,
	fund_compst_key int4 NULL,
	pmc_reg_key int4 NULL,
	day_key int4 NULL,
	acct_ext_dur_yr numeric(38,15) NULL,
	acct_ext_em_sprd numeric(38,15) NULL,
	acct_loc_dur_yr numeric(38,15) NULL,
	acct_curr_expsr numeric(38,15) NULL,
	bmk_ext_dur_yr numeric(38,15) NULL,
	bmk_ext_em_sprd numeric(38,15) NULL,
	bmk_loc_dur_yr numeric(38,15) NULL,
	bmk_curr_expsr numeric(38,15) NULL,
	var_ext_dur_yr numeric(38,15) NULL,
	var_ext_em_sprd numeric(38,15) NULL,
	var_loc_dur_yr numeric(38,15) NULL,
	var_curr_expsr numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_country_breakout_ix_if1 ON hdm.pimco_country_breakout USING btree (fund_compst_key);
CREATE INDEX pimco_country_breakout_ix_if2 ON hdm.pimco_country_breakout USING btree (pmc_reg_key);
CREATE INDEX pimco_country_breakout_ix_if3 ON hdm.pimco_country_breakout USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.pimco_derivative_bucket;

CREATE TABLE hdm.pimco_derivative_bucket (
	dervtv_buckt_key int4 NOT NULL,
	dervtv_buckt_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.pimco_derivatives;

CREATE TABLE hdm.pimco_derivatives (
	dervtv_key int4 NOT NULL,
	dervtv_buckt_key int4 NULL,
	day_key int4 NULL,
	fund_compst_key int4 NULL,
	ntional_amt numeric(38,15) NULL,
	mkt_val numeric(38,15) NULL,
	mkt_val_pct numeric(38,15) NULL,
	dur_pct numeric(38,15) NULL,
	dwe numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_derivatives_ix_if1 ON hdm.pimco_derivatives USING btree (dervtv_buckt_key);
CREATE INDEX pimco_derivatives_ix_if2 ON hdm.pimco_derivatives USING btree (day_key);
CREATE INDEX pimco_derivatives_ix_if3 ON hdm.pimco_derivatives USING btree (fund_compst_key);

-- Drop table

-- DROP TABLE hdm.pimco_duration_breakout;

CREATE TABLE hdm.pimco_duration_breakout (
	pmc_dur_brk_out_key int4 NOT NULL,
	dur_buckt_key int4 NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	acct_dur_pct numeric(38,15) NULL,
	acct_dwe numeric(38,15) NULL,
	bmk_dur_pct numeric(38,15) NULL,
	bmk_dwe numeric(38,15) NULL,
	var_dur_pct numeric(38,15) NULL,
	var_dwe numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_duration_breakout_ix_if1 ON hdm.pimco_duration_breakout USING btree (dur_buckt_key);
CREATE INDEX pimco_duration_breakout_ix_if2 ON hdm.pimco_duration_breakout USING btree (fund_compst_key);
CREATE INDEX pimco_duration_breakout_ix_if3 ON hdm.pimco_duration_breakout USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.pimco_duration_bucket;

CREATE TABLE hdm.pimco_duration_bucket (
	dur_buckt_key int4 NOT NULL,
	dur_buckt_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.pimco_maturity_breakout;

CREATE TABLE hdm.pimco_maturity_breakout (
	mtry_brk_out_key int4 NOT NULL,
	pmc_mtry_buckt_key int4 NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	acct_mkt_val numeric(38,15) NULL,
	acct_mkt_val_pct numeric(38,15) NULL,
	bmk_mkt_val_pct numeric(38,15) NULL,
	var_mkt_val_pct numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_maturity_breakout_ix_if1 ON hdm.pimco_maturity_breakout USING btree (pmc_mtry_buckt_key);
CREATE INDEX pimco_maturity_breakout_ix_if2 ON hdm.pimco_maturity_breakout USING btree (fund_compst_key);
CREATE INDEX pimco_maturity_breakout_ix_if3 ON hdm.pimco_maturity_breakout USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.pimco_maturity_bucket;

CREATE TABLE hdm.pimco_maturity_bucket (
	pmc_mtry_buckt_key int4 NOT NULL,
	mtry_buckt_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	mtry_buckt_type numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.pimco_mkt_commodity;

CREATE TABLE hdm.pimco_mkt_commodity (
	mkt_cmdty_key int4 NOT NULL,
	pmc_sctr_key int4 NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	bmk_mkt_val_pct numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_mkt_commodity_ix_if1 ON hdm.pimco_mkt_commodity USING btree (pmc_sctr_key);
CREATE INDEX pimco_mkt_commodity_ix_if2 ON hdm.pimco_mkt_commodity USING btree (fund_compst_key);
CREATE INDEX pimco_mkt_commodity_ix_if3 ON hdm.pimco_mkt_commodity USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.pimco_mkt_country;

CREATE TABLE hdm.pimco_mkt_country (
	mkt_cntr_key int4 NOT NULL,
	crty_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.pimco_mkt_maturity;

CREATE TABLE hdm.pimco_mkt_maturity (
	mktg_mtry_key int4 NOT NULL,
	day_key int4 NULL,
	fund_compst_key int4 NULL,
	pmc_mtry_buckt_key int4 NULL,
	fund_mtry numeric(38,15) NULL,
	bmk_mtry numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_mkt_maturity_ix_if1 ON hdm.pimco_mkt_maturity USING btree (day_key);
CREATE INDEX pimco_mkt_maturity_ix_if2 ON hdm.pimco_mkt_maturity USING btree (fund_compst_key);
CREATE INDEX pimco_mkt_maturity_ix_if3 ON hdm.pimco_mkt_maturity USING btree (pmc_mtry_buckt_key);

-- Drop table

-- DROP TABLE hdm.pimco_mkt_quality;

CREATE TABLE hdm.pimco_mkt_quality (
	mkt_qual_key int4 NOT NULL,
	day_key int4 NULL,
	fund_compst_key int4 NULL,
	pmc_qual_buckt_key int4 NULL,
	fund_qual_rtng numeric(38,15) NULL,
	bmk_qual_rtng numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_mkt_quality_ix_if1 ON hdm.pimco_mkt_quality USING btree (day_key);
CREATE INDEX pimco_mkt_quality_ix_if2 ON hdm.pimco_mkt_quality USING btree (fund_compst_key);
CREATE INDEX pimco_mkt_quality_ix_if3 ON hdm.pimco_mkt_quality USING btree (pmc_qual_buckt_key);

-- Drop table

-- DROP TABLE hdm.pimco_mkt_survey;

CREATE TABLE hdm.pimco_mkt_survey (
	mkt_srvy_key int4 NOT NULL,
	day_key int4 NULL,
	fund_compst_key int4 NULL,
	mkt_cntr_key int4 NULL,
	fund_crty_pct numeric(38,15) NULL,
	fund_currcy_pct numeric(38,15) NULL,
	bmk_crty_pct numeric(38,15) NULL,
	bmk_currcy_pct numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_mkt_survey_ix_if1 ON hdm.pimco_mkt_survey USING btree (day_key);
CREATE INDEX pimco_mkt_survey_ix_if2 ON hdm.pimco_mkt_survey USING btree (fund_compst_key);
CREATE INDEX pimco_mkt_survey_ix_if3 ON hdm.pimco_mkt_survey USING btree (mkt_cntr_key);

-- Drop table

-- DROP TABLE hdm.pimco_quality_breakout;

CREATE TABLE hdm.pimco_quality_breakout (
	qual_brk_out_key int4 NOT NULL,
	pmc_qual_buckt_key int4 NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	acct_expsr_amt numeric(38,15) NULL,
	acct_expsr_pct numeric(38,15) NULL,
	acct_dur_pct numeric(38,15) NULL,
	acct_dwe numeric(38,15) NULL,
	bmk_expsr_pct numeric(38,15) NULL,
	bmk_dur_pct numeric(38,15) NULL,
	bmk_dwe numeric(38,15) NULL,
	var_expsr_pct numeric(38,15) NULL,
	var_dur_pct numeric(38,15) NULL,
	var_dwe numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_quality_breakout_ix_if1 ON hdm.pimco_quality_breakout USING btree (pmc_qual_buckt_key);
CREATE INDEX pimco_quality_breakout_ix_if2 ON hdm.pimco_quality_breakout USING btree (fund_compst_key);
CREATE INDEX pimco_quality_breakout_ix_if3 ON hdm.pimco_quality_breakout USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.pimco_quality_bucket;

CREATE TABLE hdm.pimco_quality_bucket (
	pmc_qual_buckt_key int4 NOT NULL,
	qual_buckt_nm varchar(255) NULL,
	qual_sort_ord numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	qual_buckt_type numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.pimco_region;

CREATE TABLE hdm.pimco_region (
	pmc_reg_key int4 NOT NULL,
	mkt_type_desc varchar(255) NULL,
	reg_cd numeric(38,15) NULL,
	reg_desc varchar(255) NULL,
	crty_id numeric(38,15) NULL,
	crty_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.pimco_sector;

CREATE TABLE hdm.pimco_sector (
	pmc_sctr_key int4 NOT NULL,
	sctr_type varchar(25) NULL,
	sctr_lvl_desc varchar(255) NULL,
	sctr_sub_lvl_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.pimco_sector_breakout;

CREATE TABLE hdm.pimco_sector_breakout (
	sctr_brk_out_key int4 NOT NULL,
	pmc_sctr_key int4 NULL,
	fund_compst_key int4 NULL,
	day_key int4 NULL,
	acct_mkt_val numeric(38,15) NULL,
	acct_mkt_val_pct numeric(38,15) NULL,
	acct_dur_pct numeric(38,15) NULL,
	acct_dwe numeric(38,15) NULL,
	bmk_mkt_val_pct numeric(38,15) NULL,
	bmk_dur_pct numeric(38,15) NULL,
	bmk_dwe numeric(38,15) NULL,
	var_mkt_val_pct numeric(38,15) NULL,
	var_dur_pct numeric(38,15) NULL,
	var_dwe numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pimco_sector_breakout_ix_if1 ON hdm.pimco_sector_breakout USING btree (pmc_sctr_key);
CREATE INDEX pimco_sector_breakout_ix_if2 ON hdm.pimco_sector_breakout USING btree (fund_compst_key);
CREATE INDEX pimco_sector_breakout_ix_if3 ON hdm.pimco_sector_breakout USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.pimco_statistics;

CREATE TABLE hdm.pimco_statistics (
	pmc_stats_key int4 NOT NULL,
	day_key int4 NULL,
	fund_compst_key int4 NULL,
	mkt_val numeric(38,15) NULL,
	qtrly_contrbn numeric(38,15) NULL,
	est_incm numeric(38,15) NULL,
	est_ytm numeric(38,15) NULL,
	efftv_dur numeric(38,15) NULL,
	avg_mtry numeric(38,15) NULL,
	avg_qual varchar(25) NULL,
	avg_coupn numeric(38,15) NULL,
	curr_yld numeric(38,15) NULL,
	bmk_avg_qual varchar(25) NULL,
	bmk_efftv_dur numeric(38,15) NULL,
	bmk_curr_yld numeric(38,15) NULL,
	bmk_pmc_dur numeric(38,15) NULL,
	bmk_est_ytm numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	tot_net_asset numeric(38,15) NULL,
	finc_asset numeric(38,15) NULL,
	net_asset_n_cash numeric(38,15) NULL,
	fund_iss_cnt numeric(38,15) NULL,
	bmk_iss_cnt numeric(38,15) NULL,
	bmk_avg_mkt_coupn numeric(38,15) NULL,
	bmk_wgta_mtry numeric(38,15) NULL
);
CREATE INDEX pimco_statistics_ix_if1 ON hdm.pimco_statistics USING btree (day_key);
CREATE INDEX pimco_statistics_ix_if2 ON hdm.pimco_statistics USING btree (fund_compst_key);

-- Drop table

-- DROP TABLE hdm.position_recalc_accts;

CREATE TABLE hdm.position_recalc_accts (
	acct_nbr numeric(38,15) NOT NULL,
	regstn_line_1 varchar(35) NULL,
	regstn_line_2 varchar(35) NULL
);

-- Drop table

-- DROP TABLE hdm.position_type;

CREATE TABLE hdm.position_type (
	posn_type_key int4 NOT NULL,
	posn_cd varchar(25) NULL,
	posn_desc varchar(50) NULL
);

-- Drop table

-- DROP TABLE hdm.pro_expense_ratio;

CREATE TABLE hdm.pro_expense_ratio (
	pro_expns_ratio_key int4 NOT NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	gr_expns_ratio numeric(38,15) NULL,
	net_expns_ratio numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX pro_expense_ratio_ix_if1 ON hdm.pro_expense_ratio USING btree (day_key);
CREATE INDEX pro_expense_ratio_ix_if2 ON hdm.pro_expense_ratio USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.profile_values;

CREATE TABLE hdm.profile_values (
	report_name varchar(250) NULL,
	key_value varchar(250) NULL,
	recipient_name varchar(250) NULL,
	recipient_email varchar(250) NULL
);

-- Drop table

-- DROP TABLE hdm.qvd_incm_test;

CREATE TABLE hdm.qvd_incm_test (
	fund_compst_key int4 NOT NULL,
	all_compst_cd varchar(10) NULL,
	all_compst_desc varchar(20) NULL,
	asset_cls_nm varchar(40) NULL,
	asset_cls_desc varchar(255) NULL,
	sub_cls_nm varchar(40) NULL,
	sub_cls_desc varchar(255) NULL,
	styl_nm varchar(40) NULL,
	styl_desc varchar(255) NULL,
	compst_cd int4 NOT NULL,
	compst_nm varchar(255) NULL,
	perf_cd varchar(20) NULL,
	perf_nm varchar(255) NULL,
	compst_ord numeric(38,15) NULL,
	compst_stat_cd varchar(1) NULL,
	compst_stat_desc varchar(50) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	compst_src_id int4 NULL,
	fund_styl varchar(100) NULL,
	risk_ret_ord numeric(38,15) NULL,
	compst_incpn_dt timestamp NULL
);

-- Drop table

-- DROP TABLE hdm.reason_code;

CREATE TABLE hdm.reason_code (
	rsn_key int4 NOT NULL,
	entity_rsn_cd numeric(38,15) NULL,
	entity_desc varchar(255) NULL,
	rsn_cd numeric(38,15) NOT NULL,
	rsn_shrt_desc varchar(25) NULL,
	rsn_long_desc varchar(255) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL
);

-- Drop table

-- DROP TABLE hdm.reason_code_bkup;

CREATE TABLE hdm.reason_code_bkup (
	rsn_key int4 NOT NULL,
	entity_rsn_cd numeric(38,15) NULL,
	entity_desc varchar(255) NULL,
	rsn_cd numeric(38,15) NOT NULL,
	rsn_shrt_desc varchar(25) NULL,
	rsn_long_desc varchar(255) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NOT NULL,
	src_sys_id numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL
);

-- Drop table

-- DROP TABLE hdm.reason_code_old;

CREATE TABLE hdm.reason_code_old (
	rsn_key int4 NOT NULL,
	rsn_cd numeric(38,15) NOT NULL,
	rsn_shrt_desc varchar(25) NULL,
	rsn_long_desc varchar(255) NULL,
	dst_entity varchar(25) NULL,
	harbor_cctr varchar(3) NOT NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NOT NULL,
	src_sys_id numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL
);

-- Drop table

-- DROP TABLE hdm.ref_mkt_nominee_list;

CREATE TABLE hdm.ref_mkt_nominee_list (
	id numeric(38,15) NULL,
	nominee varchar(250) NULL,
	bank_name varchar(250) NULL,
	state varchar(3) NULL,
	sl_profile bpchar(1) NULL,
	sl_contact bpchar(1) NULL
);

-- Drop table

-- DROP TABLE hdm.region;

CREATE TABLE hdm.region (
	reg_key int4 NOT NULL,
	st_cd varchar(3) NOT NULL,
	st_nm varchar(60) NULL,
	mktg_reg_id numeric(38,15) NOT NULL,
	mktg_reg_nm varchar(60) NULL,
	crty_cd varchar(3) NOT NULL,
	crty_nm varchar(20) NULL,
	glbl_reg_cd varchar(3) NOT NULL,
	glbl_reg_nm varchar(60) NULL,
	reg_type_cd varchar(3) NOT NULL,
	reg_type_nm varchar(60) NULL,
	top_lvl_reg_cd varchar(3) NOT NULL,
	top_lvl_reg_nm varchar(20) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL
);

-- Drop table

-- DROP TABLE hdm.report_filters;

CREATE TABLE hdm.report_filters (
	filtr_type varchar(4) NOT NULL,
	filtr_val bytea NOT NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	filtr_emp_val bytea NULL,
	trste_flg varchar(1) NULL,
	offr_flg varchar(1) NULL
);

-- Drop table

-- DROP TABLE hdm.report_filters_backup;

CREATE TABLE hdm.report_filters_backup (
	filtr_type varchar(4) NOT NULL,
	filtr_val bytea NOT NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	filtr_emp_val bytea NULL
);

-- Drop table

-- DROP TABLE hdm.return_periods;

CREATE TABLE hdm.return_periods (
	per_key int4 NOT NULL,
	per_nm varchar(10) NULL,
	per_desc varchar(25) NULL,
	per_ut varchar(20) NULL,
	per_msr numeric(38,15) NULL,
	annl_fctr numeric(38,15) NULL,
	daily_msr_flg varchar(1) NULL,
	mo_end_msr_flg varchar(1) NULL,
	per_sort_ord numeric(38,15) NULL,
	full_desc varchar(60) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.rfs_report_date;

CREATE TABLE hdm.rfs_report_date (
	id numeric(38,15) NOT NULL,
	report_effective_date timestamp NOT NULL,
	date_range varchar(10) NOT NULL,
	insert_user varchar(30) NOT NULL,
	insert_timestamp timestamp NOT NULL
);

-- Drop table

-- DROP TABLE hdm.sdcm_cashflow;

CREATE TABLE hdm.sdcm_cashflow (
	sdcm_cashflow_key int4 NOT NULL,
	reg_key int4 NULL,
	pmt_mthd_key int4 NULL,
	dlr_key int4 NULL,
	shrhldr_key int4 NULL,
	acct_key int4 NULL,
	fund_key int4 NULL,
	spr_sheet_dt_key int4 NULL,
	trde_dt_key int4 NULL,
	confirm_dt_key int4 NULL,
	shrhldr_role_key int4 NOT NULL,
	acct_nbr numeric(38,15) NULL,
	sub_shrs numeric(38,15) NULL,
	sub_amt numeric(38,15) NULL,
	redmpn_shrs numeric(38,15) NULL,
	redmpn_amt numeric(38,15) NULL,
	ofrg_pr numeric(38,15) NULL,
	nav numeric(38,15) NULL,
	dlr_commsn_amt numeric(38,15) NULL,
	undr_wrtr_commsn_amt numeric(38,15) NULL,
	adv_commsn_amt numeric(38,15) NULL,
	net_shrs numeric(38,15) NULL,
	net_cashflow_amt numeric(38,15) NULL,
	cash_cntl_recon_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NOT NULL,
	hbr_dlr_key int4 NULL
);
CREATE INDEX sdcm_cashflow_ix_ie1 ON hdm.sdcm_cashflow USING btree (shrhldr_role_key);
CREATE INDEX sdcm_cashflow_ix_ie2 ON hdm.sdcm_cashflow USING btree (acct_nbr);
CREATE INDEX sdcm_cashflow_ix_if1 ON hdm.sdcm_cashflow USING btree (reg_key);
CREATE INDEX sdcm_cashflow_ix_if11 ON hdm.sdcm_cashflow USING btree (hbr_dlr_key);
CREATE INDEX sdcm_cashflow_ix_if2 ON hdm.sdcm_cashflow USING btree (pmt_mthd_key);
CREATE INDEX sdcm_cashflow_ix_if3 ON hdm.sdcm_cashflow USING btree (spr_sheet_dt_key);
CREATE INDEX sdcm_cashflow_ix_if4 ON hdm.sdcm_cashflow USING btree (dlr_key);
CREATE INDEX sdcm_cashflow_ix_if5 ON hdm.sdcm_cashflow USING btree (fund_key);
CREATE INDEX sdcm_cashflow_ix_if6 ON hdm.sdcm_cashflow USING btree (shrhldr_key);
CREATE INDEX sdcm_cashflow_ix_if7 ON hdm.sdcm_cashflow USING btree (acct_key);
CREATE INDEX sdcm_cashflow_ix_if8 ON hdm.sdcm_cashflow USING btree (trde_dt_key);
CREATE INDEX sdcm_cashflow_ix_if9 ON hdm.sdcm_cashflow USING btree (confirm_dt_key);

-- Drop table

-- DROP TABLE hdm.sdcm_fund_balance;

CREATE TABLE hdm.sdcm_fund_balance (
	fund_bal_key int4 NOT NULL,
	fund_key int4 NULL,
	day_key int4 NULL,
	tot_fund_shrs numeric(38,15) NULL,
	tot_fund_asset numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX sdcm_fund_balance_ix_if1 ON hdm.sdcm_fund_balance USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm."security";

CREATE TABLE hdm."security" (
	secr_key int4 NOT NULL,
	sedol_id varchar(20) NULL,
	isin_id varchar(20) NULL,
	tckr_sym varchar(50) NULL,
	secr_nm varchar(250) NULL,
	issuer_nm varchar(255) NULL,
	glbl_issuer_nm varchar(255) NULL,
	invmt_type_cd varchar(2) NULL,
	invmt_type_desc varchar(20) NULL,
	dtc_cusip varchar(50) NULL,
	asset_id varchar(20) NULL,
	muncpl_secr_st_cd varchar(2) NULL,
	muncpl_secr_st varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX security_hcf1 ON hdm.security USING btree (sedol_id, secr_key);

-- Drop table

-- DROP TABLE hdm.security_attributes;

CREATE TABLE hdm.security_attributes (
	secr_attr_key int4 NOT NULL,
	asset_id varchar(100) NOT NULL,
	ssb_fund_nbr varchar(25) NOT NULL,
	lot_acct_nbr varchar(6) NULL,
	secr_type varchar(100) NULL,
	cusip_id varchar(100) NULL,
	cash_id varchar(20) NULL,
	spot_id varchar(50) NULL,
	fwd_id varchar(20) NULL,
	shrt_term_purch_dt timestamp NULL,
	shrt_term_sttl_dt timestamp NULL,
	orig_iss_dt timestamp NULL,
	mtry_dt timestamp NULL,
	moody_rtng varchar(20) NULL,
	s_p_rtng varchar(20) NULL,
	call_put_ind varchar(1) NULL,
	repo_nbr varchar(2) NULL,
	ssb_trde_id varchar(11) NULL,
	fx_brkr_cd varchar(5) NULL,
	rpt_cls_cd varchar(2) NULL,
	rpt_cls_desc varchar(255) NULL,
	iss_cls_cd varchar(25) NULL,
	iss_cls_desc varchar(255) NULL,
	nbr_of_contrct numeric(38,15) NULL,
	orig_face_amt numeric(38,15) NULL,
	fx_closg_ind varchar(1) NULL,
	invmt_ind varchar(2) NULL,
	invmt_desc varchar(50) NULL,
	asset_grp_cd varchar(1) NULL,
	asset_grp_desc varchar(50) NULL,
	invmt_mkt_val_flg varchar(1) NULL,
	mkt_val_cd varchar(1) NULL,
	mkt_val_desc varchar(250) NULL,
	posn_type_cd varchar(1) NULL,
	posn_type_desc varchar(50) NULL,
	orig_coupn_rt numeric(38,15) NULL,
	varb_rt_freq_cd varchar(2) NULL,
	varb_rt_freq_desc varchar(255) NULL,
	varb_rt_chg_dt timestamp NULL,
	mkt_pr_src_cd varchar(2) NULL,
	mkt_pr_src_vend varchar(255) NULL,
	mkt_pr_src_desc varchar(255) NULL,
	pool_type_cd varchar(2) NULL,
	pool_type_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	confl_asset_grp_cd varchar(25) NULL,
	confl_asset_grp_nm varchar(255) NULL
);
CREATE INDEX security_attributes_ix_ie1 ON hdm.security_attributes USING btree (asset_id);
CREATE INDEX security_attributes_ix_ie2 ON hdm.security_attributes USING btree (ssb_fund_nbr);



-- Drop table

-- DROP TABLE hdm.shareholder;

CREATE TABLE hdm.shareholder (
	shrhldr_key int4 NOT NULL,
	shrhldr_role_key int4 NOT NULL,
	shrhldr_top_lvl_cd varchar(3) NULL,
	shrhldr_top_lvl_desc varchar(60) NULL,
	shrhldr_nm varchar(60) NULL,
	tax_idnfcn_nbr numeric(38,15) NULL,
	ssn_stat_desc varchar(25) NULL,
	tax_idnfcn_type varchar(25) NULL,
	cust_lgl_ownr_regstn_dt timestamp NULL,
	cust_idnfcn_nbr numeric(38,15) NOT NULL,
	cust_idnfcn_nbr_2 numeric(38,15) NOT NULL,
	vip_cust_flg varchar(1) NULL,
	send_mktg_info_flg varchar(1) NULL,
	informed_cnsnt_flg varchar(1) NULL,
	party_type varchar(60) NULL,
	cust_empl_flg varchar(1) NULL,
	mgmt_co_empl_flg varchar(1) NULL,
	lgl_stat varchar(60) NULL,
	gend varchar(60) NULL,
	brth_dt timestamp NULL,
	dcsd_dt timestamp NULL,
	emplmt_stat varchar(25) NULL,
	mrtl_stat varchar(25) NULL,
	crty_of_citzn_ship varchar(60) NULL,
	addr_type varchar(60) NULL,
	addr_line_1 varchar(60) NULL,
	addr_line_2 varchar(60) NULL,
	addr_line_3 varchar(60) NULL,
	addr_line_4 varchar(60) NULL,
	city varchar(60) NULL,
	st varchar(2) NULL,
	prov varchar(60) NULL,
	crty varchar(60) NULL,
	reg varchar(60) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	purg_prcsd_flg varchar(1) NULL,
	purg_prcsd_dt timestamp NULL
);
CREATE INDEX shareholder_ix_ie1 ON hdm.shareholder USING btree (shrhldr_role_key);

-- Drop table

-- DROP TABLE hdm.shareholder_distrib;

CREATE TABLE hdm.shareholder_distrib (
	shrhldr_dstrbtn_key int4 NOT NULL,
	acct_key int4 NULL,
	shrhldr_key int4 NULL,
	fund_key int4 NULL,
	dstrbtn_cd_key int4 NULL,
	dstrbtn_type_key int4 NULL,
	rec_day_key int4 NULL,
	paybl_day_key int4 NULL,
	re_invst_day_key int4 NULL,
	dstrbtn_rt_per_shr numeric(38,15) NULL,
	fund_expns_ratio numeric(38,15) NULL,
	re_invmt_shr_pr numeric(38,15) NULL,
	pnlty_w_holding_rt numeric(38,15) NULL,
	divd_type varchar(1) NULL,
	ltcg_flg varchar(1) NULL,
	fund_expns_amt numeric(38,15) NULL,
	fund_expns_allocn_cd varchar(2) NULL,
	shr_re_invstd numeric(38,15) NULL,
	shr_bal_aft_dstrbtn numeric(38,15) NULL,
	shrs_issd numeric(38,15) NULL,
	dstrbtn_amt numeric(38,15) NULL,
	sub_acctg_fee numeric(38,15) NULL,
	othr_fees numeric(38,15) NULL,
	forgn_tax_w_holding_rt numeric(38,15) NULL,
	forgn_tax_w_holding_amt numeric(38,15) NULL,
	us_w_holding_amt numeric(38,15) NULL,
	nra_whg_amt numeric(38,15) NULL,
	cum_discnt_nbr numeric(38,15) NULL,
	ext_pln_id varchar(9) NULL,
	wire_txn_grp numeric(38,15) NULL,
	bal_ind varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX shareholder_distrib_ix_if1 ON hdm.shareholder_distrib USING btree (acct_key);
CREATE INDEX shareholder_distrib_ix_if3 ON hdm.shareholder_distrib USING btree (fund_key);
CREATE INDEX shareholder_distrib_ix_if4 ON hdm.shareholder_distrib USING btree (rec_day_key);
CREATE INDEX shareholder_distrib_ix_if5 ON hdm.shareholder_distrib USING btree (dstrbtn_cd_key);
CREATE INDEX shareholder_distrib_ix_if6 ON hdm.shareholder_distrib USING btree (dstrbtn_type_key);
CREATE INDEX shareholder_distrib_ix_if7 ON hdm.shareholder_distrib USING btree (shrhldr_key);
CREATE INDEX shareholder_distrib_ix_if8 ON hdm.shareholder_distrib USING btree (paybl_day_key);
CREATE INDEX shareholder_distrib_ix_if9 ON hdm.shareholder_distrib USING btree (re_invst_day_key);

-- Drop table

-- DROP TABLE hdm.shd_acct_monthly_stat;

CREATE TABLE hdm.shd_acct_monthly_stat (
	fund numeric(38,15) NULL,
	state varchar(9) NULL,
	shares numeric(38,15) NULL,
	dollars numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.shdr_acct_fund;

CREATE TABLE hdm.shdr_acct_fund (
	shrhldr_acct_key int4 NOT NULL,
	dlr_branch_key int4 NOT NULL,
	fund_key int4 NOT NULL,
	acct_key int4 NOT NULL,
	shrhldr_key int4 NOT NULL,
	dlr_key int4 NOT NULL,
	reg_key int4 NOT NULL,
	prim_shrhldr_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	email_addr varchar(255) NULL,
	email_cnsnt_flg varchar(1) NULL
);
CREATE INDEX shdr_acct_fund_ix_if1 ON hdm.shdr_acct_fund USING btree (shrhldr_key);
CREATE INDEX shdr_acct_fund_ix_if2 ON hdm.shdr_acct_fund USING btree (acct_key);
CREATE INDEX shdr_acct_fund_ix_if3 ON hdm.shdr_acct_fund USING btree (fund_key);
CREATE INDEX shdr_acct_fund_ix_if5 ON hdm.shdr_acct_fund USING btree (dlr_key);
CREATE INDEX shdr_acct_fund_ix_if6 ON hdm.shdr_acct_fund USING btree (reg_key);
CREATE INDEX shdr_acct_fund_ix_if7 ON hdm.shdr_acct_fund USING btree (dlr_branch_key);

-- Drop table

-- DROP TABLE hdm.smip_fund_parameters;

CREATE TABLE hdm.smip_fund_parameters (
	fund_parm_key int4 NOT NULL,
	fund_key int4 NULL,
	min_otst_perf_lvl numeric(38,15) NULL
);
CREATE INDEX smip_fund_parameters_ix_if1 ON hdm.smip_fund_parameters USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.smip_fund_perf_level;

CREATE TABLE hdm.smip_fund_perf_level (
	fund_perf_lvl_key int4 NOT NULL,
	fund_key int4 NULL,
	min_perf_rt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	fund_inlsn_flg varchar(1) NULL,
	qntl_inlsn_flg varchar(1) NULL,
	new_fund_inlsn_flg varchar(1) NULL
);
CREATE INDEX smip_fund_perf_level_ix_if1 ON hdm.smip_fund_perf_level USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.smip_fund_performance;

CREATE TABLE hdm.smip_fund_performance (
	smip_fund_perf_key int4 NOT NULL,
	smip_mtric_key int4 NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	out_perf_basis_pt numeric(38,15) NULL,
	wgt_net_out_perf numeric(38,15) NULL,
	wgt_min_otst_perf numeric(38,15) NULL,
	fund_peer_grp_qntl_rank numeric(38,15) NULL,
	idx_peer_grp_qntl_rank numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX smip_fund_performance_ix_if1 ON hdm.smip_fund_performance USING btree (smip_mtric_key);
CREATE INDEX smip_fund_performance_ix_if2 ON hdm.smip_fund_performance USING btree (day_key);
CREATE INDEX smip_fund_performance_ix_if3 ON hdm.smip_fund_performance USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.smip_group_trust_perf;

CREATE TABLE hdm.smip_group_trust_perf (
	grp_trst_perf_key int4 NOT NULL,
	smip_mtric_key int4 NULL,
	day_key int4 NULL,
	grp_trst_perf numeric(38,15) NULL,
	bmk_perf numeric(38,15) NULL,
	perf_diff numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX smip_group_trust_perf_ix_if1 ON hdm.smip_group_trust_perf USING btree (day_key);
CREATE INDEX smip_group_trust_perf_ix_if4 ON hdm.smip_group_trust_perf USING btree (smip_mtric_key);

-- Drop table

-- DROP TABLE hdm.smip_metrics;

CREATE TABLE hdm.smip_metrics (
	smip_mtric_key int4 NOT NULL,
	mtric_cd varchar(3) NOT NULL,
	mtric_nm varchar(100) NULL,
	mtric_wgt numeric(38,15) NULL,
	per_nm varchar(20) NULL,
	per_wgt numeric(38,15) NULL,
	per_key int4 NULL,
	expctd_perf numeric(38,15) NULL,
	abnml_thrs numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	mtric_inlsn_flg varchar(1) NULL
);
CREATE INDEX smip_metrics_ix_if1 ON hdm.smip_metrics USING btree (per_key);

-- Drop table

-- DROP TABLE hdm.smip_metrics_original;

CREATE TABLE hdm.smip_metrics_original (
	smip_mtric_key int4 NOT NULL,
	mtric_cd varchar(3) NOT NULL,
	mtric_nm varchar(100) NULL,
	mtric_wgt numeric(38,15) NULL,
	per_nm varchar(20) NULL,
	per_wgt numeric(38,15) NULL,
	per_key int4 NULL,
	expctd_perf numeric(38,15) NULL,
	abnml_thrs numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	mtric_inlsn_flg varchar(1) NULL
);

-- Drop table

-- DROP TABLE hdm.smip_performance;

CREATE TABLE hdm.smip_performance (
	smip_perf_key int4 NOT NULL,
	smip_mtric_key int4 NULL,
	day_key int4 NULL,
	fund_cnt numeric(38,15) NULL,
	perfm_fund_cnt numeric(38,15) NULL,
	perfm_fund_pctg numeric(38,15) NULL,
	perf_var_pctg numeric(38,15) NULL,
	min_otst_perf numeric(38,15) NULL,
	wgt_perf_pctg numeric(38,15) NULL,
	out_perf_basis_pt numeric(38,15) NULL,
	wgt_net_out_perf numeric(38,15) NULL,
	fund_thrs_bmk_cnt numeric(38,15) NULL,
	fund_thrs_bmk_pct numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	fund_meet_bmk_cnt numeric(38,15) NULL,
	fund_meet_bmk_pct numeric(38,15) NULL,
	qntl_avg_pct numeric(38,15) NULL,
	avg_actl_perf_pct numeric(38,15) NULL,
	qntl_wgt_perf numeric(38,15) NULL
);
CREATE INDEX smip_performance_ix_if1 ON hdm.smip_performance USING btree (smip_mtric_key);
CREATE INDEX smip_performance_ix_if2 ON hdm.smip_performance USING btree (day_key);

-- Drop table

-- DROP TABLE hdm.smip_quintile_perf;

CREATE TABLE hdm.smip_quintile_perf (
	smip_qunitile_perf_key int4 NOT NULL,
	smip_mtric_key int4 NULL,
	fund_key int4 NULL,
	bmk_idx_key int4 NULL,
	day_key int4 NULL,
	fund_ret bpchar(18) NULL,
	idx_ret numeric(38,15) NULL,
	fund_peer_grp_rank numeric(38,15) NULL,
	idx_peer_grp_rank numeric(38,15) NULL,
	fund_peer_grp_qntl numeric(38,15) NULL,
	idx_peer_grp_qntl numeric(38,15) NULL,
	peer_grp_avg numeric(38,15) NULL,
	peer_grp_cnt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX smip_quintile_perf_ix_if1 ON hdm.smip_quintile_perf USING btree (fund_key);
CREATE INDEX smip_quintile_perf_ix_if2 ON hdm.smip_quintile_perf USING btree (bmk_idx_key);
CREATE INDEX smip_quintile_perf_ix_if3 ON hdm.smip_quintile_perf USING btree (day_key);
CREATE INDEX smip_quintile_perf_ix_if6 ON hdm.smip_quintile_perf USING btree (smip_mtric_key);

-- Drop table

-- DROP TABLE hdm.social_security_status;

CREATE TABLE hdm.social_security_status (
	ssn_stat_key int4 NOT NULL,
	ssn_stat_cd numeric(38,15) NOT NULL,
	ssn_stat_desc varchar(60) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NOT NULL
);

-- Drop table

-- DROP TABLE hdm.spr_sheet_cash_act;

CREATE TABLE hdm.spr_sheet_cash_act (
	spr_sheet_dt timestamp NULL
);

-- Drop table

-- DROP TABLE hdm.stg_hbr_acctmainthist;

CREATE TABLE hdm.stg_hbr_acctmainthist (
	mgmtcode varchar(2) NOT NULL,
	fund numeric(10) NOT NULL,
	account varchar(11) NOT NULL,
	view_column_nm varchar(30) NOT NULL,
	super_sheet_mnt_dt timestamp NOT NULL,
	acct_mnt_seq_id numeric(3) NOT NULL,
	load_date timestamp NOT NULL,
	load_time timestamp NOT NULL,
	after_value varchar(35) NOT NULL,
	batch_number numeric(10) NOT NULL,
	before_value varchar(35) NOT NULL
);

-- Drop table

-- DROP TABLE hdm.target_daily_weight;

CREATE TABLE hdm.target_daily_weight (
	trgt_daily_wgt_key int4 NOT NULL,
	trgt_fund_key int4 NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	shr_qty numeric(38,15) NULL,
	mkt_val numeric(38,15) NULL,
	daily_holdg_wgt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX target_daily_weight_ix_if1 ON hdm.target_daily_weight USING btree (trgt_fund_key);
CREATE INDEX target_daily_weight_ix_if2 ON hdm.target_daily_weight USING btree (day_key);
CREATE INDEX target_daily_weight_ix_if3 ON hdm.target_daily_weight USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.target_orig_glidepath;

CREATE TABLE hdm.target_orig_glidepath (
	orig_gp_key int4 NOT NULL,
	trgt_fund_key int4 NULL,
	fund_key int4 NULL,
	trgt_yr int4 NULL,
	efftv_strt_dt timestamp NULL,
	efftv_end_dt timestamp NULL,
	gp_wgt numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX target_orig_glidepath_ix_ie1 ON hdm.target_orig_glidepath USING btree (trgt_yr);
CREATE INDEX target_orig_glidepath_ix_if1 ON hdm.target_orig_glidepath USING btree (trgt_fund_key);
CREATE INDEX target_orig_glidepath_ix_if2 ON hdm.target_orig_glidepath USING btree (fund_key);

-- Drop table

-- DROP TABLE hdm.target_performance_type;

CREATE TABLE hdm.target_performance_type (
	trgt_perf_type_key int4 NOT NULL,
	trgt_perf_type varchar(25) NULL,
	trgt_perf_type_desc varchar(255) NULL,
	perf_sort_ord numeric(38,15) NULL,
	perf_type_flg varchar(1) NULL,
	obj_type varchar(25) NULL
);

-- Drop table

-- DROP TABLE hdm.target_return;

CREATE TABLE hdm.target_return (
	trgt_ret_key int4 NOT NULL,
	fund_key int4 NULL,
	trgt_perf_type_key int4 NULL,
	day_key int4 NULL,
	per_key int4 NULL,
	bmk_idx_key int4 NULL,
	trgt_ret numeric(38,15) NULL,
	beg_dt timestamp NULL,
	annlzn_fctr numeric(38,15) NULL,
	displ_sort_ord numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX target_return_ix_if1 ON hdm.target_return USING btree (fund_key);
CREATE INDEX target_return_ix_if2 ON hdm.target_return USING btree (trgt_perf_type_key);
CREATE INDEX target_return_ix_if3 ON hdm.target_return USING btree (day_key);
CREATE INDEX target_return_ix_if4 ON hdm.target_return USING btree (per_key);
CREATE INDEX target_return_ix_if6 ON hdm.target_return USING btree (bmk_idx_key);



-- Drop table

-- DROP TABLE hdm.target_wealth_index;

CREATE TABLE hdm.target_wealth_index (
	trgt_wlth_idx_key int4 NOT NULL,
	trgt_perf_type_key int4 NULL,
	day_key int4 NULL,
	fund_key int4 NULL,
	wlth_idx numeric(38,15) NULL,
	daily_ret numeric(38,15) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX target_wealth_index_ix_if1 ON hdm.target_wealth_index USING btree (trgt_perf_type_key);
CREATE INDEX target_wealth_index_ix_if2 ON hdm.target_wealth_index USING btree (day_key);
CREATE INDEX target_wealth_index_ix_if3 ON hdm.target_wealth_index USING btree (fund_key);



-- Drop table

-- DROP TABLE hdm.target_weight_stg;

CREATE TABLE hdm.target_weight_stg (
	target numeric(38,15) NULL,
	eff_start_date timestamp NULL,
	eff_end_date timestamp NULL,
	fund_nbr numeric(38,15) NULL,
	weight numeric(38,15) NULL
);

-- Drop table

-- DROP TABLE hdm.target_weights;

CREATE TABLE hdm.target_weights (
	trgt_wgt_key int4 NOT NULL,
	trgt_fund_key int4 NULL,
	efftv_strt_dt timestamp NULL,
	efftv_end_dt timestamp NULL,
	fund_key int4 NULL,
	trgt_fund_wgt numeric(38,15) NULL,
	wgt_type varchar(25) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX target_weights_ix_if1 ON hdm.target_weights USING btree (trgt_fund_key);
CREATE INDEX target_weights_ix_if2 ON hdm.target_weights USING btree (fund_key);


-- Drop table

-- DROP TABLE hdm.trade_entry_method;

CREATE TABLE hdm.trade_entry_method (
	trde_entry_mthd_key int4 NOT NULL,
	trde_entry_mthd_cd varchar(3) NOT NULL,
	trde_entry_mthd_desc varchar(60) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NOT NULL
);

-- Drop table

-- DROP TABLE hdm.trade_origin;

CREATE TABLE hdm.trade_origin (
	trde_orig_key int4 NOT NULL,
	trde_orig_id numeric(38,15) NOT NULL,
	trde_orig_desc varchar(60) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	curr_row_flg varchar(1) NULL,
	src_sys_id numeric(38,15) NULL,
	etl_load_cyc_key int4 NOT NULL
);

-- Drop table

-- DROP TABLE hdm.transaction_type;

CREATE TABLE hdm.transaction_type (
	txn_type_key int4 NOT NULL,
	txn_sfx_cd numeric(38,15) NULL,
	txn_cd numeric(38,15) NULL,
	txn_cd_desc varchar(60) NULL,
	txn_type_cd numeric(38,15) NULL,
	txn_type_desc varchar(60) NULL,
	top_lvl_txn_cd varchar(3) NOT NULL,
	top_lvl_txn_desc varchar(60) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NOT NULL,
	src_sys_id numeric(38,15) NULL,
	shr_bal_effct_flg varchar(1) NULL
);

-- Drop table

-- DROP TABLE hdm.universe;

CREATE TABLE hdm.universe (
	univ_key int4 NOT NULL,
	univ_nm varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);
CREATE INDEX universe_ix_if1 ON hdm.universe USING btree (curr_row_flg, row_strt_dttm, row_stop_dttm, etl_load_cyc_key, src_sys_id);

-- Drop table

-- DROP TABLE hdm.yield_types;

CREATE TABLE hdm.yield_types (
	yld_type_key int4 NOT NULL,
	yld_cd varchar(10) NULL,
	yld_desc varchar(255) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL
);

-- New tables

CREATE TABLE hdm.sub_advisor (
	sba_key int4 NOT NULL,
	sba_legacy_id int4 NOT NULL,
	sba_nm varchar(255) NULL,
	relshp_strt_dt timestamp NULL,
	relshp_end_dt timestamp NULL,
	actv_relshp_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	CONSTRAINT sub_advisor_ix_pk PRIMARY KEY (sba_key)
);

CREATE TABLE hdm.portfolio_manager (
	pm_key int4 NOT NULL,
	pm_legacy_id int4 NOT NULL,
	pm_nm varchar(255) NULL,
	relshp_strt_dt timestamp NULL,
	relshp_end_dt timestamp NULL,
	actv_relshp_flg varchar(1) NULL,
	curr_row_flg varchar(1) NULL,
	row_strt_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_updt_dttm timestamp NULL,
	etl_load_cyc_key int4 NULL,
	src_sys_id numeric(38,15) NULL,
	CONSTRAINT portfolio_manager_ix_pk PRIMARY KEY (pm_key)
);

create table hdm.fund_manager
(
  fund_mgr_key            integer            not null,
  fund_compst_key         integer,
  sba_key                 integer,
  pm_key                  integer,
  sba_efftv_strt_dt       date,
  sba_efftv_end_dt        date,
  pm_efftv_strt_dt        date,
  pm_efftv_end_dt         date,
  fund_mgr_efftv_strt_dt  date,
  fund_mgr_efftv_end_dt   date,
  sba_actv_flg            varchar(1),
  pm_actv_flg             varchar(1),
  curr_row_flg            varchar(1),
  row_strt_dttm           date,
  row_stop_dttm           date,
  row_updt_dttm           date,
  etl_load_cyc_key        numeric(38,15),
  src_sys_id              integer
);

-- Morningstar tables

CREATE TABLE hdm.mstr_fund (
	mstr_fund_key int8 NOT NULL,
	fund_id varchar(25) NULL,
	global_cat varchar(60) NULL,
	firm_nm varchar(255) NULL,
	mstar_cat varchar(255) NULL,
	fund_nm varchar(255) NULL,
	inception_date timestamp NULL,
	us_cat_group varchar(60) NULL,
	investment_type varchar(60) NULL,
	fund_status varchar(25) NULL,
	fund_close_date timestamp NULL,
	enhanced_idx_flg varchar(1) NULL,
	fund_of_funds_flg varchar(1) NULL,
	idx_fund_flg varchar(1) NULL,
	current_row_flg varchar(1) NULL,
	row_start_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_update_dttm timestamp NULL,
	source_system varchar(25) NULL,
	fund_standard_nm varchar(255) NULL,
	CONSTRAINT mstr_fund_ix_pk PRIMARY KEY (mstr_fund_key)
);

CREATE UNIQUE INDEX mstr_fund_ix_ak1 ON hdm.mstr_fund USING btree (fund_id);

CREATE TABLE hdm.mstr_fund_metrics (
	mstr_fund_metrics_key int8 NOT NULL,
	mstr_fund_key int8 NULL,
	day_key int8 NULL,
	aum numeric(38,15) NULL,
	net_cashflow numeric(38,15) NULL,
	holdings_count int4 NULL,
	one_yr_return numeric(20,15) NULL,
	three_yr_return numeric(20,15) NULL,
	one_yr_excess_return numeric(20,15) NULL,
	three_yr_excess_return numeric(20,15) NULL,
	beta numeric(20,15) NULL,
	tracking_error numeric(20,15) NULL,
	fund_rtng varchar(25) NULL,
	current_row_flg varchar(1) NULL,
	row_start_dttm timestamp NULL,
	row_stop_dttm timestamp NULL,
	row_update_dttm timestamp NULL,
	source_system varchar(25) NULL,
	overall_rating numeric(38,15) NULL,
	overall_anlst_rating varchar(60) NULL,
	parent_anlst_rating varchar(60) NULL,
	parent_qt_rating varchar(60) NULL,
	people_anlst_rating varchar(60) NULL,
	people_qt_rating varchar(60) NULL,
	perf_anlst_rating varchar(60) NULL,
	perf_qt_rating varchar(60) NULL,
	price_anlst_rating varchar(60) NULL,
	price_qt_rating varchar(60) NULL,
	process_anlst_rating varchar(60) NULL,
	process_qt_rating varchar(60) NULL,
	annual_rpt_net_exp_ratio numeric(38,15) NULL,
	mgmt_fee numeric(38,15) NULL,
	CONSTRAINT mstr_fund_metrics_ix_pk PRIMARY KEY (mstr_fund_metrics_key)
);

CREATE UNIQUE INDEX mstr_fund_metrics_ix_ak1 ON hdm.mstr_fund_metrics USING btree (mstr_fund_key, day_key);
CREATE INDEX mstr_fund_metrics_ix_if1 ON hdm.mstr_fund_metrics USING btree (mstr_fund_key);
CREATE INDEX mstr_fund_metrics_ix_if2 ON hdm.mstr_fund_metrics USING btree (day_key);

CREATE OR REPLACE VIEW hdm.vw_10kgrowth_chart
AS SELECT f.cal_day AS date_1,
    f.fund_nm AS fund_name,
    f.shr_cls_desc AS fund_class,
    f.quot_sym AS fund_quote,
    f.fund_compst_key AS fund_cmp,
    f.fund_sort_ord AS fund_sort,
    f.wlth_idx AS wealth_index,
    bm1.idx_nm AS benchmark_name_1,
    bm1.idx_vltn_amt AS wealth_index_1,
    bm2.idx_nm AS benchmark_name_2,
    bm2.idx_vltn_amt AS wealth_index_2,
    bm3.idx_nm AS benchmark_name_3,
    bm3.idx_vltn_amt AS wealth_index_3,
    bm4.idx_nm AS benchmark_name_4,
    bm4.wlth_idx AS wealth_index_4
   FROM ( SELECT c.cal_day,
            c.day_key,
            f_1.fund_nm,
            f_1.shr_cls_desc,
            f_1.quot_sym,
            f_1.fund_compst_key,
            f_1.fund_sort_ord,
            a.wlth_idx,
            f_1.fund_key
           FROM hdm.vw_fund_valuation a,
            hdm.vw_fund f_1,
            hdm.vw_fund_composite fc,
            hdm.vw_calendar c
          WHERE a.fund_key = f_1.fund_key AND a.day_key = c.day_key AND f_1.fund_compst_key = fc.fund_compst_key AND (f_1.fund_stat_cd::text = ANY (ARRAY[1::text::character varying::text, 5::text::character varying::text]))) f
     LEFT JOIN ( SELECT c.cal_day,
            c.day_key,
            b.idx_nm,
            b.idx_id,
            f_1.fund_compst_key,
            a.idx_vltn_amt,
            f_1.fund_key
           FROM hdm.vw_index_valuation a,
            hdm.vw_calendar c,
            hdm.vw_benchmark_index b,
            hdm.vw_fund f_1,
            hdm.vw_fund_composite fc,
            hdm.vw_fund_index_rltn bf
          WHERE a.day_key = c.day_key AND c.cal_day >= f_1.fund_incpn_dt AND a.bmk_idx_key = b.bmk_idx_key AND f_1.fund_compst_key = fc.fund_compst_key AND bf.fund_compst_key = fc.fund_compst_key AND bf.bmk_idx_key = b.bmk_idx_key AND bf.link_prio::double precision = 1::double precision AND bf.perf_run_type_key::numeric = 3::numeric AND bf.gr_ret_inlsn_flg::text = 'Y'::text) bm1 ON f.day_key = bm1.day_key AND f.fund_compst_key = bm1.fund_compst_key AND f.fund_key = bm1.fund_key
     LEFT JOIN ( SELECT c.cal_day,
            c.day_key,
            b.idx_nm,
            b.idx_id,
            f_1.fund_compst_key,
            a.idx_vltn_amt,
            f_1.fund_key
           FROM hdm.vw_index_valuation a,
            hdm.vw_calendar c,
            hdm.vw_benchmark_index b,
            hdm.vw_fund f_1,
            hdm.vw_fund_composite fc,
            hdm.vw_fund_index_rltn bf
          WHERE a.day_key = c.day_key AND c.cal_day >= f_1.fund_incpn_dt AND a.bmk_idx_key = b.bmk_idx_key AND f_1.fund_compst_key = fc.fund_compst_key AND bf.fund_compst_key = fc.fund_compst_key AND bf.bmk_idx_key = b.bmk_idx_key AND bf.link_prio::double precision = 2::double precision AND bf.perf_run_type_key::numeric = 3::numeric AND bf.gr_ret_inlsn_flg::text = 'Y'::text) bm2 ON f.day_key = bm2.day_key AND f.fund_compst_key = bm2.fund_compst_key AND f.fund_key = bm2.fund_key
     LEFT JOIN ( SELECT c.cal_day,
            c.day_key,
            b.idx_nm,
            b.idx_id,
            f_1.fund_compst_key,
            a.idx_vltn_amt,
            f_1.fund_key
           FROM hdm.vw_index_valuation a,
            hdm.vw_calendar c,
            hdm.vw_benchmark_index b,
            hdm.vw_fund f_1,
            hdm.vw_fund_composite fc,
            hdm.vw_fund_index_rltn bf
          WHERE a.day_key = c.day_key AND c.cal_day >= f_1.fund_incpn_dt AND a.bmk_idx_key = b.bmk_idx_key AND f_1.fund_compst_key = fc.fund_compst_key AND bf.fund_compst_key = fc.fund_compst_key AND bf.bmk_idx_key = b.bmk_idx_key AND bf.link_prio::double precision = 3::double precision AND bf.perf_run_type_key::numeric = 3::numeric AND bf.gr_ret_inlsn_flg::text = 'Y'::text) bm3 ON f.day_key = bm3.day_key AND f.fund_compst_key = bm3.fund_compst_key AND f.fund_key = bm3.fund_key
     LEFT JOIN ( SELECT cal.day_key,
            cal.cal_day,
            cal.fund_compst_key,
            cal.fund_key,
            cal.idx_nm,
            cal.bus_day_flg,
                CASE
                    WHEN wi.wlth_idx IS NULL THEN ( SELECT twi.wlth_idx
                       FROM hdm.vw_target_wealth_index twi,
                        hdm.vw_calendar cl,
                        hdm.vw_target_performance_type tp
                      WHERE twi.day_key = cl.day_key AND twi.trgt_perf_type_key = tp.trgt_perf_type_key AND twi.fund_key = cal.fund_key AND tp.trgt_perf_type::text = 'RBWM'::text AND cl.cal_day = (( SELECT max(vw_calendar.cal_day) AS max
                               FROM hdm.vw_calendar
                              WHERE vw_calendar.cal_day < cal.cal_day AND vw_calendar.bus_day_flg::text = 'Y'::text)))
                    ELSE wi.wlth_idx
                END AS wlth_idx
           FROM ( SELECT c.cal_day,
                    c.day_key,
                    f_1.fund_compst_key,
                    b.trgt_perf_type_key,
                    concat_ws(''::text, b.trgt_perf_type_desc, replace(replace(f_1.fund_nm::text, 'Target Retirement'::text, ''::text), 'Fund'::text, ''::text)) AS idx_nm,
                    f_1.fund_sort_ord,
                    f_1.fund_key,
                    c.bus_day_flg
                   FROM hdm.vw_calendar c,
                    hdm.vw_fund f_1,
                    hdm.vw_target_performance_type b
                  WHERE c.cal_day >= (f_1.perf_incpn_dt - ((1::numeric || ' days'::text)::interval)) AND c.cal_day < aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) AND f_1.fund_nm::text ~~ 'Target%'::text AND b.trgt_perf_type::text = 'RBWM'::text AND (f_1.fund_stat_desc::text = ANY (ARRAY['Open'::character varying::text, 'Soft Close'::character varying::text])) AND f_1.shr_cls_desc::text = 'Institutional Class'::text) cal
             LEFT JOIN ( SELECT a.day_key,
                    a.wlth_idx,
                    b.trgt_perf_type_key,
                    concat_ws(''::text, b.trgt_perf_type_desc, replace(replace(f_1.fund_nm::text, 'Target Retirement'::text, ''::text), 'Fund'::text, ''::text)) AS idx_nm,
                    f_1.fund_compst_key,
                    f_1.fund_key
                   FROM hdm.vw_target_wealth_index a,
                    hdm.vw_target_performance_type b,
                    hdm.vw_fund f_1
                  WHERE a.fund_key = f_1.fund_key AND a.trgt_perf_type_key = b.trgt_perf_type_key AND b.trgt_perf_type::text = 'RBWM'::text) wi ON cal.day_key = wi.day_key AND cal.trgt_perf_type_key = wi.trgt_perf_type_key AND cal.fund_key = wi.fund_key) bm4 ON f.day_key = bm4.day_key AND f.fund_compst_key = bm4.fund_compst_key AND f.fund_key = bm4.fund_key
  WHERE bm1.idx_nm IS NOT NULL
  ORDER BY f.fund_sort_ord, f.cal_day;

CREATE OR REPLACE VIEW hdm.vw_account
AS SELECT *
   FROM hdm.account;

CREATE OR REPLACE VIEW hdm.vw_account_category
AS SELECT *
   FROM hdm.account_category;

CREATE OR REPLACE VIEW hdm.vw_account_position
AS SELECT account_position.acct_posn_key,
    account_position.acct_key,
    account_position.fund_key,
    account_position.shrhldr_key,
    account_position.dlr_key,
    account_position.hbr_dlr_key,
    account_position.dlr_branch_key,
    account_position.day_key,
    account_position.acct_nbr,
    fund.fund_nbr,
    account_position.shrhldr_role_key,
    account_position.tot_shrs,
    account_position.tot_bal_amt,
    account_position.un_issd_shrs,
    account_position.issd_shrs,
    account_position.nav,
    account_position.rsrvd_shrs,
    account_position.accrd_divd_amt,
    account_position.escrw_shrs,
    account_position.collctd_shrs,
    account_position.collctd_bal_amt,
    account_position.curr_incm_divd,
    account_position.curr_long_term_cap_gain,
    account_position.curr_shrt_term_cap_gain,
    account_position.curr_forgn_w_holdg,
    account_position.curr_divd_w_holdg,
    account_position.curr_forgn_tax_paid_amt,
    account_position.curr_yr_1099b_gr_redmpn_amt,
    account_position.curr_yr_1099b_gr_w_holdg_amt,
    account_position.collctd_shrs_net_chg_cnt,
    account_position.tot_shrs_net_chg_cnt,
    account_position.dsgntd_w_holdg_amt,
    account_position.dsgntd_w_holdg_rt,
    account_position.divd_w_holdg_amt,
    account_position.divd_w_holdg_rt,
    account_position.shrt_term_cap_gain_w_holdg_amt,
    account_position.shrt_term_cap_gain_w_holdg_rt,
    account_position.long_term_cap_gain_w_holdg_amt,
    account_position.long_term_cap_gain_w_holdg_rt,
    account_position.partition_yr,
    account_position.acct_stat_cd,
    account_position.row_strt_dttm,
    account_position.row_stop_dttm,
    account_position.curr_row_flg,
    account_position.etl_load_cyc_key,
    account_position.src_sys_id
   FROM hdm.account_position,
    hdm.fund
  WHERE account_position.fund_key = fund.fund_key;

CREATE OR REPLACE VIEW hdm.vw_account_position_hist
AS SELECT account_position.acct_posn_key,
    account_position.acct_key,
    account_position.fund_key,
    account_position.shrhldr_key,
    account_position.dlr_key,
    account_position.hbr_dlr_key,
    account_position.dlr_branch_key,
    account_position.day_key,
    account_position.acct_nbr,
    fund.fund_nbr,
    account_position.shrhldr_role_key,
    account_position.tot_shrs,
    account_position.tot_bal_amt,
    account_position.un_issd_shrs,
    account_position.issd_shrs,
    account_position.nav,
    account_position.rsrvd_shrs,
    account_position.accrd_divd_amt,
    account_position.escrw_shrs,
    account_position.collctd_shrs,
    account_position.collctd_bal_amt,
    account_position.curr_incm_divd,
    account_position.curr_long_term_cap_gain,
    account_position.curr_shrt_term_cap_gain,
    account_position.curr_forgn_w_holdg,
    account_position.curr_divd_w_holdg,
    account_position.curr_forgn_tax_paid_amt,
    account_position.curr_yr_1099b_gr_redmpn_amt,
    account_position.curr_yr_1099b_gr_w_holdg_amt,
    account_position.collctd_shrs_net_chg_cnt,
    account_position.tot_shrs_net_chg_cnt,
    account_position.dsgntd_w_holdg_amt,
    account_position.dsgntd_w_holdg_rt,
    account_position.divd_w_holdg_amt,
    account_position.divd_w_holdg_rt,
    account_position.shrt_term_cap_gain_w_holdg_amt,
    account_position.shrt_term_cap_gain_w_holdg_rt,
    account_position.long_term_cap_gain_w_holdg_amt,
    account_position.long_term_cap_gain_w_holdg_rt,
    account_position.partition_yr,
    account_position.acct_stat_cd,
    account_position.row_strt_dttm,
    account_position.row_stop_dttm,
    account_position.curr_row_flg,
    account_position.etl_load_cyc_key,
    account_position.src_sys_id
   FROM hdm.account_position_hist account_position,
    hdm.fund
  WHERE account_position.fund_key = fund.fund_key;

CREATE OR REPLACE VIEW hdm.vw_account_position_union
AS SELECT account_position.acct_posn_key,
    account_position.acct_key,
    account_position.fund_key,
    account_position.shrhldr_key,
    account_position.dlr_key,
    account_position.hbr_dlr_key,
    account_position.dlr_branch_key,
    account_position.day_key,
    account_position.acct_nbr,
    fund.fund_nbr,
    account_position.shrhldr_role_key,
    account_position.tot_shrs,
    account_position.tot_bal_amt,
    account_position.un_issd_shrs,
    account_position.issd_shrs,
    account_position.nav,
    account_position.rsrvd_shrs,
    account_position.accrd_divd_amt,
    account_position.escrw_shrs,
    account_position.collctd_shrs,
    account_position.collctd_bal_amt,
    account_position.curr_incm_divd,
    account_position.curr_long_term_cap_gain,
    account_position.curr_shrt_term_cap_gain,
    account_position.curr_forgn_w_holdg,
    account_position.curr_divd_w_holdg,
    account_position.curr_forgn_tax_paid_amt,
    account_position.curr_yr_1099b_gr_redmpn_amt,
    account_position.curr_yr_1099b_gr_w_holdg_amt,
    account_position.collctd_shrs_net_chg_cnt,
    account_position.tot_shrs_net_chg_cnt,
    account_position.dsgntd_w_holdg_amt,
    account_position.dsgntd_w_holdg_rt,
    account_position.divd_w_holdg_amt,
    account_position.divd_w_holdg_rt,
    account_position.shrt_term_cap_gain_w_holdg_amt,
    account_position.shrt_term_cap_gain_w_holdg_rt,
    account_position.long_term_cap_gain_w_holdg_amt,
    account_position.long_term_cap_gain_w_holdg_rt,
    account_position.partition_yr,
    account_position.acct_stat_cd,
    account_position.row_strt_dttm,
    account_position.row_stop_dttm,
    account_position.curr_row_flg,
    account_position.etl_load_cyc_key,
    account_position.src_sys_id
   FROM hdm.account_position,
    hdm.fund
  WHERE account_position.fund_key = fund.fund_key
UNION ALL
 SELECT account_position.acct_posn_key,
    account_position.acct_key,
    account_position.fund_key,
    account_position.shrhldr_key,
    account_position.dlr_key,
    account_position.hbr_dlr_key,
    account_position.dlr_branch_key,
    account_position.day_key,
    account_position.acct_nbr,
    fund.fund_nbr,
    account_position.shrhldr_role_key,
    account_position.tot_shrs,
    account_position.tot_bal_amt,
    account_position.un_issd_shrs,
    account_position.issd_shrs,
    account_position.nav,
    account_position.rsrvd_shrs,
    account_position.accrd_divd_amt,
    account_position.escrw_shrs,
    account_position.collctd_shrs,
    account_position.collctd_bal_amt,
    account_position.curr_incm_divd,
    account_position.curr_long_term_cap_gain,
    account_position.curr_shrt_term_cap_gain,
    account_position.curr_forgn_w_holdg,
    account_position.curr_divd_w_holdg,
    account_position.curr_forgn_tax_paid_amt,
    account_position.curr_yr_1099b_gr_redmpn_amt,
    account_position.curr_yr_1099b_gr_w_holdg_amt,
    account_position.collctd_shrs_net_chg_cnt,
    account_position.tot_shrs_net_chg_cnt,
    account_position.dsgntd_w_holdg_amt,
    account_position.dsgntd_w_holdg_rt,
    account_position.divd_w_holdg_amt,
    account_position.divd_w_holdg_rt,
    account_position.shrt_term_cap_gain_w_holdg_amt,
    account_position.shrt_term_cap_gain_w_holdg_rt,
    account_position.long_term_cap_gain_w_holdg_amt,
    account_position.long_term_cap_gain_w_holdg_rt,
    account_position.partition_yr,
    account_position.acct_stat_cd,
    account_position.row_strt_dttm,
    account_position.row_stop_dttm,
    account_position.curr_row_flg,
    account_position.etl_load_cyc_key,
    account_position.src_sys_id
   FROM hdm.account_position_hist account_position,
    hdm.fund
  WHERE account_position.fund_key = fund.fund_key;

CREATE OR REPLACE VIEW hdm.vw_acct_yr
AS SELECT *
   FROM hdm.account_setup_year;

CREATE OR REPLACE VIEW hdm.vw_after_tax_calculation
AS SELECT *
   FROM hdm.after_tax_calculation;

CREATE OR REPLACE VIEW hdm.vw_after_tax_return
AS SELECT *
   FROM hdm.after_tax_return;

CREATE OR REPLACE VIEW hdm.vw_after_tax_wealth_index
AS SELECT *
   FROM hdm.after_tax_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_awd_privilege_type
AS SELECT *
   FROM hdm.awd_privilege_type;

CREATE OR REPLACE VIEW hdm.vw_awd_queue
AS SELECT *
   FROM hdm.awd_queue;

CREATE OR REPLACE VIEW hdm.vw_awd_record_type
AS SELECT *
   FROM hdm.awd_record_type;

CREATE OR REPLACE VIEW hdm.vw_awd_role
AS SELECT *
   FROM hdm.awd_role;

CREATE OR REPLACE VIEW hdm.vw_awd_role_privilege
AS SELECT *
   FROM hdm.awd_role_privilege;

CREATE OR REPLACE VIEW hdm.vw_awd_user
AS SELECT *
   FROM hdm.awd_user;

CREATE OR REPLACE VIEW hdm.vw_awd_user_queue
AS SELECT *
   FROM hdm.awd_user_queue;

CREATE OR REPLACE VIEW hdm.vw_awd_user_role
AS SELECT *
   FROM hdm.awd_user_role;

CREATE OR REPLACE VIEW hdm.vw_awd_user_work_type
AS SELECT *
   FROM hdm.awd_user_work_type;

CREATE OR REPLACE VIEW hdm.vw_awd_work_type
AS SELECT *
   FROM hdm.awd_work_type;

CREATE OR REPLACE VIEW hdm.vw_balance_category
AS SELECT *
   FROM hdm.balance_category;

CREATE OR REPLACE VIEW hdm.vw_benchmark_index
AS SELECT *
   FROM hdm.benchmark_index;

CREATE OR REPLACE VIEW hdm.vw_bluesky_transactions
AS SELECT *
   FROM hdm.bluesky_transactions;

CREATE OR REPLACE VIEW hdm.vw_broker
AS SELECT *
   FROM hdm.broker;

CREATE OR REPLACE VIEW hdm.vw_broker_position
AS SELECT *
   FROM hdm.broker_position;

CREATE OR REPLACE VIEW hdm.vw_bud_account
AS SELECT *
   FROM hdm.bud_account;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_accrual
AS SELECT *
   FROM hdm.bud_acct_accrual;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_accrual_fnd
AS SELECT *
   FROM hdm.bud_acct_accrual_fnd;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_adjust_fnd
AS SELECT *
   FROM hdm.bud_acct_adjust_fnd;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_transfer
AS SELECT *
   FROM hdm.bud_acct_transfer;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_transfer_fnd
AS SELECT *
   FROM hdm.bud_acct_transfer_fnd;

CREATE OR REPLACE VIEW hdm.vw_bud_asmpt_change
AS SELECT *
   FROM hdm.bud_asmpt_change;

CREATE OR REPLACE VIEW hdm.vw_bud_asmpt_change_fund
AS SELECT *
   FROM hdm.bud_asmpt_change_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_cat_accrual_dim
AS SELECT *
   FROM hdm.bud_cat_accrual_dim;

CREATE OR REPLACE VIEW hdm.vw_bud_cat_accrual_fact
AS SELECT *
   FROM hdm.bud_cat_accrual_fact;

CREATE OR REPLACE VIEW hdm.vw_bud_cat_acct_accrual
AS SELECT *
   FROM hdm.bud_cat_acct_accrual;

CREATE OR REPLACE VIEW hdm.vw_bud_data_source_dim
AS SELECT *
   FROM hdm.bud_data_source_dim;

CREATE OR REPLACE VIEW hdm.vw_bud_data_source_fact
AS SELECT *
   FROM hdm.bud_data_source_fact;

CREATE OR REPLACE VIEW hdm.vw_bud_data_source_fund
AS SELECT *
   FROM hdm.bud_data_source_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_expense_line_item
AS SELECT *
   FROM hdm.bud_expense_line_item;

CREATE OR REPLACE VIEW hdm.vw_bud_fund_composite
AS SELECT *
   FROM hdm.bud_fund_composite;

CREATE OR REPLACE VIEW hdm.vw_bud_fund_multiplier
AS SELECT *
   FROM hdm.bud_fund_multiplier;

CREATE OR REPLACE VIEW hdm.vw_bud_invoice
AS SELECT *
   FROM hdm.bud_invoice;

CREATE OR REPLACE VIEW hdm.vw_bud_invoice_alloc
AS SELECT *
   FROM hdm.bud_invoice_alloc;

CREATE OR REPLACE VIEW hdm.vw_bud_invoice_alloc_fund
AS SELECT *
   FROM hdm.bud_invoice_alloc_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_liability
AS SELECT *
   FROM hdm.bud_liability;

CREATE OR REPLACE VIEW hdm.vw_bud_liability_fund
AS SELECT *
   FROM hdm.bud_liability_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_lineitem_alloc
AS SELECT *
   FROM hdm.bud_lineitem_alloc;

CREATE OR REPLACE VIEW hdm.vw_bud_lineitem_alloc_fund
AS SELECT *
   FROM hdm.bud_lineitem_alloc_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_lineitem_alloc_fund_ds
AS SELECT laf.line_itm_allc_fund_key,
    laf.bud_strt_dt_key,
    laf.bud_fund_compst_key,
    laf.bud_key,
    laf.expns_line_item_key,
    laf.data_src_dim_key,
    laf.rt,
    laf.data_src_val,
    laf.allocn_amt,
    laf.allocn_per_fund_ntes,
    laf.adjmt,
    laf.rsdl_val,
    laf.orig_allocn,
    laf.reallocd_remng,
    laf.fytd_expns,
    laf.assmpn_chg,
    laf.to_be_reallocd,
    laf.src_bud_key,
    laf.pmt_difftl,
    laf.allocn_ntes,
    laf.insr_usr,
    laf.insr_ts,
    laf.vrsn,
    laf.curr_row_flg,
    laf.row_strt_dttm,
    laf.row_stop_dttm,
    laf.etl_load_cyc_key,
    laf.src_sys_id,
    dsf.avg_mthly_tna AS avg_mthly_tna_ds_fund,
    dsf.issr_pct AS issr_pct_ds_fund,
    dsf.rt AS rt_ds_fund,
    dsf.reqs_fee_flg AS reqs_fee_flg_ds_fund,
    dsf.src_val AS src_val_ds_fund,
    dsf.tot_fee AS tot_fee_ds_fund,
    dsf.otpt_val AS otpt_val_ds_fund,
    dsf.adjmt AS adjmt_ds_fund,
    dsf.data_src_fund_nte AS data_src_fund_nte_ds_fund,
    dsf.sby_multr AS sby_multr_ds_fund,
    dsf.hbr_nbr_accts AS hbr_nbr_accts_ds_fund,
    dsf.broadridge_nbr_accts AS broadridge_nbr_accts_ds_fund,
    dsf.reinvs AS reinvs_ds_fund,
    dsf.bllbl_act AS bllbl_act_ds_fund,
    dsf.capstk_adjmts AS capstk_adjmts_ds_fund,
    dsf.tna_val AS tna_val_ds_fund,
    dsf.rsdl_val AS rsdl_val_ds_fund,
    dsf.subs AS subs_ds_fund,
    dsf.redmpns AS redmpns_ds_fund,
    dsf.net_sales_act AS net_sales_act_ds_fund,
    dsf.annl_net_sales_act AS annl_net_sales_act_ds_fund,
    dsf.subs_including_adjmts AS subs_including_adjmts_ds_fund,
    dsf.avg_daily_tna AS avg_daily_tna_ds_fund,
    dsf.adj_avg_daily_tna AS adj_avg_daily_tna_ds_fund,
    dsf.allocn_adjmt AS allocn_adjmt_ds_fund,
    dsf.orig_allocn AS orig_allocn_ds_fund,
    dsf.greatest_commn_denomr AS greatest_commn_denomr_ds_fund,
    dsf.tot_avg_daily_tna AS tot_avg_daily_tna_ds_fund,
    dsf.tot_compnsn AS tot_compnsn_ds_fund,
    dsf.vrsn AS vrsn_ds_fund
   FROM hdm.vw_bud_lineitem_alloc_fund laf
     LEFT JOIN hdm.vw_bud_data_source_fund dsf ON laf.data_src_dim_key = dsf.data_src_dim_key AND laf.bud_fund_compst_key = dsf.bud_fund_compst_key;

CREATE OR REPLACE VIEW hdm.vw_bud_pmt_diff
AS SELECT *
   FROM hdm.bud_pmt_diff;

CREATE OR REPLACE VIEW hdm.vw_bud_pmt_diff_fnd_invc
AS SELECT *
   FROM hdm.bud_pmt_diff_fnd_invc;

CREATE OR REPLACE VIEW hdm.vw_bud_pmt_diff_fund
AS SELECT *
   FROM hdm.bud_pmt_diff_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_trustee_election
AS SELECT *
   FROM hdm.bud_trustee_election;

CREATE OR REPLACE VIEW hdm.vw_budget
AS SELECT *
   FROM hdm.budget;

CREATE OR REPLACE VIEW hdm.vw_business_line
AS SELECT *
   FROM hdm.business_line;

CREATE OR REPLACE VIEW hdm.vw_calendar
AS SELECT *
   FROM hdm.calendar;

CREATE OR REPLACE VIEW hdm.vw_cash_holding
AS SELECT *
   FROM hdm.cash_holding;

CREATE OR REPLACE VIEW hdm.vw_cash_reason
AS SELECT *
   FROM hdm.cash_reason;

CREATE OR REPLACE VIEW hdm.vw_cashflow
AS SELECT cashflow_history.cashflow_hist_key AS cashflow_key,
    cashflow_history.reg_key,
    cashflow_history.dlr_key,
    cashflow_history.hbr_dlr_key,
    cashflow_history.fund_key,
    cashflow_history.acct_key,
    cashflow_history.pmt_mthd_key,
    cashflow_history.trde_dt_key,
    cashflow_history.trde_dt_key AS spr_sheet_dt_key,
    cashflow_history.shrhldr_key,
    cashflow_history.confirm_dt_key,
    cashflow_history.shrhldr_key AS shrhldr_role_key,
    cashflow_history.acct_nbr,
    fund.fund_nbr,
    cashflow_history.sub_shrs,
    cashflow_history.sub_amt,
    cashflow_history.redmpn_shrs,
    cashflow_history.redmpn_amt,
    cashflow_history.ofrg_pr,
    cashflow_history.nav,
    cashflow_history.dlr_commsn_amt,
    cashflow_history.undr_wrtr_commsn_amt,
    cashflow_history.adv_commsn_amt,
    cashflow_history.net_cashflow_amt,
    cashflow_history.net_shrs,
    cashflow_history.row_strt_dttm,
    cashflow_history.row_stop_dttm,
    cashflow_history.curr_row_flg,
    cashflow_history.src_sys_id,
    cashflow_history.etl_load_cyc_key,
    'Settled'::text AS trans_stat
   FROM hdm.cashflow_history,
    hdm.fund
  WHERE cashflow_history.fund_key = fund.fund_key
UNION ALL
 SELECT cash.sdcm_cashflow_key AS cashflow_key,
    cash.reg_key,
    cash.dlr_key,
    cash.hbr_dlr_key,
    cash.fund_key,
    cash.acct_key,
    cash.pmt_mthd_key,
    cash.trde_dt_key,
    cash.spr_sheet_dt_key,
    cash.shrhldr_key,
    cash.confirm_dt_key,
    cash.shrhldr_role_key,
    cash.acct_nbr,
    fund.fund_nbr,
    cash.sub_shrs,
    cash.sub_amt,
    cash.redmpn_shrs,
    cash.redmpn_amt,
    cash.ofrg_pr,
    cash.nav,
    cash.dlr_commsn_amt,
    cash.undr_wrtr_commsn_amt,
    cash.adv_commsn_amt,
    cash.net_cashflow_amt,
    cash.net_shrs,
    cash.row_strt_dttm,
    cash.row_stop_dttm,
    cash.curr_row_flg,
    cash.src_sys_id,
    cash.etl_load_cyc_key,
        CASE
            WHEN cash.cash_cntl_recon_flg::text = 'Y'::text THEN 'Settled'::text
            ELSE 'Pending'::text
        END AS trans_stat
   FROM hdm.sdcm_cashflow cash,
    hdm.fund
  WHERE cash.fund_key = fund.fund_key;

CREATE OR REPLACE VIEW hdm.vw_cashflow_activity
AS SELECT *
   FROM hdm.cashflow_activity;

CREATE OR REPLACE VIEW hdm.vw_cashflow_history
AS SELECT *
   FROM hdm.cashflow_history;

CREATE OR REPLACE VIEW hdm.vw_cct_city_tax_list
AS SELECT *
   FROM hdm.cct_city_tax_list;

CREATE OR REPLACE VIEW hdm.vw_cct_harbor_group
AS SELECT *
   FROM hdm.cct_harbor_group;

CREATE OR REPLACE VIEW hdm.vw_cct_spend_category
AS SELECT *
   FROM hdm.cct_spend_category;

CREATE OR REPLACE VIEW hdm.vw_cct_supplier
AS SELECT *
   FROM hdm.cct_supplier;

CREATE OR REPLACE VIEW hdm.vw_cct_tax_detail
AS SELECT *
   FROM hdm.cct_tax_detail;

CREATE OR REPLACE VIEW hdm.vw_cct_tax_summary
AS SELECT *
   FROM hdm.cct_tax_summary;

CREATE OR REPLACE VIEW hdm.vw_cost_type
AS SELECT *
   FROM hdm.cost_type;

CREATE OR REPLACE VIEW hdm.vw_country
AS SELECT *
   FROM hdm.country;

CREATE OR REPLACE VIEW hdm.vw_country_threshold
AS SELECT *
   FROM hdm.country_threshold;

CREATE OR REPLACE VIEW hdm.vw_cube_acct_stat
AS SELECT *
   FROM hdm.account_statistics;

CREATE OR REPLACE VIEW hdm.vw_currency
AS SELECT *
   FROM hdm.currency;

CREATE OR REPLACE VIEW hdm.vw_current_account
AS SELECT *
   FROM hdm.account
  WHERE account.curr_row_flg::text = 'Y'::text;

CREATE OR REPLACE VIEW hdm.vw_current_shrhldr
AS SELECT*
   FROM hdm.shareholder
  WHERE shareholder.curr_row_flg::text = 'Y'::text;

CREATE OR REPLACE VIEW hdm.vw_daily_avg_acct_position
AS SELECT *
   FROM hdm.daily_avg_acct_position;

CREATE OR REPLACE VIEW hdm.vw_dealer
AS SELECT *
   FROM hdm.dealer;

CREATE OR REPLACE VIEW hdm.vw_dealer_asset_summary
AS SELECT *
   FROM hdm.dealer_asset_summary;

CREATE OR REPLACE VIEW hdm.vw_dealer_branch
AS SELECT *
   FROM hdm.dealer_branch;

CREATE OR REPLACE VIEW hdm.vw_dealer_change_control
AS SELECT d*
   FROM hdm.dealer_change_control;

CREATE OR REPLACE VIEW hdm.vw_dealer_change_list
AS SELECT dealer_change_list.dlr_chg_list_key,
    dealer_change_list.day_key,
    dealer_change_list.fund_key,
    dealer_change_list.acct_key,
    dealer_change_list.dlr_chg_type,
    dealer_change_list.acct_maint_seq_id,
    dealer_change_list.dlr_orig_val,
    dealer_change_list.dlr_new_val,
    dealer_change_list.batch_nbr,
    dealer_change_list.curr_row_flg,
    dealer_change_list.row_strt_dttm,
    dealer_change_list.row_stop_dttm,
    dealer_change_list.row_updt_dttm,
    dealer_change_list.etl_load_cyc_key,
    dealer_change_list.src_sys_id
   FROM hdm.dealer_change_list;

CREATE OR REPLACE VIEW hdm.vw_dealer_fee_analysis
AS SELECT *
   FROM hdm.dealer_fee_analysis;

CREATE OR REPLACE VIEW hdm.vw_dealer_fee_research
AS SELECT *
   FROM hdm.dealer_fee_research;

CREATE OR REPLACE VIEW hdm.vw_dealer_fee_schedule
AS SELECT dealer_fee_schedule.dlr_fee_sched_key,
    dealer_fee_schedule.dlr_key,
    dealer_fee_schedule.fund_key,
    dealer_fee_schedule.dlr_fee_type_key,
    dealer_fee_schedule.day_key,
    dealer_fee_schedule.fee_rt,
    dealer_fee_schedule.bus_line,
    dealer_fee_schedule.invc_fee_desc,
    dealer_fee_schedule.cmmts,
    dealer_fee_schedule.efftv_strt_dt,
    dealer_fee_schedule.efftv_end_dt,
    dealer_fee_schedule.curr_row_flg,
    dealer_fee_schedule.row_strt_dttm,
    dealer_fee_schedule.row_stop_dttm,
    dealer_fee_schedule.etl_load_cyc_key,
    dealer_fee_schedule.src_sys_id
   FROM hdm.dealer_fee_schedule;

CREATE OR REPLACE VIEW hdm.vw_dealer_fee_type
AS SELECT *
   FROM hdm.dealer_fee_type;

CREATE OR REPLACE VIEW hdm.vw_dealer_invoice
AS SELECT *
   FROM hdm.dealer_invoice;

CREATE OR REPLACE VIEW hdm.vw_dealer_invoice_model
AS SELECT *
   FROM hdm.dealer_invoice_model;

CREATE OR REPLACE VIEW hdm.vw_dealer_invoice_mtf_model
AS SELECT dealer_invoice_model.dlr_fee_type_key,
    dealer_invoice_model.dlr_key,
    dealer_invoice_model.invc_day_key,
    dealer_invoice_model.fund_key,
    dealer_invoice_model.bus_line,
    dealer_invoice_model.invc_avg_asset,
    dealer_invoice_model.tot_fee_amt,
    dealer_invoice_model.invc_freq_flg
   FROM hdm.dealer_invoice_model
UNION ALL
 SELECT mtf.dlr_fee_type_key,
    mtf.dlr_key,
    mtf.day_key AS invc_day_key,
    mtf.fund_key,
    'Calculated by Harbor'::character varying AS bus_line,
    mtf.eligbl_avg_asset AS invc_avg_asset,
    mtf.tot_compnsn AS tot_fee_amt,
    'M'::character varying AS invc_freq_flg
   FROM hdm.dealer_payment_summary mtf
  WHERE (mtf.dlr_key IN ( SELECT dealer_invoice_rule.dlr_key
           FROM hdm.dealer_invoice_rule
          GROUP BY dealer_invoice_rule.dlr_key));

CREATE OR REPLACE VIEW hdm.vw_dealer_invoice_trial
AS SELECT *
   FROM hdm.dealer_invoice_trial;

CREATE OR REPLACE VIEW hdm.vw_dealer_level_control
AS SELECT *
   FROM hdm.dealer_level_control;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_deail
AS SELECT *
   FROM hdm.dealer_payment_detail;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_detail
AS SELECT *
   FROM hdm.dealer_payment_detail;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_method
AS SELECT *
   FROM hdm.dealer_payment_method;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_summary
AS SELECT *
   FROM hdm.dealer_payment_summary;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_trial
AS SELECT *
   FROM hdm.dealer_payment_trial;

CREATE OR REPLACE VIEW hdm.vw_dealer_payout_type
AS SELECT *
   FROM hdm.dealer_payout_type;

CREATE OR REPLACE VIEW hdm.vw_distribution_code
AS SELECT *
   FROM hdm.distribution_code;

CREATE OR REPLACE VIEW hdm.vw_distribution_type
AS SELECT *
   FROM hdm.distribution_type;

CREATE OR REPLACE VIEW hdm.vw_dividend_tax_rate
AS SELECT *
   FROM hdm.dividend_tax_rate;

CREATE OR REPLACE VIEW hdm.vw_eligible_asset_list
AS SELECT *
   FROM hdm.eligible_asset_list;

CREATE OR REPLACE VIEW hdm.vw_exchange_type
AS SELECT *
   FROM hdm.exchange_type;

CREATE OR REPLACE VIEW hdm.vw_expense_ratio_history
AS SELECT *
   FROM hdm.expense_ratio_history;

CREATE OR REPLACE VIEW hdm.vw_external_fund
AS SELECT *
   FROM hdm.fid_external_fund;

CREATE OR REPLACE VIEW hdm.vw_external_fund_balance
AS SELECT *
   FROM hdm.external_fund_balance;

CREATE OR REPLACE VIEW hdm.vw_external_fund_lis
AS SELECT *
   FROM hdm.external_fund_list;

CREATE OR REPLACE VIEW hdm.vw_external_fund_list
AS SELECT *
   FROM hdm.external_fund_list;

CREATE OR REPLACE VIEW hdm.vw_facility
AS SELECT *
   FROM hdm.facility;

CREATE OR REPLACE VIEW hdm.vw_fee_schedule
AS SELECT*
   FROM hdm.fee_schedule;

CREATE OR REPLACE VIEW hdm.vw_fee_schedule_view
AS SELECT fs.dlr_key,
    fs.fund_key,
    fs.bus_line,
        CASE
            WHEN ft.dlr_fee_type_cd::text = ANY (ARRAY['MF'::character varying::text, 'PAF'::character varying::text]) THEN ''::character varying
            ELSE fs.invc_fee_desc
        END AS invc_fee_desc,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = '12B1'::text THEN fs.fee_rt::double precision
            ELSE NULL::double precision
        END) AS fee_12b1,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = 'SPC1'::text THEN fs.fee_rt::double precision
            ELSE NULL::double precision
        END) AS fee_spc1,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = ANY (ARRAY['SPC2'::character varying::text, 'SPC3'::character varying::text]) THEN fs.fee_rt::double precision
            ELSE NULL::double precision
        END) AS fee_inst,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = 'MF'::text THEN fs.fee_rt::double precision
            ELSE NULL::double precision
        END) AS fee_mf,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = 'PAF'::text THEN fs.fee_rt::double precision
            ELSE NULL::double precision
        END) AS fee_paf
   FROM hdm.dealer_fee_schedule fs
     JOIN hdm.dealer_fee_type ft ON fs.dlr_fee_type_key = ft.dlr_fee_type_key
  GROUP BY fs.dlr_key, fs.fund_key, fs.bus_line, (
        CASE
            WHEN ft.dlr_fee_type_cd::text = ANY (ARRAY['MF'::character varying::text, 'PAF'::character varying::text]) THEN ''::character varying
            ELSE fs.invc_fee_desc
        END);

CREATE OR REPLACE VIEW hdm.vw_fi_characteristic_list
AS SELECT *
   FROM hdm.fi_characteristic_list;

CREATE OR REPLACE VIEW hdm.vw_fi_characteristics
AS SELECT *
   FROM hdm.fi_characteristics;

CREATE OR REPLACE VIEW hdm.vw_fi_composition_map
AS SELECT *
   FROM hdm.fi_composition_map;

CREATE OR REPLACE VIEW hdm.vw_fi_country
AS SELECT *
   FROM hdm.fi_country;

CREATE OR REPLACE VIEW hdm.vw_fi_country_allocation
AS SELECT *
   FROM hdm.fi_country_allocation;

CREATE OR REPLACE VIEW hdm.vw_fi_credit_rating
AS SELECT *
   FROM hdm.fi_credit_rating;

CREATE OR REPLACE VIEW hdm.vw_fi_credit_rating_list
AS SELECT *
   FROM hdm.fi_credit_rating_list;

CREATE OR REPLACE VIEW hdm.vw_fi_duration_maturity
AS SELECT *
   FROM hdm.fi_duration_maturity;

CREATE OR REPLACE VIEW hdm.vw_fi_issuer
AS SELECT *
   FROM hdm.fi_issuer;

CREATE OR REPLACE VIEW hdm.vw_fi_sector
AS SELECT *
   FROM hdm.fi_sector;

CREATE OR REPLACE VIEW hdm.vw_fi_sector_allocation
AS SELECT *
   FROM hdm.fi_sector_allocation;

CREATE OR REPLACE VIEW hdm.vw_fi_time_period
AS SELECT *
   FROM hdm.fi_time_period;

CREATE OR REPLACE VIEW hdm.vw_fi_time_period_mapping
AS SELECT *
   FROM hdm.fi_time_period_mapping;

CREATE OR REPLACE VIEW hdm.vw_fi_top_issuers
AS SELECT *
   FROM hdm.fi_top_issuers;

CREATE OR REPLACE VIEW hdm.vw_fid_benchmark_performance
AS SELECT *
   FROM hdm.fid_benchmark_performance;

CREATE OR REPLACE VIEW hdm.vw_fid_benchmark_position
AS SELECT *
   FROM hdm.fid_benchmark_position;

CREATE OR REPLACE VIEW hdm.vw_fid_benchmark_weight
AS SELECT *
   FROM hdm.fid_benchmark_weight;

CREATE OR REPLACE VIEW hdm.vw_fid_bm_level_performance
AS SELECT *
   FROM hdm.fid_bm_level_performance;

CREATE OR REPLACE VIEW hdm.vw_fid_bm_level_wealth_index
AS SELECT *
   FROM hdm.fid_bm_level_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_fid_bm_wealth_index
AS SELECT *
   FROM hdm.fid_bm_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_fid_client_asset_mix
AS SELECT *
   FROM hdm.fid_client_asset_mix;

CREATE OR REPLACE VIEW hdm.vw_fid_external_benchmark
AS SELECT *
   FROM hdm.fid_external_benchmark;

CREATE OR REPLACE VIEW hdm.vw_fid_external_fund
AS SELECT *
   FROM hdm.fid_external_fund;

CREATE OR REPLACE VIEW hdm.vw_fid_fund_index_rltn
AS SELECT f.fund_key,
    br.fund_compst_key,
    br.bmk_idx_key
   FROM hdm.fund_index_rltn br,
    hdm.vw_fund f,
    hdm.vw_fund_composite c
  WHERE br.fund_compst_key = c.fund_compst_key AND f.fund_compst_key = c.fund_compst_key AND br.prim_bmk_flg::text = 'Y'::text AND br.perf_run_type_key::numeric = 1::numeric
UNION
 SELECT 1000001 AS fund_key,
    NULL::numeric AS fund_compst_key,
    10000001 AS bmk_idx_key
UNION
 SELECT 1000002 AS fund_key,
    NULL::numeric AS fund_compst_key,
    ( SELECT benchmark_index.bmk_idx_key
           FROM hdm.benchmark_index
          WHERE benchmark_index.bloomberg_sym::text = 'SPTR'::text) AS bmk_idx_key;

CREATE OR REPLACE VIEW hdm.vw_fid_fund_level_performance

AS SELECT *
   FROM hdm.fid_fund_level_performance;

CREATE OR REPLACE VIEW hdm.vw_fid_fund_level_wealth_index
AS SELECT *
   FROM hdm.fid_fund_level_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_fid_fund_performance
AS SELECT *
   FROM hdm.fid_fund_performance;

CREATE OR REPLACE VIEW hdm.vw_fid_fund_wealth_index
AS SELECT *
   FROM hdm.fid_fund_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_fid_letter_recipient
AS SELECT *
   FROM hdm.fid_letter_recipient;

CREATE OR REPLACE VIEW hdm.vw_fid_wealth_index_temp
AS SELECT *
   FROM hdm.fid_wealth_index_temp;

CREATE OR REPLACE VIEW hdm.vw_fiduciary_asset_class
AS SELECT *
   FROM hdm.fiduciary_asset_class;

CREATE OR REPLACE VIEW hdm.vw_fiduciary_fund
AS SELECT f.fund_key,
    f.fund_nm,
    f.fund_nbr,
    f.shr_cls_desc,
    c.asset_cls_nm,
    f.fund_sort_ord
   FROM hdm.vw_fund f,
    hdm.vw_fund_composite c
  WHERE f.fund_compst_key = c.fund_compst_key
UNION
 SELECT fid_external_fund.ext_fund_key AS fund_key,
    fid_external_fund.ext_fund_nm AS fund_nm,
    NULL::double precision AS fund_nbr,
    NULL::character varying AS shr_cls_desc,
    fid_external_fund.asset_cls_nm,
    99999 AS fund_sort_ord
   FROM hdm.fid_external_fund;

CREATE OR REPLACE VIEW hdm.vw_fiduciary_asset_link
AS SELECT ac.fid_asset_cls_key,
    ff.fund_key
   FROM hdm.fiduciary_asset_class ac,
    hdm.vw_fiduciary_fund ff
  WHERE ac.fid_asset_cls_desc::text = 'Domestic Equity Portfolio'::text AND ff.asset_cls_nm::text = 'Domestic'::text
UNION
 SELECT ac.fid_asset_cls_key,
    ff.fund_key
   FROM hdm.fiduciary_asset_class ac,
    hdm.vw_fiduciary_fund ff
  WHERE ac.fid_asset_cls_desc::text = 'International Equity Portfolio'::text AND ff.asset_cls_nm::text = 'International and Global'::text
UNION
 SELECT ac.fid_asset_cls_key,
    ff.fund_key
   FROM hdm.fiduciary_asset_class ac,
    hdm.vw_fiduciary_fund ff
  WHERE ac.fid_asset_cls_desc::text = 'Fixed Income Portfolio'::text AND ff.asset_cls_nm::text = 'Fixed Income'::text
UNION
 SELECT ac.fid_asset_cls_key,
    ff.fund_key
   FROM hdm.fiduciary_asset_class ac,
    hdm.vw_fiduciary_fund ff
  WHERE ac.fid_asset_cls_desc::text = 'Total Equity Portfolio'::text AND (ff.asset_cls_nm::text = ANY (ARRAY['Domestic'::character varying::text, 'International and Global'::character varying::text]))
UNION
 SELECT ac.fid_asset_cls_key,
    ff.fund_key
   FROM hdm.fiduciary_asset_class ac,
    hdm.vw_fiduciary_fund ff
  WHERE ac.fid_asset_cls_desc::text = 'Total Portfolio'::text;

CREATE OR REPLACE VIEW hdm.vw_fiduciary_benchmark_index
AS SELECT benchmark_index.bmk_idx_key AS ext_bmk_idx_key,
    benchmark_index.idx_nm
   FROM hdm.benchmark_index
UNION
 SELECT fid_external_benchmark.ext_bmk_key AS ext_bmk_idx_key,
    fid_external_benchmark.idx_nm
   FROM hdm.fid_external_benchmark;

CREATE OR REPLACE VIEW hdm.vw_fiduciary_customer
AS SELECT *
   FROM hdm.fiduciary_customer;

CREATE OR REPLACE VIEW hdm.vw_fiduciary_position
AS SELECT *
   FROM hdm.fiduciary_position;

CREATE OR REPLACE VIEW hdm.vw_fund
AS SELECT *
   FROM hdm.fund;

CREATE OR REPLACE VIEW hdm.vw_fund_annual_investment
AS SELECT *
   FROM hdm.fund_annual_investment;

CREATE OR REPLACE VIEW hdm.vw_fund_benchmark_return_union
AS SELECT 'Fund'::text AS record_type,
    ret.fund_ret_key AS return_key,
    ret.per_key,
    rp.per_nm,
    rp.per_sort_ord,
    0 AS bmk_idx_key,
    fnd.fund_nm AS instrument,
    fc.fund_compst_key,
    fnd.shr_cls_desc,
    fc.compst_nm,
    fc.compst_ord,
    ret.day_key,
    cal.cal_day,
    ret.fund_ret AS official_return,
    ret.asof_fund_ret AS asof_return,
    ret.strt_per,
    ret.annlzn_fctr,
    ret.ret_per_flg,
    fnd.fund_sort_ord AS perf_display_order,
    'Y'::character varying AS gen_rptg_flg,
    'Y'::character varying AS daily_rptg_flg,
    'Y'::character varying AS trgt_rptg_flg,
    ret.ret_vrfyn_flg
   FROM hdm.fund_return ret
     JOIN hdm.calendar cal ON ret.day_key = cal.day_key
     JOIN hdm.return_periods rp ON ret.per_key = rp.per_key
     JOIN hdm.fund fnd ON ret.fund_key = fnd.fund_key
     JOIN hdm.fund_composite fc ON fnd.fund_compst_key = fc.fund_compst_key
UNION ALL
 SELECT 'Benchmark'::text AS record_type,
    iret.idx_ret_key AS return_key,
    iret.per_key,
    irp.per_nm,
    irp.per_sort_ord,
    iret.bmk_idx_key,
    idx.idx_nm AS instrument,
    iret.fund_compst_key,
    ifnd.shr_cls_desc,
    ifc.compst_nm,
    ifc.compst_ord,
    iret.day_key,
    ical.cal_day,
    iret.idx_ret AS official_return,
    iret.idx_ret AS asof_return,
    iret.strt_per,
    iret.annlzn_fctr,
    iret.ret_per_flg,
    iret.perf_displ_ord AS perf_display_order,
    iret.gen_rptg_flg,
    iret.daily_rptg_flg,
    iret.trgt_rptg_flg,
    'Y'::character varying AS ret_vrfyn_flg
   FROM hdm.index_return iret
     JOIN hdm.calendar ical ON iret.day_key = ical.day_key
     JOIN hdm.benchmark_index idx ON iret.bmk_idx_key = idx.bmk_idx_key
     JOIN hdm.return_periods irp ON iret.per_key = irp.per_key
     JOIN hdm.fund_composite ifc ON iret.fund_compst_key = ifc.fund_compst_key
     JOIN hdm.fund ifnd ON ifc.fund_compst_key = ifnd.fund_compst_key
     JOIN hdm.fund_return fret ON iret.day_key = fret.day_key AND iret.per_key = fret.per_key AND ifnd.fund_key = fret.fund_key
  WHERE ifnd.shr_cls_desc::text = 'Institutional Class'::text;

CREATE OR REPLACE VIEW hdm.vw_fund_composite
AS SELECT *
   FROM hdm.fund_composite;

CREATE OR REPLACE VIEW hdm.vw_fund_dividend
AS SELECT *
   FROM hdm.fund_dividend;

CREATE OR REPLACE VIEW hdm.vw_fund_expense_ratio
AS SELECT *
   FROM hdm.fund_expense_ratio;

CREATE OR REPLACE VIEW hdm.vw_fund_holdings
AS SELECT *
   FROM hdm.fund_holdings;

CREATE OR REPLACE VIEW hdm.vw_fund_index_rltn
AS SELECT *
   FROM hdm.fund_index_rltn;

CREATE OR REPLACE VIEW hdm.vw_fund_return
AS SELECT *
   FROM hdm.fund_return;

CREATE OR REPLACE VIEW hdm.vw_fund_trans_hist
AS SELECT fund_trans_hist.txn_hist_key,
    fund_trans_hist.txn_type_key,
    fund_trans_hist.reg_key,
    fund_trans_hist.reg_of_sale_key,
    fund_trans_hist.dlr_key,
    fund_trans_hist.tpa_dlr_key,
    fund_trans_hist.shrhldr_key,
    fund_trans_hist.acct_key,
    fund_trans_hist.fund_key,
    fund_trans_hist.efftv_day_key,
    fund_trans_hist.sttl_day_key,
    fund_trans_hist.pmt_day_key,
    fund_trans_hist.batch_day_key,
    fund_trans_hist.dlr_branch_key,
    fund_trans_hist.trde_orig_key,
    fund_trans_hist.rsn_key,
    fund_trans_hist.ssn_stat_key,
    fund_trans_hist.trde_entry_mthd_key,
    fund_trans_hist.dlr_lvl_cntl_key,
    fund_trans_hist.pmt_mthd_key,
    fund_trans_hist.shrhldr_role_key,
    fund_trans_hist.acct_nbr,
    fund.fund_nbr,
    fund_trans_hist.fund_txn_id,
    fund_trans_hist.fund_txn_id_2,
    fund_trans_hist.ltr_of_intend_nbr,
    fund_trans_hist.batch_nbr,
    fund_trans_hist.discnt_cat,
    fund_trans_hist.fund_ord_nbr,
    fund_trans_hist.nav_cd,
    fund_trans_hist.acct_pr_sched,
    fund_trans_hist.undr_wrtr_commsn_cd,
    fund_trans_hist.cdsc_wavr_rsn_cd,
    fund_trans_hist.nscc_shrt_term_trader_wavr_rsn,
    fund_trans_hist.contrbn_tax_yr,
    fund_trans_hist.shrs,
    fund_trans_hist.shr_pr,
    fund_trans_hist.gr_amt,
    fund_trans_hist.sales_pct,
    fund_trans_hist.dlr_commsn_amt,
    fund_trans_hist.undr_wrtr_commsn_amt,
    fund_trans_hist.net_amt,
    fund_trans_hist.bluesky_txn_flg,
    fund_trans_hist.row_strt_dttm,
    fund_trans_hist.row_stop_dttm,
    fund_trans_hist.curr_row_flg,
    fund_trans_hist.src_sys_id,
    fund_trans_hist.etl_load_cyc_key,
    fund_trans_hist.txn_desc,
    fund_trans_hist.shr_bal_effct_flg,
    fund_trans_hist.bluesky_reopen_acct_flg,
    fund_trans_hist.frto_fund_key,
    fund_trans_hist.spr_sheet_day_key,
    fund_trans_hist.opr_id
   FROM hdm.fund_trans_hist,
    hdm.fund
  WHERE fund_trans_hist.fund_key = fund.fund_key;

CREATE OR REPLACE VIEW hdm.vw_fund_valuation
AS SELECT *
   FROM hdm.fund_valuation;

CREATE OR REPLACE VIEW hdm.vw_fund_yield
AS SELECT fund_yield.fund_yld_key,
    fund_yield.fund_key,
    fund_yield.day_key,
    fund_yield.yld_type_key,
    fund_yield.fund_yld,
    lag(fund_yield.fund_yld) OVER (PARTITION BY fund_yield.fund_key, fund_yield.yld_type_key ORDER BY calendar.cal_day) AS prev_day_fund_yld,
    fund_yield.curr_row_flg,
    fund_yield.row_strt_dttm,
    fund_yield.row_stop_dttm,
    fund_yield.etl_load_cyc_key,
    fund_yield.src_sys_id
   FROM hdm.fund_yield,
    hdm.calendar
  WHERE fund_yield.day_key = calendar.day_key;

CREATE OR REPLACE VIEW hdm.vw_gender
AS SELECT gender.gend_key,
    gender.gend_nm,
    gender.gend_cd
   FROM hdm.gender;

CREATE OR REPLACE VIEW hdm.vw_gross_return
AS SELECT *
   FROM hdm.gross_return;

CREATE OR REPLACE VIEW hdm.vw_gross_wealth_index
AS SELECT *
   FROM hdm.gross_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_group_trust_levels
AS SELECT *
   FROM hdm.group_trust_levels;

CREATE OR REPLACE VIEW hdm.vw_harbor_fund
AS SELECT fund.fund_key,
    fund.fund_long_desc,
    fund.fund_nbr,
    fund.fund_incpn_dt,
    fund.fund_clos_dt,
    fund.fund_abrv,
    fund.cusip_id,
    fund.fund_stat_desc,
    fund.quot_sym,
    fund.st_str_fund_nbr,
    fund.shr_cls_desc,
    fund.fund_sort_ord,
    max(cmp.compst_ord) OVER (PARTITION BY cmp.asset_cls_desc) AS cls_ord,
        CASE
            WHEN cmp.asset_cls_desc::text = 'Target Retirement Funds'::text THEN 'Target Fund'::text
            ELSE 'Non Target Fund'::text
        END AS target_fund,
    cmp.fund_compst_key,
    cmp.compst_nm,
    cmp.compst_stat_desc,
    cmp.compst_ord,
    cmp.styl_nm,
    cmp.asset_cls_desc,
    cmp.all_compst_desc
   FROM hdm.fund,
    hdm.fund_composite cmp
  WHERE fund.fund_compst_key = cmp.fund_compst_key
  ORDER BY fund.fund_sort_ord;

CREATE OR REPLACE VIEW hdm.vw_holdings_foreign_ownership
AS SELECT *
   FROM hdm.holdings_foreign_ownership;

CREATE OR REPLACE VIEW hdm.vw_holdings_open_position
AS SELECT *
   FROM hdm.holdings_open_position;

CREATE OR REPLACE VIEW hdm.vw_holdings_trade_hist
AS SELECT *
   FROM hdm.holdings_trade_hist;

CREATE OR REPLACE VIEW hdm.vw_holdings_txn_codes
AS SELECT *
   FROM hdm.holdings_txn_codes;

CREATE OR REPLACE VIEW hdm.vw_index_return
AS SELECT *
   FROM hdm.index_return;

CREATE OR REPLACE VIEW hdm.vw_index_valuation
AS SELECT *
   FROM hdm.index_valuation;

CREATE OR REPLACE VIEW hdm.vw_industry
AS SELECT *
   FROM hdm.industry;

CREATE OR REPLACE VIEW hdm.vw_intermediary_issue_type
AS SELECT *
   FROM hdm.intermediary_issue_type;

CREATE OR REPLACE VIEW hdm.vw_intermediary_issues
AS SELECT *
   FROM hdm.intermediary_issues;

CREATE OR REPLACE VIEW hdm.vw_invoice_tracking
AS SELECT *
   FROM hdm.invoice_tracking;

CREATE OR REPLACE VIEW hdm.vw_invoice_tracking_summary
AS SELECT *
   FROM hdm.invoice_tracking_summary;

CREATE OR REPLACE VIEW hdm.vw_manager_final_score
AS SELECT *
   FROM hdm.manager_final_score;

CREATE OR REPLACE VIEW hdm.vw_manager_information
AS SELECT *
   FROM hdm.manager_information;

CREATE OR REPLACE VIEW hdm.vw_manager_list
AS SELECT *
   FROM hdm.manager_list;

CREATE OR REPLACE VIEW hdm.vw_manager_rank_data
AS SELECT *
   FROM hdm.manager_rank_data;

CREATE OR REPLACE VIEW hdm.vw_mdd_cashflow
AS SELECT *
   FROM hdm.mdd_cashflow;

CREATE OR REPLACE VIEW hdm.vw_measure_category
AS SELECT *
   FROM hdm.measure_category;

CREATE OR REPLACE VIEW hdm.vw_measure_list
AS SELECT *
   FROM hdm.measure_list;

CREATE OR REPLACE VIEW hdm.vw_money_market_activity
AS SELECT *
   FROM hdm.money_market_activity;

CREATE OR REPLACE VIEW hdm.vw_monthly_avg_position
AS SELECT *
   FROM hdm.monthly_avg_position;

CREATE OR REPLACE VIEW hdm.vw_monthly_avg_posn_draft
AS SELECT *
   FROM hdm.monthly_avg_posn_draft;

CREATE OR REPLACE VIEW hdm.vw_operator
AS SELECT *
   FROM hdm.operator;

CREATE OR REPLACE VIEW hdm.vw_operator_function
AS SELECT *
   FROM hdm.operator_function;

CREATE OR REPLACE VIEW hdm.vw_payment_method
AS SELECT *
   FROM hdm.payment_method;

CREATE OR REPLACE VIEW hdm.vw_pending_all_trade
AS SELECT *
   FROM hdm.pending_all_trade;

CREATE OR REPLACE VIEW hdm.vw_pending_trade
AS SELECT *
   FROM hdm.pending_trade;

CREATE OR REPLACE VIEW hdm.vw_perf_run_type
AS SELECT *
   FROM hdm.perf_run_type;

CREATE OR REPLACE VIEW hdm.vw_pimco_account_reference
AS SELECT *
   FROM hdm.pimco_account_reference;

CREATE OR REPLACE VIEW hdm.vw_pimco_country_breakout
AS SELECT *
   FROM hdm.pimco_country_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_derivative_bucket
AS SELECT *
   FROM hdm.pimco_derivative_bucket;

CREATE OR REPLACE VIEW hdm.vw_pimco_derivatives
AS SELECT *
   FROM hdm.pimco_derivatives;

CREATE OR REPLACE VIEW hdm.vw_pimco_duration_breakout
AS SELECT *
   FROM hdm.pimco_duration_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_duration_bucket
AS SELECT *
   FROM hdm.pimco_duration_bucket;

CREATE OR REPLACE VIEW hdm.vw_pimco_maturity_breakout
AS SELECT *
   FROM hdm.pimco_maturity_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_maturity_bucket
AS SELECT *
   FROM hdm.pimco_maturity_bucket;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_commodity
AS SELECT *
   FROM hdm.pimco_mkt_commodity;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_country
AS SELECT *
   FROM hdm.pimco_mkt_country;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_maturity
AS SELECT *
   FROM hdm.pimco_mkt_maturity;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_quality
AS SELECT *
   FROM hdm.pimco_mkt_quality;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_survey
AS SELECT *
   FROM hdm.pimco_mkt_survey;

CREATE OR REPLACE VIEW hdm.vw_pimco_quality_breakout
AS SELECT *
   FROM hdm.pimco_quality_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_quality_bucket
AS SELECT *
   FROM hdm.pimco_quality_bucket;

CREATE OR REPLACE VIEW hdm.vw_pimco_region
AS SELECT *
   FROM hdm.pimco_region;

CREATE OR REPLACE VIEW hdm.vw_pimco_sector
AS SELECT *
   FROM hdm.pimco_sector;

CREATE OR REPLACE VIEW hdm.vw_pimco_sector_breakout
AS SELECT *
   FROM hdm.pimco_sector_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_statistics
AS SELECT *
   FROM hdm.pimco_statistics;

CREATE OR REPLACE VIEW hdm.vw_position_type
AS SELECT*
   FROM hdm.position_type;

CREATE OR REPLACE VIEW hdm.vw_pro_expense_ratio
AS SELECT *
   FROM hdm.pro_expense_ratio;

CREATE OR REPLACE VIEW hdm.vw_reason_code
AS SELECT *
   FROM hdm.reason_code;

CREATE OR REPLACE VIEW hdm.vw_region
AS SELECT *
   FROM hdm.region;

CREATE OR REPLACE VIEW hdm.vw_report_filters
AS SELECT *
   FROM hdm.report_filters;

CREATE OR REPLACE VIEW hdm.vw_return_periods
AS SELECT *
   FROM hdm.return_periods;

CREATE OR REPLACE VIEW hdm.vw_sdcm_cashflow
AS SELECT *
   FROM hdm.sdcm_cashflow;

CREATE OR REPLACE VIEW hdm.vw_sdcm_fund_balance
AS SELECT *
   FROM hdm.sdcm_fund_balance;

CREATE OR REPLACE VIEW hdm.vw_security
AS SELECT *
   FROM hdm.security;

CREATE OR REPLACE VIEW hdm.vw_security_attributes
AS SELECT *
   FROM hdm.security_attributes;

CREATE OR REPLACE VIEW hdm.vw_security_latest
AS SELECT DISTINCT s.secr_key,
    s.sedol_id,
    s.isin_id,
    s.tckr_sym,
    s.secr_nm,
    s.issuer_nm,
    s.glbl_issuer_nm,
    s.invmt_type_cd,
    s.invmt_type_desc,
    s.dtc_cusip,
    s.asset_id,
    s.muncpl_secr_st_cd,
    s.muncpl_secr_st,
    s.curr_row_flg,
    sa.asset_grp_cd,
    sa.asset_grp_desc
   FROM hdm.vw_security s,
    hdm.vw_security_attributes sa,
    hdm.vw_fund_holdings h,
    hdm.vw_calendar c
  WHERE s.secr_key = h.secr_key AND h.secr_attr_key = sa.secr_attr_key AND h.day_key = c.day_key AND c.cal_day = (( SELECT max(c_1.cal_day) AS max
           FROM hdm.vw_fund_holdings h_1,
            hdm.vw_calendar c_1
          WHERE c_1.day_key = h_1.day_key));

CREATE OR REPLACE VIEW hdm.vw_shareholder
AS SELECT *
   FROM hdm.shareholder;

CREATE OR REPLACE VIEW hdm.vw_shareholder_distrib
AS SELECT *
   FROM hdm.shareholder_distrib;

CREATE OR REPLACE VIEW hdm.vw_shdr_acct_fund
AS SELECT *
   FROM hdm.shdr_acct_fund;

CREATE OR REPLACE VIEW hdm.vw_smip_fund_parameters
AS SELECT *
   FROM hdm.smip_fund_parameters;

CREATE OR REPLACE VIEW hdm.vw_smip_fund_perf_level
AS SELECT *
   FROM hdm.smip_fund_perf_level;

CREATE OR REPLACE VIEW hdm.vw_smip_fund_performance
AS SELECT *
   FROM hdm.smip_fund_performance;

CREATE OR REPLACE VIEW hdm.vw_smip_group_trust_perf
AS SELECT *
   FROM hdm.smip_group_trust_perf;

CREATE OR REPLACE VIEW hdm.vw_smip_metrics
AS SELECT *
   FROM hdm.smip_metrics;

CREATE OR REPLACE VIEW hdm.vw_smip_performance
AS SELECT *
   FROM hdm.smip_performance;

CREATE OR REPLACE VIEW hdm.vw_smip_quintile_perf
AS SELECT *
   FROM hdm.smip_quintile_perf;

CREATE OR REPLACE VIEW hdm.vw_social_security_status
AS SELECT *
   FROM hdm.social_security_status;

CREATE OR REPLACE VIEW hdm.vw_target_wealth_index
AS SELECT target_wealth_index.trgt_wlth_idx_key,
    target_wealth_index.trgt_perf_type_key,
    target_wealth_index.day_key,
    target_wealth_index.fund_key,
    target_wealth_index.wlth_idx,
    target_wealth_index.daily_ret,
    target_wealth_index.curr_row_flg,
    target_wealth_index.row_strt_dttm,
    target_wealth_index.row_stop_dttm,
    target_wealth_index.etl_load_cyc_key,
    target_wealth_index.src_sys_id
   FROM hdm.target_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_target_performance_type
AS SELECT target_performance_type.trgt_perf_type_key,
    target_performance_type.trgt_perf_type,
    target_performance_type.trgt_perf_type_desc,
    target_performance_type.perf_sort_ord,
    target_performance_type.perf_type_flg,
    target_performance_type.obj_type
   FROM hdm.target_performance_type;

CREATE OR REPLACE VIEW hdm.vw_target_custom_wealth_index
AS SELECT a.trgt_perf_type_key,
    a.day_key,
    f.fund_key,
    NULL::integer AS bmk_idx_key,
    f.fund_compst_key,
    a.wlth_idx
   FROM hdm.target_wealth_index a,
    hdm.fund f
  WHERE a.fund_key = f.fund_key
UNION ALL
 SELECT t.trgt_perf_type_key,
    a.day_key,
    f.fund_key,
    NULL::integer AS bmk_idx_key,
    f.fund_compst_key,
    a.wlth_idx
   FROM hdm.fund_valuation a,
    hdm.target_performance_type t,
    hdm.fund f,
    hdm.fund_composite cf
  WHERE a.fund_key = f.fund_key AND f.fund_compst_key = cf.fund_compst_key AND t.trgt_perf_type::text = 'RNAV'::text AND cf.asset_cls_nm::text = 'Target Retirement'::text AND f.shr_cls_desc::text = 'Institutional Class'::text
UNION ALL
 SELECT t.trgt_perf_type_key,
    a.day_key,
    fund.fund_key,
    a.bmk_idx_key,
    cf.fund_compst_key,
    a.idx_vltn_amt AS wlth_idx
   FROM hdm.index_valuation a,
    hdm.target_performance_type t,
    hdm.fund_composite cf,
    hdm.fund_index_rltn rl,
    hdm.fund
  WHERE rl.fund_compst_key = cf.fund_compst_key AND a.bmk_idx_key = rl.bmk_idx_key AND cf.fund_compst_key = fund.fund_compst_key AND fund.shr_cls_cd = 1::numeric AND t.trgt_perf_type::text = 'RIDX'::text AND cf.asset_cls_nm::text = 'Target Retirement'::text AND rl.perf_run_type_key = 2;

CREATE OR REPLACE VIEW hdm.vw_target_daily_weight
AS SELECT *
   FROM hdm.target_daily_weight;

CREATE OR REPLACE VIEW hdm.vw_target_weights
AS SELECT target_weights.trgt_wgt_key,
    target_weights.trgt_fund_key,
    target_weights.efftv_strt_dt,
    target_weights.efftv_end_dt,
    target_weights.fund_key,
    target_weights.trgt_fund_wgt,
    target_weights.wgt_type,
    target_weights.curr_row_flg,
    target_weights.row_strt_dttm,
    target_weights.row_stop_dttm,
    target_weights.etl_load_cyc_key,
    target_weights.src_sys_id
   FROM hdm.target_weights;

CREATE OR REPLACE VIEW hdm.vw_target_orig_glidepath
AS SELECT *
   FROM hdm.target_orig_glidepath;


CREATE OR REPLACE VIEW hdm.vw_target_performance_type
AS SELECT *
   FROM hdm.target_performance_type;

CREATE OR REPLACE VIEW hdm.vw_target_return
AS SELECT *
   FROM hdm.target_return;

CREATE OR REPLACE VIEW hdm.vw_target_wealth_index
AS SELECT *
   FROM hdm.target_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_target_weight_return
AS SELECT c.cal_day,
    tf.fund_nm AS target_fund,
    tf.fund_sort_ord AS target_sort,
    f.fund_nm,
    f.fund_sort_ord,
    b.idx_nm,
    v.fund_ret,
    idr.idx_ret,
    wt.trgt_fund_wgt,
    wt.efftv_strt_dt,
    wt.efftv_end_dt,
    idr.idx_ret * wt.trgt_fund_wgt AS weight_ret
   FROM hdm.vw_target_weights wt,
    hdm.vw_fund tf,
    hdm.vw_fund_index_rltn rl,
    hdm.vw_fund f,
    hdm.vw_benchmark_index b,
    hdm.vw_fund_return v,
    hdm.vw_calendar c,
    hdm.vw_return_periods p,
    hdm.vw_perf_run_type t,
    hdm.vw_index_return idr
  WHERE wt.trgt_fund_key = tf.fund_key AND wt.fund_key = f.fund_key AND f.fund_compst_key = rl.fund_compst_key AND rl.bmk_idx_key = b.bmk_idx_key AND v.fund_key = f.fund_key AND v.day_key = c.day_key AND v.per_key = p.per_key AND rl.perf_run_type_key = t.perf_run_type_key AND rl.bmk_idx_key = idr.bmk_idx_key AND rl.fund_compst_key = idr.fund_compst_key AND idr.day_key = c.day_key AND idr.per_key = p.per_key AND wt.efftv_strt_dt <= c.cal_day AND wt.efftv_end_dt >= c.cal_day AND wt.wgt_type::text = 'FIRM'::text AND ((tf.fund_stat_cd::text = ANY (ARRAY[1::text::character varying::text, 5::text::character varying::text])) OR tf.fund_incpn_dt <= c.cal_day AND tf.fund_clos_dt >= c.cal_day) AND rl.prim_bmk_flg::text = 'Y'::text AND rl.link_prio::double precision = 1::double precision AND c.cal_day >= '2016-01-01 00:00:00'::timestamp without time zone AND c.cal_day <= '2016-09-30 00:00:00'::timestamp without time zone AND p.per_nm::text = '1DY'::text AND tf.fund_nbr::double precision = 2600::double precision AND t.run_type_cd::text = 'REPORTING'::text
  ORDER BY c.cal_day, f.fund_sort_ord;


CREATE OR REPLACE VIEW hdm.vw_target_weights
AS SELECT *
   FROM hdm.target_weights;

CREATE OR REPLACE VIEW hdm.vw_trade_entry_method
AS SELECT *
   FROM hdm.trade_entry_method;

CREATE OR REPLACE VIEW hdm.vw_trade_origin
AS SELECT *
   FROM hdm.trade_origin;

CREATE OR REPLACE VIEW hdm.vw_transaction_type
AS SELECT *
   FROM hdm.transaction_type;

CREATE OR REPLACE VIEW hdm.vw_universe
AS SELECT *
   FROM hdm.universe;

CREATE OR REPLACE VIEW hdm.vw_yield_types
AS SELECT *
   FROM hdm.yield_types;

create or replace view hdm.vw_mstr_fund_metrics
AS SELECT *
from hdm.mstr_fund_metrics;


create or replace view hdm.vw_mstr_fund  AS
SELECT *
from hdm.mstr_fund;

-- AWS_ORACLE_EXT related views

CREATE OR REPLACE VIEW hdm.vw_10kgrowth_chart
AS SELECT f.cal_day AS date_1,
    f.fund_nm AS fund_name,
    f.shr_cls_desc AS fund_class,
    f.quot_sym AS fund_quote,
    f.fund_compst_key AS fund_cmp,
    f.fund_sort_ord AS fund_sort,
    f.wlth_idx AS wealth_index,
    bm1.idx_nm AS benchmark_name_1,
    bm1.idx_vltn_amt AS wealth_index_1,
    bm2.idx_nm AS benchmark_name_2,
    bm2.idx_vltn_amt AS wealth_index_2,
    bm3.idx_nm AS benchmark_name_3,
    bm3.idx_vltn_amt AS wealth_index_3,
    bm4.idx_nm AS benchmark_name_4,
    bm4.wlth_idx AS wealth_index_4
   FROM ( SELECT c.cal_day,
            c.day_key,
            f_1.fund_nm,
            f_1.shr_cls_desc,
            f_1.quot_sym,
            f_1.fund_compst_key,
            f_1.fund_sort_ord,
            a.wlth_idx,
            f_1.fund_key
           FROM hdm.vw_fund_valuation a,
            hdm.vw_fund f_1,
            hdm.vw_fund_composite fc,
            hdm.vw_calendar c
          WHERE a.fund_key = f_1.fund_key AND a.day_key = c.day_key AND f_1.fund_compst_key = fc.fund_compst_key AND (f_1.fund_stat_cd::text = ANY (ARRAY[1::text::character varying::text, 5::text::character varying::text]))) f
     LEFT JOIN ( SELECT c.cal_day,
            c.day_key,
            b.idx_nm,
            b.idx_id,
            f_1.fund_compst_key,
            a.idx_vltn_amt,
            f_1.fund_key
           FROM hdm.vw_index_valuation a,
            hdm.vw_calendar c,
            hdm.vw_benchmark_index b,
            hdm.vw_fund f_1,
            hdm.vw_fund_composite fc,
            hdm.vw_fund_index_rltn bf
          WHERE a.day_key = c.day_key AND c.cal_day >= f_1.fund_incpn_dt AND a.bmk_idx_key = b.bmk_idx_key AND f_1.fund_compst_key = fc.fund_compst_key AND bf.fund_compst_key = fc.fund_compst_key AND bf.bmk_idx_key = b.bmk_idx_key AND bf.link_prio::double precision = 1::double precision AND bf.perf_run_type_key::numeric = 3::numeric AND bf.gr_ret_inlsn_flg::text = 'Y'::text) bm1 ON f.day_key = bm1.day_key AND f.fund_compst_key = bm1.fund_compst_key AND f.fund_key = bm1.fund_key
     LEFT JOIN ( SELECT c.cal_day,
            c.day_key,
            b.idx_nm,
            b.idx_id,
            f_1.fund_compst_key,
            a.idx_vltn_amt,
            f_1.fund_key
           FROM hdm.vw_index_valuation a,
            hdm.vw_calendar c,
            hdm.vw_benchmark_index b,
            hdm.vw_fund f_1,
            hdm.vw_fund_composite fc,
            hdm.vw_fund_index_rltn bf
          WHERE a.day_key = c.day_key AND c.cal_day >= f_1.fund_incpn_dt AND a.bmk_idx_key = b.bmk_idx_key AND f_1.fund_compst_key = fc.fund_compst_key AND bf.fund_compst_key = fc.fund_compst_key AND bf.bmk_idx_key = b.bmk_idx_key AND bf.link_prio::double precision = 2::double precision AND bf.perf_run_type_key::numeric = 3::numeric AND bf.gr_ret_inlsn_flg::text = 'Y'::text) bm2 ON f.day_key = bm2.day_key AND f.fund_compst_key = bm2.fund_compst_key AND f.fund_key = bm2.fund_key
     LEFT JOIN ( SELECT c.cal_day,
            c.day_key,
            b.idx_nm,
            b.idx_id,
            f_1.fund_compst_key,
            a.idx_vltn_amt,
            f_1.fund_key
           FROM hdm.vw_index_valuation a,
            hdm.vw_calendar c,
            hdm.vw_benchmark_index b,
            hdm.vw_fund f_1,
            hdm.vw_fund_composite fc,
            hdm.vw_fund_index_rltn bf
          WHERE a.day_key = c.day_key AND c.cal_day >= f_1.fund_incpn_dt AND a.bmk_idx_key = b.bmk_idx_key AND f_1.fund_compst_key = fc.fund_compst_key AND bf.fund_compst_key = fc.fund_compst_key AND bf.bmk_idx_key = b.bmk_idx_key AND bf.link_prio::double precision = 3::double precision AND bf.perf_run_type_key::numeric = 3::numeric AND bf.gr_ret_inlsn_flg::text = 'Y'::text) bm3 ON f.day_key = bm3.day_key AND f.fund_compst_key = bm3.fund_compst_key AND f.fund_key = bm3.fund_key
     LEFT JOIN ( SELECT cal.day_key,
            cal.cal_day,
            cal.fund_compst_key,
            cal.fund_key,
            cal.idx_nm,
            cal.bus_day_flg,
                CASE
                    WHEN wi.wlth_idx IS NULL THEN ( SELECT twi.wlth_idx
                       FROM hdm.vw_target_wealth_index twi,
                        hdm.vw_calendar cl,
                        hdm.vw_target_performance_type tp
                      WHERE twi.day_key = cl.day_key AND twi.trgt_perf_type_key = tp.trgt_perf_type_key AND twi.fund_key = cal.fund_key AND tp.trgt_perf_type::text = 'RBWM'::text AND cl.cal_day = (( SELECT max(vw_calendar.cal_day) AS max
                               FROM hdm.vw_calendar
                              WHERE vw_calendar.cal_day < cal.cal_day AND vw_calendar.bus_day_flg::text = 'Y'::text)))
                    ELSE wi.wlth_idx
                END AS wlth_idx
           FROM ( SELECT c.cal_day,
                    c.day_key,
                    f_1.fund_compst_key,
                    b.trgt_perf_type_key,
                    concat_ws(''::text, b.trgt_perf_type_desc, replace(replace(f_1.fund_nm::text, 'Target Retirement'::text, ''::text), 'Fund'::text, ''::text)) AS idx_nm,
                    f_1.fund_sort_ord,
                    f_1.fund_key,
                    c.bus_day_flg
                   FROM hdm.vw_calendar c,
                    hdm.vw_fund f_1,
                    hdm.vw_target_performance_type b
                  WHERE c.cal_day >= (f_1.perf_incpn_dt - ((1::numeric || ' days'::text)::interval)) AND c.cal_day < aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) AND f_1.fund_nm::text ~~ 'Target%'::text AND b.trgt_perf_type::text = 'RBWM'::text AND (f_1.fund_stat_desc::text = ANY (ARRAY['Open'::character varying::text, 'Soft Close'::character varying::text])) AND f_1.shr_cls_desc::text = 'Institutional Class'::text) cal
             LEFT JOIN ( SELECT a.day_key,
                    a.wlth_idx,
                    b.trgt_perf_type_key,
                    concat_ws(''::text, b.trgt_perf_type_desc, replace(replace(f_1.fund_nm::text, 'Target Retirement'::text, ''::text), 'Fund'::text, ''::text)) AS idx_nm,
                    f_1.fund_compst_key,
                    f_1.fund_key
                   FROM hdm.vw_target_wealth_index a,
                    hdm.vw_target_performance_type b,
                    hdm.vw_fund f_1
                  WHERE a.fund_key = f_1.fund_key AND a.trgt_perf_type_key = b.trgt_perf_type_key AND b.trgt_perf_type::text = 'RBWM'::text) wi ON cal.day_key = wi.day_key AND cal.trgt_perf_type_key = wi.trgt_perf_type_key AND cal.fund_key = wi.fund_key) bm4 ON f.day_key = bm4.day_key AND f.fund_compst_key = bm4.fund_compst_key AND f.fund_key = bm4.fund_key
  WHERE bm1.idx_nm IS NOT NULL
  ORDER BY f.fund_sort_ord, f.cal_day;

CREATE OR REPLACE VIEW hdm.vw_dealer_dst_change_list
AS SELECT aws_oracle_ext.to_number(stg_hbr_acctmainthist.fund) AS fund_nbr,
    aws_oracle_ext.to_number(stg_hbr_acctmainthist.account) AS acct_nbr,
    btrim(stg_hbr_acctmainthist.view_column_nm::text) AS change_type,
    aws_oracle_ext.to_date(aws_oracle_ext.to_char(stg_hbr_acctmainthist.super_sheet_mnt_dt, 'mm/dd/yyyy'::character varying), 'mm/dd/yyyy'::character varying) AS change_dt,
    stg_hbr_acctmainthist.load_time,
    stg_hbr_acctmainthist.acct_mnt_seq_id,
    stg_hbr_acctmainthist.batch_number,
    btrim(stg_hbr_acctmainthist.before_value::text) AS orig_value,
    btrim(stg_hbr_acctmainthist.after_value::text) AS new_value
   FROM hdm.stg_hbr_acctmainthist
  WHERE (btrim(stg_hbr_acctmainthist.view_column_nm::text) = ANY (ARRAY['DEALER'::text, 'THIRD_PARTY_ADMIN'::text, 'CUMDISC'::text])) AND stg_hbr_acctmainthist.super_sheet_mnt_dt > aws_oracle_ext.add_months(aws_oracle_ext.trunc(aws_oracle_ext.sysdate(), 'mm'::character varying), '-12'::integer);

CREATE OR REPLACE VIEW hdm.vw_dlp_accounts
AS SELECT a.acct_nbr AS acctnbr,
    a.regstn_line_1 AS reg1,
    a.regstn_line_2 AS reg2,
    a.regstn_line_3 AS reg3,
    a.regstn_line_4 AS reg4,
    a.regstn_line_5 AS reg5,
    a.regstn_line_6 AS reg6,
    a.regstn_line_7 AS reg7
   FROM hdm.vw_account a
  WHERE (a.tax_id::double precision <> ALL (ARRAY[123456789::double precision, 0::double precision, 111111111::double precision, 1::double precision, 341953397::double precision, 341953393::double precision, 341953399::double precision, 980592172::double precision, 561223381::double precision, 341572115::double precision, 341931837::double precision, 346864487::double precision, 341570877::double precision, 306006023::double precision, 341971614::double precision, 341570746::double precision, 341748997::double precision, 205075188::double precision, 264203992::double precision, 263062227::double precision, 271891764::double precision, 274941322::double precision, 451258204::double precision, 451258360::double precision, 30488315::double precision, 341570861::double precision, 47018428::double precision, 341570876::double precision, 263713174::double precision, 263713227::double precision, 263713261::double precision, 263713284::double precision, 263713315::double precision, 263713340::double precision, 263713369::double precision, 263713394::double precision, 263713422::double precision, 263713447::double precision])) AND (a.acct_nbr::double precision <> ALL (ARRAY[60000001::double precision, 60100001::double precision, 60200001::double precision, 60300001::double precision, 60400001::double precision, 60500001::double precision, 60600001::double precision, 60700001::double precision, 60800001::double precision, 60900001::double precision, 70000001::double precision, 70100001::double precision, 70200001::double precision, 70300001::double precision, 70400001::double precision, 70500001::double precision, 70600001::double precision, 70700001::double precision, 70800001::double precision, 70900001::double precision, 80000001::double precision, 80100001::double precision, 80200001::double precision, 80300001::double precision, 80400001::double precision, 80500001::double precision, 80600001::double precision, 80700001::double precision, 80800001::double precision, 80900001::double precision])) AND (a.acct_stat::text = 'Open Status'::text OR a.acct_stat::text = 'Closed Status'::text AND a.last_mntn_dt >= aws_oracle_ext.add_months(aws_oracle_ext.trunc(aws_oracle_ext.sysdate(), 'mm'::character varying), '-24'::integer))
  GROUP BY a.acct_nbr, a.regstn_line_1, a.regstn_line_2, a.regstn_line_3, a.regstn_line_4, a.regstn_line_5, a.regstn_line_6, a.regstn_line_7
UNION ALL
 SELECT 33445566 AS acctnbr,
    'John Doe'::character varying AS reg1,
    '123 Main St.'::character varying AS reg2,
    'Chicago IL 66666-6666'::character varying AS reg3,
    ''::character varying AS reg4,
    ''::character varying AS reg5,
    ''::character varying AS reg6,
    ''::character varying AS reg7
UNION ALL
 SELECT 33445567 AS acctnbr,
    'Jane Doe'::character varying AS reg1,
    '321 Side St.'::character varying AS reg2,
    'Chicago IL 60606-6666'::character varying AS reg3,
    ''::character varying AS reg4,
    ''::character varying AS reg5,
    ''::character varying AS reg6,
    ''::character varying AS reg7
UNION ALL
 SELECT 33445568 AS acctnbr,
    'John Smith'::character varying AS reg1,
    '123 Michigan Ave.'::character varying AS reg2,
    'Chicago IL 60607-6666'::character varying AS reg3,
    ''::character varying AS reg4,
    ''::character varying AS reg5,
    ''::character varying AS reg6,
    ''::character varying AS reg7
UNION ALL
 SELECT 33445569 AS acctnbr,
    'Jane Smith'::character varying AS reg1,
    '321 Wacker Dr.'::character varying AS reg2,
    'Chicago IL 60606-6666'::character varying AS reg3,
    ''::character varying AS reg4,
    ''::character varying AS reg5,
    ''::character varying AS reg6,
    ''::character varying AS reg7;

CREATE OR REPLACE VIEW hdm.vw_expense_ratio
AS SELECT mv_expense_ratio.day_key,
    mv_expense_ratio.fund_key,
    mv_expense_ratio.efftv_strt_dt,
    mv_expense_ratio.efftv_end_dt,
    mv_expense_ratio.net_expns_ratio,
    mv_expense_ratio.net_expns_ratio_ytd,
    mv_expense_ratio.gr_expns_ratio,
    mv_expense_ratio.gr_expns_ratio_ytd
   FROM hdm.mv_expense_ratio
UNION ALL
 SELECT expense_ratio_history.day_key,
    expense_ratio_history.fund_key,
    expense_ratio_history.efftv_strt_dt,
    expense_ratio_history.efftv_end_dt,
    expense_ratio_history.net_expns_ratio,
    expense_ratio_history.net_expns_ratio AS net_expns_ratio_ytd,
    NULL::double precision AS gr_expns_ratio,
    NULL::double precision AS gr_expns_ratio_ytd
   FROM hdm.expense_ratio_history
UNION ALL
 SELECT ( SELECT calendar.day_key
           FROM hdm.calendar
          WHERE calendar.cal_day = aws_oracle_ext.add_months(c.cal_day, 1)) AS day_key,
    a.fund_key,
    a.efftv_strt_dt,
    aws_oracle_ext.add_months(a.efftv_end_dt, 1) AS efftv_end_dt,
    a.net_expns_ratio,
    a.net_expns_ratio_ytd,
    a.gr_expns_ratio,
    a.gr_expns_ratio AS gr_expns_ratio_ytd
   FROM hdm.mv_expense_ratio a,
    hdm.calendar c
  WHERE a.day_key = c.day_key AND c.cal_day = (( SELECT max(cl.cal_day) AS max
           FROM hdm.calendar cl,
            hdm.mv_expense_ratio er
          WHERE cl.day_key = er.day_key)) AND c.cal_day <= aws_oracle_ext.add_months(aws_oracle_ext.trunc(aws_oracle_ext.sysdate(), 'mm'::character varying), '-1'::integer);

CREATE OR REPLACE VIEW hdm.vw_dlp_taxid
AS SELECT a.tax_id
   FROM hdm.vw_account a
  WHERE (a.tax_id::double precision <> ALL (ARRAY[123456789::double precision, 0::double precision, 111111111::double precision, 1::double precision, 341953397::double precision, 341953393::double precision, 341953399::double precision, 980592172::double precision, 561223381::double precision, 341572115::double precision, 341931837::double precision, 346864487::double precision, 341570877::double precision, 306006023::double precision, 341971614::double precision, 341570746::double precision, 341748997::double precision, 205075188::double precision, 264203992::double precision, 263062227::double precision, 271891764::double precision, 274941322::double precision, 451258204::double precision, 451258360::double precision, 30488315::double precision, 341570861::double precision, 47018428::double precision, 341570876::double precision, 263713174::double precision, 263713227::double precision, 263713261::double precision, 263713284::double precision, 263713315::double precision, 263713340::double precision, 263713369::double precision, 263713394::double precision, 263713422::double precision, 263713447::double precision])) AND (a.acct_nbr::double precision <> ALL (ARRAY[60000001::double precision, 60100001::double precision, 60200001::double precision, 60300001::double precision, 60400001::double precision, 60500001::double precision, 60600001::double precision, 60700001::double precision, 60800001::double precision, 60900001::double precision, 70000001::double precision, 70100001::double precision, 70200001::double precision, 70300001::double precision, 70400001::double precision, 70500001::double precision, 70600001::double precision, 70700001::double precision, 70800001::double precision, 70900001::double precision, 80000001::double precision, 80100001::double precision, 80200001::double precision, 80300001::double precision, 80400001::double precision, 80500001::double precision, 80600001::double precision, 80700001::double precision, 80800001::double precision, 80900001::double precision])) AND (a.acct_stat::text = 'Open Status'::text OR a.acct_stat::text = 'Closed Status'::text AND a.last_mntn_dt >= aws_oracle_ext.add_months(aws_oracle_ext.trunc(aws_oracle_ext.sysdate(), 'mm'::character varying), '-24'::integer))
  GROUP BY a.tax_id
UNION ALL
 SELECT 325689021 AS tax_id
UNION ALL
 SELECT 325689022 AS tax_id
UNION ALL
 SELECT 325689023 AS tax_id
UNION ALL
 SELECT 325689024 AS tax_id;

CREATE OR REPLACE VIEW hdm.vw_subsequent_min_positions
AS SELECT 'Acct Position'::text AS data_source,
    vw_current_account.acct_nbr,
    vw_current_account.acct_stat,
    vw_current_account.regstn_line_1,
    vw_current_account.dlr_lvl_cntl_cd,
    vw_current_account.dlr_lvl_cntl_desc,
    vw_fund.fund_nbr,
    vw_fund.fund_sort_ord,
    vw_fund_composite.compst_nm,
    vw_account_position.tot_bal_amt AS amt,
    vw_fund.reg_acct_init_invmt,
    vw_calendar.cal_day,
    vw_current_account.custodian_id
   FROM hdm.vw_current_account,
    hdm.vw_fund,
    hdm.vw_fund_composite,
    hdm.vw_account_position,
    hdm.vw_calendar,
    hdm.vw_dealer
  WHERE vw_account_position.dlr_key = vw_dealer.dlr_key AND vw_account_position.day_key = vw_calendar.day_key AND vw_fund.fund_key = vw_account_position.fund_key AND vw_current_account.acct_nbr = vw_account_position.acct_nbr AND vw_current_account.fund_nbr::double precision = vw_account_position.fund_nbr::double precision AND vw_fund.fund_compst_key = vw_fund_composite.fund_compst_key AND vw_calendar.day_key = (( SELECT vw_calendar_1.day_key
           FROM hdm.vw_calendar vw_calendar_1
          WHERE vw_calendar_1.cal_day = (( SELECT max(vw_calendar_2.cal_day) AS max
                   FROM hdm.vw_calendar vw_calendar_2
                  WHERE vw_calendar_2.bus_day_flg::text = 'Y'::text AND vw_calendar_2.cal_day < (( SELECT max(vw_calendar_3.cal_day) AS max
                           FROM hdm.vw_calendar vw_calendar_3
                          WHERE vw_calendar_3.cal_day >= (aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) - ((7::numeric || ' days'::text)::interval)) AND vw_calendar_3.cal_day <= aws_oracle_ext.trunc(aws_oracle_ext.sysdate() - ((1::numeric || ' days'::text)::interval)) AND vw_calendar_3.bus_day_flg::text = 'Y'::text)))))) AND vw_current_account.dlr_lvl_cntl_cd::numeric <> 3::numeric AND vw_fund.shr_cls_desc::text = 'Institutional Class'::text AND vw_current_account.acct_stat::text <> 'Closed Status '::text AND vw_dealer.dlr_id::text = '5471'::text AND (concat_ws(''::text, aws_oracle_ext.to_char(vw_current_account.acct_nbr), aws_oracle_ext.to_char(vw_current_account.fund_nbr)) IN ( SELECT concat_ws(''::text, aws_oracle_ext.to_char(vw_current_account_1.acct_nbr), aws_oracle_ext.to_char(vw_current_account_1.fund_nbr)) AS acct_nbr_w_fund_nbr
           FROM hdm.vw_current_account vw_current_account_1,
            hdm.vw_fund vw_fund_1,
            hdm.vw_fund_composite vw_fund_composite_1,
            hdm.vw_fund_trans_hist tran,
            hdm.vw_calendar vw_calendar_1,
            hdm.vw_dealer vw_dealer_1
          WHERE tran.fund_key = vw_fund_1.fund_key AND tran.dlr_key = vw_dealer_1.dlr_key AND tran.sttl_day_key = vw_calendar_1.day_key AND vw_current_account_1.acct_nbr = tran.acct_nbr AND vw_current_account_1.fund_nbr::double precision = tran.fund_nbr::double precision AND vw_fund_1.fund_compst_key = vw_fund_composite_1.fund_compst_key AND vw_calendar_1.day_key = (( SELECT vw_calendar_2.day_key
                   FROM hdm.vw_calendar vw_calendar_2
                  WHERE vw_calendar_2.cal_day = (( SELECT max(vw_calendar_3.cal_day) AS max
                           FROM hdm.vw_calendar vw_calendar_3
                          WHERE vw_calendar_3.cal_day >= (aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) - ((7::numeric || ' days'::text)::interval)) AND vw_calendar_3.cal_day <= aws_oracle_ext.trunc(aws_oracle_ext.sysdate() - ((1::numeric || ' days'::text)::interval)) AND vw_calendar_3.bus_day_flg::text = 'Y'::text)))) AND vw_current_account_1.dlr_lvl_cntl_cd::numeric <> 3::numeric AND vw_fund_1.shr_cls_desc::text = 'Institutional Class'::text AND vw_current_account_1.acct_stat::text <> 'Closed Status '::text AND vw_dealer_1.dlr_id::text = '5471'::text));

CREATE OR REPLACE VIEW hdm.vw_subsequent_min_transactions
AS SELECT 'TransHist'::text AS data_source,
    vw_current_account.acct_nbr,
    vw_current_account.acct_stat,
    vw_current_account.regstn_line_1,
    vw_current_account.dlr_lvl_cntl_cd,
    vw_current_account.dlr_lvl_cntl_desc,
    vw_fund.fund_nbr,
    vw_fund.fund_sort_ord,
    vw_fund_composite.compst_nm,
        CASE
            WHEN tran.shr_bal_effct_flg::text = 'S'::text THEN '-1'::integer
            ELSE 1
        END::double precision * tran.net_amt::double precision AS amt,
    0 AS reg_acct_init_invmt,
    vw_calendar.cal_day,
    vw_current_account.custodian_id
   FROM hdm.vw_current_account,
    hdm.vw_fund,
    hdm.vw_fund_composite,
    hdm.vw_fund_trans_hist tran,
    hdm.vw_calendar,
    hdm.vw_dealer
  WHERE tran.dlr_key = vw_dealer.dlr_key AND tran.sttl_day_key = vw_calendar.day_key AND vw_fund.fund_key = tran.fund_key AND vw_current_account.acct_nbr = tran.acct_nbr AND vw_current_account.fund_nbr::double precision = tran.fund_nbr::double precision AND vw_fund.fund_compst_key = vw_fund_composite.fund_compst_key AND vw_current_account.acct_setup_dt < (( SELECT max(vw_calendar_1.cal_day) AS max
           FROM hdm.vw_calendar vw_calendar_1
          WHERE vw_calendar_1.cal_day >= (aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) - ((7::numeric || ' days'::text)::interval)) AND vw_calendar_1.cal_day <= aws_oracle_ext.trunc(aws_oracle_ext.sysdate() - ((1::numeric || ' days'::text)::interval)) AND vw_calendar_1.bus_day_flg::text = 'Y'::text)) AND vw_calendar.day_key = (( SELECT vw_calendar_1.day_key
           FROM hdm.vw_calendar vw_calendar_1
          WHERE vw_calendar_1.cal_day = (( SELECT max(vw_calendar_2.cal_day) AS max
                   FROM hdm.vw_calendar vw_calendar_2
                  WHERE vw_calendar_2.cal_day >= (aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) - ((7::numeric || ' days'::text)::interval)) AND vw_calendar_2.cal_day <= aws_oracle_ext.trunc(aws_oracle_ext.sysdate() - ((1::numeric || ' days'::text)::interval)) AND vw_calendar_2.bus_day_flg::text = 'Y'::text)))) AND vw_current_account.dlr_lvl_cntl_cd::numeric <> 3::numeric AND vw_fund.shr_cls_desc::text = 'Institutional Class'::text AND vw_current_account.acct_stat::text <> 'Closed Status '::text AND vw_dealer.dlr_id::text = '5471'::text;

CREATE OR REPLACE VIEW hdm.vw_target_combine_wi
AS SELECT DISTINCT var_sbq.trgt_perf_type_key,
    var_sbq.trgt_fund_key,
    var_sbq.bmk_idx_key,
    var_sbq.day_key,
    var_sbq.wlth_idx,
    var_sbq.row_strt_dttm
   FROM ( SELECT vw_target_wealth_index.trgt_perf_type_key,
            vw_target_wealth_index.fund_key AS trgt_fund_key,
            NULL::numeric AS bmk_idx_key,
            vw_target_wealth_index.day_key,
            vw_target_wealth_index.wlth_idx,
            vw_target_wealth_index.row_strt_dttm
           FROM hdm.vw_target_wealth_index
        UNION ALL
         SELECT ( SELECT vw_target_performance_type.trgt_perf_type_key
                   FROM hdm.vw_target_performance_type
                  WHERE vw_target_performance_type.trgt_perf_type::text = 'RIDX'::text) AS trgt_perf_type_key,
            f.fund_key AS trgt_fund_key,
            idx.bmk_idx_key,
            idx.day_key,
            idx.idx_vltn_amt,
            idx.row_strt_dttm
           FROM hdm.vw_fund_composite fc,
            hdm.vw_fund f,
            hdm.vw_fund_index_rltn rl,
            hdm.vw_index_valuation idx,
            hdm.vw_benchmark_index bmk,
            hdm.vw_calendar cal
          WHERE fc.fund_compst_key = f.fund_compst_key AND fc.fund_compst_key = rl.fund_compst_key AND rl.bmk_idx_key = bmk.bmk_idx_key AND idx.bmk_idx_key = bmk.bmk_idx_key AND idx.day_key = cal.day_key AND cal.cal_day >= aws_oracle_ext.to_date('31-dec-2008'::character varying) AND fc.asset_cls_nm::text = 'Target Retirement'::text AND f.shr_cls_desc::text = 'Institutional Class'::text AND rl.perf_run_type_key::numeric = 3::numeric
        UNION ALL
         SELECT ( SELECT vw_target_performance_type.trgt_perf_type_key
                   FROM hdm.vw_target_performance_type
                  WHERE vw_target_performance_type.trgt_perf_type::text = 'RNAV'::text) AS trgt_perf_type_key,
            fnd.fund_key,
            NULL::numeric AS bmk_idx_key,
            cal.day_key,
            val.wlth_idx,
            val.row_strt_dttm
           FROM hdm.vw_fund_valuation val,
            hdm.vw_fund fnd,
            hdm.vw_calendar cal,
            hdm.vw_fund_composite cmp
          WHERE val.fund_key = fnd.fund_key AND val.day_key = cal.day_key AND fnd.fund_compst_key = cmp.fund_compst_key AND cmp.asset_cls_nm::text = 'Target Retirement'::text AND fnd.shr_cls_desc::text = 'Institutional Class'::text) var_sbq;
