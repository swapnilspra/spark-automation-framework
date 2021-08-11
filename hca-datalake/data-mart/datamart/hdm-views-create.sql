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
          WHERE a.fund_key = f_1.fund_key AND a.day_key = c.day_key AND f_1.fund_compst_key = fc.fund_compst_key AND (f_1.fund_stat_cd::text = ANY (ARRAY[1::text::character varying, 5::text::character varying]::text[]))) f
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
          WHERE a.day_key = c.day_key AND c.cal_day >= f_1.fund_incpn_dt AND a.bmk_idx_key = b.bmk_idx_key AND f_1.fund_compst_key = fc.fund_compst_key AND bf.fund_compst_key = fc.fund_compst_key AND bf.bmk_idx_key = b.bmk_idx_key AND bf.link_prio = 1::double precision AND bf.perf_run_type_key = 3::numeric AND bf.gr_ret_inlsn_flg::text = 'Y'::text) bm1 ON f.day_key = bm1.day_key AND f.fund_compst_key = bm1.fund_compst_key AND f.fund_key = bm1.fund_key
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
          WHERE a.day_key = c.day_key AND c.cal_day >= f_1.fund_incpn_dt AND a.bmk_idx_key = b.bmk_idx_key AND f_1.fund_compst_key = fc.fund_compst_key AND bf.fund_compst_key = fc.fund_compst_key AND bf.bmk_idx_key = b.bmk_idx_key AND bf.link_prio = 2::double precision AND bf.perf_run_type_key = 3::numeric AND bf.gr_ret_inlsn_flg::text = 'Y'::text) bm2 ON f.day_key = bm2.day_key AND f.fund_compst_key = bm2.fund_compst_key AND f.fund_key = bm2.fund_key
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
          WHERE a.day_key = c.day_key AND c.cal_day >= f_1.fund_incpn_dt AND a.bmk_idx_key = b.bmk_idx_key AND f_1.fund_compst_key = fc.fund_compst_key AND bf.fund_compst_key = fc.fund_compst_key AND bf.bmk_idx_key = b.bmk_idx_key AND bf.link_prio = 3::double precision AND bf.perf_run_type_key = 3::numeric AND bf.gr_ret_inlsn_flg::text = 'Y'::text) bm3 ON f.day_key = bm3.day_key AND f.fund_compst_key = bm3.fund_compst_key AND f.fund_key = bm3.fund_key
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
                  WHERE c.cal_day >= (f_1.perf_incpn_dt - ((1::numeric || ' days'::text)::interval)) AND c.cal_day < aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) AND f_1.fund_nm::text ~~ 'Target%'::text AND b.trgt_perf_type::text = 'RBWM'::text AND (f_1.fund_stat_desc::text = ANY (ARRAY['Open'::character varying, 'Soft Close'::character varying]::text[])) AND f_1.shr_cls_desc::text = 'Institutional Class'::text) cal
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
AS SELECT account.acct_key,
    account.acct_top_lvl_cd,
    account.acct_top_lvl_desc,
    account.acct_type_cd,
    account.acct_type,
    account.acct_nbr,
    account.fund_nbr,
    account.tax_id,
    account.acct_stat_cd,
    account.acct_stat,
    account.acct_strt_dt,
    account.last_mntn_dt,
    account.dlr_lvl_cntl_cd,
    account.dlr_lvl_cntl_desc,
    account.pre_authzd_chk_acct_flg,
    account.swp_acct_flg,
    account.ach_acct_flg,
    account.re_invst_to_another_acct_flg,
    account.monitored_vip_acct,
    account.fiduciary_acct_flg,
    account.mailg_flg,
    account.intr_party_flg,
    account.mgmt_co_empl_flg,
    account.trst_co_nbr,
    account.tpa_nbr,
    account.custodian_id,
    account.tpa_id,
    account.ext_pln_id,
    account.tin_cd,
    account.tin_desc,
    account.alpha_cd,
    account.cap_gains_dstrbtn_flg,
    account.chk_writing_acct_flg,
    account.divd_dstrbtn_opt_flg,
    account.expedited_redmpn_acct_flg,
    account.sub_acct_flg,
    account.forgn_tax_rt,
    account.cum_discnt_nbr,
    account.ltr_of_intend_nbr,
    account.timer_flg,
    account.gpurch_acct_flg,
    account.expdt_xchg_acct_flg,
    account.pnlty_whg_acct_flg,
    account.certfc_issuance_flg,
    account.stop_xfer_flg,
    account.bluesky_xmptn_flg,
    account.bnk_card_issd_flg,
    account.nav_acct_flg,
    account.phon_chk_redmpn_flg,
    account.house_acct_flg,
    account.divd_mail_acct_flg,
    account.stop_purch_acct_flg,
    account.stop_mail_acct_flg,
    account.fractional_chk_flg,
    account.acct_pr_sched_flg,
    account.cap_gains_dstrbtn_cd,
    account.cap_gains_dstrbtn_type,
    account.divd_dstrbtn_type_cd,
    account.divd_dstrbtn_type,
    account.cust_bin,
    account.acct_origination_id,
    account.regstn_line_1,
    account.regstn_line_2,
    account.regstn_line_3,
    account.regstn_line_4,
    account.regstn_line_5,
    account.regstn_line_6,
    account.regstn_line_7,
    account.st_nm,
    account.crty_nm,
    account.st_cd,
    account.crty_cd,
    account.row_strt_dttm,
    account.row_stop_dttm,
    account.curr_row_flg,
    account.etl_load_cyc_key,
    account.src_sys_id,
    account.legacy_type_desc,
    account.fund_strt_dt,
    account.stop_mail_dt,
    account.acct_setup_dt,
    account.internet_acct_flg,
    account.grnd_father_acct_flg,
    account.pln_clsfcn_flg,
    account.mktg_clsfcn_flg,
    account.mktg_clsfcn_desc,
    account.rep_cd,
    account.rep_nm,
    account.dlr_id,
    account.dlr_type,
    account.hbr_dlr_id,
    account.hbr_dlr_nm,
    account.trst_nm,
    account.acct_clsd_dt,
    account.redmpn_fee_xmpt_flg,
    account.pay_to_play_flg,
    account.citzn_st_crty_cd,
    account.purg_prcsd_flg,
    account.purg_prcsd_dt
   FROM hdm.account;

CREATE OR REPLACE VIEW hdm.vw_account_category
AS SELECT account_category.acct_cat_key,
    account_category.acct_cat_desc,
    account_category.maj_cat_cd,
    account_category.maj_cat_desc,
    account_category.acct_cat_ord AS acct_cat_order
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
AS SELECT account_setup_year.acct_setup_yr_key,
    account_setup_year.acct_setup_yr
   FROM hdm.account_setup_year;

CREATE OR REPLACE VIEW hdm.vw_after_tax_calculation
AS SELECT after_tax_calculation.atax_calc_key,
    after_tax_calculation.reinv_day_key,
    after_tax_calculation.day_key,
    after_tax_calculation.fund_key,
    after_tax_calculation.per_key,
    after_tax_calculation.tax_yr,
    after_tax_calculation.cb_amt,
    after_tax_calculation.shrs_calc,
    after_tax_calculation.curr_row_flg,
    after_tax_calculation.row_strt_dttm,
    after_tax_calculation.row_stop_dttm,
    after_tax_calculation.etl_load_cyc_key,
    after_tax_calculation.src_sys_id
   FROM hdm.after_tax_calculation;

CREATE OR REPLACE VIEW hdm.vw_after_tax_return
AS SELECT after_tax_return.atax_ret_key,
    after_tax_return.day_key,
    after_tax_return.fund_key,
    after_tax_return.per_key,
    after_tax_return.sold_amt,
    after_tax_return.cb_amt,
    after_tax_return.gls_amt,
    after_tax_return.st_tax_gls,
    after_tax_return.lt_tax_gls,
    after_tax_return.tax_amt,
    after_tax_return.net_prcd_atax,
    after_tax_return.strt_per,
    after_tax_return.annlzn_fctr,
    after_tax_return.atax_ret_wo_sale,
    after_tax_return.atax_ret,
    after_tax_return.curr_row_flg,
    after_tax_return.row_strt_dttm,
    after_tax_return.row_stop_dttm,
    after_tax_return.etl_load_cyc_key,
    after_tax_return.src_sys_id
   FROM hdm.after_tax_return;

CREATE OR REPLACE VIEW hdm.vw_after_tax_wealth_index
AS SELECT after_tax_wealth_index.atax_wlth_idx_key,
    after_tax_wealth_index.day_key,
    after_tax_wealth_index.reinv_day_key,
    after_tax_wealth_index.fund_key,
    after_tax_wealth_index.atax_ordn_incm,
    after_tax_wealth_index.atax_ltcg,
    after_tax_wealth_index.atax_stcg,
    after_tax_wealth_index.atax_forgn_tax_cr,
    after_tax_wealth_index.atax_tot_divd,
    after_tax_wealth_index.prir_mo_pr,
    after_tax_wealth_index.curr_mo_pr,
    after_tax_wealth_index.reinv_pr,
    after_tax_wealth_index.btax_ret,
    after_tax_wealth_index.atax_ret,
    after_tax_wealth_index.atax_reinv_per_shr,
    after_tax_wealth_index.curr_row_flg,
    after_tax_wealth_index.row_strt_dttm,
    after_tax_wealth_index.row_stop_dttm,
    after_tax_wealth_index.etl_load_cyc_key,
    after_tax_wealth_index.src_sys_id
   FROM hdm.after_tax_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_awd_privilege_type
AS SELECT awd_privilege_type.prvl_type_key,
    awd_privilege_type.prvl_type,
    awd_privilege_type.prvl_type_desc,
    awd_privilege_type.curr_row_flg,
    awd_privilege_type.row_strt_dttm,
    awd_privilege_type.row_stop_dttm,
    awd_privilege_type.etl_load_cyc_key,
    awd_privilege_type.src_sys_id
   FROM hdm.awd_privilege_type;

CREATE OR REPLACE VIEW hdm.vw_awd_queue
AS SELECT awd_queue.q_key,
    awd_queue.q_cd,
    awd_queue.q_desc,
    awd_queue.curr_row_flg,
    awd_queue.row_strt_dttm,
    awd_queue.row_stop_dttm,
    awd_queue.etl_load_cyc_key,
    awd_queue.src_sys_id
   FROM hdm.awd_queue;

CREATE OR REPLACE VIEW hdm.vw_awd_record_type
AS SELECT awd_record_type.rec_type_key,
    awd_record_type.rec_cd,
    awd_record_type.rec_desc,
    awd_record_type.curr_row_flg,
    awd_record_type.row_strt_dttm,
    awd_record_type.row_stop_dttm,
    awd_record_type.etl_load_cyc_key,
    awd_record_type.src_sys_id
   FROM hdm.awd_record_type;

CREATE OR REPLACE VIEW hdm.vw_awd_role
AS SELECT awd_role.role_key,
    awd_role.role_nm,
    awd_role.curr_row_flg,
    awd_role.row_strt_dttm,
    awd_role.row_stop_dttm,
    awd_role.etl_load_cyc_key,
    awd_role.src_sys_id
   FROM hdm.awd_role;

CREATE OR REPLACE VIEW hdm.vw_awd_role_privilege
AS SELECT awd_role_privilege.role_prvl_key,
    awd_role_privilege.rec_type_key,
    awd_role_privilege.role_key,
    awd_role_privilege.prvl_type_key,
    awd_role_privilege.day_key,
    awd_role_privilege.ut_cd,
    awd_role_privilege.exist_flg,
    awd_role_privilege.view_flg,
    awd_role_privilege.updt_flg,
    awd_role_privilege.wk_slctn_flg,
    awd_role_privilege.last_maint_dt,
    awd_role_privilege.curr_row_flg,
    awd_role_privilege.row_strt_dttm,
    awd_role_privilege.row_stop_dttm,
    awd_role_privilege.etl_load_cyc_key,
    awd_role_privilege.src_sys_id
   FROM hdm.awd_role_privilege;

CREATE OR REPLACE VIEW hdm.vw_awd_user
AS SELECT awd_user.usr_key,
    awd_user.grp_nm,
    awd_user.usr_stat_cd,
    awd_user.usr_stat_desc,
    awd_user.usr_id,
    awd_user.first_nm,
    awd_user.mint,
    awd_user.last_nm,
    awd_user.tax_id,
    awd_user.clsfcn,
    awd_user.lck_stat,
    awd_user.q_flg,
    awd_user.phon_nbr,
    awd_user.fax_nbr,
    awd_user.wk_slctn_cd,
    awd_user.fax_dept,
    awd_user.rout_usr_id,
    awd_user.sec_lvl_cd,
    awd_user.ut_cd,
    awd_user.last_maint_dt,
    awd_user.last_dt,
    awd_user.last_pswd_maint_dt,
    awd_user.awd_desktop_vrsn,
    awd_user.data_revs,
    awd_user.curr_row_flg,
    awd_user.row_strt_dttm,
    awd_user.row_stop_dttm,
    awd_user.etl_load_cyc_key,
    awd_user.src_sys_id
   FROM hdm.awd_user;

CREATE OR REPLACE VIEW hdm.vw_awd_user_queue
AS SELECT awd_user_queue.usr_q_key,
    awd_user_queue.q_key,
    awd_user_queue.usr_key,
    awd_user_queue.day_key,
    awd_user_queue.ut_cd,
    awd_user_queue.exist_flg,
    awd_user_queue.view_flg,
    awd_user_queue.updt_flg,
    awd_user_queue.wk_slctn_flg,
    awd_user_queue.curr_row_flg,
    awd_user_queue.row_strt_dttm,
    awd_user_queue.row_stop_dttm,
    awd_user_queue.etl_load_cyc_key,
    awd_user_queue.src_sys_id
   FROM hdm.awd_user_queue;

CREATE OR REPLACE VIEW hdm.vw_awd_user_role
AS SELECT awd_user_role.usr_role_key,
    awd_user_role.role_key,
    awd_user_role.usr_key,
    awd_user_role.day_key,
    awd_user_role.curr_row_flg,
    awd_user_role.row_strt_dttm,
    awd_user_role.row_stop_dttm,
    awd_user_role.etl_load_cyc_key,
    awd_user_role.src_sys_id
   FROM hdm.awd_user_role;

CREATE OR REPLACE VIEW hdm.vw_awd_user_work_type
AS SELECT awd_user_work_type.usr_wk_type_key,
    awd_user_work_type.rec_type_key,
    awd_user_work_type.wk_type_key,
    awd_user_work_type.usr_key,
    awd_user_work_type.day_key,
    awd_user_work_type.ut_cd,
    awd_user_work_type.exist_flg,
    awd_user_work_type.view_flg,
    awd_user_work_type.updt_flg,
    awd_user_work_type.wk_slctn_flg,
    awd_user_work_type.last_maint_dt,
    awd_user_work_type.curr_row_flg,
    awd_user_work_type.row_strt_dttm,
    awd_user_work_type.row_stop_dttm,
    awd_user_work_type.etl_load_cyc_key,
    awd_user_work_type.src_sys_id
   FROM hdm.awd_user_work_type;

CREATE OR REPLACE VIEW hdm.vw_awd_work_type
AS SELECT awd_work_type.wk_type_key,
    awd_work_type.wk_type_nm,
    awd_work_type.wk_type_desc,
    awd_work_type.curr_row_flg,
    awd_work_type.row_strt_dttm,
    awd_work_type.row_stop_dttm,
    awd_work_type.etl_load_cyc_key,
    awd_work_type.src_sys_id
   FROM hdm.awd_work_type;

CREATE OR REPLACE VIEW hdm.vw_balance_category
AS SELECT balance_category.bal_cat_key,
    balance_category.cat_desc,
    balance_category.cat_min,
    balance_category.cat_max
   FROM hdm.balance_category;

CREATE OR REPLACE VIEW hdm.vw_benchmark_index
AS SELECT benchmark_index.bmk_idx_key,
    benchmark_index.idx_id,
    benchmark_index.idx_nm,
    benchmark_index.idx_shrt_nm,
    benchmark_index.robeco_idx_id,
    benchmark_index.bloomberg_sym,
    benchmark_index.idx_type,
    benchmark_index.idx_type_desc,
    benchmark_index.bmk_legacy_id,
    benchmark_index.curr_row_flg,
    benchmark_index.row_strt_dttm,
    benchmark_index.row_stop_dttm,
    benchmark_index.etl_load_cyc_key,
    benchmark_index.src_sys_id
   FROM hdm.benchmark_index;

CREATE OR REPLACE VIEW hdm.vw_bluesky_transactions
AS SELECT bluesky_transactions.blue_sky_txn_key,
    bluesky_transactions.day_key,
    bluesky_transactions.fund_key,
    bluesky_transactions.reg_key,
    bluesky_transactions.shr_qty,
    bluesky_transactions.txn_amt,
    bluesky_transactions.mtd_rept_amt,
    bluesky_transactions.mtd_re_open_amt,
    bluesky_transactions.row_strt_dttm,
    bluesky_transactions.row_stop_dttm,
    bluesky_transactions.etl_load_cyc_key,
    bluesky_transactions.src_sys_id,
    bluesky_transactions.curr_row_flg
   FROM hdm.bluesky_transactions;

CREATE OR REPLACE VIEW hdm.vw_broker
AS SELECT broker.brkr_key,
    broker.posn_type_key,
    broker.brkr_nm,
    broker.st_str_nbr,
    broker.compst_nm,
    broker.brkr_acct_nbr,
    broker.curr_row_flg,
    broker.row_strt_dttm,
    broker.row_stop_dttm,
    broker.etl_load_cyc_key,
    broker.src_sys_id
   FROM hdm.broker;

CREATE OR REPLACE VIEW hdm.vw_broker_position
AS SELECT broker_position.brkr_posn_key,
    broker_position.brkr_key,
    broker_position.day_key,
    broker_position.ser_nbr,
    broker_position.sub_advsr_fund_id,
    broker_position.asset_id,
    broker_position.thrd_party_id,
    broker_position.othr_id,
    broker_position.trde_id,
    broker_position.asset_desc,
    broker_position.prod_nm,
    broker_position.asset_qty,
    broker_position.trde_dt,
    broker_position.sttl_dt,
    broker_position.txn_type,
    broker_position.purch_pr,
    broker_position.vend_pr,
    broker_position.mkt_val,
    broker_position.coupn,
    broker_position.mtry_dt,
    broker_position.trde_amt,
    broker_position.sttl_amt,
    broker_position.pldg_amt,
    broker_position.un_sttld_amt,
    broker_position.contct_nm,
    broker_position.phon_det,
    broker_position.fax_det,
    broker_position.addr,
    broker_position.email_id,
    broker_position.notes,
    broker_position.curr_row_flg,
    broker_position.row_strt_dttm,
    broker_position.row_stop_dttm,
    broker_position.etl_load_cyc_key,
    broker_position.src_sys_id
   FROM hdm.broker_position;

CREATE OR REPLACE VIEW hdm.vw_bud_account
AS SELECT bud_account.bud_acct_key,
    bud_account.acct_id,
    bud_account.acct_desc,
    bud_account.acct_type,
    bud_account.actv_flg,
    bud_account.cat_type,
    bud_account.expns_cat,
    bud_account.prpd_expns_flg,
    bud_account.acct_subt,
    bud_account.payee,
    bud_account.insr_usr,
    bud_account.insr_ts,
    bud_account.vrsn,
    bud_account.curr_row_flg,
    bud_account.row_strt_dttm,
    bud_account.row_stop_dttm,
    bud_account.etl_load_cyc_key,
    bud_account.src_sys_id
   FROM hdm.bud_account;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_accrual
AS SELECT bud_acct_accrual.acct_accrl_key,
    bud_acct_accrual.accrl_id,
    bud_acct_accrual.accrl_src,
    bud_acct_accrual.fiscl_yr,
    bud_acct_accrual.accrl_stat,
    bud_acct_accrual.ntes,
    bud_acct_accrual.insr_usr,
    bud_acct_accrual.insr_ts,
    bud_acct_accrual.vrsn,
    bud_acct_accrual.curr_row_flg,
    bud_acct_accrual.row_strt_dttm,
    bud_acct_accrual.row_stop_dttm,
    bud_acct_accrual.etl_load_cyc_key,
    bud_acct_accrual.src_sys_id
   FROM hdm.bud_acct_accrual;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_accrual_fnd
AS SELECT bud_acct_accrual_fnd.acct_accural_fund_key,
    bud_acct_accrual_fnd.acct_accrl_key,
    bud_acct_accrual_fnd.src_bud_key,
    bud_acct_accrual_fnd.src_assmpn_chg_key,
    bud_acct_accrual_fnd.expns_line_item_key,
    bud_acct_accrual_fnd.bud_fund_compst_key,
    bud_acct_accrual_fnd.curr_acct_accrl_fund_key,
    bud_acct_accrual_fnd.accrl_id,
    bud_acct_accrual_fnd.daily_accrl,
    bud_acct_accrual_fnd.orig_bud,
    bud_acct_accrual_fnd.rvsd_bud,
    bud_acct_accrual_fnd.remng_to_accrl,
    bud_acct_accrual_fnd.fytd_expns,
    bud_acct_accrual_fnd.efftv_dt,
    bud_acct_accrual_fnd.accrl_strt_dt,
    bud_acct_accrual_fnd.accrl_by_dt,
    bud_acct_accrual_fnd.ssb_efftv_dt,
    bud_acct_accrual_fnd.nbr_accrl_days,
    bud_acct_accrual_fnd.vrsn,
    bud_acct_accrual_fnd.curr_row_flg,
    bud_acct_accrual_fnd.row_strt_dttm,
    bud_acct_accrual_fnd.row_stop_dttm,
    bud_acct_accrual_fnd.etl_load_cyc_key,
    bud_acct_accrual_fnd.src_sys_id
   FROM hdm.bud_acct_accrual_fnd;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_adjust_fnd
AS SELECT bud_acct_adjust_fnd.acct_adjmt_key,
    bud_acct_adjust_fnd.bud_fund_compst_key,
    bud_acct_adjust_fnd.bud_key,
    bud_acct_adjust_fnd.expns_line_item_key,
    bud_acct_adjust_fnd.assmpn_chg_key,
    bud_acct_adjust_fnd.expns_acct_key,
    bud_acct_adjust_fnd.paybl_acct_key,
    bud_acct_adjust_fnd.dbt_acct_key,
    bud_acct_adjust_fnd.cr_acct_key,
    bud_acct_adjust_fnd.cat_accrl_dim_key,
    bud_acct_adjust_fnd.adjmt_id,
    bud_acct_adjust_fnd.fiscl_yr,
    bud_acct_adjust_fnd.adjmt_amt,
    bud_acct_adjust_fnd.act_stat,
    bud_acct_adjust_fnd.ntes,
    bud_acct_adjust_fnd.efftv_dt,
    bud_acct_adjust_fnd.ssb_efftv_dt,
    bud_acct_adjust_fnd.adjmt_type,
    bud_acct_adjust_fnd.insr_usr,
    bud_acct_adjust_fnd.insr_ts,
    bud_acct_adjust_fnd.vrsn,
    bud_acct_adjust_fnd.curr_row_flg,
    bud_acct_adjust_fnd.row_strt_dttm,
    bud_acct_adjust_fnd.row_stop_dttm,
    bud_acct_adjust_fnd.etl_load_cyc_key,
    bud_acct_adjust_fnd.src_sys_id
   FROM hdm.bud_acct_adjust_fnd;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_transfer
AS SELECT bud_acct_transfer.acct_xfer_key,
    bud_acct_transfer.xfer_id,
    bud_acct_transfer.xfer_grp_nm,
    bud_acct_transfer.entry_nm,
    bud_acct_transfer.efftv_dt,
    bud_acct_transfer.ssb_efftv_dt,
    bud_acct_transfer.insr_usr,
    bud_acct_transfer.insr_ts,
    bud_acct_transfer.editbl_flg,
    bud_acct_transfer.xfer_stat,
    bud_acct_transfer.ntes,
    bud_acct_transfer.vrsn,
    bud_acct_transfer.curr_row_flg,
    bud_acct_transfer.row_strt_dttm,
    bud_acct_transfer.row_stop_dttm,
    bud_acct_transfer.etl_load_cyc_key,
    bud_acct_transfer.src_sys_id
   FROM hdm.bud_acct_transfer;

CREATE OR REPLACE VIEW hdm.vw_bud_acct_transfer_fnd
AS SELECT bud_acct_transfer_fnd.acct_xfer_fund_key,
    bud_acct_transfer_fnd.acct_xfer_key,
    bud_acct_transfer_fnd.src_bud_acct_key,
    bud_acct_transfer_fnd.dest_bud_acct_key,
    bud_acct_transfer_fnd.src_expns_line_item_key,
    bud_acct_transfer_fnd.dest_expns_line_item_key,
    bud_acct_transfer_fnd.bud_fund_compst_key,
    bud_acct_transfer_fnd.dbt_acct_key,
    bud_acct_transfer_fnd.cr_acct_key,
    bud_acct_transfer_fnd.xfer_id,
    bud_acct_transfer_fnd.xfer_val,
    bud_acct_transfer_fnd.vrsn,
    bud_acct_transfer_fnd.curr_row_flg,
    bud_acct_transfer_fnd.row_strt_dttm,
    bud_acct_transfer_fnd.row_stop_dttm,
    bud_acct_transfer_fnd.etl_load_cyc_key,
    bud_acct_transfer_fnd.src_sys_id
   FROM hdm.bud_acct_transfer_fnd;

CREATE OR REPLACE VIEW hdm.vw_bud_asmpt_change
AS SELECT bud_asmpt_change.assmpn_chg_key,
    bud_asmpt_change.adjmt_type,
    bud_asmpt_change.chg_amt,
    bud_asmpt_change.efftv_dt,
    bud_asmpt_change.fiscl_yr,
    bud_asmpt_change.editbl_flg,
    bud_asmpt_change.assmpn_chg_ntes,
    bud_asmpt_change.assmpn_id,
    bud_asmpt_change.liab_adj_bud_id,
    bud_asmpt_change.insr_usr,
    bud_asmpt_change.insr_ts,
    bud_asmpt_change.vrsn,
    bud_asmpt_change.curr_row_flg,
    bud_asmpt_change.row_strt_dttm,
    bud_asmpt_change.row_stop_dttm,
    bud_asmpt_change.etl_load_cyc_key,
    bud_asmpt_change.src_sys_id
   FROM hdm.bud_asmpt_change;

CREATE OR REPLACE VIEW hdm.vw_bud_asmpt_change_fund
AS SELECT bud_asmpt_change_fund.assmpn_chg_fund_key,
    bud_asmpt_change_fund.bud_fund_compst_key,
    bud_asmpt_change_fund.bud_key,
    bud_asmpt_change_fund.assmpn_chg_key,
    bud_asmpt_change_fund.expns_line_item_key,
    bud_asmpt_change_fund.data_src_dim_key,
    bud_asmpt_change_fund.rt,
    bud_asmpt_change_fund.allocn,
    bud_asmpt_change_fund.data_src_val,
    bud_asmpt_change_fund.assmpn_chg_per_fund_ntes,
    bud_asmpt_change_fund.adjmt,
    bud_asmpt_change_fund.rsdl_val,
    bud_asmpt_change_fund.vrsn,
    bud_asmpt_change_fund.curr_row_flg,
    bud_asmpt_change_fund.row_strt_dttm,
    bud_asmpt_change_fund.row_stop_dttm,
    bud_asmpt_change_fund.etl_load_cyc_key,
    bud_asmpt_change_fund.src_sys_id
   FROM hdm.bud_asmpt_change_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_cat_accrual_dim
AS SELECT bud_cat_accrual_dim.cat_accrl_dim_key,
    bud_cat_accrual_dim.cat_accrl_id,
    bud_cat_accrual_dim.ntes,
    bud_cat_accrual_dim.accrl_type,
    bud_cat_accrual_dim.insr_dt,
    bud_cat_accrual_dim.insr_usr,
    bud_cat_accrual_dim.rndedup_to_1penny_flg,
    bud_cat_accrual_dim.vrsn,
    bud_cat_accrual_dim.curr_row_flg,
    bud_cat_accrual_dim.row_strt_dttm,
    bud_cat_accrual_dim.row_stop_dttm,
    bud_cat_accrual_dim.etl_load_cyc_key,
    bud_cat_accrual_dim.src_sys_id
   FROM hdm.bud_cat_accrual_dim;

CREATE OR REPLACE VIEW hdm.vw_bud_cat_accrual_fact
AS SELECT bud_cat_accrual_fact.cat_accrl_fact_key,
    bud_cat_accrual_fact.bud_fund_compst_key,
    bud_cat_accrual_fact.expns_acct_key,
    bud_cat_accrual_fact.paybl_acct_key,
    bud_cat_accrual_fact.dbt_acct_key,
    bud_cat_accrual_fact.cr_acct_key,
    bud_cat_accrual_fact.cat_accrl_dim_key,
    bud_cat_accrual_fact.cat_accrl_id,
    bud_cat_accrual_fact.daily_accrl,
    bud_cat_accrual_fact.adjmt_val,
    bud_cat_accrual_fact.fiscl_yr,
    bud_cat_accrual_fact.accrl_stat,
    bud_cat_accrual_fact.efftv_dt,
    bud_cat_accrual_fact.ssb_efftv_dt,
    bud_cat_accrual_fact.expn_dt,
    bud_cat_accrual_fact.vrsn,
    bud_cat_accrual_fact.curr_row_flg,
    bud_cat_accrual_fact.row_strt_dttm,
    bud_cat_accrual_fact.row_stop_dttm,
    bud_cat_accrual_fact.etl_load_cyc_key,
    bud_cat_accrual_fact.src_sys_id
   FROM hdm.bud_cat_accrual_fact;

CREATE OR REPLACE VIEW hdm.vw_bud_cat_acct_accrual
AS SELECT bud_cat_acct_accrual.cat_acct_accrl_key,
    bud_cat_acct_accrual.cat_accrl_dim_key,
    bud_cat_acct_accrual.acct_accrl_key,
    bud_cat_acct_accrual.curr_row_flg,
    bud_cat_acct_accrual.row_strt_dttm,
    bud_cat_acct_accrual.row_stop_dttm,
    bud_cat_acct_accrual.etl_load_cyc_key,
    bud_cat_acct_accrual.src_sys_id
   FROM hdm.bud_cat_acct_accrual;

CREATE OR REPLACE VIEW hdm.vw_bud_data_source_dim
AS SELECT bud_data_source_dim.data_src_dim_key,
    bud_data_source_dim.data_src_id,
    bud_data_source_dim.fiscl_yr,
    bud_data_source_dim.src_type,
    bud_data_source_dim.shrt_nm,
    bud_data_source_dim.src_nm,
    bud_data_source_dim.tna_dt,
    bud_data_source_dim.tna_end_dt,
    bud_data_source_dim.tna_strt_dt,
    bud_data_source_dim.nbr_accts_dt,
    bud_data_source_dim.reg24f2_dt,
    bud_data_source_dim.data_src_ntes,
    bud_data_source_dim.invc_strt_dt,
    bud_data_source_dim.invc_end_dt,
    bud_data_source_dim.invc_fiscl_yr,
    bud_data_source_dim.invc_fee_type,
    bud_data_source_dim.reg24f2_strt_dt,
    bud_data_source_dim.reg24f2_end_dt,
    bud_data_source_dim.calc_mthd,
    bud_data_source_dim.insr_usr,
    bud_data_source_dim.insr_ts,
    bud_data_source_dim.vrsn,
    bud_data_source_dim.curr_row_flg,
    bud_data_source_dim.row_strt_dttm,
    bud_data_source_dim.row_stop_dttm,
    bud_data_source_dim.etl_load_cyc_key,
    bud_data_source_dim.src_sys_id
   FROM hdm.bud_data_source_dim;

CREATE OR REPLACE VIEW hdm.vw_bud_data_source_fact
AS SELECT bud_data_source_fact.data_src_fact_key,
    bud_data_source_fact.data_src_dim_key,
    bud_data_source_fact.trste_elctn_key,
    bud_data_source_fact.qtrly_sal,
    bud_data_source_fact.neg_carrovr_bal_yr,
    bud_data_source_fact.neg_carrovr_bal_amt,
    bud_data_source_fact.fee_per_mil,
    bud_data_source_fact.fee_rqrd_min,
    bud_data_source_fact.ric_pct,
    bud_data_source_fact.tot_fee,
    bud_data_source_fact.tot_issr_mkt_cap,
    bud_data_source_fact.vrsn,
    bud_data_source_fact.curr_row_flg,
    bud_data_source_fact.row_strt_dttm,
    bud_data_source_fact.row_stop_dttm,
    bud_data_source_fact.etl_load_cyc_key,
    bud_data_source_fact.src_sys_id
   FROM hdm.bud_data_source_fact;

CREATE OR REPLACE VIEW hdm.vw_bud_data_source_fund
AS SELECT bud_data_source_fund.bud_data_src_fund_key,
    bud_data_source_fund.data_src_dim_key,
    bud_data_source_fund.bud_fund_compst_key,
    bud_data_source_fund.avg_mthly_tna,
    bud_data_source_fund.issr_pct,
    bud_data_source_fund.rt,
    bud_data_source_fund.reqs_fee_flg,
    bud_data_source_fund.src_val,
    bud_data_source_fund.tot_fee,
    bud_data_source_fund.otpt_val,
    bud_data_source_fund.adjmt,
    bud_data_source_fund.data_src_fund_nte,
    bud_data_source_fund.sby_multr,
    bud_data_source_fund.hbr_nbr_accts,
    bud_data_source_fund.broadridge_nbr_accts,
    bud_data_source_fund.reinvs,
    bud_data_source_fund.bllbl_act,
    bud_data_source_fund.capstk_adjmts,
    bud_data_source_fund.tna_val,
    bud_data_source_fund.rsdl_val,
    bud_data_source_fund.subs,
    bud_data_source_fund.redmpns,
    bud_data_source_fund.net_sales_act,
    bud_data_source_fund.annl_net_sales_act,
    bud_data_source_fund.subs_including_adjmts,
    bud_data_source_fund.avg_daily_tna,
    bud_data_source_fund.adj_avg_daily_tna,
    bud_data_source_fund.allocn_adjmt,
    bud_data_source_fund.orig_allocn,
    bud_data_source_fund.greatest_commn_denomr,
    bud_data_source_fund.tot_avg_daily_tna,
    bud_data_source_fund.tot_compnsn,
    bud_data_source_fund.vrsn,
    bud_data_source_fund.curr_row_flg,
    bud_data_source_fund.row_strt_dttm,
    bud_data_source_fund.row_stop_dttm,
    bud_data_source_fund.etl_load_cyc_key,
    bud_data_source_fund.src_sys_id
   FROM hdm.bud_data_source_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_expense_line_item
AS SELECT bud_expense_line_item.expns_line_item_key,
    bud_expense_line_item.expns_line_item_nm,
    bud_expense_line_item.expns_line_item_id,
    bud_expense_line_item.line_item_ntes,
    bud_expense_line_item.actv_flg,
    bud_expense_line_item.allocn_type,
    bud_expense_line_item.expns_item_nm,
    bud_expense_line_item.expns_cat_nm,
    bud_expense_line_item.invc_vends,
    bud_expense_line_item.expns_acct_id,
    bud_expense_line_item.paybl_acct_id,
    bud_expense_line_item.prapaid_expns_flg,
    bud_expense_line_item.hddn_flg,
    bud_expense_line_item.hddn_rsn,
    bud_expense_line_item.insr_usr,
    bud_expense_line_item.insr_ts,
    bud_expense_line_item.vrsn,
    bud_expense_line_item.curr_row_flg,
    bud_expense_line_item.row_strt_dttm,
    bud_expense_line_item.row_stop_dttm,
    bud_expense_line_item.etl_load_cyc_key,
    bud_expense_line_item.src_sys_id
   FROM hdm.bud_expense_line_item;

CREATE OR REPLACE VIEW hdm.vw_bud_fund_composite
AS SELECT bud_fund_composite.bud_fund_compst_key,
    bud_fund_composite.asset_type,
    bud_fund_composite.asset_type_desc,
    bud_fund_composite.fund_nm,
    bud_fund_composite.fund_ord,
    bud_fund_composite.ssb_cd,
    bud_fund_composite.ssb_shrt_cd,
    bud_fund_composite.parn_compst_id,
    bud_fund_composite.fund_compst_id,
    bud_fund_composite.incpn_dt,
    bud_fund_composite.closd_dt,
    bud_fund_composite.insr_usr,
    bud_fund_composite.insr_ts,
    bud_fund_composite.vrsn,
    bud_fund_composite.row_strt_dttm,
    bud_fund_composite.row_stop_dttm,
    bud_fund_composite.curr_row_flg,
    bud_fund_composite.etl_load_cyc_key,
    bud_fund_composite.src_sys_id
   FROM hdm.bud_fund_composite;

CREATE OR REPLACE VIEW hdm.vw_bud_fund_multiplier
AS SELECT bud_fund_multiplier.fund_multr_key,
    bud_fund_multiplier.bud_fund_compst_key,
    bud_fund_multiplier.fiscl_yr,
    bud_fund_multiplier.multr,
    bud_fund_multiplier.curr_row_flg,
    bud_fund_multiplier.row_strt_dttm,
    bud_fund_multiplier.row_stop_dttm,
    bud_fund_multiplier.etl_load_cyc_key,
    bud_fund_multiplier.src_sys_id
   FROM hdm.bud_fund_multiplier;

CREATE OR REPLACE VIEW hdm.vw_bud_invoice
AS SELECT bud_invoice.bud_invc_key,
    bud_invoice.invc_id,
    bud_invoice.allocn_id,
    bud_invoice.invc_line_id,
    bud_invoice.fiscl_yr,
    bud_invoice.invc_allocn_type,
    bud_invoice.invc_nbr,
    bud_invoice.invc_vend,
    bud_invoice.ssb_efftv_dt,
    bud_invoice.pend_flg,
    bud_invoice.invc_src,
    bud_invoice.invc_desc,
    bud_invoice.acctg_dt,
    bud_invoice.del_flg,
    bud_invoice.buded_flg,
    bud_invoice.rptb1099_flg,
    bud_invoice.acctg_per,
    bud_invoice.ntes,
    bud_invoice.insr_ts,
    bud_invoice.insr_usr,
    bud_invoice.vrsn,
    bud_invoice.curr_row_flg,
    bud_invoice.row_strt_dttm,
    bud_invoice.row_stop_dttm,
    bud_invoice.etl_load_cyc_key,
    bud_invoice.src_sys_id
   FROM hdm.bud_invoice;

CREATE OR REPLACE VIEW hdm.vw_bud_invoice_alloc
AS SELECT bud_invoice_alloc.bud_invc_alloc_key,
    bud_invoice_alloc.bud_invc_key,
    bud_invoice_alloc.data_src_dim_key,
    bud_invoice_alloc.expns_line_item_key,
    bud_invoice_alloc.bud_key,
    bud_invoice_alloc.tax_amt,
    bud_invoice_alloc.invc_amt,
    bud_invoice_alloc.invc_line_amt,
    bud_invoice_alloc.vrsn,
    bud_invoice_alloc.allocn_amt,
    bud_invoice_alloc.curr_row_flg,
    bud_invoice_alloc.row_strt_dttm,
    bud_invoice_alloc.row_stop_dttm,
    bud_invoice_alloc.etl_load_cyc_key,
    bud_invoice_alloc.src_sys_id
   FROM hdm.bud_invoice_alloc;

CREATE OR REPLACE VIEW hdm.vw_bud_invoice_alloc_fund
AS SELECT bud_invoice_alloc_fund.bud_invc_alloc_fund_key,
    bud_invoice_alloc_fund.bud_invc_key,
    bud_invoice_alloc_fund.data_src_dim_key,
    bud_invoice_alloc_fund.bud_fund_compst_key,
    bud_invoice_alloc_fund.expns_line_item_key,
    bud_invoice_alloc_fund.fund_adjmt,
    bud_invoice_alloc_fund.fund_rsdl_val,
    bud_invoice_alloc_fund.fund_rt,
    bud_invoice_alloc_fund.tax_rt,
    bud_invoice_alloc_fund.fund_data_src_val,
    bud_invoice_alloc_fund.fund_allocn,
    bud_invoice_alloc_fund.fund_tax_allocn,
    bud_invoice_alloc_fund.fund_invc_allocn,
    bud_invoice_alloc_fund.ntes,
    bud_invoice_alloc_fund.vrsn,
    bud_invoice_alloc_fund.curr_row_flg,
    bud_invoice_alloc_fund.row_strt_dttm,
    bud_invoice_alloc_fund.row_stop_dttm,
    bud_invoice_alloc_fund.etl_load_cyc_key,
    bud_invoice_alloc_fund.src_sys_id
   FROM hdm.bud_invoice_alloc_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_liability
AS SELECT bud_liability.liab_key,
    bud_liability.bud_id,
    bud_liability.line_item_id,
    bud_liability.paybl_acct_id,
    bud_liability.data_src_id,
    bud_liability.editbl_flg,
    bud_liability.ntes,
    bud_liability.insr_usr,
    bud_liability.insr_ts,
    bud_liability.vrsn,
    bud_liability.curr_row_flg,
    bud_liability.row_strt_dttm,
    bud_liability.row_stop_dttm,
    bud_liability.etl_load_cyc_key,
    bud_liability.src_sys_id
   FROM hdm.bud_liability;

CREATE OR REPLACE VIEW hdm.vw_bud_liability_fund
AS SELECT bud_liability_fund.liab_fund_key,
    bud_liability_fund.bud_key,
    bud_liability_fund.expns_line_item_key,
    bud_liability_fund.bud_acct_key,
    bud_liability_fund.data_src_dim_key,
    bud_liability_fund.bud_fund_compst_key,
    bud_liability_fund.liab_key,
    bud_liability_fund.prir_yr_pmt_diff_bud_key,
    bud_liability_fund.allocn,
    bud_liability_fund.data_src_val,
    bud_liability_fund.fytd_liab,
    bud_liability_fund.fytd_expns,
    bud_liability_fund.prir_yr_fye_liab,
    bud_liability_fund.prir_yr_pmt_diff,
    bud_liability_fund.pend_invc_tot,
    bud_liability_fund.prcsd_invc_tot,
    bud_liability_fund.remng_to_accrue,
    bud_liability_fund.fye_liab,
    bud_liability_fund.needed_fye_liab,
    bud_liability_fund.suggested_liab_adjmt,
    bud_liability_fund.actl_liab_adjmt,
    bud_liability_fund.one_tm_adjmts,
    bud_liability_fund.line_item_bud,
    bud_liability_fund.vrsn,
    bud_liability_fund.curr_row_flg,
    bud_liability_fund.row_strt_dttm,
    bud_liability_fund.row_stop_dttm,
    bud_liability_fund.etl_load_cyc_key,
    bud_liability_fund.src_sys_id
   FROM hdm.bud_liability_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_lineitem_alloc
AS SELECT bud_lineitem_alloc.lineitem_allocn_key,
    bud_lineitem_alloc.bud_key,
    bud_lineitem_alloc.expns_line_item_key,
    bud_lineitem_alloc.data_src_dim_key,
    bud_lineitem_alloc.allocn_amt,
    bud_lineitem_alloc.line_item_to_be_reallocd,
    bud_lineitem_alloc.ntes,
    bud_lineitem_alloc.insr_usr,
    bud_lineitem_alloc.insr_ts,
    bud_lineitem_alloc.vrsn,
    bud_lineitem_alloc.curr_row_flg,
    bud_lineitem_alloc.row_strt_dttm,
    bud_lineitem_alloc.row_stop_dttm,
    bud_lineitem_alloc.etl_load_cyc_key,
    bud_lineitem_alloc.src_sys_id
   FROM hdm.bud_lineitem_alloc;

CREATE OR REPLACE VIEW hdm.vw_bud_lineitem_alloc_fund
AS SELECT bud_lineitem_alloc_fund.line_itm_allc_fund_key,
    bud_lineitem_alloc_fund.bud_strt_dt_key,
    bud_lineitem_alloc_fund.bud_fund_compst_key,
    bud_lineitem_alloc_fund.bud_key,
    bud_lineitem_alloc_fund.expns_line_item_key,
    bud_lineitem_alloc_fund.data_src_dim_key,
    bud_lineitem_alloc_fund.rt,
    bud_lineitem_alloc_fund.data_src_val,
    bud_lineitem_alloc_fund.allocn_amt,
    bud_lineitem_alloc_fund.allocn_per_fund_ntes,
    bud_lineitem_alloc_fund.adjmt,
    bud_lineitem_alloc_fund.rsdl_val,
    bud_lineitem_alloc_fund.orig_allocn,
    bud_lineitem_alloc_fund.reallocd_remng,
    bud_lineitem_alloc_fund.fytd_expns,
    bud_lineitem_alloc_fund.assmpn_chg,
    bud_lineitem_alloc_fund.to_be_reallocd,
    bud_lineitem_alloc_fund.src_bud_key,
    bud_lineitem_alloc_fund.pmt_difftl,
    bud_lineitem_alloc_fund.allocn_ntes,
    bud_lineitem_alloc_fund.insr_usr,
    bud_lineitem_alloc_fund.insr_ts,
    bud_lineitem_alloc_fund.vrsn,
    bud_lineitem_alloc_fund.curr_row_flg,
    bud_lineitem_alloc_fund.row_strt_dttm,
    bud_lineitem_alloc_fund.row_stop_dttm,
    bud_lineitem_alloc_fund.etl_load_cyc_key,
    bud_lineitem_alloc_fund.src_sys_id
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
AS SELECT bud_pmt_diff.pmt_diff_key,
    bud_pmt_diff.bud_key,
    bud_pmt_diff.expns_line_item_key,
    bud_pmt_diff.fiscl_yr,
    bud_pmt_diff.invc_expctation,
    bud_pmt_diff.pmt_difftl,
    bud_pmt_diff.remng_liab,
    bud_pmt_diff.adjmt_type,
    bud_pmt_diff.reshuffle_pmt_diff_flg,
    bud_pmt_diff.editbl_flg,
    bud_pmt_diff.prir_yr_flg,
    bud_pmt_diff.ntes,
    bud_pmt_diff.insr_usr,
    bud_pmt_diff.insr_ts,
    bud_pmt_diff.vrsn,
    bud_pmt_diff.curr_row_flg,
    bud_pmt_diff.row_strt_dttm,
    bud_pmt_diff.row_stop_dttm,
    bud_pmt_diff.etl_load_cyc_key,
    bud_pmt_diff.src_sys_id
   FROM hdm.bud_pmt_diff;

CREATE OR REPLACE VIEW hdm.vw_bud_pmt_diff_fnd_invc
AS SELECT bud_pmt_diff_fnd_invc.pmt_diff_fund_invc_key,
    bud_pmt_diff_fnd_invc.bud_key,
    bud_pmt_diff_fnd_invc.expns_line_item_key,
    bud_pmt_diff_fnd_invc.bud_fund_compst_key,
    bud_pmt_diff_fnd_invc.fund_lvl_bud_key,
    bud_pmt_diff_fnd_invc.bud_invc_key,
    bud_pmt_diff_fnd_invc.fiscl_yr,
    bud_pmt_diff_fnd_invc.allocn_val,
    bud_pmt_diff_fnd_invc.stat,
    bud_pmt_diff_fnd_invc.prir_yr_flg,
    bud_pmt_diff_fnd_invc.vrsn,
    bud_pmt_diff_fnd_invc.curr_row_flg,
    bud_pmt_diff_fnd_invc.row_strt_dttm,
    bud_pmt_diff_fnd_invc.row_stop_dttm,
    bud_pmt_diff_fnd_invc.etl_load_cyc_key,
    bud_pmt_diff_fnd_invc.src_sys_id
   FROM hdm.bud_pmt_diff_fnd_invc;

CREATE OR REPLACE VIEW hdm.vw_bud_pmt_diff_fund
AS SELECT bud_pmt_diff_fund.pmt_diff_fund_key,
    bud_pmt_diff_fund.bud_key,
    bud_pmt_diff_fund.expns_line_item_key,
    bud_pmt_diff_fund.bud_fund_compst_key,
    bud_pmt_diff_fund.fund_lvl_bud_key,
    bud_pmt_diff_fund.fiscl_yr,
    bud_pmt_diff_fund.orig_allocn,
    bud_pmt_diff_fund.tot_invcs_paid,
    bud_pmt_diff_fund.tot_invcs_pend,
    bud_pmt_diff_fund.acct_xfers,
    bud_pmt_diff_fund.pmt_difftl,
    bud_pmt_diff_fund.remng_liab,
    bud_pmt_diff_fund.assmpn_chg_allocn,
    bud_pmt_diff_fund.prir_yr_flg,
    bud_pmt_diff_fund.vrsn,
    bud_pmt_diff_fund.curr_row_flg,
    bud_pmt_diff_fund.row_strt_dttm,
    bud_pmt_diff_fund.row_stop_dttm,
    bud_pmt_diff_fund.etl_load_cyc_key,
    bud_pmt_diff_fund.src_sys_id
   FROM hdm.bud_pmt_diff_fund;

CREATE OR REPLACE VIEW hdm.vw_bud_trustee_election
AS SELECT bud_trustee_election.trste_elctn_key,
    bud_trustee_election.bud_fund_compst_key,
    bud_trustee_election.trste_elctn_id,
    bud_trustee_election.trste_nm,
    bud_trustee_election.fiscl_yr,
    bud_trustee_election.data_src_val,
    bud_trustee_election.ntes,
    bud_trustee_election.insr_usr,
    bud_trustee_election.insr_ts,
    bud_trustee_election.vrsn,
    bud_trustee_election.curr_row_flg,
    bud_trustee_election.row_strt_dttm,
    bud_trustee_election.row_stop_dttm,
    bud_trustee_election.etl_load_cyc_key,
    bud_trustee_election.src_sys_id
   FROM hdm.bud_trustee_election;

CREATE OR REPLACE VIEW hdm.vw_budget
AS SELECT budget.bud_key,
    budget.bud_id,
    budget.bud_type,
    budget.end_dt,
    budget.fiscl_yr,
    budget.init_bud_w,
    budget.strt_dt,
    budget.bud_desc,
    budget.bud_src,
    budget.fiscl_ye_flg,
    budget.insr_usr,
    budget.insr_ts,
    budget.vrsn,
    budget.curr_row_flg,
    budget.row_strt_dttm,
    budget.row_stop_dttm,
    budget.etl_load_cyc_key,
    budget.src_sys_id
   FROM hdm.budget;

CREATE OR REPLACE VIEW hdm.vw_business_line
AS SELECT business_line.bus_line_key,
    business_line.bus_line,
    business_line.invc_fee_desc,
    business_line.curr_row_flg,
    business_line.row_strt_dttm,
    business_line.row_stop_dttm,
    business_line.etl_load_cyc_key,
    business_line.src_sys_id
   FROM hdm.business_line;

CREATE OR REPLACE VIEW hdm.vw_calendar
AS SELECT calendar.day_key,
    calendar.cal_day,
    calendar.cal_day_shrt_nm,
    calendar.cal_day_long_nm,
    calendar.bus_day_flg,
    calendar.mkt_hol_flg,
    calendar.wk_cd,
    calendar.wk_nbr,
    calendar.wk_shrt_nm,
    calendar.wk_long_nm,
    calendar.wk_ending_dt,
    calendar.wk_days_cnt,
    calendar.mo_cd,
    calendar.mo_nbr,
    calendar.mo_nm,
    calendar.mo_shrt_nm,
    calendar.mo_long_nm,
    calendar.cal_mo_end_flg,
    calendar.bus_mo_end_flg,
    calendar.mo_ending_dt,
    calendar.mo_days_cnt,
    calendar.qtr_cd,
    calendar.qtr_nbr,
    calendar.qtr_shrt_nm,
    calendar.qtr_long_nm,
    calendar.cal_qtr_end_flg,
    calendar.bus_qtr_end_flg,
    calendar.qtr_ending_dt,
    calendar.qtr_days_cnt,
    calendar.yr_cd,
    calendar.yr_nbr,
    calendar.yr_shrt_nm,
    calendar.yr_long_nm,
    calendar.yr_ending_dt,
    calendar.ye_flg,
    calendar.yr_last_bus_day_flg,
    calendar.yr_days_cnt,
    calendar.fiscl_wk_cd,
    calendar.fiscl_wk_nbr,
    calendar.fiscl_wk_shrt_nm,
    calendar.fiscl_wk_long_nm,
    calendar.fiscl_wk_ending_dt,
    calendar.fiscl_wk_days_cnt,
    calendar.fiscl_mo_cd,
    calendar.fiscl_mo_nbr,
    calendar.fiscl_mo_shrt_nm,
    calendar.fiscl_mo_long_nm,
    calendar.fiscl_mo_ending_dt,
    calendar.fiscl_mo_end_flg,
    calendar.fiscl_mo_last_bus_day_flg,
    calendar.fiscl_mo_days_cnt,
    calendar.fiscl_qtr_cd,
    calendar.fiscl_qtr_nbr,
    calendar.fiscl_qtr_shrt_nm,
    calendar.fiscl_qtr_long_nm,
    calendar.fiscl_qtr_ending_flg,
    calendar.fiscl_qtr_last_bus_day_flg,
    calendar.fiscl_qtr_ending_dt,
    calendar.fiscl_qtr_days_cnt,
    calendar.fiscl_yr_cd,
    calendar.fiscl_yr_shrt_nm,
    calendar.fiscl_yr_long_nm,
    calendar.fiscl_yr_ending_dt,
    calendar.fiscl_yr_ending_flg,
    calendar.fiscl_yr_last_bus_day_flg,
    calendar.fiscl_yr_nbr,
    calendar.fiscl_yr_days_cnt,
    calendar.curr_row_flg,
    calendar.row_strt_dttm,
    calendar.row_stop_dttm,
    calendar.etl_load_cyc_key,
    calendar.src_sys_id
   FROM hdm.calendar;

CREATE OR REPLACE VIEW hdm.vw_cash_holding
AS SELECT cash_holding.cash_holdg_key,
    cash_holding.fund_compst_key,
    cash_holding.day_key,
    cash_holding.cash,
    cash_holding.shrt_term_invmt,
    cash_holding.recbl,
    cash_holding.paybl,
    cash_holding.net_shrs,
    cash_holding.net_asset,
    cash_holding.curr_row_flg,
    cash_holding.row_strt_dttm,
    cash_holding.row_stop_dttm,
    cash_holding.etl_load_cyc_key,
    cash_holding.src_sys_id,
    cash_holding.cash_coltrl
   FROM hdm.cash_holding;

CREATE OR REPLACE VIEW hdm.vw_cash_reason
AS SELECT cash_reason.cash_rsn__key,
    cash_reason.cash_rsn_cd,
    cash_reason.cash_rsn_desc
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
AS SELECT cashflow_activity.cashflow_act_hist_key,
    cashflow_activity.fund_key,
    cashflow_activity.day_key,
    cashflow_activity.sub_shrs,
    cashflow_activity.sub_amt,
    cashflow_activity.redmpn_shrs,
    cashflow_activity.redmpn_amt,
    cashflow_activity.net_shrs,
    cashflow_activity.net_cashflow_amt,
    cashflow_activity.curr_row_flg,
    cashflow_activity.row_strt_dttm,
    cashflow_activity.row_stop_dttm,
    cashflow_activity.etl_load_cyc_key,
    cashflow_activity.src_sys_id
   FROM hdm.cashflow_activity;

CREATE OR REPLACE VIEW hdm.vw_cashflow_history
AS SELECT cashflow_history.cashflow_hist_key,
    cashflow_history.reg_key,
    cashflow_history.dlr_key,
    cashflow_history.shrhldr_key,
    cashflow_history.acct_key,
    cashflow_history.fund_key,
    cashflow_history.pmt_mthd_key,
    cashflow_history.trde_dt_key,
    cashflow_history.confirm_dt_key,
    cashflow_history.acct_nbr,
    cashflow_history.sub_shrs,
    cashflow_history.sub_amt,
    cashflow_history.redmpn_shrs,
    cashflow_history.redmpn_amt,
    cashflow_history.ofrg_pr,
    cashflow_history.nav,
    cashflow_history.dlr_commsn_amt,
    cashflow_history.undr_wrtr_commsn_amt,
    cashflow_history.adv_commsn_amt,
    cashflow_history.net_shrs,
    cashflow_history.net_cashflow_amt,
    cashflow_history.row_strt_dttm,
    cashflow_history.row_stop_dttm,
    cashflow_history.etl_load_cyc_key,
    cashflow_history.src_sys_id,
    cashflow_history.curr_row_flg
   FROM hdm.cashflow_history;

CREATE OR REPLACE VIEW hdm.vw_cct_city_tax_list
AS SELECT cct_city_tax_list.city_tax_list_key,
    cct_city_tax_list.city_tax_desc,
    cct_city_tax_list.curr_row_flg,
    cct_city_tax_list.row_strt_dttm,
    cct_city_tax_list.row_stop_dttm,
    cct_city_tax_list.etl_load_cyc_key,
    cct_city_tax_list.src_sys_id
   FROM hdm.cct_city_tax_list;

CREATE OR REPLACE VIEW hdm.vw_cct_harbor_group
AS SELECT cct_harbor_group.hbr_grp_key,
    cct_harbor_group.co_nm,
    cct_harbor_group.curr_row_flg,
    cct_harbor_group.row_strt_dttm,
    cct_harbor_group.row_stop_dttm,
    cct_harbor_group.etl_load_cyc_key,
    cct_harbor_group.src_sys_id
   FROM hdm.cct_harbor_group;

CREATE OR REPLACE VIEW hdm.vw_cct_spend_category
AS SELECT cct_spend_category.spend_cat_key,
    cct_spend_category.spend_cat,
    cct_spend_category.curr_row_flg,
    cct_spend_category.row_strt_dttm,
    cct_spend_category.row_stop_dttm,
    cct_spend_category.etl_load_cyc_key,
    cct_spend_category.src_sys_id
   FROM hdm.cct_spend_category;

CREATE OR REPLACE VIEW hdm.vw_cct_supplier
AS SELECT cct_supplier.suppl_key,
    cct_supplier.suppl_nm,
    cct_supplier.curr_row_flg,
    cct_supplier.row_strt_dttm,
    cct_supplier.row_stop_dttm,
    cct_supplier.etl_load_cyc_key,
    cct_supplier.src_sys_id
   FROM hdm.cct_supplier;

CREATE OR REPLACE VIEW hdm.vw_cct_tax_detail
AS SELECT cct_tax_detail.tax_det_key,
    cct_tax_detail.tax_sum_key,
    cct_tax_detail.city_tax_list_key,
    cct_tax_detail.city_tax_amt,
    cct_tax_detail.curr_row_flg,
    cct_tax_detail.row_strt_dttm,
    cct_tax_detail.row_stop_dttm,
    cct_tax_detail.etl_load_cyc_key,
    cct_tax_detail.src_sys_id
   FROM hdm.cct_tax_detail;

CREATE OR REPLACE VIEW hdm.vw_cct_tax_summary
AS SELECT cct_tax_summary.tax_sum_key,
    cct_tax_summary.hbr_grp_key,
    cct_tax_summary.spend_cat_key,
    cct_tax_summary.suppl_key,
    cct_tax_summary.day_key,
    cct_tax_summary.key_ref,
    cct_tax_summary.line_memo,
    cct_tax_summary.suppl_ref_nbr,
    cct_tax_summary.invc_line_desc,
    cct_tax_summary.tax_anlys_conclsn,
    cct_tax_summary.invc_amt,
    cct_tax_summary.tot_tax_amt,
    cct_tax_summary.curr_row_flg,
    cct_tax_summary.row_strt_dttm,
    cct_tax_summary.row_stop_dttm,
    cct_tax_summary.etl_load_cyc_key,
    cct_tax_summary.src_sys_id
   FROM hdm.cct_tax_summary;

CREATE OR REPLACE VIEW hdm.vw_cost_type
AS SELECT cost_type.cost_type_key,
    cost_type.cost_type_id,
    cost_type.cost_type_desc
   FROM hdm.cost_type;

CREATE OR REPLACE VIEW hdm.vw_country
AS SELECT country.crty_key,
    country.crty_nm,
    country.crty_reg,
    country.crty_cd,
    country.crty_factset_reg,
    country.curr_row_flg,
    country.row_strt_dttm,
    country.row_stop_dttm,
    country.etl_load_cyc_key,
    country.src_sys_id
   FROM hdm.country;

CREATE OR REPLACE VIEW hdm.vw_country_threshold
AS SELECT country_threshold.crty_thrs_key,
    country_threshold.crty_key,
    country_threshold.thrs_desc,
    country_threshold.intrl_trig,
    country_threshold.ofcl_trig,
    country_threshold.thrs_days,
    country_threshold.reglty_filng_days,
    country_threshold.filng_days,
    country_threshold.intrl_trig_displ,
    country_threshold.ofcl_trig_displ,
    country_threshold.thrs_days_displ,
    country_threshold.reglty_filng_displ_days,
    country_threshold.filng_displ_days,
    country_threshold.threshold_type,
    country_threshold.custom_ind,
    country_threshold.thrshld_mlstn,
    country_threshold.thrshld_mlstn_displ,
    country_threshold.periodic_trshld,
    country_threshold.periodic_thrsld_disp,
    country_threshold.any_amt_ind,
    country_threshold.curr_row_flg,
    country_threshold.row_strt_dttm,
    country_threshold.row_stop_dttm,
    country_threshold.etl_load_cyc_key,
    country_threshold.src_sys_id
   FROM hdm.country_threshold;

CREATE OR REPLACE VIEW hdm.vw_cube_acct_stat
AS SELECT account_statistics.fund_key,
    account_statistics.day_key,
    account_statistics.acct_setup_yr_key AS acct_yr_key,
    account_statistics.gend_key,
    account_statistics.acct_cat_key,
    account_statistics.bal_cat_key,
    account_statistics.acct_bal_amt AS sum_bal,
    account_statistics.tot_shrs AS sum_shr,
    account_statistics.acct_cnt AS cnt_bal,
    account_statistics.shrhldr_cnt AS cnt_taxid
   FROM hdm.account_statistics;

CREATE OR REPLACE VIEW hdm.vw_currency
AS SELECT currency.currcy_key,
    currency.currcy_cd,
    currency.currcy_nm,
    currency.currcy_desc,
    currency.curr_row_flg,
    currency.row_strt_dttm,
    currency.row_stop_dttm,
    currency.etl_load_cyc_key,
    currency.src_sys_id
   FROM hdm.currency;

CREATE OR REPLACE VIEW hdm.vw_current_account
AS SELECT account.acct_key,
    account.acct_top_lvl_cd,
    account.acct_top_lvl_desc,
    account.acct_type_cd,
    account.acct_type,
    account.acct_nbr,
    account.fund_nbr,
    account.tax_id,
    account.acct_stat_cd,
    account.acct_stat,
    account.acct_strt_dt,
    account.last_mntn_dt,
    account.dlr_lvl_cntl_cd,
    account.dlr_lvl_cntl_desc,
    account.pre_authzd_chk_acct_flg,
    account.swp_acct_flg,
    account.ach_acct_flg,
    account.re_invst_to_another_acct_flg,
    account.monitored_vip_acct,
    account.fiduciary_acct_flg,
    account.mailg_flg,
    account.intr_party_flg,
    account.mgmt_co_empl_flg,
    account.trst_co_nbr,
    account.tpa_nbr,
    account.custodian_id,
    account.tpa_id,
    account.ext_pln_id,
    account.tin_cd,
    account.tin_desc,
    account.alpha_cd,
    account.cap_gains_dstrbtn_flg,
    account.chk_writing_acct_flg,
    account.divd_dstrbtn_opt_flg,
    account.expedited_redmpn_acct_flg,
    account.sub_acct_flg,
    account.forgn_tax_rt,
    account.cum_discnt_nbr,
    account.ltr_of_intend_nbr,
    account.timer_flg,
    account.gpurch_acct_flg,
    account.expdt_xchg_acct_flg,
    account.pnlty_whg_acct_flg,
    account.certfc_issuance_flg,
    account.stop_xfer_flg,
    account.bluesky_xmptn_flg,
    account.bnk_card_issd_flg,
    account.nav_acct_flg,
    account.phon_chk_redmpn_flg,
    account.house_acct_flg,
    account.divd_mail_acct_flg,
    account.stop_purch_acct_flg,
    account.stop_mail_acct_flg,
    account.fractional_chk_flg,
    account.acct_pr_sched_flg,
    account.cap_gains_dstrbtn_cd,
    account.cap_gains_dstrbtn_type,
    account.divd_dstrbtn_type_cd,
    account.divd_dstrbtn_type,
    account.cust_bin,
    account.acct_origination_id,
    account.regstn_line_1,
    account.regstn_line_2,
    account.regstn_line_3,
    account.regstn_line_4,
    account.regstn_line_5,
    account.regstn_line_6,
    account.regstn_line_7,
    account.st_nm,
    account.crty_nm,
    account.st_cd,
    account.crty_cd,
    account.row_strt_dttm,
    account.row_stop_dttm,
    account.curr_row_flg,
    account.etl_load_cyc_key,
    account.src_sys_id,
    account.legacy_type_desc,
    account.fund_strt_dt,
    account.stop_mail_dt,
    account.acct_setup_dt,
    account.internet_acct_flg,
    account.grnd_father_acct_flg,
    account.pln_clsfcn_flg,
    account.mktg_clsfcn_flg,
    account.mktg_clsfcn_desc,
    account.rep_cd,
    account.rep_nm,
    account.dlr_id,
    account.dlr_type,
    account.hbr_dlr_id,
    account.hbr_dlr_nm,
    account.trst_nm,
    account.acct_clsd_dt,
    account.redmpn_fee_xmpt_flg
   FROM hdm.account
  WHERE account.curr_row_flg::text = 'Y'::text;

CREATE OR REPLACE VIEW hdm.vw_current_shrhldr
AS SELECT shareholder.shrhldr_key,
    shareholder.shrhldr_role_key,
    shareholder.shrhldr_top_lvl_cd,
    shareholder.shrhldr_top_lvl_desc,
    shareholder.shrhldr_nm,
    shareholder.tax_idnfcn_nbr,
    shareholder.ssn_stat_desc,
    shareholder.tax_idnfcn_type,
    shareholder.cust_lgl_ownr_regstn_dt,
    shareholder.cust_idnfcn_nbr,
    shareholder.cust_idnfcn_nbr_2,
    shareholder.vip_cust_flg,
    shareholder.send_mktg_info_flg,
    shareholder.informed_cnsnt_flg,
    shareholder.party_type,
    shareholder.cust_empl_flg,
    shareholder.mgmt_co_empl_flg,
    shareholder.lgl_stat,
    shareholder.gend,
    shareholder.brth_dt,
    shareholder.dcsd_dt,
    shareholder.emplmt_stat,
    shareholder.mrtl_stat,
    shareholder.crty_of_citzn_ship,
    shareholder.addr_type,
    shareholder.addr_line_1,
    shareholder.addr_line_2,
    shareholder.addr_line_3,
    shareholder.addr_line_4,
    shareholder.city,
    shareholder.st,
    shareholder.prov,
    shareholder.crty,
    shareholder.reg,
    shareholder.row_strt_dttm,
    shareholder.row_stop_dttm,
    shareholder.curr_row_flg,
    shareholder.etl_load_cyc_key,
    shareholder.src_sys_id,
    shareholder.purg_prcsd_flg,
    shareholder.purg_prcsd_dt
   FROM hdm.shareholder
  WHERE shareholder.curr_row_flg::text = 'Y'::text;

CREATE OR REPLACE VIEW hdm.vw_daily_avg_acct_position
AS SELECT daily_avg_acct_position.daily_avg_acct_posn_key,
    daily_avg_acct_position.dlr_key,
    daily_avg_acct_position.day_key,
    daily_avg_acct_position.fund_key,
    daily_avg_acct_position.acct_key,
    daily_avg_acct_position.avg_daily_shrs,
    daily_avg_acct_position.avg_daily_bal,
    daily_avg_acct_position.curr_row_flg,
    daily_avg_acct_position.row_strt_dttm,
    daily_avg_acct_position.row_stop_dttm,
    daily_avg_acct_position.etl_load_cyc_key,
    daily_avg_acct_position.src_sys_id,
    daily_avg_acct_position.tpa_key
   FROM hdm.daily_avg_acct_position;

CREATE OR REPLACE VIEW hdm.vw_dealer
AS SELECT dealer.dlr_key,
    dealer.dlr_top_lvl,
    dealer.dlr_top_lvl_desc,
    dealer.dlr_grp_id,
    dealer.dlr_grp_nm,
    dealer.dlr_id,
    dealer.branch_estb_dt,
    dealer.branch_closed_dt,
    dealer.sellg_agrmt_rqrd_flg,
    dealer.sellg_agrmt_12b1_flg,
    dealer.payout_freq_cd,
    dealer.payout_freq_desc,
    dealer.stmt_freq_cd,
    dealer.stmt_freq_desc,
    dealer.recv_12b1_tape_flg,
    dealer.auto_divd_chk_flg,
    dealer.wire_incl_accrl_flg,
    dealer.sipc_insd_flg,
    dealer.dlr_fdic_sell_flg,
    dealer.sec_id,
    dealer.mbr_ship_type_cd,
    dealer.mbr_ship_type,
    dealer.inval_rep_hndlg_cd,
    dealer.inval_rep_hndlg_desc,
    dealer.invmt_slip_addr_cd,
    dealer.invmt_slip_addr,
    dealer.trste_dflt_dlr_id,
    dealer.grp_sales_chnl_id,
    dealer.grp_fincl_inst_id,
    dealer.divd_confirm_sprsn_flg,
    dealer.daily_confirm_sprsn_flg,
    dealer.onln_inval_rep_hndlg_flg,
    dealer.dlr_rep_st_lic_vldn_flg,
    dealer.dlr_inval_rep_st_lic_actn_flg,
    dealer.nasd_regstn_vldn_lic_flg,
    dealer.inval_rep_nasd_regstr_flg,
    dealer.dlr_nm,
    dealer.addr_line_1,
    dealer.addr_line_2,
    dealer.addr_line_3,
    dealer.addr_line_4,
    dealer.city_nm,
    dealer.st,
    dealer.postl_zip_cd,
    dealer.crty,
    dealer.row_strt_dttm,
    dealer.row_stop_dttm,
    dealer.curr_row_flg,
    dealer.etl_load_cyc_key,
    dealer.src_sys_id,
    dealer.spr_onbs_flg,
    dealer.onbs_flg,
    dealer.intrl_cntl_doc_dt,
    dealer.ltst_cntl_audt_recpt_dt,
    dealer.inactv_dt,
    dealer.actv_flg
   FROM hdm.dealer;

CREATE OR REPLACE VIEW hdm.vw_dealer_asset_summary
AS SELECT dealer_asset_summary.dlr_fee_sum_key,
    dealer_asset_summary.dlr_key,
    dealer_asset_summary.fund_key,
    dealer_asset_summary.day_key,
    dealer_asset_summary.prir_per_asset,
    dealer_asset_summary.curr_per_asset,
    dealer_asset_summary.prir_per_eligbl_asset,
    dealer_asset_summary.curr_per_eligbl_asset,
    dealer_asset_summary.prir_per_inelig_asset,
    dealer_asset_summary.curr_per_inelig_asset,
    dealer_asset_summary.prir_per_ta_asset,
    dealer_asset_summary.curr_per_ta_asset,
    dealer_asset_summary.prir_per_ta_eligbl_asset,
    dealer_asset_summary.curr_per_ta_eligbl_asset,
    dealer_asset_summary.prir_per_ta_inelig_asset,
    dealer_asset_summary.curr_per_ta_inelig_asset,
    dealer_asset_summary.prir_per_avg_asset,
    dealer_asset_summary.curr_per_avg_asset,
    dealer_asset_summary.prir_per_avg_eligbl_asset,
    dealer_asset_summary.curr_per_avg_eligbl_asset,
    dealer_asset_summary.prir_per_avg_inelig_asset,
    dealer_asset_summary.curr_per_avg_inelig_asset,
    dealer_asset_summary.prir_per_avg_ta_asset,
    dealer_asset_summary.curr_per_avg_ta_asset,
    dealer_asset_summary.prir_avg_ta_eligbl_asset,
    dealer_asset_summary.curr_avg_ta_inelig_asset,
    dealer_asset_summary.prir_avg_ta_inelig_asset,
    dealer_asset_summary.curr_avg_ta_eligbl_asset,
    dealer_asset_summary.sub_amt,
    dealer_asset_summary.redmpn_amt,
    dealer_asset_summary.net_cashflow_amt,
    dealer_asset_summary.mkt_val_chg,
    dealer_asset_summary.avg_sub_amt,
    dealer_asset_summary.avg_redmpn_amt,
    dealer_asset_summary.avg_net_amt,
    dealer_asset_summary.avg_mkt_val_chg,
    dealer_asset_summary.dlr_tpa_flg,
    dealer_asset_summary.curr_row_flg,
    dealer_asset_summary.row_strt_dttm,
    dealer_asset_summary.row_stop_dttm,
    dealer_asset_summary.etl_load_cyc_key,
    dealer_asset_summary.src_sys_id
   FROM hdm.dealer_asset_summary;

CREATE OR REPLACE VIEW hdm.vw_dealer_branch
AS SELECT dealer_branch.dlr_branch_key,
    dealer_branch.dlr_id,
    dealer_branch.branch_id,
    dealer_branch.branch_nm,
    dealer_branch.branch_addr_line_1,
    dealer_branch.branch_addr_line_2,
    dealer_branch.branch_addr_line_3,
    dealer_branch.branch_addr_line_4,
    dealer_branch.branch_city,
    dealer_branch.branch_st_cd,
    dealer_branch.branch_st,
    dealer_branch.branch_crty,
    dealer_branch.branch_zip_cd,
    dealer_branch.branch_estb_dt,
    dealer_branch.branch_closed_dt,
    dealer_branch.branch_pay_ofc_id,
    dealer_branch.branch_func_type_id,
    dealer_branch.branch_forgn_tax_cd,
    dealer_branch.curr_row_flg,
    dealer_branch.row_strt_dttm,
    dealer_branch.row_stop_dttm,
    dealer_branch.etl_load_cyc_key,
    dealer_branch.src_sys_id
   FROM hdm.dealer_branch;

CREATE OR REPLACE VIEW hdm.vw_dealer_change_control
AS SELECT dealer_change_control.dlr_chg_cntl_key,
    dealer_change_control.dlr_key,
    dealer_change_control.day_key,
    dealer_change_control.chg_cntl_id,
    dealer_change_control.chg_type,
    dealer_change_control.chg_desc,
    dealer_change_control.recvg_dlr_nm,
    dealer_change_control.recvg_dlr_acct_type,
    dealer_change_control.acct_move_cnt,
    dealer_change_control.bsky_ntfn_flg,
    dealer_change_control.hbr_impct,
    dealer_change_control.cusip_list,
    dealer_change_control.delvy_dlr_mtrx_lvl,
    dealer_change_control.delvy_dlr_acct_type,
    dealer_change_control.asset_move_val,
    dealer_change_control.delvy_dlr_plfm_nm,
    dealer_change_control.delvy_dlr_fees,
    dealer_change_control.recvg_dlr_id,
    dealer_change_control.recvg_dlr_mtrx_lvl,
    dealer_change_control.recvg_dlr_fees,
    dealer_change_control.cnvrsn_strt_dt,
    dealer_change_control.cnvrsn_end_dt,
    dealer_change_control.chg_rsn,
    dealer_change_control.chg_contnt_type,
    dealer_change_control.contct_nm,
    dealer_change_control.ufid_stat,
    dealer_change_control.dlr_grping_stat,
    dealer_change_control.dlr_prof_stat,
    dealer_change_control.mtf_stat,
    dealer_change_control.live_link_stat,
    dealer_change_control.rfrser_rqst,
    dealer_change_control.commfee_cmmt,
    dealer_change_control.cumdisc_cd,
    dealer_change_control.mtf_inlsn_flg,
    dealer_change_control.awd_send_val,
    dealer_change_control.mtf_excln_flg,
    dealer_change_control.excln_cmmts,
    dealer_change_control.inlsn_cmmts,
    dealer_change_control.cnvrsn_stat,
    dealer_change_control.dlr_tot_asset_pct,
    dealer_change_control.chg_cntl_wkfl,
    dealer_change_control.fee_impct_flg,
    dealer_change_control.creatr,
    dealer_change_control.modfr,
    dealer_change_control.creatn_dt,
    dealer_change_control.chg_dt,
    dealer_change_control.curr_row_flg,
    dealer_change_control.row_strt_dttm,
    dealer_change_control.row_stop_dttm,
    dealer_change_control.etl_load_cyc_key,
    dealer_change_control.src_sys_id
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
AS SELECT dealer_fee_analysis.dlr_fee_anlys_key,
    dealer_fee_analysis.dlr_key,
    dealer_fee_analysis.fund_key,
    dealer_fee_analysis.day_key,
    dealer_fee_analysis.dlr_fee_type_key,
    dealer_fee_analysis.contrctl_rt,
    dealer_fee_analysis.actl_fee,
    dealer_fee_analysis.actl_rt,
    dealer_fee_analysis.avg_daily_asset,
    dealer_fee_analysis.avg_daily_asset_prir_per,
    dealer_fee_analysis.avg_eligbl_daily_asset,
    dealer_fee_analysis.avg_eligbl_asset_prir_per,
    dealer_fee_analysis.drvvd_rt,
    dealer_fee_analysis.prir_per_fee,
    dealer_fee_analysis.prir_per_rt,
    dealer_fee_analysis.fee_diff,
    dealer_fee_analysis.asset_diff,
    dealer_fee_analysis.curr_row_flg,
    dealer_fee_analysis.row_strt_dttm,
    dealer_fee_analysis.row_stop_dttm,
    dealer_fee_analysis.etl_load_cyc_key,
    dealer_fee_analysis.src_sys_id
   FROM hdm.dealer_fee_analysis;

CREATE OR REPLACE VIEW hdm.vw_dealer_fee_research
AS SELECT dealer_fee_research.dlr_fee_rsrh_key,
    dealer_fee_research.dlr_key,
    dealer_fee_research.invc_day_key,
    dealer_fee_research.pmt_day_key,
    dealer_fee_research.fund_key,
    dealer_fee_research.acct_key,
    dealer_fee_research.dlr_fee_type_key,
    dealer_fee_research.hbr_avg_aum,
    dealer_fee_research.hbr_avg_shrs,
    dealer_fee_research.fee_bp,
    dealer_fee_research.fee_calc,
    dealer_fee_research.hbr_avg_aum_prev,
    dealer_fee_research.hbr_avg_shrs_prev,
    dealer_fee_research.invc_freq_flg,
    dealer_fee_research.billg_type,
    dealer_fee_research.fee_chrgd,
    dealer_fee_research.avg_aum,
    dealer_fee_research.avg_bp,
    dealer_fee_research.fee_chrgd_prev,
    dealer_fee_research.avg_aum_prev,
    dealer_fee_research.avg_bp_prev,
    dealer_fee_research.fee_chrgd_flg,
    dealer_fee_research.cusip_lvl_aum_diff_flg,
    dealer_fee_research.cusip_lvl_fee_diff_flg,
    dealer_fee_research.cusip_curr_aum_diff_flg,
    dealer_fee_research.acct_curr_aum_diff_flg,
    dealer_fee_research.fee_diff_flg,
    dealer_fee_research.aum_diff_flg,
    dealer_fee_research.nav_mo_avg,
    dealer_fee_research.nav_mo_avg_prev,
    dealer_fee_research.curr_per_days,
    dealer_fee_research.prir_per_days,
    dealer_fee_research.curr_row_flg,
    dealer_fee_research.row_strt_dttm,
    dealer_fee_research.row_stop_dttm,
    dealer_fee_research.etl_load_cyc_key,
    dealer_fee_research.src_sys_id
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
AS SELECT dealer_fee_type.dlr_fee_type_key,
    dealer_fee_type.dlr_fee_type_cd,
    dealer_fee_type.dlr_fee_type_desc
   FROM hdm.dealer_fee_type;

CREATE OR REPLACE VIEW hdm.vw_dealer_invoice
AS SELECT dealer_invoice.dlr_invc_key,
    dealer_invoice.dlr_fee_type_key,
    dealer_invoice.dlr_key,
    dealer_invoice.fund_key,
    dealer_invoice.invc_day_key,
    dealer_invoice.pmt_day_key,
    dealer_invoice.acct_key,
    dealer_invoice.bus_line,
    dealer_invoice.invc_avg_asset,
    dealer_invoice.fee_rt,
    dealer_invoice.tot_fee_amt,
    dealer_invoice.posn_cnt,
    dealer_invoice.per_acct_fee,
    dealer_invoice.dlr_portal_asset,
    dealer_invoice.portal_posn_cnt,
    dealer_invoice.invc_freq_flg,
    dealer_invoice.invc_nbr,
    dealer_invoice.invc_dt,
    dealer_invoice.curr_row_flg,
    dealer_invoice.row_strt_dttm,
    dealer_invoice.row_stop_dttm,
    dealer_invoice.etl_load_cyc_key,
    dealer_invoice.src_sys_id,
    dealer_invoice.rec_splt_flg,
    dealer_invoice.dlr_cat,
    dealer_invoice.invc_cat
   FROM hdm.dealer_invoice;

CREATE OR REPLACE VIEW hdm.vw_dealer_invoice_model
AS SELECT dealer_invoice_model.dlr_invc_mdl_key,
    dealer_invoice_model.dlr_fee_type_key,
    dealer_invoice_model.dlr_key,
    dealer_invoice_model.invc_day_key,
    dealer_invoice_model.pmt_day_key,
    dealer_invoice_model.fund_key,
    dealer_invoice_model.bus_line,
    dealer_invoice_model.invc_avg_asset,
    dealer_invoice_model.tot_fee_amt,
    dealer_invoice_model.posn_cnt,
    dealer_invoice_model.per_acct_fee,
    dealer_invoice_model.dlr_portal_asset,
    dealer_invoice_model.portal_posn_cnt,
    dealer_invoice_model.invc_freq_flg,
    dealer_invoice_model.rec_splt_flg,
    dealer_invoice_model.invc_nbr,
    dealer_invoice_model.invc_dt,
    dealer_invoice_model.curr_row_flg,
    dealer_invoice_model.row_strt_dttm,
    dealer_invoice_model.row_stop_dttm,
    dealer_invoice_model.etl_load_cyc_key,
    dealer_invoice_model.src_sys_id
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
AS SELECT dealer_invoice_trial.dlr_invc_trl_key,
    dealer_invoice_trial.dlr_key,
    dealer_invoice_trial.acct_key,
    dealer_invoice_trial.fund_key,
    dealer_invoice_trial.invc_day_key,
    dealer_invoice_trial.dlr_fee_type_key,
    dealer_invoice_trial.pmt_day_key,
    dealer_invoice_trial.fee_rt,
    dealer_invoice_trial.tot_fee_amt,
    dealer_invoice_trial.invc_nbr,
    dealer_invoice_trial.invc_dt,
    dealer_invoice_trial.bus_line,
    dealer_invoice_trial.invc_avg_asset,
    dealer_invoice_trial.posn_cnt,
    dealer_invoice_trial.per_acct_fee,
    dealer_invoice_trial.dlr_portal_asset,
    dealer_invoice_trial.portal_posn_cnt,
    dealer_invoice_trial.invc_freq_flg,
    dealer_invoice_trial.rec_splt_flg,
    dealer_invoice_trial.dlr_cat,
    dealer_invoice_trial.curr_row_flg,
    dealer_invoice_trial.row_strt_dttm,
    dealer_invoice_trial.row_stop_dttm,
    dealer_invoice_trial.etl_load_cyc_key,
    dealer_invoice_trial.src_sys_id
   FROM hdm.dealer_invoice_trial;

CREATE OR REPLACE VIEW hdm.vw_dealer_level_control
AS SELECT dealer_level_control.dlr_lvl_cntl_key,
    dealer_level_control.dlr_lvl_cntl_cd,
    dealer_level_control.dlr_lvl_cntl_desc,
    dealer_level_control.row_strt_dttm,
    dealer_level_control.row_stop_dttm,
    dealer_level_control.curr_row_flg,
    dealer_level_control.src_sys_id,
    dealer_level_control.etl_load_cyc_key
   FROM hdm.dealer_level_control;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_deail
AS SELECT dealer_payment_detail.dlr_pmt_det_key,
    dealer_payment_detail.day_key,
    dealer_payment_detail.dlr_key,
    dealer_payment_detail.fund_key,
    dealer_payment_detail.dlr_branch_key,
    dealer_payment_detail.dlr_fee_type_key,
    dealer_payment_detail.dlr_pmt_mthd_key,
    dealer_payment_detail.acct_key,
    dealer_payment_detail.wk_ord_id,
    dealer_payment_detail.nscc_flg,
    dealer_payment_detail.nscc_nm_use_flg,
    dealer_payment_detail.pmt_ofst_cd,
    dealer_payment_detail.payee_type,
    dealer_payment_detail.splt_compnsn_flg,
    dealer_payment_detail.prev_compnsn_ovrd_type,
    dealer_payment_detail.eligbl_compnsn_ovrd_type,
    dealer_payment_detail.payout_type_key,
    dealer_payment_detail.pre_agreed_shr_rt,
    dealer_payment_detail.pre_agreed_asset,
    dealer_payment_detail.pre_agreed_fee,
    dealer_payment_detail.prev_splt_fee,
    dealer_payment_detail.tot_eligbl_rt,
    dealer_payment_detail.tot_eligbl_asset,
    dealer_payment_detail.tot_eligbl_fee,
    dealer_payment_detail.tot_compnsn,
    dealer_payment_detail.curr_row_flg,
    dealer_payment_detail.row_strt_dttm,
    dealer_payment_detail.row_stop_dttm,
    dealer_payment_detail.etl_load_cyc_key,
    dealer_payment_detail.src_sys_id,
    dealer_payment_detail.avg_eligbl_bp,
    dealer_payment_detail.avg_eligbl_asset,
    dealer_payment_detail.avg_eligbl_fees
   FROM hdm.dealer_payment_detail;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_detail
AS SELECT dealer_payment_detail.dlr_pmt_det_key,
    dealer_payment_detail.day_key,
    dealer_payment_detail.dlr_key,
    dealer_payment_detail.fund_key,
    dealer_payment_detail.dlr_branch_key,
    dealer_payment_detail.dlr_fee_type_key,
    dealer_payment_detail.dlr_pmt_mthd_key,
    dealer_payment_detail.acct_key,
    dealer_payment_detail.wk_ord_id,
    dealer_payment_detail.nscc_flg,
    dealer_payment_detail.nscc_nm_use_flg,
    dealer_payment_detail.pmt_ofst_cd,
    dealer_payment_detail.payee_type,
    dealer_payment_detail.splt_compnsn_flg,
    dealer_payment_detail.prev_compnsn_ovrd_type,
    dealer_payment_detail.eligbl_compnsn_ovrd_type,
    dealer_payment_detail.payout_type_key,
    dealer_payment_detail.pre_agreed_shr_rt,
    dealer_payment_detail.pre_agreed_asset,
    dealer_payment_detail.pre_agreed_fee,
    dealer_payment_detail.prev_splt_fee,
    dealer_payment_detail.tot_eligbl_rt,
    dealer_payment_detail.tot_eligbl_asset,
    dealer_payment_detail.tot_eligbl_fee,
    dealer_payment_detail.tot_compnsn,
    dealer_payment_detail.curr_row_flg,
    dealer_payment_detail.row_strt_dttm,
    dealer_payment_detail.row_stop_dttm,
    dealer_payment_detail.etl_load_cyc_key,
    dealer_payment_detail.src_sys_id,
    dealer_payment_detail.avg_eligbl_bp,
    dealer_payment_detail.avg_eligbl_asset,
    dealer_payment_detail.avg_eligbl_fees,
    dealer_payment_detail.pmt_day_key
   FROM hdm.dealer_payment_detail;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_method
AS SELECT dealer_payment_method.dlr_pmt_mthd_key,
    dealer_payment_method.pmt_mthd_cd,
    dealer_payment_method.pmt_mthd_desc
   FROM hdm.dealer_payment_method;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_summary
AS SELECT dealer_payment_summary.dlr_pmt_sum_key,
    dealer_payment_summary.dlr_fee_type_key,
    dealer_payment_summary.day_key,
    dealer_payment_summary.fund_key,
    dealer_payment_summary.dlr_key,
    dealer_payment_summary.dlr_pmt_mthd_key,
    dealer_payment_summary.wk_ord_nbr,
    dealer_payment_summary.compnsn_pyee_type_cd,
    dealer_payment_summary.pre_agreed_sched_ovrd_type,
    dealer_payment_summary.pre_agree_shr_rt,
    dealer_payment_summary.pre_agree_avg_asset,
    dealer_payment_summary.pre_agree_compnsn,
    dealer_payment_summary.eligbl_compnsn_ovrd_type,
    dealer_payment_summary.eligbl_shr_rt,
    dealer_payment_summary.eligbl_avg_asset,
    dealer_payment_summary.eligbl_compnsn,
    dealer_payment_summary.tot_compnsn,
    dealer_payment_summary.curr_row_flg,
    dealer_payment_summary.row_strt_dttm,
    dealer_payment_summary.row_stop_dttm,
    dealer_payment_summary.etl_load_cyc_key,
    dealer_payment_summary.src_sys_id,
    dealer_payment_summary.compnsn_diff,
    dealer_payment_summary.prev_per_compnsn,
    dealer_payment_summary.avg_asset_diff,
    dealer_payment_summary.prev_per_asset,
    dealer_payment_summary.trl_mode_flg,
    dealer_payment_summary.pmt_day_key,
    dealer_payment_summary.bus_line
   FROM hdm.dealer_payment_summary;

CREATE OR REPLACE VIEW hdm.vw_dealer_payment_trial
AS SELECT dealer_payment_trial.dlr_pmt_trl_key,
    dealer_payment_trial.dlr_key,
    dealer_payment_trial.fund_key,
    dealer_payment_trial.dlr_branch_key,
    dealer_payment_trial.dlr_fee_type_key,
    dealer_payment_trial.dlr_pmt_mthd_key,
    dealer_payment_trial.acct_key,
    dealer_payment_trial.payout_type_key,
    dealer_payment_trial.pmt_day_key,
    dealer_payment_trial.invc_day_key,
    dealer_payment_trial.wk_ord_id,
    dealer_payment_trial.nscc_flg,
    dealer_payment_trial.nscc_nm_use_flg,
    dealer_payment_trial.pmt_ofst_cd,
    dealer_payment_trial.payee_type,
    dealer_payment_trial.splt_compnsn_flg,
    dealer_payment_trial.prev_compnsn_ovrd_type,
    dealer_payment_trial.eligbl_compnsn_ovrd_type,
    dealer_payment_trial.pre_agrd_shr_rt,
    dealer_payment_trial.pre_agrd_asset,
    dealer_payment_trial.pre_agrd_fee,
    dealer_payment_trial.prev_splt_fee,
    dealer_payment_trial.tot_eligbl_rt,
    dealer_payment_trial.tot_eligbl_asset,
    dealer_payment_trial.tot_eligbl_fee,
    dealer_payment_trial.tot_compnsn,
    dealer_payment_trial.avg_eligbl_bp,
    dealer_payment_trial.avg_eligbl_asset,
    dealer_payment_trial.avg_eligbl_fees,
    dealer_payment_trial.curr_row_flg,
    dealer_payment_trial.row_strt_dttm,
    dealer_payment_trial.row_stop_dttm,
    dealer_payment_trial.etl_load_cyc_key,
    dealer_payment_trial.src_sys_id
   FROM hdm.dealer_payment_trial;

CREATE OR REPLACE VIEW hdm.vw_dealer_payout_type
AS SELECT dealer_payout_type.payout_type_key,
    dealer_payout_type.payout_type_cd,
    dealer_payout_type.payout_type_desc
   FROM hdm.dealer_payout_type;

CREATE OR REPLACE VIEW hdm.vw_distribution_code
AS SELECT distribution_code.dstrbtn_cd_key,
    distribution_code.dstrbtn_cd,
    distribution_code.dstrbtn_desc
   FROM hdm.distribution_code;

CREATE OR REPLACE VIEW hdm.vw_distribution_type
AS SELECT distribution_type.dstrbtn_type_key,
    distribution_type.dstrbtn_type_cd,
    distribution_type.dstrbtn_type_desc
   FROM hdm.distribution_type;

CREATE OR REPLACE VIEW hdm.vw_dividend_tax_rate
AS SELECT dividend_tax_rate.divd_tax_rt_key,
    dividend_tax_rate.tax_yr,
    dividend_tax_rate.ordn_incm_rt,
    dividend_tax_rate.ltcg_rt,
    dividend_tax_rate.stcg_rt,
    dividend_tax_rate.curr_row_flg,
    dividend_tax_rate.row_strt_dttm,
    dividend_tax_rate.row_stop_dttm,
    dividend_tax_rate.etl_load_cyc_key,
    dividend_tax_rate.src_sys_id
   FROM hdm.dividend_tax_rate;

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
  WHERE (a.tax_id <> ALL (ARRAY[123456789::double precision, 0::double precision, 111111111::double precision, 1::double precision, 341953397::double precision, 341953393::double precision, 341953399::double precision, 980592172::double precision, 561223381::double precision, 341572115::double precision, 341931837::double precision, 346864487::double precision, 341570877::double precision, 306006023::double precision, 341971614::double precision, 341570746::double precision, 341748997::double precision, 205075188::double precision, 264203992::double precision, 263062227::double precision, 271891764::double precision, 274941322::double precision, 451258204::double precision, 451258360::double precision, 30488315::double precision, 341570861::double precision, 47018428::double precision, 341570876::double precision, 263713174::double precision, 263713227::double precision, 263713261::double precision, 263713284::double precision, 263713315::double precision, 263713340::double precision, 263713369::double precision, 263713394::double precision, 263713422::double precision, 263713447::double precision])) AND (a.acct_nbr <> ALL (ARRAY[60000001::double precision, 60100001::double precision, 60200001::double precision, 60300001::double precision, 60400001::double precision, 60500001::double precision, 60600001::double precision, 60700001::double precision, 60800001::double precision, 60900001::double precision, 70000001::double precision, 70100001::double precision, 70200001::double precision, 70300001::double precision, 70400001::double precision, 70500001::double precision, 70600001::double precision, 70700001::double precision, 70800001::double precision, 70900001::double precision, 80000001::double precision, 80100001::double precision, 80200001::double precision, 80300001::double precision, 80400001::double precision, 80500001::double precision, 80600001::double precision, 80700001::double precision, 80800001::double precision, 80900001::double precision])) AND (a.acct_stat::text = 'Open Status'::text OR a.acct_stat::text = 'Closed Status'::text AND a.last_mntn_dt >= aws_oracle_ext.add_months(aws_oracle_ext.trunc(aws_oracle_ext.sysdate(), 'mm'::character varying), '-24'::integer))
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

CREATE OR REPLACE VIEW hdm.vw_dlp_taxid
AS SELECT a.tax_id
   FROM hdm.vw_account a
  WHERE (a.tax_id <> ALL (ARRAY[123456789::double precision, 0::double precision, 111111111::double precision, 1::double precision, 341953397::double precision, 341953393::double precision, 341953399::double precision, 980592172::double precision, 561223381::double precision, 341572115::double precision, 341931837::double precision, 346864487::double precision, 341570877::double precision, 306006023::double precision, 341971614::double precision, 341570746::double precision, 341748997::double precision, 205075188::double precision, 264203992::double precision, 263062227::double precision, 271891764::double precision, 274941322::double precision, 451258204::double precision, 451258360::double precision, 30488315::double precision, 341570861::double precision, 47018428::double precision, 341570876::double precision, 263713174::double precision, 263713227::double precision, 263713261::double precision, 263713284::double precision, 263713315::double precision, 263713340::double precision, 263713369::double precision, 263713394::double precision, 263713422::double precision, 263713447::double precision])) AND (a.acct_nbr <> ALL (ARRAY[60000001::double precision, 60100001::double precision, 60200001::double precision, 60300001::double precision, 60400001::double precision, 60500001::double precision, 60600001::double precision, 60700001::double precision, 60800001::double precision, 60900001::double precision, 70000001::double precision, 70100001::double precision, 70200001::double precision, 70300001::double precision, 70400001::double precision, 70500001::double precision, 70600001::double precision, 70700001::double precision, 70800001::double precision, 70900001::double precision, 80000001::double precision, 80100001::double precision, 80200001::double precision, 80300001::double precision, 80400001::double precision, 80500001::double precision, 80600001::double precision, 80700001::double precision, 80800001::double precision, 80900001::double precision])) AND (a.acct_stat::text = 'Open Status'::text OR a.acct_stat::text = 'Closed Status'::text AND a.last_mntn_dt >= aws_oracle_ext.add_months(aws_oracle_ext.trunc(aws_oracle_ext.sysdate(), 'mm'::character varying), '-24'::integer))
  GROUP BY a.tax_id
UNION ALL
 SELECT 325689021 AS tax_id
UNION ALL
 SELECT 325689022 AS tax_id
UNION ALL
 SELECT 325689023 AS tax_id
UNION ALL
 SELECT 325689024 AS tax_id;

CREATE OR REPLACE VIEW hdm.vw_eligible_asset_list
AS SELECT eligible_asset_list.eligbl_asset_list_key,
    eligible_asset_list.dlr_key,
    eligible_asset_list.inlsn_excln_flg,
    eligible_asset_list.cum_discnt_nbr,
    eligible_asset_list.shr_cls_cd,
    eligible_asset_list.qlfyd_rtrmt_flg,
    eligible_asset_list.efftv_strt_dt,
    eligible_asset_list.efftv_end_dt,
    eligible_asset_list.curr_row_flg,
    eligible_asset_list.row_strt_dttm,
    eligible_asset_list.row_stop_dttm,
    eligible_asset_list.etl_load_cyc_key,
    eligible_asset_list.src_sys_id
   FROM hdm.eligible_asset_list;

CREATE OR REPLACE VIEW hdm.vw_exchange_type
AS SELECT exchange_type.xchg_type_key,
    exchange_type.xchg_type_cd,
    exchange_type.xchg_type_desc
   FROM hdm.exchange_type;

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

CREATE OR REPLACE VIEW hdm.vw_expense_ratio_history
AS SELECT expense_ratio_history.expns_ratio_hist_key,
    expense_ratio_history.day_key,
    expense_ratio_history.fund_key,
    expense_ratio_history.efftv_strt_dt,
    expense_ratio_history.efftv_end_dt,
    expense_ratio_history.net_expns_ratio
   FROM hdm.expense_ratio_history;

CREATE OR REPLACE VIEW hdm.vw_external_fund
AS SELECT fid_external_fund.ext_fund_key,
    fid_external_fund.ext_fund_nm,
    fid_external_fund.asset_cls_nm
   FROM hdm.fid_external_fund;

CREATE OR REPLACE VIEW hdm.vw_external_fund_balance
AS SELECT external_fund_balance.ext_fund_bal_key,
    external_fund_balance.fund_key,
    external_fund_balance.day_key,
    external_fund_balance.ext_src,
    external_fund_balance.shrs,
    external_fund_balance.bal_amt,
    external_fund_balance.curr_row_flg,
    external_fund_balance.row_strt_dttm,
    external_fund_balance.row_stop_dttm,
    external_fund_balance.etl_load_cyc_key,
    external_fund_balance.src_sys_id,
    external_fund_balance.offr_id,
    external_fund_balance.offr_nm
   FROM hdm.external_fund_balance;

CREATE OR REPLACE VIEW hdm.vw_external_fund_lis
AS SELECT external_fund_list.ext_fund_list_key,
    external_fund_list.ext_fund_id,
    external_fund_list.ext_fund_desc,
    external_fund_list.fund_key,
    external_fund_list.curr_row_flg,
    external_fund_list.row_strt_dttm,
    external_fund_list.row_stop_dttm,
    external_fund_list.etl_load_cyc_key,
    external_fund_list.src_sys_id,
    external_fund_list.fund_tickr_sym
   FROM hdm.external_fund_list;

CREATE OR REPLACE VIEW hdm.vw_external_fund_list
AS SELECT external_fund_list.ext_fund_list_key,
    external_fund_list.ext_fund_id,
    external_fund_list.ext_fund_desc,
    external_fund_list.fund_key,
    external_fund_list.curr_row_flg,
    external_fund_list.row_strt_dttm,
    external_fund_list.row_stop_dttm,
    external_fund_list.etl_load_cyc_key,
    external_fund_list.src_sys_id,
    external_fund_list.fund_tickr_sym
   FROM hdm.external_fund_list;

CREATE OR REPLACE VIEW hdm.vw_facility
AS SELECT facility.fac_key,
    facility.fac_id,
    facility.sub_fac_nm,
    facility.fac_func_id,
    facility.day_key,
    facility.curr_row_flg,
    facility.row_strt_dttm,
    facility.row_stop_dttm,
    facility.etl_load_cyc_key,
    facility.src_sys_id
   FROM hdm.facility;

CREATE OR REPLACE VIEW hdm.vw_fee_schedule
AS SELECT fee_schedule.fee_sched_key,
    fee_schedule.dlr_key,
    fee_schedule.fund_key,
    fee_schedule.dlr_fee_type_key,
    fee_schedule.contrct_strt_day_key,
    fee_schedule.contrct_end_day_key,
    fee_schedule.dlr_billg_type_flg,
    fee_schedule.fee_rt,
    fee_schedule.curr_row_flg,
    fee_schedule.row_strt_dttm,
    fee_schedule.row_stop_dttm,
    fee_schedule.etl_load_cyc_key,
    fee_schedule.src_sys_id
   FROM hdm.fee_schedule;

CREATE OR REPLACE VIEW hdm.vw_fee_schedule_view
AS SELECT fs.dlr_key,
    fs.fund_key,
    fs.bus_line,
        CASE
            WHEN ft.dlr_fee_type_cd::text = ANY (ARRAY['MF'::character varying, 'PAF'::character varying]::text[]) THEN ''::character varying
            ELSE fs.invc_fee_desc
        END AS invc_fee_desc,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = '12B1'::text THEN fs.fee_rt
            ELSE NULL::double precision
        END) AS fee_12b1,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = 'SPC1'::text THEN fs.fee_rt
            ELSE NULL::double precision
        END) AS fee_spc1,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = ANY (ARRAY['SPC2'::character varying, 'SPC3'::character varying]::text[]) THEN fs.fee_rt
            ELSE NULL::double precision
        END) AS fee_inst,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = 'MF'::text THEN fs.fee_rt
            ELSE NULL::double precision
        END) AS fee_mf,
    sum(
        CASE
            WHEN ft.dlr_fee_type_cd::text = 'PAF'::text THEN fs.fee_rt
            ELSE NULL::double precision
        END) AS fee_paf
   FROM hdm.dealer_fee_schedule fs
     JOIN hdm.dealer_fee_type ft ON fs.dlr_fee_type_key = ft.dlr_fee_type_key
  GROUP BY fs.dlr_key, fs.fund_key, fs.bus_line, (
        CASE
            WHEN ft.dlr_fee_type_cd::text = ANY (ARRAY['MF'::character varying, 'PAF'::character varying]::text[]) THEN ''::character varying
            ELSE fs.invc_fee_desc
        END);

CREATE OR REPLACE VIEW hdm.vw_fi_characteristic_list
AS SELECT fi_characteristic_list.fi_charctc_list_key,
    fi_characteristic_list.charctc_nm,
    fi_characteristic_list.curr_row_flg,
    fi_characteristic_list.row_strt_dttm,
    fi_characteristic_list.row_stop_dttm,
    fi_characteristic_list.row_updt_dttm,
    fi_characteristic_list.etl_load_cyc_key,
    fi_characteristic_list.src_sys_id
   FROM hdm.fi_characteristic_list;

CREATE OR REPLACE VIEW hdm.vw_fi_characteristics
AS SELECT fi_characteristics.fi_charctc_fact_key,
    fi_characteristics.fund_compst_key,
    fi_characteristics.day_key,
    fi_characteristics.fi_charctc_list_key,
    fi_characteristics.fund_charctc_val,
    fi_characteristics.prim_bmk_charctc,
    fi_characteristics.secy_bmk_charctc,
    fi_characteristics.curr_row_flg,
    fi_characteristics.row_strt_dttm,
    fi_characteristics.row_stop_dttm,
    fi_characteristics.row_updt_dttm,
    fi_characteristics.etl_load_cyc_key,
    fi_characteristics.src_sys_id
   FROM hdm.fi_characteristics;

CREATE OR REPLACE VIEW hdm.vw_fi_composition_map
AS SELECT fi_composition_map.fi_compsn_map_key,
    fi_composition_map.src_type,
    fi_composition_map.compst_nm,
    fi_composition_map.src_compsn_nm,
    fi_composition_map.trgt_compsn_nm,
    fi_composition_map.curr_row_flg,
    fi_composition_map.row_strt_dttm,
    fi_composition_map.row_stop_dttm,
    fi_composition_map.row_updt_dttm,
    fi_composition_map.etl_load_cyc_key,
    fi_composition_map.src_sys_id
   FROM hdm.fi_composition_map;

CREATE OR REPLACE VIEW hdm.vw_fi_country
AS SELECT fi_country.fi_crty_key,
    fi_country.reg,
    fi_country.crty_type,
    fi_country.crty,
    fi_country.curr_row_flg,
    fi_country.row_strt_dttm,
    fi_country.row_stop_dttm,
    fi_country.row_updt_dttm,
    fi_country.etl_load_cyc_key,
    fi_country.src_sys_id
   FROM hdm.fi_country;

CREATE OR REPLACE VIEW hdm.vw_fi_country_allocation
AS SELECT fi_country_allocation.fi_crty_allocn_key,
    fi_country_allocation.fund_compst_key,
    fi_country_allocation.day_key,
    fi_country_allocation.fi_crty_key,
    fi_country_allocation.fund_allocn,
    fi_country_allocation.prim_bmk_allocn,
    fi_country_allocation.secy_bmk_allocn,
    fi_country_allocation.fund_ext__dur_yr,
    fi_country_allocation.fund_ext_em_sprd,
    fi_country_allocation.fund_loc_dur_yr,
    fi_country_allocation.fund_curr_expsr,
    fi_country_allocation.bmk_ext_dur_yr,
    fi_country_allocation.bmk_ext_em_sprd,
    fi_country_allocation.bmk_loc_dur_yr,
    fi_country_allocation.bmk_curr_expsr,
    fi_country_allocation.curr_row_flg,
    fi_country_allocation.row_strt_dttm,
    fi_country_allocation.row_stop_dttm,
    fi_country_allocation.row_updt_dttm,
    fi_country_allocation.etl_load_cyc_key,
    fi_country_allocation.src_sys_id
   FROM hdm.fi_country_allocation;

CREATE OR REPLACE VIEW hdm.vw_fi_credit_rating
AS SELECT fi_credit_rating.fi_cr_rtngs_key,
    fi_credit_rating.fund_compst_key,
    fi_credit_rating.day_key,
    fi_credit_rating.fi_cr_rtng_list_key,
    fi_credit_rating.fund_rtng_exclsv_cash,
    fi_credit_rating.fund_rtng_inclv_cash,
    fi_credit_rating.prim_bmk_rtng,
    fi_credit_rating.secy_bmk_rtng,
    fi_credit_rating.fund_dwe_exclsv_cash,
    fi_credit_rating.fund_dwe_inclv_cash,
    fi_credit_rating.prim_bmk_dwe,
    fi_credit_rating.secy_bmk_dwe,
    fi_credit_rating.fund_expsr_amt,
    fi_credit_rating.fund_expsr_rt,
    fi_credit_rating.fund_dur_rt,
    fi_credit_rating.fund_dwe,
    fi_credit_rating.bmk_expsr_rt,
    fi_credit_rating.bmk_dur_rt,
    fi_credit_rating.bmk_dwe,
    fi_credit_rating.curr_row_flg,
    fi_credit_rating.row_strt_dttm,
    fi_credit_rating.row_stop_dttm,
    fi_credit_rating.row_updt_dttm,
    fi_credit_rating.etl_load_cyc_key,
    fi_credit_rating.src_sys_id
   FROM hdm.fi_credit_rating;

CREATE OR REPLACE VIEW hdm.vw_fi_credit_rating_list
AS SELECT fi_credit_rating_list.fi_cr_rtng_list_key,
    fi_credit_rating_list.cr_rtng_nm,
    fi_credit_rating_list.rtng_sort_ord,
    fi_credit_rating_list.curr_row_flg,
    fi_credit_rating_list.row_strt_dttm,
    fi_credit_rating_list.row_stop_dttm,
    fi_credit_rating_list.row_updt_dttm,
    fi_credit_rating_list.etl_load_cyc_key,
    fi_credit_rating_list.src_sys_id
   FROM hdm.fi_credit_rating_list;

CREATE OR REPLACE VIEW hdm.vw_fi_duration_maturity
AS SELECT fi_duration_maturity.fi_dur_mtry_key,
    fi_duration_maturity.fund_compst_key,
    fi_duration_maturity.day_key,
    fi_duration_maturity.fi_tm_per_key,
    fi_duration_maturity.fund_yca_dur_inclv_cash,
    fi_duration_maturity.fund_yca_dur_exclsv_cash,
    fi_duration_maturity.prim_bmk_dur,
    fi_duration_maturity.secy_bmk_dur,
    fi_duration_maturity.fund_yca_mtry_inclv_cash,
    fi_duration_maturity.fund_yca_mtry_exclsv_cash,
    fi_duration_maturity.prim_bmk_mtry,
    fi_duration_maturity.secy_bmk_mtry,
    fi_duration_maturity.fund_dwe_inclv_cash,
    fi_duration_maturity.fund_dwe_exclsv_cash,
    fi_duration_maturity.prim_bmk_dwe,
    fi_duration_maturity.secy_bmk_dwe,
    fi_duration_maturity.curr_row_flg,
    fi_duration_maturity.row_strt_dttm,
    fi_duration_maturity.row_stop_dttm,
    fi_duration_maturity.row_updt_dttm,
    fi_duration_maturity.etl_load_cyc_key,
    fi_duration_maturity.src_sys_id
   FROM hdm.fi_duration_maturity;

CREATE OR REPLACE VIEW hdm.vw_fi_issuer
AS SELECT fi_issuer.fi_issr_key,
    fi_issuer.issr_nm,
    fi_issuer.curr_row_flg,
    fi_issuer.row_strt_dttm,
    fi_issuer.row_stop_dttm,
    fi_issuer.row_updt_dttm,
    fi_issuer.etl_load_cyc_key,
    fi_issuer.src_sys_id
   FROM hdm.fi_issuer;

CREATE OR REPLACE VIEW hdm.vw_fi_sector
AS SELECT fi_sector.fi_sctr_key,
    fi_sector.sctr_type,
    fi_sector.sctr_nm,
    fi_sector.sub_sctr_nm,
    fi_sector.curr_row_flg,
    fi_sector.row_strt_dttm,
    fi_sector.row_stop_dttm,
    fi_sector.row_updt_dttm,
    fi_sector.etl_load_cyc_key,
    fi_sector.src_sys_id
   FROM hdm.fi_sector;

CREATE OR REPLACE VIEW hdm.vw_fi_sector_allocation
AS SELECT fi_sector_allocation.fi_sctr_allocn_key,
    fi_sector_allocation.fund_compst_key,
    fi_sector_allocation.day_key,
    fi_sector_allocation.fi_sctr_key,
    fi_sector_allocation.fund_wgt_exclsv_cash,
    fi_sector_allocation.fund_wgt_inclv_cash,
    fi_sector_allocation.prim_bmk_wgt,
    fi_sector_allocation.scnday_bmk_wgt,
    fi_sector_allocation.fund_dwe_exclsv_cash,
    fi_sector_allocation.fund_dwe_inclv_cash,
    fi_sector_allocation.prim_bmk_dwe,
    fi_sector_allocation.curr_row_flg,
    fi_sector_allocation.row_strt_dttm,
    fi_sector_allocation.row_stop_dttm,
    fi_sector_allocation.row_updt_dttm,
    fi_sector_allocation.etl_load_cyc_key,
    fi_sector_allocation.src_sys_id
   FROM hdm.fi_sector_allocation;

CREATE OR REPLACE VIEW hdm.vw_fi_time_period
AS SELECT fi_time_period.fi_tm_per_key,
    fi_time_period.tm_per,
    fi_time_period.curr_row_flg,
    fi_time_period.row_strt_dttm,
    fi_time_period.row_stop_dttm,
    fi_time_period.row_updt_dttm,
    fi_time_period.etl_load_cyc_key,
    fi_time_period.src_sys_id
   FROM hdm.fi_time_period;

CREATE OR REPLACE VIEW hdm.vw_fi_time_period_mapping
AS SELECT fi_time_period_mapping.fi_tm_per_mpng_key,
    fi_time_period_mapping.src_per,
    fi_time_period_mapping.trgt_per,
    fi_time_period_mapping.curr_row_flg,
    fi_time_period_mapping.row_strt_dttm,
    fi_time_period_mapping.row_stop_dttm,
    fi_time_period_mapping.row_updt_dttm,
    fi_time_period_mapping.etl_load_cyc_key,
    fi_time_period_mapping.src_sys_id
   FROM hdm.fi_time_period_mapping;

CREATE OR REPLACE VIEW hdm.vw_fi_top_issuers
AS SELECT fi_top_issuers.fi_top_issr_key,
    fi_top_issuers.fund_compst_key,
    fi_top_issuers.day_key,
    fi_top_issuers.fi_issr_key,
    fi_top_issuers.issr_pct,
    fi_top_issuers.curr_row_flg,
    fi_top_issuers.row_strt_dttm,
    fi_top_issuers.row_stop_dttm,
    fi_top_issuers.row_updt_dttm,
    fi_top_issuers.etl_load_cyc_key,
    fi_top_issuers.src_sys_id
   FROM hdm.fi_top_issuers;

CREATE OR REPLACE VIEW hdm.vw_fid_benchmark_performance
AS SELECT fid_benchmark_performance.fid_bmk_perf_key,
    fid_benchmark_performance.day_key,
    fid_benchmark_performance.fid_asset_cls_key,
    fid_benchmark_performance.fid_cust_key,
    fid_benchmark_performance.per_key,
    fid_benchmark_performance.bmk_ret,
    fid_benchmark_performance.annlzn_fctr,
    fid_benchmark_performance.strt_dt,
    fid_benchmark_performance.curr_row_flg,
    fid_benchmark_performance.row_strt_dttm,
    fid_benchmark_performance.row_stop_dttm,
    fid_benchmark_performance.etl_load_cyc_key,
    fid_benchmark_performance.src_sys_id
   FROM hdm.fid_benchmark_performance;

CREATE OR REPLACE VIEW hdm.vw_fid_benchmark_position
AS SELECT fid_benchmark_position.bmk_posn_key,
    fid_benchmark_position.fid_cust_key,
    fid_benchmark_position.day_key,
    fid_benchmark_position.fund_key,
    fid_benchmark_position.bmk_idx_key,
    fid_benchmark_position.mo_ret,
    fid_benchmark_position.curr_row_flg,
    fid_benchmark_position.row_strt_dttm,
    fid_benchmark_position.row_stop_dttm,
    fid_benchmark_position.etl_load_cyc_key,
    fid_benchmark_position.src_sys_id
   FROM hdm.fid_benchmark_position;

CREATE OR REPLACE VIEW hdm.vw_fid_benchmark_weight
AS SELECT fid_benchmark_weight.bmk_wgt_key,
    fid_benchmark_weight.bmk_idx_key,
    fid_benchmark_weight.fid_cust_key,
    fid_benchmark_weight.fid_asset_cls_key,
    fid_benchmark_weight.fund_key,
    fid_benchmark_weight.efftv_strt_dt,
    fid_benchmark_weight.efftv_end_dt,
    fid_benchmark_weight.bmk_wgt,
    fid_benchmark_weight.curr_row_flg,
    fid_benchmark_weight.row_strt_dttm,
    fid_benchmark_weight.row_stop_dttm,
    fid_benchmark_weight.etl_load_cyc_key,
    fid_benchmark_weight.src_sys_id
   FROM hdm.fid_benchmark_weight;

CREATE OR REPLACE VIEW hdm.vw_fid_bm_level_performance
AS SELECT fid_bm_level_performance.bm_lvl_perf_key,
    fid_bm_level_performance.fund_key,
    fid_bm_level_performance.bmk_idx_key,
    fid_bm_level_performance.day_key,
    fid_bm_level_performance.fid_cust_key,
    fid_bm_level_performance.per_key,
    fid_bm_level_performance.bmk_ret,
    fid_bm_level_performance.annlzn_fctr,
    fid_bm_level_performance.strt_dt,
    fid_bm_level_performance.incpn_dt,
    fid_bm_level_performance.curr_row_flg,
    fid_bm_level_performance.row_strt_dttm,
    fid_bm_level_performance.row_stop_dttm,
    fid_bm_level_performance.etl_load_cyc_key,
    fid_bm_level_performance.src_sys_id
   FROM hdm.fid_bm_level_performance;

CREATE OR REPLACE VIEW hdm.vw_fid_bm_level_wealth_index
AS SELECT fid_bm_level_wealth_index.bm_lvl_wlth_idx_key,
    fid_bm_level_wealth_index.fund_key,
    fid_bm_level_wealth_index.bmk_idx_key,
    fid_bm_level_wealth_index.day_key,
    fid_bm_level_wealth_index.fid_cust_key,
    fid_bm_level_wealth_index.wlth_idx,
    fid_bm_level_wealth_index.curr_row_flg,
    fid_bm_level_wealth_index.row_strt_dttm,
    fid_bm_level_wealth_index.row_stop_dttm,
    fid_bm_level_wealth_index.etl_load_cyc_key,
    fid_bm_level_wealth_index.src_sys_id
   FROM hdm.fid_bm_level_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_fid_bm_wealth_index
AS SELECT fid_bm_wealth_index.bmk_wlth_idx_key,
    fid_bm_wealth_index.day_key,
    fid_bm_wealth_index.fid_asset_cls_key,
    fid_bm_wealth_index.fid_cust_key,
    fid_bm_wealth_index.asset_ret,
    fid_bm_wealth_index.wlth_idx,
    fid_bm_wealth_index.curr_row_flg,
    fid_bm_wealth_index.row_strt_dttm,
    fid_bm_wealth_index.row_stop_dttm,
    fid_bm_wealth_index.etl_load_cyc_key,
    fid_bm_wealth_index.src_sys_id
   FROM hdm.fid_bm_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_fid_client_asset_mix
AS SELECT fid_client_asset_mix.fid_client_asset_mix_key,
    fid_client_asset_mix.fid_asset_cls_key,
    fid_client_asset_mix.fid_cust_key,
    fid_client_asset_mix.incpn_dt
   FROM hdm.fid_client_asset_mix;

CREATE OR REPLACE VIEW hdm.vw_fid_external_benchmark
AS SELECT fid_external_benchmark.ext_bmk_key,
    fid_external_benchmark.idx_nm
   FROM hdm.fid_external_benchmark;

CREATE OR REPLACE VIEW hdm.vw_fid_external_fund
AS SELECT fid_external_fund.ext_fund_key,
    fid_external_fund.ext_fund_nm,
    fid_external_fund.asset_cls_nm
   FROM hdm.fid_external_fund;

CREATE OR REPLACE VIEW hdm.vw_fid_fund_index_rltn
AS SELECT f.fund_key,
    br.fund_compst_key,
    br.bmk_idx_key
   FROM hdm.fund_index_rltn br,
    hdm.vw_fund f,
    hdm.vw_fund_composite c
  WHERE br.fund_compst_key = c.fund_compst_key AND f.fund_compst_key = c.fund_compst_key AND br.prim_bmk_flg::text = 'Y'::text AND br.perf_run_type_key = 1::numeric
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
AS SELECT fid_fund_level_performance.fund_lvl_perf_key,
    fid_fund_level_performance.fund_key,
    fid_fund_level_performance.day_key,
    fid_fund_level_performance.fid_cust_key,
    fid_fund_level_performance.per_key,
    fid_fund_level_performance.fund_ret,
    fid_fund_level_performance.annlzn_fctr,
    fid_fund_level_performance.strt_dt,
    fid_fund_level_performance.incpn_dt,
    fid_fund_level_performance.curr_row_flg,
    fid_fund_level_performance.row_strt_dttm,
    fid_fund_level_performance.row_stop_dttm,
    fid_fund_level_performance.etl_load_cyc_key,
    fid_fund_level_performance.src_sys_id
   FROM hdm.fid_fund_level_performance;

CREATE OR REPLACE VIEW hdm.vw_fid_fund_level_wealth_index
AS SELECT fid_fund_level_wealth_index.fund_lvl_wlth_idx_key,
    fid_fund_level_wealth_index.day_key,
    fid_fund_level_wealth_index.fund_key,
    fid_fund_level_wealth_index.fid_cust_key,
    fid_fund_level_wealth_index.wlth_idx,
    fid_fund_level_wealth_index.curr_row_flg,
    fid_fund_level_wealth_index.row_strt_dttm,
    fid_fund_level_wealth_index.row_stop_dttm,
    fid_fund_level_wealth_index.etl_load_cyc_key,
    fid_fund_level_wealth_index.src_sys_id
   FROM hdm.fid_fund_level_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_fid_fund_performance
AS SELECT fid_fund_performance.fid_fund_perf_key,
    fid_fund_performance.day_key,
    fid_fund_performance.fid_asset_cls_key,
    fid_fund_performance.fid_cust_key,
    fid_fund_performance.per_key,
    fid_fund_performance.fund_ret,
    fid_fund_performance.annlzn_fctr,
    fid_fund_performance.strt_dt,
    fid_fund_performance.curr_row_flg,
    fid_fund_performance.row_strt_dttm,
    fid_fund_performance.row_stop_dttm,
    fid_fund_performance.etl_load_cyc_key,
    fid_fund_performance.src_sys_id
   FROM hdm.fid_fund_performance;

CREATE OR REPLACE VIEW hdm.vw_fid_fund_wealth_index
AS SELECT fid_fund_wealth_index.fid_wlth_idx_key,
    fid_fund_wealth_index.day_key,
    fid_fund_wealth_index.fid_asset_cls_key,
    fid_fund_wealth_index.fid_cust_key,
    fid_fund_wealth_index.mkt_val,
    fid_fund_wealth_index.asset_ret,
    fid_fund_wealth_index.wlth_idx,
    fid_fund_wealth_index.curr_row_flg,
    fid_fund_wealth_index.row_strt_dttm,
    fid_fund_wealth_index.row_stop_dttm,
    fid_fund_wealth_index.etl_load_cyc_key,
    fid_fund_wealth_index.src_sys_id,
    fid_fund_wealth_index.mkt_val_adj
   FROM hdm.fid_fund_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_fid_letter_recipient
AS SELECT fid_letter_recipient.ltr_recpnt_key,
    fid_letter_recipient.fid_cust_key,
    fid_letter_recipient.recpnt_nm,
    fid_letter_recipient.addr_line_1,
    fid_letter_recipient.addr_line_2,
    fid_letter_recipient.addr_line_3,
    fid_letter_recipient.city,
    fid_letter_recipient.st,
    fid_letter_recipient.zip_cd,
    fid_letter_recipient.actv_flg,
    fid_letter_recipient.curr_row_flg,
    fid_letter_recipient.row_strt_dttm,
    fid_letter_recipient.row_stop_dttm,
    fid_letter_recipient.etl_load_cyc_key,
    fid_letter_recipient.src_sys_id
   FROM hdm.fid_letter_recipient;

CREATE OR REPLACE VIEW hdm.vw_fid_wealth_index_temp
AS SELECT fid_wealth_index_temp.fid_asset_cls_key,
    fid_wealth_index_temp.fid_cust_key,
    fid_wealth_index_temp.day_key,
    fid_wealth_index_temp.asset_ret,
    fid_wealth_index_temp.wlth_idx
   FROM hdm.fid_wealth_index_temp;

CREATE OR REPLACE VIEW hdm.vw_fiduciary_asset_class
AS SELECT fiduciary_asset_class.fid_asset_cls_key,
    fiduciary_asset_class.fid_asset_cls_desc
   FROM hdm.fiduciary_asset_class;

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
  WHERE ac.fid_asset_cls_desc::text = 'Total Equity Portfolio'::text AND (ff.asset_cls_nm::text = ANY (ARRAY['Domestic'::character varying, 'International and Global'::character varying]::text[]))
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
AS SELECT fiduciary_customer.fid_cust_key,
    fiduciary_customer.fid_cust_nm,
    fiduciary_customer.fid_cust_acct_nbr
   FROM hdm.fiduciary_customer;

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

CREATE OR REPLACE VIEW hdm.vw_fiduciary_position
AS SELECT fiduciary_position.fid_posn_key,
    fiduciary_position.fid_cust_key,
    fiduciary_position.day_key,
    fiduciary_position.fund_key,
    fiduciary_position.nav,
    fiduciary_position.shrs,
    fiduciary_position.mkt_val,
    fiduciary_position.mo_ret,
    fiduciary_position.curr_row_flg,
    fiduciary_position.row_strt_dttm,
    fiduciary_position.row_stop_dttm,
    fiduciary_position.etl_load_cyc_key,
    fiduciary_position.src_sys_id
   FROM hdm.fiduciary_position;

CREATE OR REPLACE VIEW hdm.vw_fund
AS SELECT fund.fund_key,
    fund.all_fund_cd,
    fund.all_fund_desc,
    fund.fund_compst_key,
    fund.shr_cls_cd,
    fund.shr_cls_desc,
    fund.fund_nm,
    fund.fund_long_desc,
    fund.fund_abrv,
    fund.fund_stat_cd,
    fund.fund_stat_desc,
    fund.fund_incpn_dt,
    fund.fund_clos_dt,
    fund.fund_nbr,
    fund.fund_sort_ord,
    fund.addr_id,
    fund.cusip_id,
    fund.quot_sym,
    fund.st_str_fund_nbr,
    fund.tax_id,
    fund.risk_lvl,
    fund.divd_freq_cd,
    fund.divd_freq_desc,
    fund.cap_gain_freq_cd,
    fund.cap_gain_freq_desc,
    fund.reg_acct_init_invmt,
    fund.ira_acct_init_invmt,
    fund.ugma_acct_init_invmt,
    fund.row_strt_dttm,
    fund.row_stop_dttm,
    fund.curr_row_flg,
    fund.src_sys_id,
    fund.etl_load_cyc_key,
    fund.perf_incpn_dt
   FROM hdm.fund;

CREATE OR REPLACE VIEW hdm.vw_fund_annual_investment
AS SELECT fund_annual_investment.fund_key,
    fund_annual_investment.annl_invmt_amt,
    fund_annual_investment.curr_row_flg,
    fund_annual_investment.row_strt_dttm,
    fund_annual_investment.row_stop_dttm,
    fund_annual_investment.etl_load_cyc_key,
    fund_annual_investment.src_sys_id
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
AS SELECT fund_composite.fund_compst_key,
    fund_composite.all_compst_cd,
    fund_composite.all_compst_desc,
    fund_composite.asset_cls_nm,
    fund_composite.asset_cls_desc,
    fund_composite.sub_cls_nm,
    fund_composite.sub_cls_desc,
    fund_composite.styl_nm,
    fund_composite.styl_desc,
    fund_composite.compst_cd,
    fund_composite.compst_nm,
    fund_composite.perf_cd,
    fund_composite.perf_nm,
    fund_composite.compst_ord,
    fund_composite.compst_stat_cd,
    fund_composite.compst_stat_desc,
    fund_composite.row_strt_dttm,
    fund_composite.row_stop_dttm,
    fund_composite.curr_row_flg,
    fund_composite.etl_load_cyc_key,
    fund_composite.src_sys_id,
    fund_composite.compst_src_id,
    fund_composite.fund_styl,
    fund_composite.risk_ret_ord,
    fund_composite.compst_incpn_dt,
    fund_composite.sales_cat,
    fund_composite.st_str_fund_nbr
   FROM hdm.fund_composite;

CREATE OR REPLACE VIEW hdm.vw_fund_dividend
AS SELECT fund_dividend.fund_divd_key,
    fund_dividend.fund_key,
    fund_dividend.rec_day_key,
    fund_dividend.reinvest_day_key,
    fund_dividend.paybl_day_key,
    fund_dividend.tot_divd,
    fund_dividend.net_invmt_incm,
    fund_dividend.shrs_of_rec,
    fund_dividend.re_invst_pr,
    fund_dividend.ordn_incm,
    fund_dividend.tot_shrt_term_gains,
    fund_dividend.tot_mid_term_gains,
    fund_dividend.tot_long_term_gains,
    fund_dividend.shrt_term_cap_gains,
    fund_dividend.mid_term_cap_gains,
    fund_dividend.long_term_cap_gains,
    fund_dividend.non_txbl_discnt,
    fund_dividend.forgn_tax_cr,
    fund_dividend.re_invst_idx,
    fund_dividend.est_divd_flg,
    fund_dividend.curr_row_flg,
    fund_dividend.row_strt_dttm,
    fund_dividend.row_stop_dttm,
    fund_dividend.etl_load_cyc_key,
    fund_dividend.src_sys_id,
    fund_dividend.qdi
   FROM hdm.fund_dividend;

CREATE OR REPLACE VIEW hdm.vw_fund_expense_ratio
AS SELECT fund_expense_ratio.fund_expns_ratio_key,
    fund_expense_ratio.expns_ratio_wavr_type_key,
    fund_expense_ratio.fund_key,
    fund_expense_ratio.semi_annl_rpt_dt,
    fund_expense_ratio.efftv_strt_dt,
    fund_expense_ratio.efftv_end_dt,
    fund_expense_ratio.gr_expns_ratio,
    fund_expense_ratio.net_expns_ratio,
    fund_expense_ratio.expns_wavr_ratio,
    fund_expense_ratio.cayman_ratio,
    fund_expense_ratio.curr_row_flg,
    fund_expense_ratio.row_strt_dttm,
    fund_expense_ratio.row_stop_dttm,
    fund_expense_ratio.etl_load_cyc_key,
    fund_expense_ratio.src_sys_id,
    fund_expense_ratio.manual_entry_flg,
    fund_expense_ratio.per_flg,
    fund_expense_ratio.pass_thru_mgmt_fee_wavr
   FROM hdm.fund_expense_ratio;

CREATE OR REPLACE VIEW hdm.vw_fund_holdings
AS SELECT fund_holdings.fund_holdg_key,
    fund_holdings.day_key,
    fund_holdings.fund_compst_key,
    fund_holdings.secr_key,
    fund_holdings.secr_attr_key,
    fund_holdings.indtry_key,
    fund_holdings.inc_crty_key,
    fund_holdings.iss_crty_key,
    fund_holdings.trde_crty_key,
    fund_holdings.nra_tax_crty_key,
    fund_holdings.base_crty_key,
    fund_holdings.base_currcy_key,
    fund_holdings.loc_currcy_key,
    fund_holdings.mkt_pr_currcy_key,
    fund_holdings.incm_currcy_key,
    fund_holdings.row_seq_nbr,
    fund_holdings.days_to_mtry,
    fund_holdings.par_shrs,
    fund_holdings.orig_base_cost,
    fund_holdings.orig_loc_cost,
    fund_holdings.idfd_base_cost,
    fund_holdings.idfd_loc_cost,
    fund_holdings.base_mkt_val,
    fund_holdings.loc_mkt_val,
    fund_holdings.curr_base_pr,
    fund_holdings.curr_loc_pr,
    fund_holdings.urlz_gl_loc,
    fund_holdings.urlz_gl_base,
    fund_holdings.avg_cost_base_par_shrs,
    fund_holdings.avg_cost_loc_par_shrs,
    fund_holdings.orig_strk_pr,
    fund_holdings.curr_strk_pr,
    fund_holdings.vend_suppld_iss_curr_pr,
    fund_holdings.base_net_amort,
    fund_holdings.loc_net_amort,
    fund_holdings.gr_base_amt_accrd,
    fund_holdings.gr_loc_amt_accrd,
    fund_holdings.curr_varb_rt,
    fund_holdings.partition_yr,
    fund_holdings.curr_row_flg,
    fund_holdings.row_strt_dttm,
    fund_holdings.row_stop_dttm,
    fund_holdings.etl_load_cyc_key,
    fund_holdings.src_sys_id
   FROM hdm.fund_holdings;

CREATE OR REPLACE VIEW hdm.vw_fund_index_rltn
AS SELECT fund_index_rltn.fund_idx_key,
    fund_index_rltn.fund_compst_key,
    fund_index_rltn.bmk_idx_key,
    fund_index_rltn.perf_run_type_key,
    fund_index_rltn.link_prio,
    fund_index_rltn.rpt_sort_ord,
    fund_index_rltn.prim_bmk_flg,
    fund_index_rltn.gr_ret_inlsn_flg,
    fund_index_rltn.curr_row_flg,
    fund_index_rltn.row_strt_dttm,
    fund_index_rltn.row_stop_dttm,
    fund_index_rltn.etl_load_cyc_key,
    fund_index_rltn.src_sys_id
   FROM hdm.fund_index_rltn;

CREATE OR REPLACE VIEW hdm.vw_fund_return
AS SELECT fund_return.fund_ret_key,
    fund_return.fund_key,
    fund_return.per_key,
    fund_return.day_key,
    fund_return.strt_per,
    fund_return.fund_ret,
    fund_return.annlzn_fctr,
    fund_return.asof_fund_ret,
    fund_return.ret_per_flg,
    fund_return.curr_row_flg,
    fund_return.row_strt_dttm,
    fund_return.row_stop_dttm,
    fund_return.etl_load_cyc_key,
    fund_return.src_sys_id,
    fund_return.ret_vrfyn_flg
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
AS SELECT fund_valuation.fund_vltn_key,
    fund_valuation.fund_key,
    fund_valuation.day_key,
    fund_valuation.nav_amt,
    fund_valuation.tot_shrs,
    fund_valuation.pr_per_shr,
    fund_valuation.wlth_idx,
    fund_valuation.asof_pr,
    fund_valuation.daily_pr_diff,
    fund_valuation.asof_wlth_idx,
    fund_valuation.hsg_pr_revs,
    fund_valuation.curr_row_flg,
    fund_valuation.row_strt_dttm,
    fund_valuation.row_stop_dttm,
    fund_valuation.etl_load_cyc_key,
    fund_valuation.src_sys_id
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
AS SELECT gross_return.gr_ret_key,
    gross_return.day_key,
    gross_return.fund_key,
    gross_return.bmk_idx_key,
    gross_return.per_key,
    gross_return.gr_ret,
    gross_return.info_ratio,
    gross_return.curr_row_flg,
    gross_return.row_strt_dttm,
    gross_return.row_stop_dttm,
    gross_return.etl_load_cyc_key,
    gross_return.src_sys_id,
    gross_return.idx_ret
   FROM hdm.gross_return;

CREATE OR REPLACE VIEW hdm.vw_gross_wealth_index
AS SELECT gross_wealth_index.gr_wlth_idx_key,
    gross_wealth_index.day_key,
    gross_wealth_index.fund_key,
    gross_wealth_index.gr_wlth_idx,
    gross_wealth_index.net_wlth_idx,
    gross_wealth_index.curr_row_flg,
    gross_wealth_index.row_strt_dttm,
    gross_wealth_index.row_stop_dttm,
    gross_wealth_index.etl_load_cyc_key,
    gross_wealth_index.src_sys_id
   FROM hdm.gross_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_group_trust_levels
AS SELECT group_trust_levels.grp_trst_lvl_key,
    group_trust_levels.asset_type,
    group_trust_levels.perf_thrs,
    group_trust_levels.asset_allocn,
    group_trust_levels.asset_allocn_prxy,
    group_trust_levels.accptbl_allocn_prxy,
    group_trust_levels.curr_row_flg,
    group_trust_levels.row_strt_dttm,
    group_trust_levels.row_stop_dttm,
    group_trust_levels.etl_load_cyc_key,
    group_trust_levels.src_sys_id
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
AS SELECT holdings_foreign_ownership.holdg_open_posn_key,
    holdings_foreign_ownership.fund_compst_key,
    holdings_foreign_ownership.secr_key,
    holdings_foreign_ownership.inc_crty_key,
    holdings_foreign_ownership.iss_crty_key,
    holdings_foreign_ownership.trde_crty_key,
    holdings_foreign_ownership.thrs_crty_key,
    holdings_foreign_ownership.day_key,
    holdings_foreign_ownership.tot_long_posn,
    holdings_foreign_ownership.actl_cstdy_posn,
    holdings_foreign_ownership.pend_sale_qty,
    holdings_foreign_ownership.pend_buy_qty,
    holdings_foreign_ownership.free_qty,
    holdings_foreign_ownership.oshr_qty,
    holdings_foreign_ownership.par_shr_qty,
    holdings_foreign_ownership.tot_shrt_posn,
    holdings_foreign_ownership.on_loan_qty,
    holdings_foreign_ownership.new_secr_flg,
    holdings_foreign_ownership.all_cls_oshr_qty,
    holdings_foreign_ownership.all_voting_oshr_qty,
    holdings_foreign_ownership.all_non_voting_oshr_qty,
    holdings_foreign_ownership.voting_shr_ind,
    holdings_foreign_ownership.curr_row_flg,
    holdings_foreign_ownership.row_strt_dttm,
    holdings_foreign_ownership.row_stop_dttm,
    holdings_foreign_ownership.etl_load_cyc_key,
    holdings_foreign_ownership.src_sys_id,
    holdings_foreign_ownership.prc_shrs_cls,
    holdings_foreign_ownership.prc_shrs_acls,
    holdings_foreign_ownership.prc_shrs_voting,
    holdings_foreign_ownership.prc_shrs_nonvoting,
    holdings_foreign_ownership.breach_ind_cls,
    holdings_foreign_ownership.breach_ind_acls,
    holdings_foreign_ownership.breach_ind_voting,
    holdings_foreign_ownership.breach_ind_nonvoting,
    holdings_foreign_ownership.thrsh_br_cls,
    holdings_foreign_ownership.thrsh_br_acls,
    holdings_foreign_ownership.thrsh_br_voting,
    holdings_foreign_ownership.thrsh_br_nonvoting,
    holdings_foreign_ownership.new_br_ind_cls,
    holdings_foreign_ownership.new_br_ind_acls,
    holdings_foreign_ownership.new_br_ind_voting,
    holdings_foreign_ownership.new_br_ind_nonvoting,
    holdings_foreign_ownership.spec_alert_cls,
    holdings_foreign_ownership.spec_alert_acls,
    holdings_foreign_ownership.spec_alert_voting,
    holdings_foreign_ownership.spec_alert_nonvoting,
    holdings_foreign_ownership.prox_int_br_cls,
    holdings_foreign_ownership.prox_ext_br_cls,
    holdings_foreign_ownership.prox_int_br_acls,
    holdings_foreign_ownership.prox_ext_br_acls,
    holdings_foreign_ownership.prox_int_br_voting,
    holdings_foreign_ownership.prox_ext_br_voting,
    holdings_foreign_ownership.prox_int_br_nonvoting,
    holdings_foreign_ownership.prox_ext_br_nonvoting,
    holdings_foreign_ownership.breach_ind_int_cls,
    holdings_foreign_ownership.breach_ind_int_acls,
    holdings_foreign_ownership.breach_ind_int_voting,
    holdings_foreign_ownership.breach_ind_int_nonvoting,
    holdings_foreign_ownership.issuer_nm,
    holdings_foreign_ownership.adr_spsr_ship_type
   FROM hdm.holdings_foreign_ownership;

CREATE OR REPLACE VIEW hdm.vw_holdings_open_position
AS SELECT holdings_open_position.holdg_open_posn_key,
    holdings_open_position.fund_compst_key,
    holdings_open_position.secr_key,
    holdings_open_position.inc_crty_key,
    holdings_open_position.iss_crty_key,
    holdings_open_position.trde_crty_key,
    holdings_open_position.thrs_crty_key,
    holdings_open_position.day_key,
    holdings_open_position.tot_long_posn,
    holdings_open_position.actl_cstdy_posn,
    holdings_open_position.pend_sale_qty,
    holdings_open_position.pend_buy_qty,
    holdings_open_position.free_qty,
    holdings_open_position.oshr_qty,
    holdings_open_position.par_shr_qty,
    holdings_open_position.tot_shrt_posn,
    holdings_open_position.on_loan_qty,
    holdings_open_position.new_secr_flg,
    holdings_open_position.all_cls_oshr_qty,
    holdings_open_position.all_voting_oshr_qty,
    holdings_open_position.all_non_voting_oshr_qty,
    holdings_open_position.voting_shr_ind,
    holdings_open_position.curr_row_flg,
    holdings_open_position.row_strt_dttm,
    holdings_open_position.row_stop_dttm,
    holdings_open_position.etl_load_cyc_key,
    holdings_open_position.src_sys_id,
    holdings_open_position.prc_shrs_cls,
    holdings_open_position.prc_shrs_acls,
    holdings_open_position.prc_shrs_voting,
    holdings_open_position.prc_shrs_nonvoting,
    holdings_open_position.breach_ind_cls,
    holdings_open_position.breach_ind_acls,
    holdings_open_position.breach_ind_voting,
    holdings_open_position.breach_ind_nonvoting,
    holdings_open_position.thrsh_br_cls,
    holdings_open_position.thrsh_br_acls,
    holdings_open_position.thrsh_br_voting,
    holdings_open_position.thrsh_br_nonvoting,
    holdings_open_position.new_br_ind_cls,
    holdings_open_position.new_br_ind_acls,
    holdings_open_position.new_br_ind_voting,
    holdings_open_position.new_br_ind_nonvoting,
    holdings_open_position.spec_alert_cls,
    holdings_open_position.spec_alert_acls,
    holdings_open_position.spec_alert_voting,
    holdings_open_position.spec_alert_nonvoting,
    holdings_open_position.prox_int_br_cls,
    holdings_open_position.prox_ext_br_cls,
    holdings_open_position.prox_int_br_acls,
    holdings_open_position.prox_ext_br_acls,
    holdings_open_position.prox_int_br_voting,
    holdings_open_position.prox_ext_br_voting,
    holdings_open_position.prox_int_br_nonvoting,
    holdings_open_position.prox_ext_br_nonvoting,
    holdings_open_position.breach_ind_int_cls,
    holdings_open_position.breach_ind_int_acls,
    holdings_open_position.breach_ind_int_voting,
    holdings_open_position.breach_ind_int_nonvoting,
    holdings_open_position.issuer_nm,
    holdings_open_position.adr_spsr_ship_type
   FROM hdm.holdings_open_position;

CREATE OR REPLACE VIEW hdm.vw_holdings_trade_hist
AS SELECT holdings_trade_hist.holdg_trde_hist_key,
    holdings_trade_hist.fund_compst_key,
    holdings_trade_hist.base_currcy_key,
    holdings_trade_hist.secr_key,
    holdings_trade_hist.trde_day_key,
    holdings_trade_hist.holdg_txn_cd_key,
    holdings_trade_hist.cash_rsn__key,
    holdings_trade_hist.cost_type_key,
    holdings_trade_hist.loc_currcy_key,
    holdings_trade_hist.sttl_currcy_key,
    holdings_trade_hist.trde_crty_key,
    holdings_trade_hist.xchg_type_key,
    holdings_trade_hist.indtry_key,
    holdings_trade_hist.post_day_key,
    holdings_trade_hist.sttl_day_key,
    holdings_trade_hist.ssb_trde_id,
    holdings_trade_hist.lot_acct_nbr,
    holdings_trade_hist.lot_nbr,
    holdings_trade_hist.acct_end_dt,
    holdings_trade_hist.contrctl_sttl_dt,
    holdings_trade_hist.sttl_locn_cd,
    holdings_trade_hist.sttl_dlr_id,
    holdings_trade_hist.stk_xchg_city_cd,
    holdings_trade_hist.shrt_term_secr_type,
    holdings_trade_hist.exectg_dlr_id,
    holdings_trade_hist.brkr_rsn_cd,
    holdings_trade_hist.inrt,
    holdings_trade_hist.mtry_dt,
    holdings_trade_hist.cncl_flg,
    holdings_trade_hist.trde_flg,
    holdings_trade_hist.rstc_flg,
    holdings_trade_hist.ssb_trade_cncl_id,
    holdings_trade_hist.post_type_cd,
    holdings_trade_hist.shr_qty,
    holdings_trade_hist.tot_amort_sold,
    holdings_trade_hist.orig_face_mbs_qty,
    holdings_trade_hist.base_intr_purch_sold,
    holdings_trade_hist.base_nra_tax_whg,
    holdings_trade_hist.base_nra_tax_refnd,
    holdings_trade_hist.opt_contrct_ut,
    holdings_trade_hist.strk_pr,
    holdings_trade_hist.base_net_amt,
    holdings_trade_hist.base_ut_pr,
    holdings_trade_hist.gr_intr_btsld,
    holdings_trade_hist.base_tax,
    holdings_trade_hist.base_commsn,
    holdings_trade_hist.base_fees,
    holdings_trade_hist.base_othr_fees,
    holdings_trade_hist.base_id_cost,
    holdings_trade_hist.base_curr_id_cost,
    holdings_trade_hist.base_amort_bkd,
    holdings_trade_hist.xchgrt,
    holdings_trade_hist.sttl_dt_xchgrt,
    holdings_trade_hist.base_avg_gls,
    holdings_trade_hist.base_lt_gls,
    holdings_trade_hist.base_shtm_gls,
    holdings_trade_hist.base_avg_currcy_gls,
    holdings_trade_hist.base_tax_expns_gls,
    holdings_trade_hist.curr_row_flg,
    holdings_trade_hist.row_strt_dttm,
    holdings_trade_hist.row_stop_dttm,
    holdings_trade_hist.etl_load_cyc_key,
    holdings_trade_hist.src_sys_id
   FROM hdm.holdings_trade_hist;

CREATE OR REPLACE VIEW hdm.vw_holdings_txn_codes
AS SELECT holdings_txn_codes.holdg_txn_cd_key,
    holdings_txn_codes.txn_type_cd,
    holdings_txn_codes.txn_type_desc,
    holdings_txn_codes.txn_cd,
    holdings_txn_codes.txn_cd_desc,
    holdings_txn_codes.curr_row_flg,
    holdings_txn_codes.row_strt_dttm,
    holdings_txn_codes.row_stop_dttm,
    holdings_txn_codes.etl_load_cyc_key,
    holdings_txn_codes.src_sys_id
   FROM hdm.holdings_txn_codes;

CREATE OR REPLACE VIEW hdm.vw_index_return
AS SELECT index_return.idx_ret_key,
    index_return.per_key,
    index_return.bmk_idx_key,
    index_return.fund_compst_key,
    index_return.day_key,
    index_return.idx_ret,
    index_return.strt_per,
    index_return.annlzn_fctr,
    index_return.ret_per_flg,
    index_return.perf_displ_ord,
    index_return.gen_rptg_flg,
    index_return.daily_rptg_flg,
    index_return.trgt_rptg_flg,
    index_return.curr_row_flg,
    index_return.row_strt_dttm,
    index_return.row_stop_dttm,
    index_return.etl_load_cyc_key,
    index_return.src_sys_id
   FROM hdm.index_return;

CREATE OR REPLACE VIEW hdm.vw_index_valuation
AS SELECT index_valuation.idx_vltn_key,
    index_valuation.bmk_idx_key,
    index_valuation.day_key,
    index_valuation.idx_vltn_amt,
    index_valuation.wlth_idx,
    index_valuation.daily_ret,
    index_valuation.curr_row_flg,
    index_valuation.row_strt_dttm,
    index_valuation.row_stop_dttm,
    index_valuation.etl_load_cyc_key,
    index_valuation.src_sys_id
   FROM hdm.index_valuation;

CREATE OR REPLACE VIEW hdm.vw_industry
AS SELECT industry.indtry_key,
    industry.sctr_cd,
    industry.sctr_desc,
    industry.indtry_cd,
    industry.indtry_desc,
    industry.curr_row_flg,
    industry.row_strt_dttm,
    industry.row_stop_dttm,
    industry.etl_load_cyc_key,
    industry.src_sys_id
   FROM hdm.industry;

CREATE OR REPLACE VIEW hdm.vw_intermediary_issue_type
AS SELECT intermediary_issue_type.intrm_iss_type_key,
    intermediary_issue_type.intrm_iss_type
   FROM hdm.intermediary_issue_type;

CREATE OR REPLACE VIEW hdm.vw_intermediary_issues
AS SELECT intermediary_issues.intrm_iss_key,
    intermediary_issues.day_key,
    intermediary_issues.dlr_key,
    intermediary_issues.intrm_iss_type_key,
    intermediary_issues.iss_cnt,
    intermediary_issues.curr_row_flg,
    intermediary_issues.row_strt_dttm,
    intermediary_issues.row_stop_dttm,
    intermediary_issues.etl_load_cyc_key,
    intermediary_issues.src_sys_id
   FROM hdm.intermediary_issues;

CREATE OR REPLACE VIEW hdm.vw_invoice_tracking
AS SELECT invoice_tracking.invc_trckg_key,
    invoice_tracking.invc_trckg_sum_key,
    invoice_tracking.dlr_fee_type_key,
    invoice_tracking.shr_cls_cd,
    invoice_tracking.shr_cls,
    invoice_tracking.fees,
    invoice_tracking.curr_row_flg,
    invoice_tracking.row_strt_dttm,
    invoice_tracking.row_stop_dttm,
    invoice_tracking.etl_load_cyc_key,
    invoice_tracking.src_sys_id
   FROM hdm.invoice_tracking;

CREATE OR REPLACE VIEW hdm.vw_invoice_tracking_summary
AS SELECT invoice_tracking_summary.invc_trckg_sum_key,
    invoice_tracking_summary.dlr_key,
    invoice_tracking_summary.pmt_day_key,
    invoice_tracking_summary.invc_trckg_id,
    invoice_tracking_summary.invc_yr,
    invoice_tracking_summary.invc_per,
    invoice_tracking_summary.invc_recpt_dt,
    invoice_tracking_summary.invc_tot_amt,
    invoice_tracking_summary.pmt_tot_amt,
    invoice_tracking_summary.anlyst_cmmts,
    invoice_tracking_summary.invc_anlyst,
    invoice_tracking_summary.invc_qc1,
    invoice_tracking_summary.invc_qc2,
    invoice_tracking_summary.accptbl_var,
    invoice_tracking_summary.pmt_freq,
    invoice_tracking_summary.invc_per_strt_dt,
    invoice_tracking_summary.invc_per_end_dt,
    invoice_tracking_summary.modfr,
    invoice_tracking_summary.creatn_dt,
    invoice_tracking_summary.creatr,
    invoice_tracking_summary.chg_dt,
    invoice_tracking_summary.curr_row_flg,
    invoice_tracking_summary.row_strt_dttm,
    invoice_tracking_summary.row_stop_dttm,
    invoice_tracking_summary.etl_load_cyc_key,
    invoice_tracking_summary.src_sys_id
   FROM hdm.invoice_tracking_summary;

CREATE OR REPLACE VIEW hdm.vw_manager_final_score
AS SELECT manager_final_score.fnl_scor_key,
    manager_final_score.mgr_list_key,
    manager_final_score.msr_cat_key,
    manager_final_score.day_key,
    manager_final_score.univ_key,
    manager_final_score.yr3_scor,
    manager_final_score.yr5_scor,
    manager_final_score.yr10_scor,
    manager_final_score.yr3_rank,
    manager_final_score.yr5_rank,
    manager_final_score.yr10_rank,
    manager_final_score.curr_row_flg,
    manager_final_score.row_strt_dttm,
    manager_final_score.row_stop_dttm,
    manager_final_score.etl_load_cyc_key,
    manager_final_score.src_sys_id
   FROM hdm.manager_final_score;

CREATE OR REPLACE VIEW hdm.vw_manager_information
AS SELECT manager_information.mgr_info_key,
    manager_information.day_key,
    manager_information.mgr_list_key,
    manager_information.univ_key,
    manager_information.aum_tot,
    manager_information.firm_incpn_yr,
    manager_information.prod_tot,
    manager_information.prfrd_bmk,
    manager_information.prim_univ,
    manager_information.prod_stat,
    manager_information.curr_holdg_cnt,
    manager_information.pct_holdg_top_ten,
    manager_information.annl_ltm,
    manager_information.curr_cash_posn,
    manager_information.wgta_mkt_cap,
    manager_information.mdn_mkt_cap,
    manager_information.prim_eqty_cap,
    manager_information.prim_eqty_styl,
    manager_information.curr_row_flg,
    manager_information.row_strt_dttm,
    manager_information.row_stop_dttm,
    manager_information.etl_load_cyc_key,
    manager_information.src_sys_id
   FROM hdm.manager_information;

CREATE OR REPLACE VIEW hdm.vw_manager_list
AS SELECT manager_list.mgr_list_key,
    manager_list.mgr_nm,
    manager_list.prod_nm,
    manager_list.idx_flg,
    manager_list.curr_row_flg,
    manager_list.row_strt_dttm,
    manager_list.row_stop_dttm,
    manager_list.etl_load_cyc_key,
    manager_list.src_sys_id
   FROM hdm.manager_list;

CREATE OR REPLACE VIEW hdm.vw_manager_rank_data
AS SELECT manager_rank_data.mgr_rank_data_key,
    manager_rank_data.mgr_list_key,
    manager_rank_data.msr_list_key,
    manager_rank_data.day_key,
    manager_rank_data.univ_key,
    manager_rank_data.ret_set,
    manager_rank_data.ret_val,
    manager_rank_data.ret_rank,
    manager_rank_data.ret_dt,
    manager_rank_data.curr_row_flg,
    manager_rank_data.row_strt_dttm,
    manager_rank_data.row_stop_dttm,
    manager_rank_data.etl_load_cyc_key,
    manager_rank_data.src_sys_id
   FROM hdm.manager_rank_data;

CREATE OR REPLACE VIEW hdm.vw_mdd_cashflow
AS SELECT mdd_cashflow.mdd_cashflow_key,
    mdd_cashflow.reg_key,
    mdd_cashflow.dlr_key,
    mdd_cashflow.fund_key,
    mdd_cashflow.acct_key,
    mdd_cashflow.trde_dt_key,
    mdd_cashflow.confirm_dt_key,
    mdd_cashflow.spr_sheet_day_key,
    mdd_cashflow.sub_shrs,
    mdd_cashflow.sub_amt,
    mdd_cashflow.redmpn_shrs,
    mdd_cashflow.redmpn_amt,
    mdd_cashflow.ofrg_pr,
    mdd_cashflow.net_cashflow_amt,
    mdd_cashflow.net_shrs,
    mdd_cashflow.row_strt_dttm,
    mdd_cashflow.row_stop_dttm,
    mdd_cashflow.curr_row_flg,
    mdd_cashflow.src_sys_id,
    mdd_cashflow.etl_load_cyc_key
   FROM hdm.mdd_cashflow;

CREATE OR REPLACE VIEW hdm.vw_measure_category
AS SELECT measure_category.msr_cat_key,
    measure_category.msr_cat_desc,
    measure_category.sub_cat_desc,
    measure_category.cat_ord,
    measure_category.curr_row_flg,
    measure_category.row_strt_dttm,
    measure_category.row_stop_dttm,
    measure_category.etl_load_cyc_key,
    measure_category.src_sys_id
   FROM hdm.measure_category;

CREATE OR REPLACE VIEW hdm.vw_measure_list
AS SELECT measure_list.msr_list_key,
    measure_list.msr_cat_key,
    measure_list.msr_id,
    measure_list.msr_nm,
    measure_list.cat_wgt,
    measure_list.yr3_wgt_flg,
    measure_list.yr5_wgt_flg,
    measure_list.yr10_wgt_flg,
    measure_list.rank_ord,
    measure_list.msr_grp,
    measure_list.msr_ord,
    measure_list.curr_row_flg,
    measure_list.row_strt_dttm,
    measure_list.row_stop_dttm,
    measure_list.etl_load_cyc_key,
    measure_list.src_sys_id
   FROM hdm.measure_list;

CREATE OR REPLACE VIEW hdm.vw_money_market_activity
AS SELECT money_market_activity.mony_mkt_key,
    money_market_activity.fund_key,
    money_market_activity.day_key,
    money_market_activity.gain_loss_amt,
    money_market_activity.dstrbtn_amt,
    money_market_activity.divd_paybl_amt,
    money_market_activity.basis_points,
    money_market_activity.days_to_mtry,
    money_market_activity.intr_w_gain_loss,
    money_market_activity.intr_wo_gain_loss,
    money_market_activity.curr_row_flg,
    money_market_activity.row_strt_dttm,
    money_market_activity.row_stop_dttm,
    money_market_activity.etl_load_cyc_key,
    money_market_activity.src_sys_id,
    money_market_activity.weighted_avg_life,
    money_market_activity.net_asset_at_cost,
    money_market_activity.net_asset_at_mkt,
    money_market_activity.invmt_at_cost,
    money_market_activity.invmt_at_val,
    money_market_activity.mkt_based_pr,
    money_market_activity.sub_w_drv_in_amt,
    money_market_activity.redmpn_amt,
    money_market_activity.net_shrhldr_cash_flow_amt,
    money_market_activity.daily_lqd_asset_amt,
    money_market_activity.daily_lqd_asset_rt,
    money_market_activity.wkl_lqd_asset_amt,
    money_market_activity.wkl_lqd_asset_rt,
    money_market_activity.shdw_nav,
    money_market_activity.cnst_nav
   FROM hdm.money_market_activity;

CREATE OR REPLACE VIEW hdm.vw_monthly_avg_position
AS SELECT monthly_avg_position.mthly_avg_posn_key,
    monthly_avg_position.dlr_key,
    monthly_avg_position.fund_key,
    monthly_avg_position.day_key,
    monthly_avg_position.tot_shrs,
    monthly_avg_position.tot_bal_amt,
    monthly_avg_position.tot_amt_excl_tpa,
    monthly_avg_position.tot_eligbl_bal,
    monthly_avg_position.tot_eligbl_excl_tpa,
    monthly_avg_position.dlr_tpa_flg,
    monthly_avg_position.curr_row_flg,
    monthly_avg_position.row_strt_dttm,
    monthly_avg_position.row_stop_dttm,
    monthly_avg_position.etl_load_cyc_key,
    monthly_avg_position.src_sys_id
   FROM hdm.monthly_avg_position;

CREATE OR REPLACE VIEW hdm.vw_monthly_avg_posn_draft
AS SELECT monthly_avg_posn_draft.mthly_avg_posn_drft_key,
    monthly_avg_posn_draft.dlr_key,
    monthly_avg_posn_draft.day_key,
    monthly_avg_posn_draft.fund_key,
    monthly_avg_posn_draft.tot_shrs,
    monthly_avg_posn_draft.tot_bal_amt,
    monthly_avg_posn_draft.tot_amt_excl_tpa,
    monthly_avg_posn_draft.tot_eligbl_bal,
    monthly_avg_posn_draft.tot_eligbl_excl_tpa,
    monthly_avg_posn_draft.dlr_tpa_flg,
    monthly_avg_posn_draft.curr_row_flg,
    monthly_avg_posn_draft.row_strt_dttm,
    monthly_avg_posn_draft.row_stop_dttm,
    monthly_avg_posn_draft.etl_load_cyc_key,
    monthly_avg_posn_draft.src_sys_id
   FROM hdm.monthly_avg_posn_draft;

CREATE OR REPLACE VIEW hdm.vw_operator
AS SELECT operator.opr_key,
    operator.day_key,
    operator.opr_id,
    operator.first_logon_dt,
    operator.iws_access_cd,
    operator.last_logon_dt,
    operator.opr_secr_last_maint,
    operator.last_maint_opr,
    operator.logon_cnt,
    operator.opr_expn_flg,
    operator.opr_nm,
    operator.expctd_strt_dt,
    operator.last_pswd_rst_dt,
    operator.pswd_chg_dt,
    operator.pswd_try_cnt,
    operator.ta3270_access_flg,
    operator.curr_row_flg,
    operator.row_strt_dttm,
    operator.row_stop_dttm,
    operator.etl_load_cyc_key,
    operator.src_sys_id,
    operator.parn_opr_key
   FROM hdm.operator;

CREATE OR REPLACE VIEW hdm.vw_operator_function
AS SELECT operator_function.opr_func_key,
    operator_function.day_key,
    operator_function.fac_key,
    operator_function.opr_key,
    operator_function.authzn_flg,
    operator_function.opr_func_last_mntn_dt,
    operator_function.add_func_flg,
    operator_function.brws_func_flg,
    operator_function.del_func_flg,
    operator_function.updt_func_flg,
    operator_function.minr_add_flg,
    operator_function.minr_brws_flg,
    operator_function.minr_del_flg,
    operator_function.minr_updt_flg,
    operator_function.curr_row_flg,
    operator_function.row_strt_dttm,
    operator_function.row_stop_dttm,
    operator_function.etl_load_cyc_key,
    operator_function.src_sys_id
   FROM hdm.operator_function;

CREATE OR REPLACE VIEW hdm.vw_payment_method
AS SELECT payment_method.pmt_mthd_key,
    payment_method.pmt_mthd_top_lvl_cd,
    payment_method.pmt_mthd_top_lvl_desc,
    payment_method.pmt_mthd_cd,
    payment_method.pmt_mthd_desc,
    payment_method.row_strt_dttm,
    payment_method.row_stop_dttm,
    payment_method.curr_row_flg,
    payment_method.src_sys_id,
    payment_method.etl_load_cyc_key
   FROM hdm.payment_method;

CREATE OR REPLACE VIEW hdm.vw_pending_all_trade
AS SELECT pending_all_trade.pend_all_trde_key,
    pending_all_trade.dlr_key,
    pending_all_trade.fund_key,
    pending_all_trade.acct_key,
    pending_all_trade.txn_type_key,
    pending_all_trade.trde_day_key,
    pending_all_trade.sttl_day_key,
    pending_all_trade.log_day_key,
    pending_all_trade.txn_ref_id,
    pending_all_trade.grp_txn_ref,
    pending_all_trade.entry_type,
    pending_all_trade.nca_flg,
    pending_all_trade.rvrs_flg,
    pending_all_trade.trde_delvy_flg,
    pending_all_trade.rep_cd,
    pending_all_trade.aft_hrs_flg,
    pending_all_trade.nscc_confrm_id,
    pending_all_trade.trde_src,
    pending_all_trade.dln_levy,
    pending_all_trade.pln_id,
    pending_all_trade.txn_pr,
    pending_all_trade.gr_cash_val,
    pending_all_trade.cb_val,
    pending_all_trade.txn_tax_amt,
    pending_all_trade.shr_val,
    pending_all_trade.net_cash_val,
    pending_all_trade.ltst_batch_flg,
    pending_all_trade.load_seq_nbr,
    pending_all_trade.curr_row_flg,
    pending_all_trade.row_strt_dttm,
    pending_all_trade.row_stop_dttm,
    pending_all_trade.etl_load_cyc_key,
    pending_all_trade.src_sys_id,
    pending_all_trade.row_updt_dttm
   FROM hdm.pending_all_trade;

CREATE OR REPLACE VIEW hdm.vw_pending_trade
AS SELECT pending_trade.pend_trde_key,
    pending_trade.dlr_key,
    pending_trade.fund_key,
    pending_trade.acct_key,
    pending_trade.txn_type_key,
    pending_trade.trde_day_key,
    pending_trade.sttl_day_key,
    pending_trade.log_day_key,
    pending_trade.txn_ref_id,
    pending_trade.grp_txn_ref,
    pending_trade.entry_type,
    pending_trade.nca_flg,
    pending_trade.rvrs_flg,
    pending_trade.trde_delvy_flg,
    pending_trade.rep_cd,
    pending_trade.aft_hrs_flg,
    pending_trade.nscc_confrm_id,
    pending_trade.trde_src,
    pending_trade.dln_levy,
    pending_trade.pln_id,
    pending_trade.txn_pr,
    pending_trade.gr_cash_val,
    pending_trade.cb_val,
    pending_trade.txn_tax_amt,
    pending_trade.shr_val,
    pending_trade.net_cash_val,
    pending_trade.ltst_batch_flg,
    pending_trade.curr_row_flg,
    pending_trade.row_strt_dttm,
    pending_trade.row_stop_dttm,
    pending_trade.etl_load_cyc_key,
    pending_trade.src_sys_id,
    pending_trade.load_seq_nbr
   FROM hdm.pending_trade;

CREATE OR REPLACE VIEW hdm.vw_perf_run_type
AS SELECT perf_run_type.perf_run_type_key,
    perf_run_type.run_type_cd,
    perf_run_type.run_type_desc
   FROM hdm.perf_run_type;

CREATE OR REPLACE VIEW hdm.vw_pimco_account_reference
AS SELECT pimco_account_reference.pmc_acct_ref_key,
    pimco_account_reference.fund_compst_key,
    pimco_account_reference.pmc_acct_nbr,
    pimco_account_reference.curr_row_flg,
    pimco_account_reference.row_strt_dttm,
    pimco_account_reference.row_stop_dttm,
    pimco_account_reference.etl_load_cyc_key,
    pimco_account_reference.src_sys_id
   FROM hdm.pimco_account_reference;

CREATE OR REPLACE VIEW hdm.vw_pimco_country_breakout
AS SELECT pimco_country_breakout.crty_brk_out_key,
    pimco_country_breakout.fund_compst_key,
    pimco_country_breakout.pmc_reg_key,
    pimco_country_breakout.day_key,
    pimco_country_breakout.acct_ext_dur_yr,
    pimco_country_breakout.acct_ext_em_sprd,
    pimco_country_breakout.acct_loc_dur_yr,
    pimco_country_breakout.acct_curr_expsr,
    pimco_country_breakout.bmk_ext_dur_yr,
    pimco_country_breakout.bmk_ext_em_sprd,
    pimco_country_breakout.bmk_loc_dur_yr,
    pimco_country_breakout.bmk_curr_expsr,
    pimco_country_breakout.var_ext_dur_yr,
    pimco_country_breakout.var_ext_em_sprd,
    pimco_country_breakout.var_loc_dur_yr,
    pimco_country_breakout.var_curr_expsr,
    pimco_country_breakout.curr_row_flg,
    pimco_country_breakout.row_strt_dttm,
    pimco_country_breakout.row_stop_dttm,
    pimco_country_breakout.etl_load_cyc_key,
    pimco_country_breakout.src_sys_id
   FROM hdm.pimco_country_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_derivative_bucket
AS SELECT pimco_derivative_bucket.dervtv_buckt_key,
    pimco_derivative_bucket.dervtv_buckt_nm,
    pimco_derivative_bucket.curr_row_flg,
    pimco_derivative_bucket.row_strt_dttm,
    pimco_derivative_bucket.row_stop_dttm,
    pimco_derivative_bucket.etl_load_cyc_key,
    pimco_derivative_bucket.src_sys_id
   FROM hdm.pimco_derivative_bucket;

CREATE OR REPLACE VIEW hdm.vw_pimco_derivatives
AS SELECT pimco_derivatives.dervtv_key,
    pimco_derivatives.dervtv_buckt_key,
    pimco_derivatives.day_key,
    pimco_derivatives.fund_compst_key,
    pimco_derivatives.ntional_amt,
    pimco_derivatives.mkt_val,
    pimco_derivatives.mkt_val_pct,
    pimco_derivatives.dur_pct,
    pimco_derivatives.dwe,
    pimco_derivatives.curr_row_flg,
    pimco_derivatives.row_strt_dttm,
    pimco_derivatives.row_stop_dttm,
    pimco_derivatives.etl_load_cyc_key,
    pimco_derivatives.src_sys_id
   FROM hdm.pimco_derivatives;

CREATE OR REPLACE VIEW hdm.vw_pimco_duration_breakout
AS SELECT pimco_duration_breakout.pmc_dur_brk_out_key,
    pimco_duration_breakout.dur_buckt_key,
    pimco_duration_breakout.fund_compst_key,
    pimco_duration_breakout.day_key,
    pimco_duration_breakout.acct_dur_pct,
    pimco_duration_breakout.acct_dwe,
    pimco_duration_breakout.bmk_dur_pct,
    pimco_duration_breakout.bmk_dwe,
    pimco_duration_breakout.var_dur_pct,
    pimco_duration_breakout.var_dwe,
    pimco_duration_breakout.curr_row_flg,
    pimco_duration_breakout.row_strt_dttm,
    pimco_duration_breakout.row_stop_dttm,
    pimco_duration_breakout.etl_load_cyc_key,
    pimco_duration_breakout.src_sys_id
   FROM hdm.pimco_duration_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_duration_bucket
AS SELECT pimco_duration_bucket.dur_buckt_key,
    pimco_duration_bucket.dur_buckt_nm,
    pimco_duration_bucket.curr_row_flg,
    pimco_duration_bucket.row_strt_dttm,
    pimco_duration_bucket.row_stop_dttm,
    pimco_duration_bucket.etl_load_cyc_key,
    pimco_duration_bucket.src_sys_id
   FROM hdm.pimco_duration_bucket;

CREATE OR REPLACE VIEW hdm.vw_pimco_maturity_breakout
AS SELECT pimco_maturity_breakout.mtry_brk_out_key,
    pimco_maturity_breakout.pmc_mtry_buckt_key,
    pimco_maturity_breakout.fund_compst_key,
    pimco_maturity_breakout.day_key,
    pimco_maturity_breakout.acct_mkt_val,
    pimco_maturity_breakout.acct_mkt_val_pct,
    pimco_maturity_breakout.bmk_mkt_val_pct,
    pimco_maturity_breakout.var_mkt_val_pct,
    pimco_maturity_breakout.curr_row_flg,
    pimco_maturity_breakout.row_strt_dttm,
    pimco_maturity_breakout.row_stop_dttm,
    pimco_maturity_breakout.etl_load_cyc_key,
    pimco_maturity_breakout.src_sys_id
   FROM hdm.pimco_maturity_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_maturity_bucket
AS SELECT pimco_maturity_bucket.pmc_mtry_buckt_key,
    pimco_maturity_bucket.mtry_buckt_nm,
    pimco_maturity_bucket.curr_row_flg,
    pimco_maturity_bucket.row_strt_dttm,
    pimco_maturity_bucket.row_stop_dttm,
    pimco_maturity_bucket.etl_load_cyc_key,
    pimco_maturity_bucket.src_sys_id,
    pimco_maturity_bucket.mtry_buckt_type
   FROM hdm.pimco_maturity_bucket;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_commodity
AS SELECT pimco_mkt_commodity.mkt_cmdty_key,
    pimco_mkt_commodity.pmc_sctr_key,
    pimco_mkt_commodity.fund_compst_key,
    pimco_mkt_commodity.day_key,
    pimco_mkt_commodity.bmk_mkt_val_pct,
    pimco_mkt_commodity.curr_row_flg,
    pimco_mkt_commodity.row_strt_dttm,
    pimco_mkt_commodity.row_stop_dttm,
    pimco_mkt_commodity.etl_load_cyc_key,
    pimco_mkt_commodity.src_sys_id
   FROM hdm.pimco_mkt_commodity;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_country
AS SELECT pimco_mkt_country.mkt_cntr_key,
    pimco_mkt_country.crty_nm,
    pimco_mkt_country.curr_row_flg,
    pimco_mkt_country.row_strt_dttm,
    pimco_mkt_country.row_stop_dttm,
    pimco_mkt_country.etl_load_cyc_key,
    pimco_mkt_country.src_sys_id
   FROM hdm.pimco_mkt_country;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_maturity
AS SELECT pimco_mkt_maturity.mktg_mtry_key,
    pimco_mkt_maturity.day_key,
    pimco_mkt_maturity.fund_compst_key,
    pimco_mkt_maturity.pmc_mtry_buckt_key,
    pimco_mkt_maturity.fund_mtry,
    pimco_mkt_maturity.bmk_mtry,
    pimco_mkt_maturity.curr_row_flg,
    pimco_mkt_maturity.row_strt_dttm,
    pimco_mkt_maturity.row_stop_dttm,
    pimco_mkt_maturity.etl_load_cyc_key,
    pimco_mkt_maturity.src_sys_id
   FROM hdm.pimco_mkt_maturity;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_quality
AS SELECT pimco_mkt_quality.mkt_qual_key,
    pimco_mkt_quality.day_key,
    pimco_mkt_quality.fund_compst_key,
    pimco_mkt_quality.pmc_qual_buckt_key,
    pimco_mkt_quality.fund_qual_rtng,
    pimco_mkt_quality.bmk_qual_rtng,
    pimco_mkt_quality.curr_row_flg,
    pimco_mkt_quality.row_strt_dttm,
    pimco_mkt_quality.row_stop_dttm,
    pimco_mkt_quality.etl_load_cyc_key,
    pimco_mkt_quality.src_sys_id
   FROM hdm.pimco_mkt_quality;

CREATE OR REPLACE VIEW hdm.vw_pimco_mkt_survey
AS SELECT pimco_mkt_survey.mkt_srvy_key,
    pimco_mkt_survey.day_key,
    pimco_mkt_survey.fund_compst_key,
    pimco_mkt_survey.mkt_cntr_key,
    pimco_mkt_survey.fund_crty_pct,
    pimco_mkt_survey.fund_currcy_pct,
    pimco_mkt_survey.bmk_crty_pct,
    pimco_mkt_survey.bmk_currcy_pct,
    pimco_mkt_survey.curr_row_flg,
    pimco_mkt_survey.row_strt_dttm,
    pimco_mkt_survey.row_stop_dttm,
    pimco_mkt_survey.etl_load_cyc_key,
    pimco_mkt_survey.src_sys_id
   FROM hdm.pimco_mkt_survey;

CREATE OR REPLACE VIEW hdm.vw_pimco_quality_breakout
AS SELECT pimco_quality_breakout.qual_brk_out_key,
    pimco_quality_breakout.pmc_qual_buckt_key,
    pimco_quality_breakout.fund_compst_key,
    pimco_quality_breakout.day_key,
    pimco_quality_breakout.acct_expsr_amt,
    pimco_quality_breakout.acct_expsr_pct,
    pimco_quality_breakout.acct_dur_pct,
    pimco_quality_breakout.acct_dwe,
    pimco_quality_breakout.bmk_expsr_pct,
    pimco_quality_breakout.bmk_dur_pct,
    pimco_quality_breakout.bmk_dwe,
    pimco_quality_breakout.var_expsr_pct,
    pimco_quality_breakout.var_dur_pct,
    pimco_quality_breakout.var_dwe,
    pimco_quality_breakout.curr_row_flg,
    pimco_quality_breakout.row_strt_dttm,
    pimco_quality_breakout.row_stop_dttm,
    pimco_quality_breakout.etl_load_cyc_key,
    pimco_quality_breakout.src_sys_id
   FROM hdm.pimco_quality_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_quality_bucket
AS SELECT pimco_quality_bucket.pmc_qual_buckt_key,
    pimco_quality_bucket.qual_buckt_nm,
    pimco_quality_bucket.qual_sort_ord,
    pimco_quality_bucket.curr_row_flg,
    pimco_quality_bucket.row_strt_dttm,
    pimco_quality_bucket.row_stop_dttm,
    pimco_quality_bucket.etl_load_cyc_key,
    pimco_quality_bucket.src_sys_id,
    pimco_quality_bucket.qual_buckt_type
   FROM hdm.pimco_quality_bucket;

CREATE OR REPLACE VIEW hdm.vw_pimco_region
AS SELECT pimco_region.pmc_reg_key,
    pimco_region.mkt_type_desc,
    pimco_region.reg_cd,
    pimco_region.reg_desc,
    pimco_region.crty_id,
    pimco_region.crty_nm,
    pimco_region.curr_row_flg,
    pimco_region.row_strt_dttm,
    pimco_region.row_stop_dttm,
    pimco_region.etl_load_cyc_key,
    pimco_region.src_sys_id
   FROM hdm.pimco_region;

CREATE OR REPLACE VIEW hdm.vw_pimco_sector
AS SELECT pimco_sector.pmc_sctr_key,
    pimco_sector.sctr_type,
    pimco_sector.sctr_lvl_desc,
    pimco_sector.sctr_sub_lvl_desc,
    pimco_sector.curr_row_flg,
    pimco_sector.row_strt_dttm,
    pimco_sector.row_stop_dttm,
    pimco_sector.etl_load_cyc_key,
    pimco_sector.src_sys_id
   FROM hdm.pimco_sector;

CREATE OR REPLACE VIEW hdm.vw_pimco_sector_breakout
AS SELECT pimco_sector_breakout.sctr_brk_out_key,
    pimco_sector_breakout.pmc_sctr_key,
    pimco_sector_breakout.fund_compst_key,
    pimco_sector_breakout.day_key,
    pimco_sector_breakout.acct_mkt_val,
    pimco_sector_breakout.acct_mkt_val_pct,
    pimco_sector_breakout.acct_dur_pct,
    pimco_sector_breakout.acct_dwe,
    pimco_sector_breakout.bmk_mkt_val_pct,
    pimco_sector_breakout.bmk_dur_pct,
    pimco_sector_breakout.bmk_dwe,
    pimco_sector_breakout.var_mkt_val_pct,
    pimco_sector_breakout.var_dur_pct,
    pimco_sector_breakout.var_dwe,
    pimco_sector_breakout.curr_row_flg,
    pimco_sector_breakout.row_strt_dttm,
    pimco_sector_breakout.row_stop_dttm,
    pimco_sector_breakout.etl_load_cyc_key,
    pimco_sector_breakout.src_sys_id
   FROM hdm.pimco_sector_breakout;

CREATE OR REPLACE VIEW hdm.vw_pimco_statistics
AS SELECT pimco_statistics.pmc_stats_key,
    pimco_statistics.day_key,
    pimco_statistics.fund_compst_key,
    pimco_statistics.mkt_val,
    pimco_statistics.qtrly_contrbn,
    pimco_statistics.est_incm,
    pimco_statistics.est_ytm,
    pimco_statistics.efftv_dur,
    pimco_statistics.avg_mtry,
    pimco_statistics.avg_qual,
    pimco_statistics.avg_coupn,
    pimco_statistics.curr_yld,
    pimco_statistics.bmk_avg_qual,
    pimco_statistics.bmk_efftv_dur,
    pimco_statistics.bmk_curr_yld,
    pimco_statistics.bmk_pmc_dur,
    pimco_statistics.bmk_est_ytm,
    pimco_statistics.curr_row_flg,
    pimco_statistics.row_strt_dttm,
    pimco_statistics.row_stop_dttm,
    pimco_statistics.etl_load_cyc_key,
    pimco_statistics.src_sys_id,
    pimco_statistics.tot_net_asset,
    pimco_statistics.finc_asset,
    pimco_statistics.net_asset_n_cash,
    pimco_statistics.fund_iss_cnt,
    pimco_statistics.bmk_iss_cnt,
    pimco_statistics.bmk_avg_mkt_coupn,
    pimco_statistics.bmk_wgta_mtry
   FROM hdm.pimco_statistics;

CREATE OR REPLACE VIEW hdm.vw_position_type
AS SELECT position_type.posn_type_key,
    position_type.posn_cd,
    position_type.posn_desc
   FROM hdm.position_type;

CREATE OR REPLACE VIEW hdm.vw_pro_expense_ratio
AS SELECT pro_expense_ratio.pro_expns_ratio_key,
    pro_expense_ratio.day_key,
    pro_expense_ratio.fund_key,
    pro_expense_ratio.gr_expns_ratio,
    pro_expense_ratio.net_expns_ratio,
    pro_expense_ratio.curr_row_flg,
    pro_expense_ratio.row_strt_dttm,
    pro_expense_ratio.row_stop_dttm,
    pro_expense_ratio.etl_load_cyc_key,
    pro_expense_ratio.src_sys_id
   FROM hdm.pro_expense_ratio;

CREATE OR REPLACE VIEW hdm.vw_reason_code
AS SELECT reason_code.rsn_key,
    reason_code.entity_rsn_cd,
    reason_code.entity_desc,
    reason_code.rsn_cd,
    reason_code.rsn_shrt_desc,
    reason_code.rsn_long_desc,
    reason_code.row_strt_dttm,
    reason_code.row_stop_dttm,
    reason_code.etl_load_cyc_key,
    reason_code.src_sys_id,
    reason_code.curr_row_flg
   FROM hdm.reason_code;

CREATE OR REPLACE VIEW hdm.vw_region
AS SELECT region.reg_key,
    region.st_cd,
    region.st_nm,
    region.mktg_reg_id,
    region.mktg_reg_nm,
    region.crty_cd,
    region.crty_nm,
    region.glbl_reg_cd,
    region.glbl_reg_nm,
    region.reg_type_cd,
    region.reg_type_nm,
    region.top_lvl_reg_cd,
    region.top_lvl_reg_nm,
    region.row_strt_dttm,
    region.row_stop_dttm,
    region.etl_load_cyc_key,
    region.src_sys_id,
    region.curr_row_flg
   FROM hdm.region;

CREATE OR REPLACE VIEW hdm.vw_report_filters
AS SELECT report_filters.filtr_type,
    report_filters.filtr_val,
    report_filters.row_strt_dttm,
    report_filters.row_stop_dttm,
    report_filters.curr_row_flg,
    report_filters.filtr_emp_val,
    report_filters.trste_flg,
    report_filters.offr_flg
   FROM hdm.report_filters;

CREATE OR REPLACE VIEW hdm.vw_return_periods
AS SELECT return_periods.per_key,
    return_periods.per_nm,
    return_periods.per_desc,
    return_periods.per_ut,
    return_periods.per_msr,
    return_periods.annl_fctr,
    return_periods.daily_msr_flg,
    return_periods.mo_end_msr_flg,
    return_periods.per_sort_ord,
    return_periods.full_desc,
    return_periods.curr_row_flg,
    return_periods.row_strt_dttm,
    return_periods.row_stop_dttm,
    return_periods.etl_load_cyc_key,
    return_periods.src_sys_id
   FROM hdm.return_periods;

CREATE OR REPLACE VIEW hdm.vw_sdcm_cashflow
AS SELECT sdcm_cashflow.sdcm_cashflow_key,
    sdcm_cashflow.reg_key,
    sdcm_cashflow.pmt_mthd_key,
    sdcm_cashflow.dlr_key,
    sdcm_cashflow.shrhldr_key,
    sdcm_cashflow.acct_key,
    sdcm_cashflow.fund_key,
    sdcm_cashflow.spr_sheet_dt_key,
    sdcm_cashflow.trde_dt_key,
    sdcm_cashflow.confirm_dt_key,
    sdcm_cashflow.shrhldr_role_key,
    sdcm_cashflow.acct_nbr,
    sdcm_cashflow.sub_shrs,
    sdcm_cashflow.sub_amt,
    sdcm_cashflow.redmpn_shrs,
    sdcm_cashflow.redmpn_amt,
    sdcm_cashflow.ofrg_pr,
    sdcm_cashflow.nav,
    sdcm_cashflow.dlr_commsn_amt,
    sdcm_cashflow.undr_wrtr_commsn_amt,
    sdcm_cashflow.adv_commsn_amt,
    sdcm_cashflow.net_shrs,
    sdcm_cashflow.net_cashflow_amt,
    sdcm_cashflow.cash_cntl_recon_flg,
    sdcm_cashflow.row_strt_dttm,
    sdcm_cashflow.row_stop_dttm,
    sdcm_cashflow.curr_row_flg,
    sdcm_cashflow.src_sys_id,
    sdcm_cashflow.etl_load_cyc_key
   FROM hdm.sdcm_cashflow;

CREATE OR REPLACE VIEW hdm.vw_sdcm_fund_balance
AS SELECT sdcm_fund_balance.fund_bal_key,
    sdcm_fund_balance.fund_key,
    sdcm_fund_balance.day_key,
    sdcm_fund_balance.tot_fund_shrs,
    sdcm_fund_balance.tot_fund_asset,
    sdcm_fund_balance.curr_row_flg,
    sdcm_fund_balance.row_strt_dttm,
    sdcm_fund_balance.row_stop_dttm,
    sdcm_fund_balance.etl_load_cyc_key,
    sdcm_fund_balance.src_sys_id
   FROM hdm.sdcm_fund_balance;

CREATE OR REPLACE VIEW hdm.vw_security
AS SELECT security.secr_key,
    security.sedol_id,
    security.isin_id,
    security.tckr_sym,
    security.secr_nm,
    security.issuer_nm,
    security.glbl_issuer_nm,
    security.invmt_type_cd,
    security.invmt_type_desc,
    security.dtc_cusip,
    security.asset_id,
    security.muncpl_secr_st_cd,
    security.muncpl_secr_st,
    security.curr_row_flg,
    security.row_strt_dttm,
    security.row_stop_dttm,
    security.etl_load_cyc_key,
    security.src_sys_id
   FROM hdm.security;

CREATE OR REPLACE VIEW hdm.vw_security_attributes
AS SELECT security_attributes.secr_attr_key,
    security_attributes.asset_id,
    security_attributes.ssb_fund_nbr,
    security_attributes.lot_acct_nbr,
    security_attributes.secr_type,
    security_attributes.cusip_id,
    security_attributes.cash_id,
    security_attributes.spot_id,
    security_attributes.fwd_id,
    security_attributes.shrt_term_purch_dt,
    security_attributes.shrt_term_sttl_dt,
    security_attributes.orig_iss_dt,
    security_attributes.mtry_dt,
    security_attributes.moody_rtng,
    security_attributes.s_p_rtng,
    security_attributes.call_put_ind,
    security_attributes.repo_nbr,
    security_attributes.ssb_trde_id,
    security_attributes.fx_brkr_cd,
    security_attributes.rpt_cls_cd,
    security_attributes.rpt_cls_desc,
    security_attributes.iss_cls_cd,
    security_attributes.iss_cls_desc,
    security_attributes.nbr_of_contrct,
    security_attributes.orig_face_amt,
    security_attributes.fx_closg_ind,
    security_attributes.invmt_ind,
    security_attributes.invmt_desc,
    security_attributes.asset_grp_cd,
    security_attributes.asset_grp_desc,
    security_attributes.invmt_mkt_val_flg,
    security_attributes.mkt_val_cd,
    security_attributes.mkt_val_desc,
    security_attributes.posn_type_cd,
    security_attributes.posn_type_desc,
    security_attributes.orig_coupn_rt,
    security_attributes.varb_rt_freq_cd,
    security_attributes.varb_rt_freq_desc,
    security_attributes.varb_rt_chg_dt,
    security_attributes.mkt_pr_src_cd,
    security_attributes.mkt_pr_src_vend,
    security_attributes.mkt_pr_src_desc,
    security_attributes.pool_type_cd,
    security_attributes.pool_type_desc,
    security_attributes.curr_row_flg,
    security_attributes.row_strt_dttm,
    security_attributes.row_stop_dttm,
    security_attributes.etl_load_cyc_key,
    security_attributes.src_sys_id
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
AS SELECT shareholder.shrhldr_key,
    shareholder.shrhldr_role_key,
    shareholder.shrhldr_top_lvl_cd,
    shareholder.shrhldr_top_lvl_desc,
    shareholder.shrhldr_nm,
    shareholder.tax_idnfcn_nbr,
    shareholder.ssn_stat_desc,
    shareholder.tax_idnfcn_type,
    shareholder.cust_lgl_ownr_regstn_dt,
    shareholder.cust_idnfcn_nbr,
    shareholder.cust_idnfcn_nbr_2,
    shareholder.vip_cust_flg,
    shareholder.send_mktg_info_flg,
    shareholder.informed_cnsnt_flg,
    shareholder.party_type,
    shareholder.cust_empl_flg,
    shareholder.mgmt_co_empl_flg,
    shareholder.lgl_stat,
    shareholder.gend,
    shareholder.brth_dt,
    shareholder.dcsd_dt,
    shareholder.emplmt_stat,
    shareholder.mrtl_stat,
    shareholder.crty_of_citzn_ship,
    shareholder.addr_type,
    shareholder.addr_line_1,
    shareholder.addr_line_2,
    shareholder.addr_line_3,
    shareholder.addr_line_4,
    shareholder.city,
    shareholder.st,
    shareholder.prov,
    shareholder.crty,
    shareholder.reg,
    shareholder.row_strt_dttm,
    shareholder.row_stop_dttm,
    shareholder.curr_row_flg,
    shareholder.etl_load_cyc_key,
    shareholder.src_sys_id,
    shareholder.purg_prcsd_flg,
    shareholder.purg_prcsd_dt
   FROM hdm.shareholder;

CREATE OR REPLACE VIEW hdm.vw_shareholder_distrib
AS SELECT shareholder_distrib.shrhldr_dstrbtn_key,
    shareholder_distrib.acct_key,
    shareholder_distrib.shrhldr_key,
    shareholder_distrib.fund_key,
    shareholder_distrib.dstrbtn_cd_key,
    shareholder_distrib.dstrbtn_type_key,
    shareholder_distrib.rec_day_key,
    shareholder_distrib.paybl_day_key,
    shareholder_distrib.re_invst_day_key,
    shareholder_distrib.dstrbtn_rt_per_shr,
    shareholder_distrib.fund_expns_ratio,
    shareholder_distrib.re_invmt_shr_pr,
    shareholder_distrib.pnlty_w_holding_rt,
    shareholder_distrib.divd_type,
    shareholder_distrib.ltcg_flg,
    shareholder_distrib.fund_expns_amt,
    shareholder_distrib.fund_expns_allocn_cd,
    shareholder_distrib.shr_re_invstd,
    shareholder_distrib.shr_bal_aft_dstrbtn,
    shareholder_distrib.shrs_issd,
    shareholder_distrib.dstrbtn_amt,
    shareholder_distrib.sub_acctg_fee,
    shareholder_distrib.othr_fees,
    shareholder_distrib.forgn_tax_w_holding_rt,
    shareholder_distrib.forgn_tax_w_holding_amt,
    shareholder_distrib.us_w_holding_amt,
    shareholder_distrib.nra_whg_amt,
    shareholder_distrib.cum_discnt_nbr,
    shareholder_distrib.ext_pln_id,
    shareholder_distrib.wire_txn_grp,
    shareholder_distrib.bal_ind,
    shareholder_distrib.curr_row_flg,
    shareholder_distrib.row_strt_dttm,
    shareholder_distrib.row_stop_dttm,
    shareholder_distrib.etl_load_cyc_key,
    shareholder_distrib.src_sys_id
   FROM hdm.shareholder_distrib;

CREATE OR REPLACE VIEW hdm.vw_shdr_acct_fund
AS SELECT shdr_acct_fund.acct_key,
    shdr_acct_fund.fund_key,
    shdr_acct_fund.dlr_key,
    shdr_acct_fund.dlr_branch_key,
    shdr_acct_fund.shrhldr_key,
    shdr_acct_fund.reg_key,
    shdr_acct_fund.prim_shrhldr_flg,
    shdr_acct_fund.row_strt_dttm,
    shdr_acct_fund.row_stop_dttm,
    shdr_acct_fund.etl_load_cyc_key,
    shdr_acct_fund.src_sys_id,
    shdr_acct_fund.curr_row_flg
   FROM hdm.shdr_acct_fund;

CREATE OR REPLACE VIEW hdm.vw_smip_fund_parameters
AS SELECT smip_fund_parameters.fund_parm_key,
    smip_fund_parameters.fund_key,
    smip_fund_parameters.min_otst_perf_lvl
   FROM hdm.smip_fund_parameters;

CREATE OR REPLACE VIEW hdm.vw_smip_fund_perf_level
AS SELECT smip_fund_perf_level.fund_perf_lvl_key,
    smip_fund_perf_level.fund_key,
    smip_fund_perf_level.min_perf_rt,
    smip_fund_perf_level.curr_row_flg,
    smip_fund_perf_level.row_strt_dttm,
    smip_fund_perf_level.row_stop_dttm,
    smip_fund_perf_level.etl_load_cyc_key,
    smip_fund_perf_level.src_sys_id
   FROM hdm.smip_fund_perf_level;

CREATE OR REPLACE VIEW hdm.vw_smip_fund_performance
AS SELECT smip_fund_performance.smip_fund_perf_key,
    smip_fund_performance.smip_mtric_key,
    smip_fund_performance.day_key,
    smip_fund_performance.fund_key,
    smip_fund_performance.out_perf_basis_pt,
    smip_fund_performance.wgt_net_out_perf,
    smip_fund_performance.wgt_min_otst_perf,
    smip_fund_performance.fund_peer_grp_qntl_rank,
    smip_fund_performance.idx_peer_grp_qntl_rank,
    smip_fund_performance.curr_row_flg,
    smip_fund_performance.row_strt_dttm,
    smip_fund_performance.row_stop_dttm,
    smip_fund_performance.etl_load_cyc_key,
    smip_fund_performance.src_sys_id
   FROM hdm.smip_fund_performance;

CREATE OR REPLACE VIEW hdm.vw_smip_group_trust_perf
AS SELECT smip_group_trust_perf.grp_trst_perf_key,
    smip_group_trust_perf.smip_mtric_key,
    smip_group_trust_perf.day_key,
    smip_group_trust_perf.grp_trst_perf,
    smip_group_trust_perf.bmk_perf,
    smip_group_trust_perf.perf_diff,
    smip_group_trust_perf.curr_row_flg,
    smip_group_trust_perf.row_strt_dttm,
    smip_group_trust_perf.row_stop_dttm,
    smip_group_trust_perf.etl_load_cyc_key,
    smip_group_trust_perf.src_sys_id
   FROM hdm.smip_group_trust_perf;

CREATE OR REPLACE VIEW hdm.vw_smip_metrics
AS SELECT smip_metrics.smip_mtric_key,
    smip_metrics.mtric_cd,
    smip_metrics.mtric_nm,
    smip_metrics.mtric_wgt,
    smip_metrics.per_nm,
    smip_metrics.per_wgt,
    smip_metrics.per_key,
    smip_metrics.expctd_perf,
    smip_metrics.abnml_thrs,
    smip_metrics.curr_row_flg,
    smip_metrics.row_strt_dttm,
    smip_metrics.row_stop_dttm,
    smip_metrics.etl_load_cyc_key,
    smip_metrics.src_sys_id
   FROM hdm.smip_metrics;

CREATE OR REPLACE VIEW hdm.vw_smip_performance
AS SELECT smip_performance.smip_perf_key,
    smip_performance.smip_mtric_key,
    smip_performance.day_key,
    smip_performance.fund_cnt,
    smip_performance.perfm_fund_cnt,
    smip_performance.perfm_fund_pctg,
    smip_performance.perf_var_pctg,
    smip_performance.min_otst_perf,
    smip_performance.wgt_perf_pctg,
    smip_performance.out_perf_basis_pt,
    smip_performance.wgt_net_out_perf,
    smip_performance.fund_thrs_bmk_cnt,
    smip_performance.fund_thrs_bmk_pct,
    smip_performance.curr_row_flg,
    smip_performance.row_strt_dttm,
    smip_performance.row_stop_dttm,
    smip_performance.etl_load_cyc_key,
    smip_performance.src_sys_id,
    smip_performance.fund_meet_bmk_cnt,
    smip_performance.fund_meet_bmk_pct,
    smip_performance.qntl_avg_pct,
    smip_performance.avg_actl_perf_pct,
    smip_performance.qntl_wgt_perf
   FROM hdm.smip_performance;

CREATE OR REPLACE VIEW hdm.vw_smip_quintile_perf
AS SELECT smip_quintile_perf.smip_qunitile_perf_key,
    smip_quintile_perf.smip_mtric_key,
    smip_quintile_perf.fund_key,
    smip_quintile_perf.bmk_idx_key,
    smip_quintile_perf.day_key,
    smip_quintile_perf.fund_ret,
    smip_quintile_perf.idx_ret,
    smip_quintile_perf.fund_peer_grp_rank,
    smip_quintile_perf.idx_peer_grp_rank,
    smip_quintile_perf.fund_peer_grp_qntl,
    smip_quintile_perf.idx_peer_grp_qntl,
    smip_quintile_perf.peer_grp_avg,
    smip_quintile_perf.peer_grp_cnt,
    smip_quintile_perf.curr_row_flg,
    smip_quintile_perf.row_strt_dttm,
    smip_quintile_perf.row_stop_dttm,
    smip_quintile_perf.etl_load_cyc_key,
    smip_quintile_perf.src_sys_id
   FROM hdm.smip_quintile_perf;

CREATE OR REPLACE VIEW hdm.vw_social_security_status
AS SELECT social_security_status.ssn_stat_key,
    social_security_status.ssn_stat_cd,
    social_security_status.ssn_stat_desc,
    social_security_status.row_strt_dttm,
    social_security_status.row_stop_dttm,
    social_security_status.curr_row_flg,
    social_security_status.src_sys_id,
    social_security_status.etl_load_cyc_key
   FROM hdm.social_security_status;

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
  WHERE vw_account_position.dlr_key = vw_dealer.dlr_key AND vw_account_position.day_key = vw_calendar.day_key AND vw_fund.fund_key = vw_account_position.fund_key AND vw_current_account.acct_nbr = vw_account_position.acct_nbr AND vw_current_account.fund_nbr::double precision = vw_account_position.fund_nbr AND vw_fund.fund_compst_key = vw_fund_composite.fund_compst_key AND vw_calendar.day_key = (( SELECT vw_calendar_1.day_key
           FROM hdm.vw_calendar vw_calendar_1
          WHERE vw_calendar_1.cal_day = (( SELECT max(vw_calendar_2.cal_day) AS max
                   FROM hdm.vw_calendar vw_calendar_2
                  WHERE vw_calendar_2.bus_day_flg::text = 'Y'::text AND vw_calendar_2.cal_day < (( SELECT max(vw_calendar_3.cal_day) AS max
                           FROM hdm.vw_calendar vw_calendar_3
                          WHERE vw_calendar_3.cal_day >= (aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) - ((7::numeric || ' days'::text)::interval)) AND vw_calendar_3.cal_day <= aws_oracle_ext.trunc(aws_oracle_ext.sysdate() - ((1::numeric || ' days'::text)::interval)) AND vw_calendar_3.bus_day_flg::text = 'Y'::text)))))) AND vw_current_account.dlr_lvl_cntl_cd <> 3::numeric AND vw_fund.shr_cls_desc::text = 'Institutional Class'::text AND vw_current_account.acct_stat::text <> 'Closed Status '::text AND vw_dealer.dlr_id::text = '5471'::text AND (concat_ws(''::text, aws_oracle_ext.to_char(vw_current_account.acct_nbr), aws_oracle_ext.to_char(vw_current_account.fund_nbr)) IN ( SELECT concat_ws(''::text, aws_oracle_ext.to_char(vw_current_account_1.acct_nbr), aws_oracle_ext.to_char(vw_current_account_1.fund_nbr)) AS acct_nbr_w_fund_nbr
           FROM hdm.vw_current_account vw_current_account_1,
            hdm.vw_fund vw_fund_1,
            hdm.vw_fund_composite vw_fund_composite_1,
            hdm.vw_fund_trans_hist tran,
            hdm.vw_calendar vw_calendar_1,
            hdm.vw_dealer vw_dealer_1
          WHERE tran.fund_key = vw_fund_1.fund_key AND tran.dlr_key = vw_dealer_1.dlr_key AND tran.sttl_day_key = vw_calendar_1.day_key AND vw_current_account_1.acct_nbr = tran.acct_nbr AND vw_current_account_1.fund_nbr::double precision = tran.fund_nbr AND vw_fund_1.fund_compst_key = vw_fund_composite_1.fund_compst_key AND vw_calendar_1.day_key = (( SELECT vw_calendar_2.day_key
                   FROM hdm.vw_calendar vw_calendar_2
                  WHERE vw_calendar_2.cal_day = (( SELECT max(vw_calendar_3.cal_day) AS max
                           FROM hdm.vw_calendar vw_calendar_3
                          WHERE vw_calendar_3.cal_day >= (aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) - ((7::numeric || ' days'::text)::interval)) AND vw_calendar_3.cal_day <= aws_oracle_ext.trunc(aws_oracle_ext.sysdate() - ((1::numeric || ' days'::text)::interval)) AND vw_calendar_3.bus_day_flg::text = 'Y'::text)))) AND vw_current_account_1.dlr_lvl_cntl_cd <> 3::numeric AND vw_fund_1.shr_cls_desc::text = 'Institutional Class'::text AND vw_current_account_1.acct_stat::text <> 'Closed Status '::text AND vw_dealer_1.dlr_id::text = '5471'::text));

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
        END::double precision * tran.net_amt AS amt,
    0 AS reg_acct_init_invmt,
    vw_calendar.cal_day,
    vw_current_account.custodian_id
   FROM hdm.vw_current_account,
    hdm.vw_fund,
    hdm.vw_fund_composite,
    hdm.vw_fund_trans_hist tran,
    hdm.vw_calendar,
    hdm.vw_dealer
  WHERE tran.dlr_key = vw_dealer.dlr_key AND tran.sttl_day_key = vw_calendar.day_key AND vw_fund.fund_key = tran.fund_key AND vw_current_account.acct_nbr = tran.acct_nbr AND vw_current_account.fund_nbr::double precision = tran.fund_nbr AND vw_fund.fund_compst_key = vw_fund_composite.fund_compst_key AND vw_current_account.acct_setup_dt < (( SELECT max(vw_calendar_1.cal_day) AS max
           FROM hdm.vw_calendar vw_calendar_1
          WHERE vw_calendar_1.cal_day >= (aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) - ((7::numeric || ' days'::text)::interval)) AND vw_calendar_1.cal_day <= aws_oracle_ext.trunc(aws_oracle_ext.sysdate() - ((1::numeric || ' days'::text)::interval)) AND vw_calendar_1.bus_day_flg::text = 'Y'::text)) AND vw_calendar.day_key = (( SELECT vw_calendar_1.day_key
           FROM hdm.vw_calendar vw_calendar_1
          WHERE vw_calendar_1.cal_day = (( SELECT max(vw_calendar_2.cal_day) AS max
                   FROM hdm.vw_calendar vw_calendar_2
                  WHERE vw_calendar_2.cal_day >= (aws_oracle_ext.trunc(aws_oracle_ext.sysdate()) - ((7::numeric || ' days'::text)::interval)) AND vw_calendar_2.cal_day <= aws_oracle_ext.trunc(aws_oracle_ext.sysdate() - ((1::numeric || ' days'::text)::interval)) AND vw_calendar_2.bus_day_flg::text = 'Y'::text)))) AND vw_current_account.dlr_lvl_cntl_cd <> 3::numeric AND vw_fund.shr_cls_desc::text = 'Institutional Class'::text AND vw_current_account.acct_stat::text <> 'Closed Status '::text AND vw_dealer.dlr_id::text = '5471'::text;

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
          WHERE fc.fund_compst_key = f.fund_compst_key AND fc.fund_compst_key = rl.fund_compst_key AND rl.bmk_idx_key = bmk.bmk_idx_key AND idx.bmk_idx_key = bmk.bmk_idx_key AND idx.day_key = cal.day_key AND cal.cal_day >= aws_oracle_ext.to_date('31-dec-2008'::character varying) AND fc.asset_cls_nm::text = 'Target Retirement'::text AND f.shr_cls_desc::text = 'Institutional Class'::text AND rl.perf_run_type_key = 3::numeric
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

CREATE OR REPLACE VIEW hdm.vw_target_daily_weight
AS SELECT target_daily_weight.trgt_daily_wgt_key,
    target_daily_weight.trgt_fund_key,
    target_daily_weight.day_key,
    target_daily_weight.fund_key,
    target_daily_weight.shr_qty,
    target_daily_weight.mkt_val,
    target_daily_weight.daily_holdg_wgt,
    target_daily_weight.curr_row_flg,
    target_daily_weight.row_strt_dttm,
    target_daily_weight.row_stop_dttm,
    target_daily_weight.etl_load_cyc_key,
    target_daily_weight.src_sys_id
   FROM hdm.target_daily_weight;

CREATE OR REPLACE VIEW hdm.vw_target_orig_glidepath
AS SELECT target_orig_glidepath.orig_gp_key,
    target_orig_glidepath.trgt_fund_key,
    target_orig_glidepath.fund_key,
    target_orig_glidepath.trgt_yr,
    target_orig_glidepath.efftv_strt_dt,
    target_orig_glidepath.efftv_end_dt,
    target_orig_glidepath.gp_wgt,
    target_orig_glidepath.curr_row_flg,
    target_orig_glidepath.row_strt_dttm,
    target_orig_glidepath.row_stop_dttm,
    target_orig_glidepath.etl_load_cyc_key,
    target_orig_glidepath.src_sys_id
   FROM hdm.target_orig_glidepath;

CREATE OR REPLACE VIEW hdm.vw_target_performance_type
AS SELECT target_performance_type.trgt_perf_type_key,
    target_performance_type.trgt_perf_type,
    target_performance_type.trgt_perf_type_desc,
    target_performance_type.perf_sort_ord,
    target_performance_type.perf_type_flg,
    target_performance_type.obj_type
   FROM hdm.target_performance_type;

CREATE OR REPLACE VIEW hdm.vw_target_return
AS SELECT target_return.trgt_ret_key,
    target_return.fund_key,
    target_return.trgt_perf_type_key,
    target_return.day_key,
    target_return.per_key,
    target_return.bmk_idx_key,
    target_return.trgt_ret,
    target_return.beg_dt,
    target_return.annlzn_fctr,
    target_return.displ_sort_ord,
    target_return.curr_row_flg,
    target_return.row_strt_dttm,
    target_return.row_stop_dttm,
    target_return.etl_load_cyc_key,
    target_return.src_sys_id
   FROM hdm.target_return;

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
  WHERE wt.trgt_fund_key = tf.fund_key AND wt.fund_key = f.fund_key AND f.fund_compst_key = rl.fund_compst_key AND rl.bmk_idx_key = b.bmk_idx_key AND v.fund_key = f.fund_key AND v.day_key = c.day_key AND v.per_key = p.per_key AND rl.perf_run_type_key = t.perf_run_type_key AND rl.bmk_idx_key = idr.bmk_idx_key AND rl.fund_compst_key = idr.fund_compst_key AND idr.day_key = c.day_key AND idr.per_key = p.per_key AND wt.efftv_strt_dt <= c.cal_day AND wt.efftv_end_dt >= c.cal_day AND wt.wgt_type::text = 'FIRM'::text AND ((tf.fund_stat_cd::text = ANY (ARRAY[1::text::character varying, 5::text::character varying]::text[])) OR tf.fund_incpn_dt <= c.cal_day AND tf.fund_clos_dt >= c.cal_day) AND rl.prim_bmk_flg::text = 'Y'::text AND rl.link_prio = 1::double precision AND c.cal_day >= '2016-01-01 00:00:00'::timestamp without time zone AND c.cal_day <= '2016-09-30 00:00:00'::timestamp without time zone AND p.per_nm::text = '1DY'::text AND tf.fund_nbr = 2600::double precision AND t.run_type_cd::text = 'REPORTING'::text
  ORDER BY c.cal_day, f.fund_sort_ord;

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

CREATE OR REPLACE VIEW hdm.vw_tgtx_return
AS SELECT tgtx_return.trgt_ret_key,
    tgtx_return.fund_key,
    tgtx_return.trgt_perf_type_key,
    tgtx_return.day_key,
    tgtx_return.per_key,
    tgtx_return.bmk_idx_key,
    tgtx_return.trgt_ret,
    tgtx_return.beg_dt,
    tgtx_return.annlzn_fctr,
    tgtx_return.displ_sort_ord,
    tgtx_return.curr_row_flg,
    tgtx_return.row_strt_dttm,
    tgtx_return.row_stop_dttm,
    tgtx_return.etl_load_cyc_key,
    tgtx_return.src_sys_id
   FROM hdm.tgtx_return;

CREATE OR REPLACE VIEW hdm.vw_tgtx_wealth_index
AS SELECT tgtx_wealth_index.trgt_wlth_idx_key,
    tgtx_wealth_index.trgt_perf_type_key,
    tgtx_wealth_index.day_key,
    tgtx_wealth_index.fund_key,
    tgtx_wealth_index.wlth_idx,
    tgtx_wealth_index.daily_ret,
    tgtx_wealth_index.curr_row_flg,
    tgtx_wealth_index.row_strt_dttm,
    tgtx_wealth_index.row_stop_dttm,
    tgtx_wealth_index.etl_load_cyc_key,
    tgtx_wealth_index.src_sys_id
   FROM hdm.tgtx_wealth_index;

CREATE OR REPLACE VIEW hdm.vw_tgtx_weights
AS SELECT tgtx_weights.trgt_wgt_key,
    tgtx_weights.trgt_fund_key,
    tgtx_weights.efftv_strt_dt,
    tgtx_weights.efftv_end_dt,
    tgtx_weights.fund_key,
    tgtx_weights.trgt_fund_wgt,
    tgtx_weights.wgt_type,
    tgtx_weights.curr_row_flg,
    tgtx_weights.row_strt_dttm,
    tgtx_weights.row_stop_dttm,
    tgtx_weights.etl_load_cyc_key,
    tgtx_weights.src_sys_id
   FROM hdm.tgtx_weights;

CREATE OR REPLACE VIEW hdm.vw_trade_entry_method
AS SELECT trade_entry_method.trde_entry_mthd_key,
    trade_entry_method.trde_entry_mthd_cd,
    trade_entry_method.trde_entry_mthd_desc,
    trade_entry_method.row_strt_dttm,
    trade_entry_method.row_stop_dttm,
    trade_entry_method.curr_row_flg,
    trade_entry_method.src_sys_id,
    trade_entry_method.etl_load_cyc_key
   FROM hdm.trade_entry_method;

CREATE OR REPLACE VIEW hdm.vw_trade_origin
AS SELECT trade_origin.trde_orig_key,
    trade_origin.trde_orig_id,
    trade_origin.trde_orig_desc,
    trade_origin.row_strt_dttm,
    trade_origin.row_stop_dttm,
    trade_origin.curr_row_flg,
    trade_origin.src_sys_id,
    trade_origin.etl_load_cyc_key
   FROM hdm.trade_origin;

CREATE OR REPLACE VIEW hdm.vw_transaction_type
AS SELECT transaction_type.txn_type_key,
    transaction_type.txn_sfx_cd,
    transaction_type.txn_cd,
    transaction_type.txn_cd_desc,
    transaction_type.txn_type_cd,
    transaction_type.txn_type_desc,
    transaction_type.top_lvl_txn_cd,
    transaction_type.top_lvl_txn_desc,
    transaction_type.curr_row_flg,
    transaction_type.row_strt_dttm,
    transaction_type.row_stop_dttm,
    transaction_type.etl_load_cyc_key,
    transaction_type.src_sys_id,
    transaction_type.shr_bal_effct_flg
   FROM hdm.transaction_type;

CREATE OR REPLACE VIEW hdm.vw_universe
AS SELECT universe.univ_key,
    universe.univ_nm,
    universe.curr_row_flg,
    universe.row_strt_dttm,
    universe.row_stop_dttm,
    universe.etl_load_cyc_key,
    universe.src_sys_id
   FROM hdm.universe;

CREATE OR REPLACE VIEW hdm.vw_yield_types
AS SELECT yield_types.yld_type_key,
    yield_types.yld_cd,
    yield_types.yld_desc,
    yield_types.curr_row_flg,
    yield_types.row_strt_dttm,
    yield_types.row_stop_dttm,
    yield_types.etl_load_cyc_key,
    yield_types.src_sys_id
   FROM hdm.yield_types;
