# AFTER_TAX_WEALTH_INDEX

This table holds the .
## DDL

|Column Name |SQL Type |Length |Nullable |Default Value |PK |
|---        |---     |---   |---   |--- |--- |
|[curr_row_flg](#curr_row_flg)|character varying|1|YES||NO
|[atax_wlth_idx_key](#atax_wlth_idx_key)|integer|(32,0)|NO||YES
|[day_key](#day_key)|integer|(32,0)|YES||NO
|[reinv_day_key](#reinv_day_key)|integer|(32,0)|YES||NO
|[fund_key](#fund_key)|integer|(32,0)|YES||NO
|[atax_ordn_incm](#atax_ordn_incm)|numeric|(38,15)|YES||NO
|[atax_ltcg](#atax_ltcg)|numeric|(38,15)|YES||NO
|[atax_stcg](#atax_stcg)|numeric|(38,15)|YES||NO
|[atax_forgn_tax_cr](#atax_forgn_tax_cr)|numeric|(38,15)|YES||NO
|[atax_tot_divd](#atax_tot_divd)|numeric|(38,15)|YES||NO
|[prir_mo_pr](#prir_mo_pr)|numeric|(38,15)|YES||NO
|[curr_mo_pr](#curr_mo_pr)|numeric|(38,15)|YES||NO
|[reinv_pr](#reinv_pr)|numeric|(38,15)|YES||NO
|[btax_ret](#btax_ret)|numeric|(38,15)|YES||NO
|[atax_ret](#atax_ret)|numeric|(38,15)|YES||NO
|[atax_reinv_per_shr](#atax_reinv_per_shr)|numeric|(38,15)|YES||NO
|[cb_amt](#cb_amt)|numeric|(38,15)|YES||NO
|[shrs_calc](#shrs_calc)|numeric|(38,15)|YES||NO
|[sold_amt](#sold_amt)|numeric|(38,15)|YES||NO
|[gls_amt](#gls_amt)|numeric|(38,15)|YES||NO
|[st_tax_gls](#st_tax_gls)|numeric|(38,15)|YES||NO
|[lt_tax_gls](#lt_tax_gls)|numeric|(38,15)|YES||NO
|[tax_amt](#tax_amt)|numeric|(38,15)|YES||NO
|[net_prcd_atax](#net_prcd_atax)|numeric|(38,15)|YES||NO
|[etl_load_cyc_key](#etl_load_cyc_key)|integer|(32,0)|YES||NO
|[src_sys_id](#src_sys_id)|numeric|(38,15)|YES||NO
|[row_strt_dttm](#row_strt_dttm)|timestamp without time zone|6|YES||NO
|[row_stop_dttm](#row_stop_dttm)|timestamp without time zone|6|YES||NO

### atax_wlth_idx_key
#### Description



#### Value Range

Number		
#### Logic

Auto Increment Sequence Number

### day_key
#### Description

#### Value Range

Number		

#### Logic

Calendar.day_key





### reinv_day_key
#### Description



#### Value Range

N/A

#### Logic


Fund_dividend.reinvest_day_key




### fund_key
#### Description



#### Value Range

N/A

#### Logic


FUND_DIVIDEND.FUND_KEY




### atax_ordn_incm
#### Description



#### Value Range

N/A

#### Logic

at_oi

Calcualted from below query





### atax_ltcg
#### Description



#### Value Range

N/A

#### Logic

at_ltcg

Calcualted from below query



### atax_stcg
#### Description



#### Value Range

N/A

#### Logic

at_stcg

Calcualted from below query



### atax_forgn_tax_cr
#### Description



#### Value Range

N/A

#### Logic

at_ftc

Calcualted from below query



### atax_tot_divd
#### Description



#### Value Range

N/A

#### Logic

tot_at_div

Calcualted from below query



### prir_mo_pr
#### Description



#### Value Range

N/A

#### Logic

pm_nav


Calcualted from below query




### curr_mo_pr
#### Description



#### Value Range

N/A

#### Logic

cm_nav

Calcualted from below query




### reinv_pr
#### Description



#### Value Range

N/A

#### Logic

re_invst_pr

Calcualted from below query




### btax_ret
#### Description



#### Value Range

N/A

#### Logic

bt_ret

Calcualted from below query



### atax_ret
#### Description



#### Value Range

N/A

#### Logic

at_ret

Calcualted from below query



### atax_reinv_per_shr
#### Description



#### Value Range

N/A

#### Logic


at_share_reinv

Calcualted from below query


### cb_amt
#### Description



#### Value Range

N/A

#### Logic






### shrs_calc
#### Description



#### Value Range

N/A

#### Logic







### sold_amt
#### Description



#### Value Range

N/A

#### Logic






### gls_amt
#### Description



#### Value Range

N/A

#### Logic







### st_tax_gls
#### Description



#### Value Range

N/A

#### Logic






### lt_tax_gls
#### Description



#### Value Range

N/A

#### Logic







### tax_amt
#### Description



#### Value Range

N/A

#### Logic






### net_prcd_atax
#### Description



#### Value Range

N/A

#### Logic





### curr_row_flg
#### Description



#### Value Range

N/A

#### Logic







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




##

Procedure location: https://github.com/harborcapital/hca-datalake/pull/435
Procedure Name: pkg_after_tax


pkg_after_tax.sql - Package is used to load the data into after_tax_wealth_index. Below Query loads the data into after_tax_wealth_index

pkg_after_tax - Runs once quaterly based on the dividend file, After the fund dividend job is loaded this package is kicked off

LOAD_AFTER_TAX_WI - 4 times a year (reinvest date from dividend as P_END_DATE)
if you are running after the quarter end we need to take the end of the month date

We need to run twice each quarter
once for the p_target_flg = 'Y' and once for p_target_flg ='N'

# Package Location
Placed in GIT



## Logic Used to load data using Store Procedure

(
                select rownum + t_key atax_wi_key,fund_key,day_key,reinvest_day_key,cal_day,MO_ENDING_DT,tot_divd,at_oi,at_stcg,at_ltcg,at_ftc,tax_yr,
				(at_oi + at_stcg + at_ltcg + at_ftc) tot_at_div,cm_nav,pm_nav,re_invst_pr,
                ((re_invst_pr + tot_divd) / pm_nav) * cm_nav/re_invst_pr - 1 bt_ret,
                ((re_invst_pr + (at_oi + at_stcg + at_ltcg + at_ftc)) / pm_nav) * cm_nav/re_invst_pr - 1 at_ret,
                (at_oi + at_stcg + at_ltcg + at_ftc) / re_invst_pr at_share_reinv
                from (
                        select cal_day,(select day_key from calendar where cal_day = cal.mo_ending_dt) day_key,cal.mo_ending_dt,
                        fund_key,reinvest_day_key,tot_divd,ordn_incm,shrt_term_cap_gains,long_term_cap_gains,forgn_tax_cr,re_invst_pr,div.qdi,
          (Case When nvl(qdi,0) > ordn_incm Then ordn_incm * (1 - tax.ltcg_rt) Else ((ordn_incm - nvl(qdi,0)) * (1 - tax.ordn_incm_rt)) + (nvl(qdi,0) * (1 - ltcg_rt)) end)    at_oi,
          (Case When nvl(qdi,0) > (ordn_incm + shrt_term_cap_gains) Then shrt_term_cap_gains * (1- ltcg_rt)
                When nvl(qdi,0) > ordn_incm then ((shrt_term_cap_gains - (nvl(qdi,0) - ordn_incm)) * (1-stcg_rt)) + (nvl(qdi,0) - ordn_incm) * (1- ltcg_rt)
                              Else shrt_term_cap_gains * (1-stcg_rt) end) at_stcg,
                        long_term_cap_gains * (1-ltcg_rt) at_ltcg,
                        (Case When nvl(qdi,0) > (ordn_incm + shrt_term_cap_gains + forgn_tax_cr) Then forgn_tax_cr * (1- ltcg_rt)
                              When nvl(qdi,0) > (ordn_incm + shrt_term_cap_gains)
							  then ((forgn_tax_cr - (nvl(qdi,0) - ordn_incm - shrt_term_cap_gains)) * (1-stcg_rt)) + (nvl(qdi,0) - ordn_incm - shrt_term_cap_gains) * (1- ltcg_rt)
                              Else forgn_tax_cr * (1-stcg_rt) end) at_ftc,    
                        --forgn_tax_cr - (forgn_tax_cr * ordn_incm_rt) at_ftc,
                        tax_yr,
                        (select pr_per_shr from fund_valuation a,calendar c where a.day_key=c.day_key and cal_day = cal.mo_ending_dt and fund_key = div.fund_key) cm_nav,
                        (select pr_per_shr from fund_valuation a,calendar c where a.day_key=c.day_key
						and cal_day = add_months(cal.mo_ending_dt,-1) and fund_key = div.fund_key) pm_nav
                        from fund_dividend div,calendar cal,dividend_tax_rate tax
                        where div.reinvest_day_key= cal.day_key
                        and cal.yr_nbr=tax.tax_yr
                        and cal_day = t_date
                        --and fund_key = (select fund_key from fund where fund_nbr=2011)
                     )      
            )
