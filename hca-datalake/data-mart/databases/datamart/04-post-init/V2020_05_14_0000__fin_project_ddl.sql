
DROP TABLE IF EXISTS hdm.fin_characteristics  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_holding_attribution CASCADE ;

DROP TABLE IF EXISTS hdm.fin_industry_attribution CASCADE ;

DROP TABLE IF EXISTS hdm.fin_characteristics_list CASCADE ;

DROP TABLE IF EXISTS hdm.fin_country_attribution CASCADE ;

DROP TABLE IF EXISTS hdm.fin_security  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_sector_attribution  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_position  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_region_attribution  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_total_attribution  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_mpt_statistics  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_region  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_industry  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_sector  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_country  CASCADE ;

DROP TABLE IF EXISTS hdm.fin_attribution_type  CASCADE;

CREATE TABLE hdm.fin_attribution_type
( 
	attrib_type_key      integer  NOT NULL ,
	attrib_type_name     varchar(60)  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_attribution_type_ix_PK PRIMARY KEY (attrib_type_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_attribution_type AS  SELECT * FROM hdm.fin_attribution_type;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_attribution_type TO writer;

GRANT SELECT ON hdm.fin_attribution_type TO reader;

GRANT SELECT ON hdm.vw_fin_attribution_type TO reporter;

CREATE INDEX fin_attribution_type_ix_IF1 ON hdm.fin_attribution_type
( 
	curr_row_flg ASC,
	row_strt_dttm ASC,
	row_stop_dttm ASC,
	row_updt_dttm ASC,
	etl_load_cyc_key ASC,
	src_sys_id ASC
);

CREATE TABLE hdm.fin_characteristics
( 
	fin_char_key         integer  NOT NULL ,
	day_key              integer  NULL ,
	fund_compst_key      integer  NULL ,
	bmk_idx_key          integer  NULL ,
	fin_char_list_key    integer  NULL ,
	char_value           numeric(38,15)  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_characteristics_ix_PK PRIMARY KEY (fin_char_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_characteristics AS  SELECT * FROM hdm.fin_characteristics;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_characteristics TO writer;

GRANT SELECT ON hdm.fin_characteristics TO reader;

GRANT SELECT ON hdm.vw_fin_characteristics TO reporter;

ALTER TABLE hdm.fin_characteristics
	ADD CONSTRAINT fin_characteristics_ix_AK1 UNIQUE (day_key,fund_compst_key,fin_char_list_key,bmk_idx_key);

CREATE INDEX fin_characteristics_ix_IF1 ON hdm.fin_characteristics
( 
	day_key  ASC
);

CREATE INDEX fin_characteristics_ix_IF2 ON hdm.fin_characteristics
( 
	fund_compst_key ASC
);

CREATE INDEX fin_characteristics_ix_IF3 ON hdm.fin_characteristics
( 
	bmk_idx_key ASC
);

CREATE INDEX fin_characteristics_ix_IF4 ON hdm.fin_characteristics
( 
	fin_char_list_key ASC
);

CREATE TABLE hdm.fin_characteristics_list
( 
	fin_char_list_key    integer  NOT NULL ,
	char_name            varchar(255)  NULL ,
	char_addnl_desc      varchar(255)  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_characteristics_list_ix_PK PRIMARY KEY (fin_char_list_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_characteristics_list AS  SELECT * FROM hdm.fin_characteristics_list;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_characteristics_list TO writer;

GRANT SELECT ON hdm.fin_characteristics_list TO reader;

GRANT SELECT ON hdm.vw_fin_characteristics_list TO reporter;

ALTER TABLE hdm.fin_characteristics_list
	ADD CONSTRAINT fin_characteristics_list_ix_AK1 UNIQUE (char_name);

CREATE TABLE hdm.fin_country
( 
	fin_country_key      integer  NOT NULL ,
 	ipm_region           varchar(60)  NULL ,
	emerging_region      varchar(60)  NULL ,   
	country_name         varchar(60)  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_country_ix_PK PRIMARY KEY (fin_country_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_country AS  SELECT * FROM hdm.fin_country;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_country TO writer;

GRANT SELECT ON hdm.fin_country TO reader;

GRANT SELECT ON hdm.vw_fin_country TO reporter;

ALTER TABLE hdm.fin_country
	ADD CONSTRAINT fin_country_ix_AK1 UNIQUE (country_name);

CREATE TABLE hdm.fin_country_attribution
( 
	fin_country_attrib_key integer  NOT NULL ,
	fund_compst_key      integer  NULL ,
	day_key              integer  NULL ,
	bmk_idx_key          integer  NULL ,
	fin_country_key      integer  NULL ,
	per_key              integer  NULL ,
	pf_currency_code     varchar(25)  NULL ,
	pf_begin_weight      numeric(38,15)  NULL ,
	pf_average_weight    numeric(38,15)  NULL ,
	pf_ending_weight     numeric(38,15)  NULL ,
	bm_average_weight    numeric(38,15)  NULL ,
	bm_ending_weight     numeric(38,15)  NULL ,
	pf_total_return      numeric(38,15)  NULL ,
	bm_total_return      numeric(38,15)  NULL ,
	pf_local_total_return numeric(38,15)  NULL ,
	bm_local_total_return numeric(38,15)  NULL ,
	pf_return_contrib    numeric(38,15)  NULL ,
	bm_return_contrib    numeric(38,15)  NULL ,
	pf_currency_contrib_return numeric(38,15)  NULL ,
	bm_currency_contrib_return numeric(38,15)  NULL ,
	allocation_effect    numeric(38,15)  NULL ,
	selection_interaction numeric(38,15)  NULL ,
	local_selection_interaction numeric(38,15)  NULL ,
	total_effect         numeric(38,15)  NULL ,
	total_currency_effect numeric(38,15)  NULL ,
	country_allocation_effect numeric(38,15)  NULL ,
	country_selection_effect numeric(38,15)  NULL ,
	latest_purchase_date date  NULL ,
	latest_sale_date     date  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_country_attribution_ix_PK PRIMARY KEY (fin_country_attrib_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_country_attribution AS  SELECT * FROM hdm.fin_country_attribution;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_country_attribution TO writer;

GRANT SELECT ON hdm.fin_country_attribution TO reader;

GRANT SELECT ON hdm.vw_fin_country_attribution TO reporter;

ALTER TABLE hdm.fin_country_attribution
	ADD CONSTRAINT fin_country_attribution_ix_AK1 UNIQUE (fund_compst_key,bmk_idx_key,day_key,fin_country_key,per_key);

CREATE INDEX fin_country_attribution_ix_IF1 ON hdm.fin_country_attribution
( 
	fund_compst_key ASC
);

CREATE INDEX fin_country_attribution_ix_IF2 ON hdm.fin_country_attribution
( 
	day_key  ASC
);

CREATE INDEX fin_country_attribution_ix_IF3 ON hdm.fin_country_attribution
( 
	bmk_idx_key ASC
);

CREATE INDEX fin_country_attribution_ix_IF4 ON hdm.fin_country_attribution
( 
	fin_country_key ASC
);

CREATE INDEX fin_country_attribution_ix_IF6 ON hdm.fin_country_attribution
( 
	per_key  ASC
);

CREATE TABLE hdm.fin_holding_attribution
( 
	fin_holding_attrib_key integer  NOT NULL ,
	day_key              integer  NULL ,
	fund_compst_key      integer  NULL ,
	bmk_idx_key          integer  NULL ,
	fin_security_key     integer  NULL ,
	per_key              integer  NULL ,
	security_type_flag   integer  NULL ,
	pf_currency_code     varchar(25)  NULL ,
	pf_begin_weight      numeric(38,15)  NULL ,
	pf_average_weight    numeric(38,15)  NULL ,
	pf_ending_weight     numeric(38,15)  NULL ,
	bm_average_weight    numeric(38,15)  NULL ,
	bm_ending_weight     numeric(38,15)  NULL ,
	pf_total_return      numeric(38,15)  NULL ,
	bm_total_return      numeric(38,15)  NULL ,
	pf_local_total_return numeric(38,15)  NULL ,
	bm_local_total_return numeric(38,15)  NULL ,
	pf_return_contrib    numeric(38,15)  NULL ,
	bm_return_contrib    numeric(38,15)  NULL ,
	pf_currency_contrib_return numeric(38,15)  NULL ,
	bm_currency_contrib_return numeric(38,15)  NULL ,
	allocation_effect    numeric(38,15)  NULL ,
	selection_interaction numeric(38,15)  NULL ,
	local_selection_interaction numeric(38,15)  NULL ,
	total_effect         numeric(38,15)  NULL ,
	total_currency_effect numeric(38,15)  NULL ,
	latest_purchase_date date  NULL ,
	latest_sale_date     date  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_holding_attribution_ix_PK PRIMARY KEY (fin_holding_attrib_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_holding_attribution AS  SELECT * FROM hdm.fin_holding_attribution;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_holding_attribution TO writer;

GRANT SELECT ON hdm.fin_holding_attribution TO reader;

GRANT SELECT ON hdm.vw_fin_holding_attribution TO reporter;

ALTER TABLE hdm.fin_holding_attribution
	ADD CONSTRAINT fin_holding_attribution_ix_AK1 UNIQUE (fund_compst_key,day_key,per_key,bmk_idx_key);

CREATE INDEX fin_holding_attribution_ix_IF1 ON hdm.fin_holding_attribution
( 
	fund_compst_key ASC
);

CREATE INDEX fin_holding_attribution_ix_IF2 ON hdm.fin_holding_attribution
( 
	day_key  ASC
);

CREATE INDEX fin_holding_attribution_ix_IF3 ON hdm.fin_holding_attribution
( 
	bmk_idx_key ASC
);

CREATE INDEX fin_holding_attribution_ix_IF4 ON hdm.fin_holding_attribution
( 
	fin_security_key ASC
);

CREATE INDEX fin_holding_attribution_ix_IF5 ON hdm.fin_holding_attribution
( 
	per_key  ASC
);

CREATE TABLE hdm.fin_industry
( 
	fin_industry_key     integer  NOT NULL ,
	fin_sector_key       integer  NULL ,
	industry_name        varchar(255)  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_industry_ix_PK PRIMARY KEY (fin_industry_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_industry AS  SELECT * FROM hdm.fin_industry;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_industry TO writer;

GRANT SELECT ON hdm.fin_industry TO reader;

GRANT SELECT ON hdm.vw_fin_industry TO reporter;

ALTER TABLE hdm.fin_industry
	ADD CONSTRAINT fin_industry_ix_AK1 UNIQUE (fin_sector_key,industry_name);

CREATE INDEX fin_industry_ix_IF1 ON hdm.fin_industry
( 
	fin_sector_key ASC
);

CREATE TABLE hdm.fin_industry_attribution
( 
	fin_industry_attrib_key integer  NOT NULL ,
	fund_compst_key      integer  NULL ,
	day_key              integer  NULL ,
	bmk_idx_key          integer  NULL ,
	fin_industry_key     integer  NULL ,
	per_key              integer  NULL ,
	pf_currency_code     varchar(25)  NULL ,
	pf_begin_weight      numeric(38,15)  NULL ,
	pf_average_weight    numeric(38,15)  NULL ,
	pf_ending_weight     numeric(38,15)  NULL ,
	bm_ending_weight     numeric(38,15)  NULL ,
	pf_local_total_return numeric(38,15)  NULL ,
	bm_local_total_return numeric(38,15)  NULL ,
	pf_return_contrib    numeric(38,15)  NULL ,
	bm_return_contrib    numeric(38,15)  NULL ,
	total_currency_effect numeric(38,15)  NULL ,
	bm_average_weight    numeric(38,15)  NULL ,
	pf_total_return      numeric(38,15)  NULL ,
	bm_total_return      numeric(38,15)  NULL ,
	pf_currency_contrib_return numeric(38,15)  NULL ,
	bm_currency_contrib_return numeric(38,15)  NULL ,
	allocation_effect    numeric(38,15)  NULL ,
	local_selection_interaction numeric(38,15)  NULL ,
	selection_interaction numeric(38,15)  NULL ,
	total_effect         numeric(38,15)  NULL ,
	latest_purchase_date date  NULL ,
	latest_sale_date     date  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_industry_attribution_ix_PK PRIMARY KEY (fin_industry_attrib_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_industry_attribution AS  SELECT * FROM hdm.fin_industry_attribution;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_industry_attribution TO writer;

GRANT SELECT ON hdm.fin_industry_attribution TO reader;

GRANT SELECT ON hdm.vw_fin_industry_attribution TO reporter;

ALTER TABLE hdm.fin_industry_attribution
	ADD CONSTRAINT fin_industry_attribution_ix_AK1 UNIQUE (fund_compst_key,fin_industry_key,bmk_idx_key,day_key,per_key);

CREATE INDEX fin_industry_attribution_ix_IF1 ON hdm.fin_industry_attribution
( 
	fund_compst_key ASC
);

CREATE INDEX fin_industry_attribution_ix_IF2 ON hdm.fin_industry_attribution
( 
	day_key  ASC
);

CREATE INDEX fin_industry_attribution_ix_IF3 ON hdm.fin_industry_attribution
( 
	bmk_idx_key ASC
);

CREATE INDEX fin_industry_attribution_ix_IF4 ON hdm.fin_industry_attribution
( 
	fin_industry_key ASC
);

CREATE INDEX fin_industry_attribution_ix_IF6 ON hdm.fin_industry_attribution
( 
	per_key  ASC
);

CREATE TABLE hdm.fin_mpt_statistics
( 
	mpt_stats_key        integer  NOT NULL ,
	day_key              integer  NULL ,
	fund_key             integer  NULL ,
	bmk_idx_key          integer  NULL ,
	per_key              integer  NULL ,
	alpha                numeric(38,15)  NULL ,
	beta                 numeric(38,15)  NULL ,
	r_squared            numeric(38,15)  NULL ,
	sharpe_rate          numeric(38,15)  NULL ,
	std_deviation        numeric(38,15)  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_mpt_statistics_ix_PK PRIMARY KEY (mpt_stats_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_mpt_statistics AS  SELECT * FROM hdm.fin_mpt_statistics;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_mpt_statistics TO writer;

GRANT SELECT ON hdm.fin_mpt_statistics TO reader;

GRANT SELECT ON hdm.vw_fin_mpt_statistics TO reporter;

ALTER TABLE hdm.fin_mpt_statistics
	ADD CONSTRAINT fin_mpt_statistics_ix_AK1 UNIQUE (fund_key,day_key,per_key,bmk_idx_key);

CREATE INDEX fin_mpt_statistics_ix_IF1 ON hdm.fin_mpt_statistics
( 
	day_key  ASC
);

CREATE INDEX fin_mpt_statistics_ix_IF3 ON hdm.fin_mpt_statistics
( 
	bmk_idx_key ASC
);

CREATE INDEX fin_mpt_statistics_ix_IF4 ON hdm.fin_mpt_statistics
( 
	per_key  ASC
);

CREATE INDEX fin_mpt_statistics_ix_IF6 ON hdm.fin_mpt_statistics
( 
	fund_key ASC
);

CREATE TABLE hdm.fin_position
( 
	fin_position_key     integer  NOT NULL ,
	fund_compst_key      integer  NULL ,
	day_key              integer  NULL ,
	bmk_idx_key          integer  NULL ,
	fin_security_key     integer  NULL ,
	currency_code        varchar(25)  NULL ,
	security_price       numeric(38,15)  NULL ,
	pf_shares            numeric(38,15)  NULL ,
	pf_weight            numeric(38,15)  NULL ,
	bm_weight            numeric(38,15)  NULL ,
	pf_ending_market_value numeric(38,15)  NULL ,
	pe_fy1_est           numeric(38,15)  NULL ,
	pe_ratio             numeric(38,15)  NULL ,
	pb_ratio             numeric(38,15)  NULL ,
	market_cap           numeric(38,15)  NULL ,
	hist_eps_growth_5yr  numeric(38,15)  NULL ,
	est_eps_growth_3to5_yr numeric(38,15)  NULL ,
	pf_mpt_beta          numeric(38,15)  NULL ,
	latest_purchase_date date  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_position_ix_PK PRIMARY KEY (fin_position_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_position AS  SELECT * FROM hdm.fin_position;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_position TO writer;

GRANT SELECT ON hdm.fin_position TO reader;

GRANT SELECT ON hdm.vw_fin_position TO reporter;

ALTER TABLE hdm.fin_position
	ADD CONSTRAINT fin_position_ix_AK1 UNIQUE (fund_compst_key,day_key,fin_security_key,bmk_idx_key);

CREATE INDEX fin_position_ix_IF1 ON hdm.fin_position
( 
	fund_compst_key ASC
);

CREATE INDEX fin_position_ix_IF2 ON hdm.fin_position
( 
	day_key  ASC
);

CREATE INDEX fin_position_ix_IF3 ON hdm.fin_position
( 
	bmk_idx_key ASC
);

CREATE INDEX fin_position_ix_IF4 ON hdm.fin_position
( 
	fin_security_key ASC
);

CREATE TABLE hdm.fin_region
( 
	fin_region_key       integer  NOT NULL ,
	region_type          varchar(25)  NULL ,
	region_name          varchar(255)  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_region_ix_PK PRIMARY KEY (fin_region_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_region AS  SELECT * FROM hdm.fin_region;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_region TO writer;

GRANT SELECT ON hdm.fin_region TO reader;

GRANT SELECT ON hdm.vw_fin_region TO reporter;

ALTER TABLE hdm.fin_region
	ADD CONSTRAINT fin_region_ix_AK1 UNIQUE (region_type,region_name);

CREATE INDEX fin_region_ix_IF1 ON hdm.fin_region
( 
	curr_row_flg ASC,
	row_strt_dttm ASC,
	row_stop_dttm ASC,
	row_updt_dttm ASC,
	etl_load_cyc_key ASC,
	src_sys_id ASC
);

CREATE TABLE hdm.fin_region_attribution
( 
	region_attrib_key    integer  NOT NULL ,
	fund_compst_key      integer  NULL ,
	day_key              integer  NULL ,
	bmk_idx_key          integer  NULL ,
	fin_region_key       integer  NULL ,
	per_key              integer  NULL ,
	pf_currency_code     varchar(25)  NULL ,
	pf_begin_weight      numeric(38,15)  NULL ,
	pf_average_weight    numeric(38,15)  NULL ,
	pf_ending_weight     numeric(38,15)  NULL ,
	bm_average_weight    numeric(38,15)  NULL ,
	bm_ending_weight     numeric(38,15)  NULL ,
	pf_total_return      numeric(38,15)  NULL ,
	bm_total_return      numeric(38,15)  NULL ,
	pf_local_total_return numeric(38,15)  NULL ,
	bm_local_total_return numeric(38,15)  NULL ,
	pf_return_contrib    numeric(38,15)  NULL ,
	bm_return_contrib    numeric(38,15)  NULL ,
	pf_currency_contrib_return numeric(38,15)  NULL ,
	bm_currency_contrib_return numeric(38,15)  NULL ,
	allocation_effect    numeric(38,15)  NULL ,
	selection_interaction numeric(38,15)  NULL ,
	local_selection_interaction numeric(38,15)  NULL ,
	total_effect         numeric(38,15)  NULL ,
	total_currency_effect numeric(38,15)  NULL ,
	country_allocation_effect numeric(38,15)  NULL ,
	country_selection_effect numeric(38,15)  NULL ,
	latest_purchase_date date  NULL ,
	latest_sale_date     date  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_region_attribution_ix_PK PRIMARY KEY (region_attrib_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_region_attribution AS  SELECT * FROM hdm.fin_region_attribution;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_region_attribution TO writer;

GRANT SELECT ON hdm.fin_region_attribution TO reader;

GRANT SELECT ON hdm.vw_fin_region_attribution TO reporter;

ALTER TABLE hdm.fin_region_attribution
	ADD CONSTRAINT fin_region_attribution_ix_AK1 UNIQUE (fund_compst_key,bmk_idx_key,day_key,fin_region_key,per_key);

CREATE INDEX fin_region_attribution_ix_IF1 ON hdm.fin_region_attribution
( 
	fund_compst_key ASC
);

CREATE INDEX fin_region_attribution_ix_IF2 ON hdm.fin_region_attribution
( 
	day_key  ASC
);

CREATE INDEX fin_region_attribution_ix_IF3 ON hdm.fin_region_attribution
( 
	bmk_idx_key ASC
);

CREATE INDEX fin_region_attribution_ix_IF4 ON hdm.fin_region_attribution
( 
	fin_region_key ASC
);

CREATE INDEX fin_region_attribution_ix_IF6 ON hdm.fin_region_attribution
( 
	per_key  ASC
);

CREATE TABLE hdm.fin_sector
( 
	fin_sector_key       integer  NOT NULL ,
	sector_name          varchar(255)  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_sector_ix_PK PRIMARY KEY (fin_sector_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_sector AS  SELECT * FROM hdm.fin_sector;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_sector TO writer;

GRANT SELECT ON hdm.fin_sector TO reader;

GRANT SELECT ON hdm.vw_fin_sector TO reporter;

ALTER TABLE hdm.fin_sector
	ADD CONSTRAINT fin_sector_ix_AK1 UNIQUE (sector_name);

CREATE TABLE hdm.fin_sector_attribution
( 
	fin_sector_attrib_key integer  NOT NULL ,
	fund_compst_key      integer  NULL ,
	day_key              integer  NULL ,
	bmk_idx_key          integer  NULL ,
	fin_sector_key       integer  NULL ,
	per_key              integer  NULL ,
	pf_currency_code     varchar(25)  NULL ,
	pf_begin_weight      numeric(38,15)  NULL ,
	pf_average_weight    numeric(38,15)  NULL ,
	pf_ending_weight     numeric(38,15)  NULL ,
	bm_average_weight    numeric(38,15)  NULL ,
	bm_ending_weight     numeric(38,15)  NULL ,
	pf_total_return      numeric(38,15)  NULL ,
	bm_total_return      numeric(38,15)  NULL ,
	pf_local_total_return numeric(38,15)  NULL ,
	bm_local_total_return numeric(38,15)  NULL ,
	pf_return_contrib    numeric(38,15)  NULL ,
	bm_return_contrib    numeric(38,15)  NULL ,
	pf_currency_contrib_return numeric(38,15)  NULL ,
	bm_currency_contrib_return numeric(38,15)  NULL ,
	allocation_effect    numeric(38,15)  NULL ,
	local_selection_interaction numeric(38,15)  NULL ,
	selection_interaction numeric(38,15)  NULL ,
	total_effect         numeric(38,15)  NULL ,
	total_currency_effect numeric(38,15)  NULL ,
	latest_purchase_date date  NULL ,
	latest_sale_date     date  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_sector_attribution_ix_PK PRIMARY KEY (fin_sector_attrib_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_sector_attribution AS  SELECT * FROM hdm.fin_sector_attribution;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_sector_attribution TO writer;

GRANT SELECT ON hdm.fin_sector_attribution TO reader;

GRANT SELECT ON hdm.vw_fin_sector_attribution TO reporter;

ALTER TABLE hdm.fin_sector_attribution
	ADD CONSTRAINT fin_sector_attribution_ix_AK1 UNIQUE (fund_compst_key,bmk_idx_key,day_key,fin_sector_key,per_key);

CREATE INDEX fin_sector_attribution_ix_IF1 ON hdm.fin_sector_attribution
( 
	fund_compst_key ASC
);

CREATE INDEX fin_sector_attribution_ix_IF2 ON hdm.fin_sector_attribution
( 
	day_key  ASC
);

CREATE INDEX fin_sector_attribution_ix_IF3 ON hdm.fin_sector_attribution
( 
	bmk_idx_key ASC
);

CREATE INDEX fin_sector_attribution_ix_IF4 ON hdm.fin_sector_attribution
( 
	fin_sector_key ASC
);

CREATE INDEX fin_sector_attribution_ix_IF6 ON hdm.fin_sector_attribution
( 
	per_key  ASC
);

CREATE TABLE hdm.fin_security
( 
	fin_security_key     integer  NOT NULL ,
	fin_industry_key     integer  NULL ,
	fin_country_key      integer  NULL ,
	security_id          varchar(25)  NULL ,
	security_name        varchar(255)  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_security_ix_PK PRIMARY KEY (fin_security_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_security AS  SELECT * FROM hdm.fin_security;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_security TO writer;

GRANT SELECT ON hdm.fin_security TO reader;

GRANT SELECT ON hdm.vw_fin_security TO reporter;

ALTER TABLE hdm.fin_security
	ADD CONSTRAINT fin_security_ix_AK1 UNIQUE (fin_country_key,fin_industry_key,security_id);

CREATE INDEX fin_security_ix_IF1 ON hdm.fin_security
( 
	fin_industry_key ASC
);

CREATE INDEX fin_security_ix_IF2 ON hdm.fin_security
( 
	fin_country_key ASC
);

CREATE TABLE hdm.fin_total_attribution
( 
	fin_total_attrib_key integer  NOT NULL ,
	fund_compst_key      integer  NULL ,
	day_key              integer  NULL ,
	bmk_idx_key          integer  NULL ,
	per_key              integer  NULL ,
	attrib_type_key      integer  NULL ,
	pf_currency_code     varchar(25)  NULL ,
	pf_begin_weight      numeric(38,15)  NULL ,
	pf_average_weight    numeric(38,15)  NULL ,
	pf_ending_weight     numeric(38,15)  NULL ,
	bm_average_weight    numeric(38,15)  NULL ,
	bm_ending_weight     numeric(38,15)  NULL ,
	pf_total_return      numeric(38,15)  NULL ,
	bm_total_return      numeric(38,15)  NULL ,
	pf_local_total_return numeric(38,15)  NULL ,
	bm_local_total_return numeric(38,15)  NULL ,
	pf_return_contrib    numeric(38,15)  NULL ,
	bm_return_contrib    numeric(38,15)  NULL ,
	pf_currency_contrib_return numeric(38,15)  NULL ,
	bm_currency_contrib_return numeric(38,15)  NULL ,
	allocation_effect    numeric(38,15)  NULL ,
	selection_interaction numeric(38,15)  NULL ,
	local_selection_interaction numeric(38,15)  NULL ,
	total_effect         numeric(38,15)  NULL ,
	total_currency_effect numeric(38,15)  NULL ,
	country_allocation_effect numeric(38,15)  NULL ,
	country_selection_effect numeric(38,15)  NULL ,
	latest_sale_date     date  NULL ,
	latest_purchase_date date  NULL ,
	curr_row_flg         varchar(1)  NULL ,
	row_strt_dttm        date  NULL ,
	row_stop_dttm        date  NULL ,
	row_updt_dttm        date  NULL ,
	etl_load_cyc_key     integer  NULL ,
	src_sys_id           integer  NULL ,
	CONSTRAINT fin_total_attribution_ix_PK PRIMARY KEY (fin_total_attrib_key)
);



CREATE OR REPLACE VIEW hdm.vw_fin_total_attribution AS  SELECT * FROM hdm.fin_total_attribution;

GRANT SELECT,INSERT,UPDATE,DELETE ON hdm.fin_total_attribution TO writer;

GRANT SELECT ON hdm.fin_total_attribution TO reader;

GRANT SELECT ON hdm.vw_fin_total_attribution TO reporter;

ALTER TABLE hdm.fin_total_attribution
	ADD CONSTRAINT fin_total_attribution_ix_AK1 UNIQUE (fund_compst_key,bmk_idx_key,day_key,attrib_type_key,per_key);

CREATE INDEX fin_total_attribution_ix_IF1 ON hdm.fin_total_attribution
( 
	fund_compst_key ASC
);

CREATE INDEX fin_total_attribution_ix_IF2 ON hdm.fin_total_attribution
( 
	day_key  ASC
);

CREATE INDEX fin_total_attribution_ix_IF3 ON hdm.fin_total_attribution
( 
	bmk_idx_key ASC
);

CREATE INDEX fin_total_attribution_ix_IF5 ON hdm.fin_total_attribution
( 
	per_key  ASC
);

CREATE INDEX fin_total_attribution_ix_IF6 ON hdm.fin_total_attribution
( 
	attrib_type_key ASC
);


ALTER TABLE hdm.fin_characteristics
	ADD CONSTRAINT fin_characteristics_ix_IF1 FOREIGN KEY (day_key) REFERENCES hdm.calendar(day_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_characteristics
	ADD CONSTRAINT fin_characteristics_ix_IF2 FOREIGN KEY (fund_compst_key) REFERENCES hdm.fund_composite(fund_compst_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_characteristics
	ADD CONSTRAINT fin_characteristics_ix_IF3 FOREIGN KEY (bmk_idx_key) REFERENCES hdm.benchmark_index(bmk_idx_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_characteristics
	ADD CONSTRAINT fin_characteristics_ix_IF4 FOREIGN KEY (fin_char_list_key) REFERENCES hdm.fin_characteristics_list(fin_char_list_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_country_attribution
	ADD CONSTRAINT fin_country_attribution_ix_IF1 FOREIGN KEY (fund_compst_key) REFERENCES hdm.fund_composite(fund_compst_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_country_attribution
	ADD CONSTRAINT fin_country_attribution_ix_IF2 FOREIGN KEY (day_key) REFERENCES hdm.calendar(day_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_country_attribution
	ADD CONSTRAINT fin_country_attribution_ix_IF3 FOREIGN KEY (bmk_idx_key) REFERENCES hdm.benchmark_index(bmk_idx_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_country_attribution
	ADD CONSTRAINT fin_country_attribution_ix_IF4 FOREIGN KEY (fin_country_key) REFERENCES hdm.fin_country(fin_country_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_country_attribution
	ADD CONSTRAINT fin_country_attribution_ix_IF6 FOREIGN KEY (per_key) REFERENCES hdm.return_periods(per_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_holding_attribution
	ADD CONSTRAINT fin_holding_attribution_ix_IF1 FOREIGN KEY (fund_compst_key) REFERENCES hdm.fund_composite(fund_compst_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_holding_attribution
	ADD CONSTRAINT fin_holding_attribution_ix_IF2 FOREIGN KEY (day_key) REFERENCES hdm.calendar(day_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_holding_attribution
	ADD CONSTRAINT fin_holding_attribution_ix_IF3 FOREIGN KEY (bmk_idx_key) REFERENCES hdm.benchmark_index(bmk_idx_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_holding_attribution
	ADD CONSTRAINT fin_holding_attribution_ix_IF4 FOREIGN KEY (fin_security_key) REFERENCES hdm.fin_security(fin_security_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_holding_attribution
	ADD CONSTRAINT fin_holding_attribution_ix_IF5 FOREIGN KEY (per_key) REFERENCES hdm.return_periods(per_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_industry
	ADD CONSTRAINT fin_industry_ix_IF1 FOREIGN KEY (fin_sector_key) REFERENCES hdm.fin_sector(fin_sector_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_industry_attribution
	ADD CONSTRAINT fin_industry_attribution_ix_IF1 FOREIGN KEY (fund_compst_key) REFERENCES hdm.fund_composite(fund_compst_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_industry_attribution
	ADD CONSTRAINT fin_industry_attribution_ix_IF2 FOREIGN KEY (day_key) REFERENCES hdm.calendar(day_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_industry_attribution
	ADD CONSTRAINT fin_industry_attribution_ix_IF3 FOREIGN KEY (bmk_idx_key) REFERENCES hdm.benchmark_index(bmk_idx_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_industry_attribution
	ADD CONSTRAINT fin_industry_attribution_ix_IF4 FOREIGN KEY (fin_industry_key) REFERENCES hdm.fin_industry(fin_industry_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_industry_attribution
	ADD CONSTRAINT fin_industry_attribution_ix_IF6 FOREIGN KEY (per_key) REFERENCES hdm.return_periods(per_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_mpt_statistics
	ADD CONSTRAINT fin_mpt_statistics_ix_IF1 FOREIGN KEY (day_key) REFERENCES hdm.calendar(day_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_mpt_statistics
	ADD CONSTRAINT fin_mpt_statistics_ix_IF3 FOREIGN KEY (bmk_idx_key) REFERENCES hdm.benchmark_index(bmk_idx_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_mpt_statistics
	ADD CONSTRAINT fin_mpt_statistics_ix_IF4 FOREIGN KEY (per_key) REFERENCES hdm.return_periods(per_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_mpt_statistics
	ADD CONSTRAINT fin_mpt_statistics_ix_IF6 FOREIGN KEY (fund_key) REFERENCES hdm.fund(fund_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_position
	ADD CONSTRAINT fin_position_ix_IF1 FOREIGN KEY (fund_compst_key) REFERENCES hdm.fund_composite(fund_compst_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_position
	ADD CONSTRAINT fin_position_ix_IF2 FOREIGN KEY (day_key) REFERENCES hdm.calendar(day_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_position
	ADD CONSTRAINT fin_position_ix_IF3 FOREIGN KEY (bmk_idx_key) REFERENCES hdm.benchmark_index(bmk_idx_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_position
	ADD CONSTRAINT fin_position_ix_IF4 FOREIGN KEY (fin_security_key) REFERENCES hdm.fin_security(fin_security_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_region_attribution
	ADD CONSTRAINT fin_region_attribution_ix_IF1 FOREIGN KEY (fund_compst_key) REFERENCES hdm.fund_composite(fund_compst_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_region_attribution
	ADD CONSTRAINT fin_region_attribution_ix_IF2 FOREIGN KEY (day_key) REFERENCES hdm.calendar(day_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_region_attribution
	ADD CONSTRAINT fin_region_attribution_ix_IF3 FOREIGN KEY (bmk_idx_key) REFERENCES hdm.benchmark_index(bmk_idx_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_region_attribution
	ADD CONSTRAINT fin_region_attribution_ix_IF4 FOREIGN KEY (fin_region_key) REFERENCES hdm.fin_region(fin_region_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_region_attribution
	ADD CONSTRAINT fin_region_attribution_ix_IF6 FOREIGN KEY (per_key) REFERENCES hdm.return_periods(per_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_sector_attribution
	ADD CONSTRAINT fin_sector_attribution_ix_IF1 FOREIGN KEY (fund_compst_key) REFERENCES hdm.fund_composite(fund_compst_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_sector_attribution
	ADD CONSTRAINT fin_sector_attribution_ix_IF2 FOREIGN KEY (day_key) REFERENCES hdm.calendar(day_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_sector_attribution
	ADD CONSTRAINT fin_sector_attribution_ix_IF3 FOREIGN KEY (bmk_idx_key) REFERENCES hdm.benchmark_index(bmk_idx_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_sector_attribution
	ADD CONSTRAINT fin_sector_attribution_ix_IF4 FOREIGN KEY (fin_sector_key) REFERENCES hdm.fin_sector(fin_sector_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_sector_attribution
	ADD CONSTRAINT fin_sector_attribution_ix_IF6 FOREIGN KEY (per_key) REFERENCES hdm.return_periods(per_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_security
	ADD CONSTRAINT fin_security_ix_IF1 FOREIGN KEY (fin_industry_key) REFERENCES hdm.fin_industry(fin_industry_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_security
	ADD CONSTRAINT fin_security_ix_IF2 FOREIGN KEY (fin_country_key) REFERENCES hdm.fin_country(fin_country_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;


ALTER TABLE hdm.fin_total_attribution
	ADD CONSTRAINT fin_total_attribution_ix_IF1 FOREIGN KEY (fund_compst_key) REFERENCES hdm.fund_composite(fund_compst_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_total_attribution
	ADD CONSTRAINT fin_total_attribution_ix_IF2 FOREIGN KEY (day_key) REFERENCES hdm.calendar(day_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_total_attribution
	ADD CONSTRAINT fin_total_attribution_ix_IF3 FOREIGN KEY (bmk_idx_key) REFERENCES hdm.benchmark_index(bmk_idx_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_total_attribution
	ADD CONSTRAINT fin_total_attribution_ix_IF5 FOREIGN KEY (per_key) REFERENCES hdm.return_periods(per_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

ALTER TABLE hdm.fin_total_attribution
	ADD CONSTRAINT fin_total_attribution_ix_IF6 FOREIGN KEY (attrib_type_key) REFERENCES hdm.fin_attribution_type(attrib_type_key)
		ON UPDATE SET NULL
		ON DELETE SET NULL;

COMMENT ON TABLE hdm.fin_attribution_type IS 'Each attribution file contains totals by fund/period.  This table defines these periods.  They are IPM Region, Emerging Region, Sector';

COMMENT ON COLUMN hdm.fin_attribution_type.attrib_type_key IS 'Attribution Type Key';

COMMENT ON COLUMN hdm.fin_attribution_type.attrib_type_name IS 'Attribution Type Name';

COMMENT ON COLUMN hdm.fin_attribution_type.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_attribution_type.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_attribution_type.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_attribution_type.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_attribution_type.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_attribution_type.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_characteristics.fin_char_key IS 'FIN Characteristics Key';

COMMENT ON COLUMN hdm.fin_characteristics.fin_char_list_key IS 'FIN Characteristics List Key';

COMMENT ON COLUMN hdm.fin_characteristics.char_value IS 'Characteristics Value';

COMMENT ON COLUMN hdm.fin_characteristics.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_characteristics.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_characteristics.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_characteristics.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_characteristics.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_characteristics.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_characteristics_list.fin_char_list_key IS 'FIN Characteristics List Key';

COMMENT ON COLUMN hdm.fin_characteristics_list.char_name IS 'Characteristics Name';

COMMENT ON COLUMN hdm.fin_characteristics_list.char_addnl_desc IS 'Characteristics Additional Description';

COMMENT ON COLUMN hdm.fin_characteristics_list.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_characteristics_list.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_characteristics_list.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_characteristics_list.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_characteristics_list.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_characteristics_list.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_country.fin_country_key IS 'FIN Country Key';

COMMENT ON COLUMN hdm.fin_country.country_name IS 'Country Name';

COMMENT ON COLUMN hdm.fin_country.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_country.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_country.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_country.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_country.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_country.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_country.ipm_region IS 'Ipm Region';

COMMENT ON COLUMN hdm.fin_country.emerging_region IS 'Emerging Region';

COMMENT ON COLUMN hdm.fin_country_attribution.fin_country_attrib_key IS 'FIN Holding Attribution Key';

COMMENT ON COLUMN hdm.fin_country_attribution.pf_currency_code IS 'Portfolio Currency Code';

COMMENT ON COLUMN hdm.fin_country_attribution.pf_begin_weight IS 'Portfolio Begin Weight';

COMMENT ON COLUMN hdm.fin_country_attribution.pf_average_weight IS 'Portfolio Average Weight';

COMMENT ON COLUMN hdm.fin_country_attribution.pf_ending_weight IS 'Portfolio Ending Weight';

COMMENT ON COLUMN hdm.fin_country_attribution.pf_total_return IS 'Portfolio Total Return';

COMMENT ON COLUMN hdm.fin_country_attribution.bm_total_return IS 'Benchmark Total Return';

COMMENT ON COLUMN hdm.fin_country_attribution.pf_currency_contrib_return IS 'Portfolio Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_country_attribution.bm_currency_contrib_return IS 'Benchmark Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_country_attribution.allocation_effect IS 'Allocation Effect';

COMMENT ON COLUMN hdm.fin_country_attribution.selection_interaction IS 'Selection +  Interaction';

COMMENT ON COLUMN hdm.fin_country_attribution.total_effect IS 'Total Effect';

COMMENT ON COLUMN hdm.fin_country_attribution.latest_purchase_date IS 'Latest Purchase Date';

COMMENT ON COLUMN hdm.fin_country_attribution.latest_sale_date IS 'Most Recent Sale Date';

COMMENT ON COLUMN hdm.fin_country_attribution.fin_country_key IS 'FIN Country Key';

COMMENT ON COLUMN hdm.fin_country_attribution.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_country_attribution.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_country_attribution.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_country_attribution.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_country_attribution.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_country_attribution.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_country_attribution.total_currency_effect IS 'Total Currency Effect';

COMMENT ON COLUMN hdm.fin_country_attribution.bm_average_weight IS 'Benchmark Average Weight';

COMMENT ON COLUMN hdm.fin_country_attribution.bm_ending_weight IS 'Benchmark Ending Weight';

COMMENT ON COLUMN hdm.fin_country_attribution.pf_local_total_return IS 'Portfolio Local Total Return';

COMMENT ON COLUMN hdm.fin_country_attribution.bm_local_total_return IS 'Benchmark Local Total Return';

COMMENT ON COLUMN hdm.fin_country_attribution.pf_return_contrib IS 'Portfolio Return Contribution';

COMMENT ON COLUMN hdm.fin_country_attribution.bm_return_contrib IS 'Benchmark Return Contribution';

COMMENT ON COLUMN hdm.fin_country_attribution.local_selection_interaction IS 'Local Selection Interaction';

COMMENT ON COLUMN hdm.fin_country_attribution.country_allocation_effect IS 'Country Allocation Effect';

COMMENT ON COLUMN hdm.fin_country_attribution.country_selection_effect IS 'Country Selection Effect';

COMMENT ON COLUMN hdm.fin_holding_attribution.fin_holding_attrib_key IS 'FIN Holding Attribution Key';

COMMENT ON COLUMN hdm.fin_holding_attribution.fin_security_key IS 'FIN Security Key';

COMMENT ON COLUMN hdm.fin_holding_attribution.pf_currency_code IS 'Portfolio Currency Code';

COMMENT ON COLUMN hdm.fin_holding_attribution.pf_begin_weight IS 'Portfolio Begin Weight';

COMMENT ON COLUMN hdm.fin_holding_attribution.pf_average_weight IS 'Portfolio Average Weight';

COMMENT ON COLUMN hdm.fin_holding_attribution.pf_ending_weight IS 'Portfolio Ending Weight';

COMMENT ON COLUMN hdm.fin_holding_attribution.pf_total_return IS 'Portfolio Total Return';

COMMENT ON COLUMN hdm.fin_holding_attribution.bm_total_return IS 'Benchmark Total Return';

COMMENT ON COLUMN hdm.fin_holding_attribution.pf_currency_contrib_return IS 'Portfolio Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_holding_attribution.bm_currency_contrib_return IS 'Benchmark Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_holding_attribution.allocation_effect IS 'Allocation Effect';

COMMENT ON COLUMN hdm.fin_holding_attribution.selection_interaction IS 'Selection +  Interaction';

COMMENT ON COLUMN hdm.fin_holding_attribution.total_effect IS 'Total Effect';

COMMENT ON COLUMN hdm.fin_holding_attribution.latest_purchase_date IS 'Latest Purchase Date';

COMMENT ON COLUMN hdm.fin_holding_attribution.latest_sale_date IS 'Most Recent Sale Date';

COMMENT ON COLUMN hdm.fin_holding_attribution.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_holding_attribution.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_holding_attribution.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_holding_attribution.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_holding_attribution.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_holding_attribution.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_holding_attribution.total_currency_effect IS 'Total Currency Effect';

COMMENT ON COLUMN hdm.fin_holding_attribution.bm_average_weight IS 'Benchmark Average Weight';

COMMENT ON COLUMN hdm.fin_holding_attribution.bm_ending_weight IS 'Benchmark Ending Weight';

COMMENT ON COLUMN hdm.fin_holding_attribution.pf_local_total_return IS 'Portfolio Local Total Return';

COMMENT ON COLUMN hdm.fin_holding_attribution.bm_local_total_return IS 'Benchmark Local Total Return';

COMMENT ON COLUMN hdm.fin_holding_attribution.pf_return_contrib IS 'Portfolio Return Contribution';

COMMENT ON COLUMN hdm.fin_holding_attribution.bm_return_contrib IS 'Benchmark Return Contribution';

COMMENT ON COLUMN hdm.fin_holding_attribution.local_selection_interaction IS 'Local Selection Interaction';

COMMENT ON COLUMN hdm.fin_holding_attribution.security_type_flag IS 'Security Type Flag : 1= Portfolio security, 2=Benchmark security, 3= Both';

COMMENT ON COLUMN hdm.fin_industry.fin_industry_key IS 'FIN Industry Key';

COMMENT ON COLUMN hdm.fin_industry.fin_sector_key IS 'FIN Sector Key';

COMMENT ON COLUMN hdm.fin_industry.industry_name IS 'Industry Name';

COMMENT ON COLUMN hdm.fin_industry.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_industry.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_industry.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_industry.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_industry.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_industry.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_industry_attribution.fin_industry_attrib_key IS 'FIN Holding Attribution Key';

COMMENT ON COLUMN hdm.fin_industry_attribution.pf_currency_code IS 'Portfolio Currency Code';

COMMENT ON COLUMN hdm.fin_industry_attribution.pf_begin_weight IS 'Portfolio Begin Weight';

COMMENT ON COLUMN hdm.fin_industry_attribution.pf_average_weight IS 'Portfolio Average Weight';

COMMENT ON COLUMN hdm.fin_industry_attribution.pf_ending_weight IS 'Portfolio Ending Weight';

COMMENT ON COLUMN hdm.fin_industry_attribution.pf_total_return IS 'Portfolio Total Return';

COMMENT ON COLUMN hdm.fin_industry_attribution.bm_total_return IS 'Benchmark Total Return';

COMMENT ON COLUMN hdm.fin_industry_attribution.pf_currency_contrib_return IS 'Portfolio Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_industry_attribution.bm_currency_contrib_return IS 'Benchmark Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_industry_attribution.allocation_effect IS 'Allocation Effect';

COMMENT ON COLUMN hdm.fin_industry_attribution.selection_interaction IS 'Selection +  Interaction';

COMMENT ON COLUMN hdm.fin_industry_attribution.total_effect IS 'Total Effect';

COMMENT ON COLUMN hdm.fin_industry_attribution.latest_purchase_date IS 'Latest Purchase Date';

COMMENT ON COLUMN hdm.fin_industry_attribution.latest_sale_date IS 'Most Recent Sale Date';

COMMENT ON COLUMN hdm.fin_industry_attribution.fin_industry_key IS 'FIN Industry Key';

COMMENT ON COLUMN hdm.fin_industry_attribution.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_industry_attribution.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_industry_attribution.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_industry_attribution.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_industry_attribution.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_industry_attribution.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_industry_attribution.total_currency_effect IS 'Total Currency Effect';

COMMENT ON COLUMN hdm.fin_industry_attribution.bm_average_weight IS 'Benchmark Average Weight';

COMMENT ON COLUMN hdm.fin_industry_attribution.bm_ending_weight IS 'Benchmark Ending Weight';

COMMENT ON COLUMN hdm.fin_industry_attribution.pf_local_total_return IS 'Portfolio Local Total Return';

COMMENT ON COLUMN hdm.fin_industry_attribution.bm_local_total_return IS 'Benchmark Local Total Return';

COMMENT ON COLUMN hdm.fin_industry_attribution.pf_return_contrib IS 'Portfolio Return Contribution';

COMMENT ON COLUMN hdm.fin_industry_attribution.bm_return_contrib IS 'Benchmark Return Contribution';

COMMENT ON COLUMN hdm.fin_industry_attribution.local_selection_interaction IS 'Local Selection Interaction';

COMMENT ON TABLE hdm.fin_mpt_statistics IS 'Modern Portfolio Theory';

COMMENT ON COLUMN hdm.fin_mpt_statistics.mpt_stats_key IS 'MPT Statistics Key';

COMMENT ON COLUMN hdm.fin_mpt_statistics.alpha IS 'Alpha';

COMMENT ON COLUMN hdm.fin_mpt_statistics.beta IS 'Beta';

COMMENT ON COLUMN hdm.fin_mpt_statistics.r_squared IS 'R Squared';

COMMENT ON COLUMN hdm.fin_mpt_statistics.sharpe_rate IS 'Sharpe Rate';

COMMENT ON COLUMN hdm.fin_mpt_statistics.std_deviation IS 'Standard Deviation';

COMMENT ON COLUMN hdm.fin_mpt_statistics.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_mpt_statistics.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_mpt_statistics.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_mpt_statistics.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_mpt_statistics.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_mpt_statistics.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_position.fin_position_key IS 'FIN Position Key';

COMMENT ON COLUMN hdm.fin_position.fin_security_key IS 'FIN Security Key';

COMMENT ON COLUMN hdm.fin_position.currency_code IS 'Currency Code';

COMMENT ON COLUMN hdm.fin_position.security_price IS 'Security Price';

COMMENT ON COLUMN hdm.fin_position.pf_shares IS 'Portfolio Shares';

COMMENT ON COLUMN hdm.fin_position.pf_weight IS 'Portfolio Weight';

COMMENT ON COLUMN hdm.fin_position.bm_weight IS 'Benchmark Weight';

COMMENT ON COLUMN hdm.fin_position.pf_ending_market_value IS 'Portfolio  Ending Market Value';

COMMENT ON COLUMN hdm.fin_position.pe_ratio IS 'Price To Earning Ratio';

COMMENT ON COLUMN hdm.fin_position.pb_ratio IS 'Price To Book Ratio';

COMMENT ON COLUMN hdm.fin_position.est_eps_growth_3to5_yr IS 'Estimated Eps Growth 3 5 Yr';

COMMENT ON COLUMN hdm.fin_position.pf_mpt_beta IS 'Portfolio MPT Beta';

COMMENT ON COLUMN hdm.fin_position.latest_purchase_date IS 'Latest Purchase Date';

COMMENT ON COLUMN hdm.fin_position.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_position.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_position.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_position.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_position.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_position.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_region.fin_region_key IS 'FIN Region Key';

COMMENT ON COLUMN hdm.fin_region.region_name IS 'Harbor IPM Region Name';

COMMENT ON COLUMN hdm.fin_region.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_region.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_region.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_region.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_region.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_region.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_region.region_type IS 'Region Type';

COMMENT ON COLUMN hdm.fin_region_attribution.region_attrib_key IS 'FIN Holding Attribution Key';

COMMENT ON COLUMN hdm.fin_region_attribution.pf_currency_code IS 'Portfolio Currency Code';

COMMENT ON COLUMN hdm.fin_region_attribution.pf_begin_weight IS 'Portfolio Begin Weight';

COMMENT ON COLUMN hdm.fin_region_attribution.pf_average_weight IS 'Portfolio Average Weight';

COMMENT ON COLUMN hdm.fin_region_attribution.pf_ending_weight IS 'Portfolio Ending Weight';

COMMENT ON COLUMN hdm.fin_region_attribution.pf_total_return IS 'Portfolio Total Return';

COMMENT ON COLUMN hdm.fin_region_attribution.bm_total_return IS 'Benchmark Total Return';

COMMENT ON COLUMN hdm.fin_region_attribution.pf_currency_contrib_return IS 'Portfolio Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_region_attribution.bm_currency_contrib_return IS 'Benchmark Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_region_attribution.allocation_effect IS 'Allocation Effect';

COMMENT ON COLUMN hdm.fin_region_attribution.selection_interaction IS 'Selection +  Interaction';

COMMENT ON COLUMN hdm.fin_region_attribution.total_effect IS 'Total Effect';

COMMENT ON COLUMN hdm.fin_region_attribution.latest_purchase_date IS 'Latest Purchase Date';

COMMENT ON COLUMN hdm.fin_region_attribution.latest_sale_date IS 'Most Recent Sale Date';

COMMENT ON COLUMN hdm.fin_region_attribution.fin_region_key IS 'FIN Region Key';

COMMENT ON COLUMN hdm.fin_region_attribution.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_region_attribution.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_region_attribution.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_region_attribution.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_region_attribution.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_region_attribution.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_region_attribution.total_currency_effect IS 'Total Currency Effect';

COMMENT ON COLUMN hdm.fin_region_attribution.bm_average_weight IS 'Benchmark Average Weight';

COMMENT ON COLUMN hdm.fin_region_attribution.bm_ending_weight IS 'Benchmark Ending Weight';

COMMENT ON COLUMN hdm.fin_region_attribution.pf_local_total_return IS 'Portfolio Local Total Return';

COMMENT ON COLUMN hdm.fin_region_attribution.bm_local_total_return IS 'Benchmark Local Total Return';

COMMENT ON COLUMN hdm.fin_region_attribution.pf_return_contrib IS 'Portfolio Return Contribution';

COMMENT ON COLUMN hdm.fin_region_attribution.bm_return_contrib IS 'Benchmark Return Contribution';

COMMENT ON COLUMN hdm.fin_region_attribution.local_selection_interaction IS 'Local Selection Interaction';

COMMENT ON COLUMN hdm.fin_region_attribution.country_allocation_effect IS 'Country Allocation Effect';

COMMENT ON COLUMN hdm.fin_region_attribution.country_selection_effect IS 'Country Selection Effect';

COMMENT ON COLUMN hdm.fin_sector.fin_sector_key IS 'FIN Sector Key';

COMMENT ON COLUMN hdm.fin_sector.sector_name IS 'Sector Name';

COMMENT ON COLUMN hdm.fin_sector.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_sector.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_sector.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_sector.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_sector.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_sector.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_sector_attribution.fin_sector_attrib_key IS 'FIN Holding Attribution Key';

COMMENT ON COLUMN hdm.fin_sector_attribution.pf_currency_code IS 'Portfolio Currency Code';

COMMENT ON COLUMN hdm.fin_sector_attribution.pf_begin_weight IS 'Portfolio Begin Weight';

COMMENT ON COLUMN hdm.fin_sector_attribution.pf_average_weight IS 'Portfolio Average Weight';

COMMENT ON COLUMN hdm.fin_sector_attribution.pf_ending_weight IS 'Portfolio Ending Weight';

COMMENT ON COLUMN hdm.fin_sector_attribution.pf_total_return IS 'Portfolio Total Return';

COMMENT ON COLUMN hdm.fin_sector_attribution.bm_total_return IS 'Benchmark Total Return';

COMMENT ON COLUMN hdm.fin_sector_attribution.pf_currency_contrib_return IS 'Portfolio Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_sector_attribution.bm_currency_contrib_return IS 'Benchmark Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_sector_attribution.allocation_effect IS 'Allocation Effect';

COMMENT ON COLUMN hdm.fin_sector_attribution.selection_interaction IS 'Selection +  Interaction';

COMMENT ON COLUMN hdm.fin_sector_attribution.total_effect IS 'Total Effect';

COMMENT ON COLUMN hdm.fin_sector_attribution.latest_purchase_date IS 'Latest Purchase Date';

COMMENT ON COLUMN hdm.fin_sector_attribution.latest_sale_date IS 'Most Recent Sale Date';

COMMENT ON COLUMN hdm.fin_sector_attribution.fin_sector_key IS 'FIN Sector Key';

COMMENT ON COLUMN hdm.fin_sector_attribution.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_sector_attribution.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_sector_attribution.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_sector_attribution.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_sector_attribution.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_sector_attribution.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_sector_attribution.total_currency_effect IS 'Total Currency Effect';

COMMENT ON COLUMN hdm.fin_sector_attribution.bm_average_weight IS 'Benchmark Average Weight';

COMMENT ON COLUMN hdm.fin_sector_attribution.bm_ending_weight IS 'Benchmark Ending Weight';

COMMENT ON COLUMN hdm.fin_sector_attribution.pf_local_total_return IS 'Portfolio Local Total Return';

COMMENT ON COLUMN hdm.fin_sector_attribution.bm_local_total_return IS 'Benchmark Local Total Return';

COMMENT ON COLUMN hdm.fin_sector_attribution.pf_return_contrib IS 'Portfolio Return Contribution';

COMMENT ON COLUMN hdm.fin_sector_attribution.bm_return_contrib IS 'Benchmark Return Contribution';

COMMENT ON COLUMN hdm.fin_sector_attribution.local_selection_interaction IS 'Local Selection Interaction';

COMMENT ON COLUMN hdm.fin_security.fin_security_key IS 'FIN Security Key';

COMMENT ON COLUMN hdm.fin_security.fin_industry_key IS 'FIN Industry Key';

COMMENT ON COLUMN hdm.fin_security.fin_country_key IS 'FIN Country Key';

COMMENT ON COLUMN hdm.fin_security.security_name IS 'Security Name';

COMMENT ON COLUMN hdm.fin_security.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_security.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_security.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_security.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_security.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_security.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_total_attribution.fin_total_attrib_key IS 'Fin Holding Attribution Key';

COMMENT ON COLUMN hdm.fin_total_attribution.pf_currency_code IS 'Portfolio Currency Code';

COMMENT ON COLUMN hdm.fin_total_attribution.pf_begin_weight IS 'Portfolio Begin Weight';

COMMENT ON COLUMN hdm.fin_total_attribution.pf_average_weight IS 'Portfolio Average Weight';

COMMENT ON COLUMN hdm.fin_total_attribution.pf_ending_weight IS 'Portfolio Ending Weight';

COMMENT ON COLUMN hdm.fin_total_attribution.pf_total_return IS 'Portfolio Total Return';

COMMENT ON COLUMN hdm.fin_total_attribution.bm_total_return IS 'Benchmark Total Return';

COMMENT ON COLUMN hdm.fin_total_attribution.pf_currency_contrib_return IS 'Portfolio Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_total_attribution.bm_currency_contrib_return IS 'Benchmark Contribution to Currency Return';

COMMENT ON COLUMN hdm.fin_total_attribution.allocation_effect IS 'Allocation Effect';

COMMENT ON COLUMN hdm.fin_total_attribution.selection_interaction IS 'Selection +  Interaction';

COMMENT ON COLUMN hdm.fin_total_attribution.total_effect IS 'Total Effect';

COMMENT ON COLUMN hdm.fin_total_attribution.latest_purchase_date IS 'Latest Purchase Date';

COMMENT ON COLUMN hdm.fin_total_attribution.latest_sale_date IS 'Most Recent Sale Date';

COMMENT ON COLUMN hdm.fin_total_attribution.curr_row_flg IS 'Current Row Flag';

COMMENT ON COLUMN hdm.fin_total_attribution.row_strt_dttm IS 'Row Start Date Time';

COMMENT ON COLUMN hdm.fin_total_attribution.row_stop_dttm IS 'Row Stop Date Time';

COMMENT ON COLUMN hdm.fin_total_attribution.row_updt_dttm IS 'Row Update Date Time';

COMMENT ON COLUMN hdm.fin_total_attribution.etl_load_cyc_key IS 'ETL Load Cycle Key';

COMMENT ON COLUMN hdm.fin_total_attribution.src_sys_id IS 'Source System Identifier';

COMMENT ON COLUMN hdm.fin_total_attribution.bm_average_weight IS 'Benchmark Average Weight';

COMMENT ON COLUMN hdm.fin_total_attribution.bm_ending_weight IS 'Benchmark Ending Weight';

COMMENT ON COLUMN hdm.fin_total_attribution.pf_local_total_return IS 'Portfolio Local Total Return';

COMMENT ON COLUMN hdm.fin_total_attribution.bm_local_total_return IS 'Benchmark Local Total Return';

COMMENT ON COLUMN hdm.fin_total_attribution.pf_return_contrib IS 'Portfolio Return Contribution';

COMMENT ON COLUMN hdm.fin_total_attribution.bm_return_contrib IS 'Benchmark Return Contribution';

COMMENT ON COLUMN hdm.fin_total_attribution.local_selection_interaction IS 'Local Selection Interaction';

COMMENT ON COLUMN hdm.fin_total_attribution.attrib_type_key IS 'Attribution Type Key';

COMMENT ON COLUMN hdm.fin_total_attribution.total_currency_effect IS 'Total Currency Effect';

COMMENT ON COLUMN hdm.fin_total_attribution.country_allocation_effect IS 'Country Allocation Effect';

COMMENT ON COLUMN hdm.fin_total_attribution.country_selection_effect IS 'Country Selection Effect';