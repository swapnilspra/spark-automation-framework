from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob # must be imported after spark has been set up
import datetime
import calendar
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window


class Job(ETLJob):
    target_table = "index_return"
    business_key = ["bmk_idx_key", "fund_compst_key", "per_key", "day_key"]
    primary_key = {"idx_ret_key": "int"}
    sources:Dict[str,Dict[str,Any]] = {
        "fund_composite": {
            "type": "table",
            "source": "fund_composite"
        },
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "return_periods": {
            "type": "table",
            "source": "return_periods"
        },
        "calendar": {
            "type": "table",
            "source": "calendar"
        },
        "fund_manager": {
            "type": "table",
            "source": "fund_manager"
        },
        "fund_index_rltn": {
            "type": "table",
            "source": "fund_index_rltn"
        },
        "benchmark_index": {
            "type": "table",
            "source": "benchmark_index"
        },
        "perf_run_type": {
            "type": "table",
            "source": "perf_run_type"
        }
    }

    joins: List[Dict[str, Any]] = []

    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fund_compst_key"), "target": "fund_compst_key"},
        {"source": F.col("per_key"), "target": "per_key"},
        {"source": F.col("bmk_idx_key"), "target": "bmk_idx_key"},
        {"source": F.col("t_sort"), "target": "perf_displ_ord"},
        {"source": F.col("t_day_key"), "target": "day_key"},
        {"source": F.col("t_beg_dt"), "target": "strt_per"},
        {"source": F.col("t_return"), "target": "idx_ret"},
        {"source": F.col("t_annl"), "target": "annlzn_fctr"},
        {"source": F.col("ret_per_flg"), "target": "ret_per_flg"},
        {"source": F.col("gen_rptg_flg"), "target": "gen_rptg_flg"},
        {"source": F.col("daily_rptg_flg"), "target": "daily_rptg_flg"},
        {"source": F.col("trgt_rptg_flg"), "target": "trgt_rptg_flg"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

    def join(self,inputs:Dict[str,pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        # convert p_end_dt to timestamp
        if not self._args.get("p_end_dt",None):
            # default value is first day of the current month
            p_end_dt = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)-datetime.timedelta(days=1)
        else:
            p_end_dt = datetime.datetime.strptime(self._args["p_end_dt"],"%m/%d/%Y")

        df_joined = super().join(inputs)

        # Create dataframes from tables
        df_fund = inputs["fund"]
        df_fund_composite = inputs["fund_composite"]
        df_return_period = inputs["return_periods"]
        df_fund_manager = inputs["fund_manager"]
        df_fund_index_rltn = inputs["fund_index_rltn"]
        df_benchmark_index = inputs["benchmark_index"]
        df_perf_run_type = inputs["perf_run_type"]

        # Start Populate the dataframe df_joined for the etl pipeline

        # Create df_sba
        df_sba = df_fund_composite.alias("fund_composite")\
            .join(df_fund_manager.alias("fund_manager"), [F.col("fund_composite.fund_compst_key") == F.col("fund_manager.fund_compst_key")], how="inner")\
            .select(F.col("fund_composite.fund_compst_key"), F.col("fund_composite.compst_nm"), F.col("fund_composite.compst_incpn_dt"),
                    F.col("fund_manager.sba_efftv_strt_dt"), F.col("fund_manager.sba_efftv_end_dt"), F.col("fund_manager.sba_actv_flg"),
                    F.col("fund_composite.compst_stat_cd"), F.col("fund_composite.compst_ord"),F.col("fund_composite.compst_stat_desc"), F.col("fund_composite.compst_incpn_dt"))

        df_sba = df_sba\
            .where((((F.col("compst_stat_cd") == 'O') | (F.col("compst_stat_cd") == 'S')) | F.lit(p_end_dt).between(
                F.col("sba_efftv_strt_dt"), F.coalesce(F.col("sba_efftv_end_dt"), F.lit(p_end_dt)))) & (F.lit(p_end_dt).between(
                       F.col("sba_efftv_strt_dt"), F.coalesce(F.col("sba_efftv_end_dt"), F.lit(p_end_dt)))) &
                   ((F.col("sba_actv_flg") == 'Y') | (F.lit(p_end_dt).between(
                       F.col("sba_efftv_strt_dt"), F.coalesce(F.col("sba_efftv_end_dt"), F.lit(p_end_dt))))))

        df_sba = df_sba\
            .groupBy("fund_compst_key","compst_nm", "compst_ord", "compst_incpn_dt","compst_stat_desc")\
            .agg(F.to_timestamp(F.max("sba_efftv_strt_dt")).alias("sba_incpn_dt"))

        df_fund_agg = df_fund\
            .select("fund_compst_key",F.col("perf_incpn_dt"), F.col("fund_clos_dt"))

        df_fund_agg = df_fund_agg\
            .groupBy("fund_compst_key")\
            .agg(F.min(F.col("perf_incpn_dt")).alias("fund_incpn_dt"), F.min(F.col("fund_clos_dt")).alias("fund_clos_dt"))

        df_tbl_index = df_fund_composite.alias("fund_composite")\
            .join(df_fund_index_rltn.alias("fund_index_rltn"), F.col("fund_index_rltn.fund_compst_key") == F.col("fund_composite.fund_compst_key"), how="inner")\
            .join(df_fund_agg.alias("fund_agg"), F.col("fund_agg.fund_compst_key") == F.col("fund_composite.fund_compst_key"), how="inner") \
            .join(df_perf_run_type.alias("perf_run_type"), F.col("fund_index_rltn.perf_run_type_key") == F.col("perf_run_type.perf_run_type_key"), how="inner")\
            .join(df_sba.alias("sba"), F.col("sba.fund_compst_key") == F.col("fund_composite.fund_compst_key"), how="inner")\
            .join(df_benchmark_index.alias("benchmark_index"), F.col("fund_index_rltn.bmk_idx_key") == F.col("benchmark_index.bmk_idx_key"), how="inner")\
            .select(F.col("fund_agg.fund_incpn_dt"), F.col("sba.sba_incpn_dt"), F.col("perf_run_type.run_type_cd"), F.col("fund_index_rltn.rpt_sort_ord"),
                    F.col("fund_composite.fund_compst_key"), F.col("benchmark_index.bmk_idx_key"))\
            .where((((F.col("fund_composite.compst_stat_cd") == 'O') | (F.col("fund_composite.compst_stat_cd") == 'S')) |
                   (F.coalesce(F.col("fund_agg.fund_clos_dt"), F.lit(p_end_dt)) >= F.lit(p_end_dt))) &
                   (F.col("fund_agg.fund_incpn_dt") <= F.lit(p_end_dt)))

        df_tbl_index = df_tbl_index\
            .withColumn("report_type", F.when(F.col('perf_run_type.run_type_cd') == 'REPORTING', F.lit(4))
                        .otherwise(F.when(F.col('perf_run_type.run_type_cd') == 'DAILY_PERF', F.lit(2))
                        .otherwise(F.when(F.col('perf_run_type.run_type_cd') == 'TARGET_FUNDS', F.lit(1))
                        .otherwise(F.lit(1)))))

        df_tbl_index = df_tbl_index\
            .groupBy("fund_compst_key", "bmk_idx_key", "fund_incpn_dt")\
            .agg(F.max("sba_incpn_dt").alias("sba_incpn_dt_t"), F.min("rpt_sort_ord").alias("rpt_sort_ord"),
                 F.sum("report_type").alias("report_type"))

        df_joined = df_tbl_index.alias("tbl_index")
        df_joined = df_joined\
            .withColumn("t_incp", F.floor(F.months_between(F.lit(p_end_dt), F.to_timestamp(F.col("tbl_index.fund_incpn_dt")))/12))

        df_tbl_per = df_return_period
        df_tbl_per = df_tbl_per.alias("tbl_per")\
            .where(((F.col("tbl_per.daily_msr_flg") == 'Y') | (F.col("tbl_per.mo_end_msr_flg") == 'Y')))

        df_joined = df_joined\
            .join(df_tbl_per, ~(
                (F.col("tbl_per.per_msr") > F.col("t_incp")) &
                (F.col("tbl_per.per_ut") == 'YR')
            ), how="inner")

        return df_joined

    def transform(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        df_transformed = self.calc_transformed(df_joined)
        df_transformed = super().transform(df_transformed)
        return df_transformed

    def calc_transformed(self, df_joined: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        # convert p_end_dt to timestamp
        if not self._args.get("p_end_dt",None):
            # default value is first day of the current month
            p_end_dt = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)-datetime.timedelta(days=1)
        else:
            p_end_dt = datetime.datetime.strptime(self._args["p_end_dt"],"%m/%d/%Y")

        df_transformed = df_joined

        # Count days in current Year to t_curr_yr_days
        df_transformed = df_transformed\
            .withColumn('t_curr_yr_days', F.datediff(F.lit(p_end_dt),
                                                     F.add_months(F.lit(p_end_dt), -12)))

        df_transformed = df_transformed\
            .withColumn("t_gen_incpn_dt",
                        F.when(F.col("tbl_per.per_nm") == 'SUBI', F.col("tbl_index.sba_incpn_dt_t")).otherwise(F.col("tbl_index.fund_incpn_dt")))

        df_calendar = common.utils.read_table_snapshot(
            table_name="calendar",
            env=self._env,
            spark=self._spark)

        # added to include any fund that started on first business day and not the first calendar day.
        # t_bus_day_month added to include any fund that started on first business day and not the first calendar day.
        df_calendar_month = df_calendar\
            .select("cal_day")\
            .where(F.col('cal_day').between(F.date_trunc('month', F.lit(p_end_dt)),
                                            F.lit(p_end_dt)) & (F.col("bus_day_flg") == 'Y'))

        df_calendar_month = df_calendar_month\
            .agg(F.min(F.col("cal_day")).alias("t_bus_day_month")).collect()

        t_bus_day_month = df_calendar_month[0]['t_bus_day_month']

        # t_bus_day_quarter added to include any fund that started on first business day and not the first calendar day.
        df_calendar_quarter = df_calendar\
            .select("cal_day")\
            .where(F.col('cal_day').between(F.lit(F.date_add(F.date_trunc('quarter', F.lit(p_end_dt)), -1)),
                                            F.lit(p_end_dt)) & (F.col("bus_day_flg") == 'Y'))

        df_calendar_quarter = df_calendar_quarter\
            .agg(F.min(F.col("cal_day")).alias("t_bus_day_quarter")).collect()

        t_bus_day_quarter = df_calendar_quarter[0]['t_bus_day_quarter']

        # create t_beg_dt using the  get_begin_dt udf
        df_transformed = df_transformed\
            .withColumn("t_beg_dt",  get_begin_dt(F.col("tbl_per.per_ut"), F.col("tbl_per.per_msr"),
                                                  F.col("t_gen_incpn_dt"),
                                                  F.lit(p_end_dt),
                                                  F.lit(t_bus_day_month),
                                                  F.lit(t_bus_day_quarter)))

        df_transformed = df_transformed\
            .withColumn('t_annl_yr', F.when(F.col('tbl_per.per_ut') == 'IN',
                                            F.floor(F.months_between(F.date_trunc('day', F.lit(p_end_dt)), F.col('t_gen_incpn_dt')) / 12)).cast(T.IntegerType()))

        df_transformed = df_transformed\
            .withColumn('t_days_month_between', F.when(F.col('tbl_per.per_ut') == 'IN',
                                                       (F.floor(F.months_between(F.lit(p_end_dt), F.col('t_gen_incpn_dt')) / 12) * 12)).cast(T.IntegerType()))

        df_transformed = df_transformed\
            .withColumn('t_days_add_month', F.when(F.col('tbl_per.per_ut') == 'IN',
                                                   F.date_add(F.expr("add_months(t_gen_incpn_dt,t_days_month_between)"),-1))).drop(F.col('t_days_month_between'))

        df_transformed = df_transformed\
            .withColumn('t_days', F.when(F.col('tbl_per.per_ut') == 'IN',
                                         F.datediff(F.lit(p_end_dt), F.col('t_days_add_month')))).drop(F.col('t_days_add_month'))

        df_transformed = df_transformed\
            .withColumn('t_annl', F.when(F.col('tbl_per.per_ut') == 'IN',
                                         (F.col('t_annl_yr') + (F.col('t_days') / F.col('t_curr_yr_days')))).otherwise(F.col('tbl_per.annl_fctr')))

        df_transformed = df_transformed\
            .withColumn('t_annl', F.col("t_annl").cast("double"))

        df_transformed = df_transformed\
            .withColumn('t_annl', F.when(F.col('t_annl') < 1, 1).otherwise(F.col('t_annl')))

        # compare calender to p_end_dt in order to get p_end_dt day_key
        df_day_key = df_calendar\
            .select(F.col('cal_day'), F.col('day_key'))

        df_day_key = df_day_key\
            .where(F.col('cal_day') == F.lit(p_end_dt)).select('day_key')

        df_day_key = df_day_key\
            .withColumn('t_day_key', F.col('day_key')).drop('day_key').collect()

        t_day_key = df_day_key[0]['t_day_key']

        df_transformed = df_transformed\
            .withColumn('t_day_key', F.lit(t_day_key))

        df_transformed = df_transformed\
            .withColumn('ret_per_flg',
                        F.when(F.lit(p_end_dt) == F.last_day(F.lit(p_end_dt)), 'M').otherwise('D'))

        df_transformed = df_transformed\
            .withColumn('gen_rptg_flg',
                        F.when(F.col('tbl_index.report_type') == F.lit(4), 'Y').otherwise(F.when(F.col('tbl_index.report_type') == F.lit(6), 'Y').otherwise('N')))

        df_transformed = df_transformed\
            .withColumn('daily_rptg_flg',
                        F.when(F.col('tbl_index.report_type') == F.lit(2), 'Y').otherwise(F.when(F.col('tbl_index.report_type') == F.lit(6), 'Y').otherwise('N')))

        df_transformed = df_transformed\
            .withColumn('trgt_rptg_flg',
                        F.when(F.col('tbl_index.report_type') == F.lit(1), 'Y').otherwise('N'))

        # get t_return using calc_return UDF
        df_index_valuation = common.utils.read_table_snapshot(
            table_name="index_valuation",
            env=self._env,
            spark=self._spark)

        df_index_valuation = df_index_valuation \
            .select(F.col("bmk_idx_key").alias("bmk_idx_join"), F.col("day_key").alias("day_key_join"),
                    F.col("idx_vltn_amt"))

        # get the start wealth Index
        df_calendar_begin = df_calendar\
            .select(F.col('cal_day'), F.col('day_key'))

        df_join_begin = df_index_valuation.alias("index_valuation")\
            .join(df_calendar_begin.alias("calendar"),
                  F.col("index_valuation.day_key_join") == F.col("calendar.day_key"), how="inner")\
            .select(F.col('index_valuation.bmk_idx_join'), F.col('calendar.cal_day'),
                    F.col('index_valuation.idx_vltn_amt').alias('t_begin_wi').cast(T.DoubleType()))

        df_transformed = df_transformed\
            .join(df_join_begin.alias("join_begin"), [F.col("tbl_index.bmk_idx_key") == F.col("join_begin.bmk_idx_join"),
                                                      F.col('join_begin.cal_day') == F.col("t_beg_dt")],how="left")\
            .drop('bmk_idx_join', 'cal_day')

        # get the end wealth Index
        df_calendar_end = df_calendar \
            .select(F.col('cal_day'), F.col('day_key'))

        df_join_end = df_index_valuation.alias("index_valuation")\
            .join(df_calendar_end.alias("calendar"), [F.col("index_valuation.day_key_join") == F.col("calendar.day_key"),
                                                      F.col("calendar.cal_day") == F.lit(p_end_dt)], how="inner")\
            .select(F.col("index_valuation.bmk_idx_join"), F.col('cal_day'),
                    F.col('index_valuation.idx_vltn_amt').alias('t_end_wi').cast(T.DoubleType()))

        df_transformed = df_transformed\
            .join(df_join_end.alias("join_end"), F.col("tbl_index.bmk_idx_key") == F.col("join_end.bmk_idx_join"), how="left")\
            .drop('bmk_idx_join', 'cal_day')

        # get t_return using calc_return UDF
        df_transformed = df_transformed \
            .withColumn('t_return', calc_return(F.col('t_end_wi'), F.col('t_begin_wi'), F.col('t_annl')))

        # filter down only records where t_return is not null
        df_transformed = df_transformed.filter(~F.isnull(F.col("t_return")))

        w = Window().orderBy(F.lit('dummy'))
        df_transformed = df_transformed.withColumn("t_sort", F.row_number().over(w))
        return df_transformed


# UDF
@F.udf(T.DateType())
def get_begin_dt(p_unit, p_measure, p_inc_dt, p_end_dt, t_bus_day_month, t_bus_day_quarter):
    """
        p_unit = per_ut
        p_measure = per_msr
        p_inc_dt = t_gen_incpn_dt
        p_end_dt = p_end_dt
        """
    # relativedelta function used for calculate months(add months) - this attribute missing in datetime module
    if p_unit == 'YR':
        t_date = p_end_dt + datetime.timedelta(days=1) - relativedelta(months=12 * p_measure) - datetime.timedelta(days=1)
    elif p_unit == 'MO':
        t_date = p_end_dt + datetime.timedelta(days=1) - relativedelta(months=1 * p_measure) - datetime.timedelta(days=1)
#   added to include any fund that started on first business day and not the first calendar day.
        if t_bus_day_month < p_inc_dt:
            t_date = None
    elif p_unit == 'DY':
        if p_measure == 1:
            # Consider if Month end DAte is a holiday
            # if calculating on the inception date, go with the last day.  There will be a WI.
            if p_end_dt == p_inc_dt:
                t_date = p_end_dt - datetime.timedelta(days=1)
            else:
                t_date = p_end_dt - datetime.timedelta(days=1 * p_measure)
        else:
            t_date = p_end_dt - datetime.timedelta(days=1 * p_measure)
        if t_date < (p_inc_dt - datetime.timedelta(days=1)):
            t_date = None
    elif p_unit == 'WK':
        t_date = p_end_dt - datetime.timedelta(days=7 * p_measure)
        if t_date < (p_inc_dt - datetime.timedelta(days=1)):
            t_date = None
    elif p_unit == 'MT':
        t_date = p_end_dt.replace(day=1) - datetime.timedelta(days=1)
        if t_date < p_inc_dt and p_end_dt != (
        datetime.datetime(p_end_dt.year, p_end_dt.month, calendar.monthrange(p_end_dt.year, p_end_dt.month)[-1])):
            # Check if it is a month end return calculation (Date is last date of the month)
            t_date = p_inc_dt - datetime.timedelta(days=1)
    elif p_unit == 'QT':
        t_date = datetime.datetime(p_end_dt.year, 3 * int((p_end_dt.month - 1) / 3 + 1) - 2, 1).replace(day=1) - datetime.timedelta(days=1)
#   added to include any fund that started on first business day and not the first calendar day.
        if t_date < p_inc_dt and p_end_dt != (
        datetime.datetime(p_end_dt.year, p_end_dt.month, calendar.monthrange(p_end_dt.year, p_end_dt.month)[-1])):
            t_date = p_inc_dt - datetime.timedelta(days=1)
        elif t_bus_day_quarter < p_inc_dt:
            t_date = None
    elif p_unit == 'YT':
        t_date = p_end_dt.replace(day=1, month=1) - datetime.timedelta(days=1)
#   If fund inception date is current year, YTD should start from Inception date
#   For Month end return, do not calculate YTD if Inception is in current Year.
        if t_date < p_inc_dt and p_end_dt == (
        datetime.datetime(p_end_dt.year, p_end_dt.month, calendar.monthrange(p_end_dt.year, p_end_dt.month)[-1])):
            t_date = None
        elif t_date < p_inc_dt and p_end_dt != (
        datetime.datetime(p_end_dt.year, p_end_dt.month, calendar.monthrange(p_end_dt.year, p_end_dt.month)[-1])):
            # Check if it is a month end return calculation (Date is last date of the month)
            t_date = p_inc_dt - datetime.timedelta(days=1)
    elif p_unit == 'IN':
        t_date = p_inc_dt - datetime.timedelta(days=1)

    return t_date


# calc_return function
@F.udf(T.DoubleType())
def calc_return(t_end_wi, t_begin_wi, p_annl_fctr):
    if t_end_wi is None or t_begin_wi is None or p_annl_fctr is None:
        return None
    return pow((t_end_wi / t_begin_wi), (1 / p_annl_fctr)) - 1