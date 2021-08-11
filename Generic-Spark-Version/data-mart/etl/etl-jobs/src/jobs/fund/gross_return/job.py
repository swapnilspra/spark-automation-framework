from pyspark.sql.types import ArrayType, StringType, MapType, StructField, StructType, FloatType
from typing import Dict, List, Any
import math
import logging
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob  # must be imported after spark has been set up
import datetime
import pyspark.sql.types as T
from dateutil.relativedelta import relativedelta
import calendar
import decimal as D


class Job(ETLJob):
    target_table = "gross_return"
    business_key = ["fund_key", "day_key", "per_key", "bmk_idx_key"]
    primary_key = {"gr_ret_key": "int"}
    sources: Dict[str, Dict[str, Any]] = {
        "fund": {
            "type": "table",
            "source": "fund"
        },
        "benchmark_index": {
            "type": "table",
            "source": "benchmark_index"
        },
        "return_periods": {
            "type": "table",
            "source": "return_periods"
        },
        "fund_index_rltn": {
            "type": "table",
            "source": "fund_index_rltn"
        }
    }
    joins: List[Dict[str, Any]] = [
    ]
    target_mappings: List[Dict[str, Any]] = [

        {"source": F.col("fund_key"), "target": "fund_key"},
        {"source": F.col("per_key"), "target": "per_key"},
        {"source": F.col("t_day_key"), "target": "day_key"},
        {"source": F.col("bmk_idx_key"), "target": "bmk_idx_key"},
        {"source": F.col("t_return"), "target": "gr_ret"},
        {"source": F.col("t_info_ratio"), "target": "info_ratio"},
        {"source": F.col("t_idx_ret"), "target": "idx_ret"},
        {"source": F.lit("Y"), "target": "curr_row_flg"}
    ]

    # override transform to add forward filling where needed
    def join(self, inputs: Dict[str, pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        # convert p_end_dt to timestamp
        if not self._args.get("p_end_dt",None):
            # default value is first day of the current month
            p_end_dt = datetime.datetime.today().replace(day=1,hour=0, minute=0, second=0, microsecond=0)-datetime.timedelta(days=1)
        else:
            p_end_dt = datetime.datetime.strptime(self._args["p_end_dt"],"%m/%d/%Y")

        df_joined = super().join(inputs)

        # Create dataframes from tables
        df_fund = inputs["fund"]
        df_fund_index_rltn = inputs["fund_index_rltn"]
        df_benchmark_index = inputs["benchmark_index"]
        df_return_periods = inputs["return_periods"]

        df_tbl_fund = df_fund.alias("fund")\
            .join(df_fund_index_rltn, F.col("fund.fund_compst_key") == F.col("fund_index_rltn.fund_compst_key"), how="inner")\
            .join(df_benchmark_index.alias("benchmark_index"), F.col("fund_index_rltn.bmk_idx_key") == F.col("benchmark_index.bmk_idx_key"), how="inner")\
            .select(F.col("fund.fund_key"), F.col("fund.perf_incpn_dt").alias("fund_incpn_dt"),
                    F.col("fund.fund_compst_key"), F.col("fund_index_rltn.bmk_idx_key"))\
            .where(((F.col("fund_index_rltn.link_prio") == 1) | (F.col("fund_index_rltn.prim_bmk_flg") == 'z')) &
                   ((F.col("fund_index_rltn.prim_bmk_flg") =='z') | (F.col("fund_index_rltn.prim_bmk_flg") =='Y')) &
                   (F.col("fund_index_rltn.perf_run_type_key") == 1) &
                   ((F.col("fund.fund_stat_cd") == 1) | (F.col("fund.fund_stat_cd") == 5)) &
                   (F.col("fund.perf_incpn_dt") <= F.lit(p_end_dt)))

        df_joined = df_tbl_fund.alias("tbl_fund")
        df_joined = df_joined\
            .withColumn("t_incp", F.floor(F.months_between(F.date_add(F.lit(p_end_dt),1),
                                                           F.to_timestamp(F.col("tbl_fund.fund_incpn_dt"))) / 12))

        df_tbl_per = df_return_periods
        df_tbl_per = df_tbl_per.alias("tbl_per")\
            .where(((F.col("tbl_per.daily_msr_flg") == 'Y') & (F.col("tbl_per.mo_end_msr_flg") == 'Y')))

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
            p_end_dt = datetime.datetime.today().replace(day=1,hour=0, minute=0, second=0, microsecond=0)-datetime.timedelta(days=1)
        else:
            p_end_dt = datetime.datetime.strptime(self._args["p_end_dt"],"%m/%d/%Y")

        df_transformed = df_joined

        # Count days in current Year
        df_transformed = df_transformed\
            .withColumn('t_curr_yr_days', F.datediff(F.lit(p_end_dt),
                                                     F.add_months(F.lit(p_end_dt), -12)))

        df_calendar = common.utils.read_table_snapshot(
            table_name="calendar",
            env=self._env,
            spark=self._spark)

        # t_bus_day_month added to include any fund that started on first business day and not the first calendar day.
        df_calendar_month = df_calendar\
            .select("cal_day")\
            .where(F.col('cal_day').between(F.date_trunc('month', F.lit(p_end_dt)),
                                            F.lit(p_end_dt)) & (F.col("bus_day_flg") == 'Y'))

        df_calendar_month = df_calendar_month\
            .agg(F.min(df_calendar.cal_day).alias("t_bus_day_month")).collect()

        t_bus_day_month = df_calendar_month[0]['t_bus_day_month']

        # t_bus_day_quarter added to include any fund that started on first business day and not the first calendar day.
        df_calendar_quarter = df_calendar\
            .select("cal_day")\
            .where(F.col('cal_day').between(F.lit(F.date_add(F.date_trunc('quarter', F.lit(p_end_dt)), -1)),
                                            F.lit(p_end_dt)) & (F.col("bus_day_flg") == 'Y'))

        df_calendar_quarter = df_calendar_quarter\
            .agg(F.min(df_calendar.cal_day).alias("t_bus_day_quarter")).collect()

        t_bus_day_quarter = df_calendar_quarter[0]['t_bus_day_quarter']

        # create t_beg_dt using the  get_begin_dt udf
        df_transformed = df_transformed\
            .withColumn("t_beg_dt", get_begin_dt(F.col("tbl_per.per_ut"), F.col("tbl_per.per_msr"), F.col("tbl_fund.fund_incpn_dt"),
                                                                F.lit(p_end_dt),
                                                                F.lit(t_bus_day_month),
                                                                F.lit(t_bus_day_quarter)))

        df_transformed = df_transformed\
            .withColumn('t_annl_yr', F.when(F.col('tbl_per.per_ut') == 'IN',
                                            F.floor(F.months_between(F.date_trunc('day', F.lit(p_end_dt)),
                                                                     F.col('tbl_fund.fund_incpn_dt')) / 12).cast(T.IntegerType())))

        df_transformed = df_transformed \
            .withColumn('t_days_month_between', F.when(F.col('tbl_per.per_ut') == 'IN',
                                                       (F.floor(F.months_between(
                                                           F.lit(p_end_dt),
                                                           F.col('tbl_fund.fund_incpn_dt')) / 12) * 12)).cast(T.IntegerType()))

        df_transformed = df_transformed\
            .withColumn('t_days_add_month', F.when(F.col('tbl_per.per_ut') == 'IN',
                                                   F.date_add(F.expr("add_months(tbl_fund.fund_incpn_dt,t_days_month_between)"),
                                                              -1))).drop(F.col('t_days_month_between'))

        df_transformed = df_transformed\
            .withColumn('t_days', F.when(F.col('tbl_per.per_ut') == 'IN',
                                         F.datediff(F.lit(p_end_dt),
                                                    F.col('t_days_add_month')))).drop(F.col('t_days_add_month'))

        # Count days in current Year
        df_transformed = df_transformed\
            .withColumn('t_curr_yr_days', F.when(F.col('tbl_per.per_ut') == 'IN',
                                                 F.datediff(F.lit(p_end_dt), F.add_months(F.lit(p_end_dt), -12))))

        df_transformed = df_transformed\
            .withColumn('t_annl', F.when(F.col('tbl_per.per_ut') == 'IN',
                                         (F.col('t_annl_yr') + (F.col('t_days') / F.col('t_curr_yr_days')))).otherwise(F.col('tbl_per.annl_fctr')))

        df_transformed = df_transformed \
            .withColumn('t_annl', F.col("t_annl").cast("double"))

        df_transformed = df_transformed\
            .withColumn('t_annl', F.when(F.col('t_annl') < F.lit(1), F.lit(1)).otherwise(F.col('t_annl')))

        #  calc_gr_return function:
        df_gross_wealth_index = common.utils.read_table_snapshot(
            table_name="gross_wealth_index",
            env=self._env,
            spark=self._spark)

        df_gross_wealth_index = df_gross_wealth_index.alias("gross_wealth_index")\
            .select(F.col("gross_wealth_index.gr_wlth_idx"), F.col("gross_wealth_index.fund_key").alias("fund_key_join"), F.col("gross_wealth_index.day_key").alias("day_key_join"))

        df_calendar_gr_return = df_calendar\
            .select(F.col('cal_day'), F.col('day_key'))

        df_join_gr_return = df_gross_wealth_index.alias("gross_wealth_index")\
            .join(df_calendar_gr_return.alias("calendar"), F.col("gross_wealth_index.day_key_join") == F.col("calendar.day_key"), how="inner")\
            .select(F.col('gross_wealth_index.fund_key_join'), F.col("calendar.cal_day"), F.col("gross_wealth_index.gr_wlth_idx"))

        df_join_gr_return = df_join_gr_return\
            .groupBy("fund_key_join", "cal_day")\
            .agg(F.sum(F.log(F.col("gross_wealth_index.gr_wlth_idx"))).alias("t_retval"))

        df_join_gr_return = df_join_gr_return\
            .withColumn("t_retval", F.exp(F.col("t_retval")))

        df_transformed = df_transformed\
            .join(df_join_gr_return.alias("join_gr_return"),
                  [F.col("join_gr_return.fund_key_join") == F.col("tbl_fund.fund_key"),
                   F.col("join_gr_return.cal_day") == F.col("t_beg_dt"),
                   F.col("join_gr_return.cal_day") <= F.lit(p_end_dt)], how="left")\
            .drop('fund_key_join', 'cal_day')

        df_transformed = df_transformed\
            .withColumn('t_return', calc_gr_return(F.col('t_retval'), F.col('t_annl')))

        #  calc_t_inf_ratio function:

        # define dataframes
        df_fund_index_rltn = common.utils.read_table_snapshot(
            table_name="fund_index_rltn",
            env=self._env,
            spark=self._spark)

        df_fund = common.utils.read_table_snapshot(
            table_name="fund",
            env=self._env,
            spark=self._spark)

        df_benchmark_index = common.utils.read_table_snapshot(
            table_name="benchmark_index",
            env=self._env,
            spark=self._spark)

        df_gross_wealth_index = common.utils.read_table_snapshot(
            table_name="gross_wealth_index",
            env=self._env,
            spark=self._spark)

        df_index_return = common.utils.read_table_snapshot(
            table_name="index_return",
            env=self._env,
            spark=self._spark)

        df_return_periods = common.utils.read_table_snapshot(
            table_name="return_periods",
            env=self._env,
            spark=self._spark)

        df_transformed_gr = df_transformed

        df_gr = df_fund.alias("fund")\
            .join(df_fund_index_rltn.alias("fund_index_rltn"), F.col("fund.fund_compst_key") == F.col("fund_index_rltn.fund_compst_key"), how="inner")\
            .join(df_benchmark_index.alias("benchmark_index"), F.col("fund_index_rltn.bmk_idx_key") == F.col("benchmark_index.bmk_idx_key"), how="inner")\
            .select(F.col("fund.fund_key"), F.col("fund.fund_incpn_dt"), F.col("fund.fund_compst_key"), F.col("fund_index_rltn.bmk_idx_key"))\
            .where(((F.col("fund_index_rltn.link_prio") == 1) | (F.col("fund_index_rltn.prim_bmk_flg") == 'z')) &
                   ((F.col("fund_index_rltn.prim_bmk_flg") =='z') | (F.col("fund_index_rltn.prim_bmk_flg") =='Y')) &
                   (F.col("fund_index_rltn.perf_run_type_key") == 1) &
                   ((F.col("fund.fund_stat_cd") == 1) | (F.col("fund.fund_stat_cd") == 5)) &
                   (F.col("fund.perf_incpn_dt") <= F.lit(p_end_dt)))

        df_calc_inf_ratio_calc = df_gross_wealth_index.alias("gross_wealth_index")\
            .join(df_calendar.alias("calendar"), F.col("calendar.day_key") == F.col("gross_wealth_index.day_key"), how="inner")\
            .join(df_fund.alias("fund"), F.col("fund.fund_key") == F.col("gross_wealth_index.fund_key"), how="inner")\
            .join(df_index_return.alias("index_return"), F.col("index_return.day_key") == F.col("gross_wealth_index.day_key"), how="inner")\
            .join(df_return_periods.alias("return_periods"), F.col("return_periods.per_key") == F.col("index_return.per_key"), how="inner")\
            .join(df_gr.alias("gr"), F.col("gr.bmk_idx_key") == F.col("index_return.bmk_idx_key"), how="inner")\
            .join(df_transformed_gr.alias("transformed_gr"), F.col("calendar.cal_day").between(F.add_months(F.col("transformed_gr.t_beg_dt"), 1), F.lit(p_end_dt)), how="inner")\
            .select(F.col("return_periods.per_nm"), F.col("calendar.cal_day"), F.col("gr.fund_key").alias("fund_key_join"),
                    F.col("index_return.bmk_idx_key").alias("bmk_idx_key_join"),
                    F.col("gross_wealth_index.gr_wlth_idx"), (1 + F.col("index_return.idx_ret")).alias("idx_rtn_1"), F.col("index_return.idx_ret"),
                    (F.col("gross_wealth_index.gr_wlth_idx")/(1 + F.col("index_return.idx_ret"))).alias("excess_ret"),
                    F.log(F.col("gross_wealth_index.gr_wlth_idx")/(1 + F.col("index_return.idx_ret"))).alias("Log_ER"))\
            .where((F.col("fund.fund_compst_key") == F.col("index_return.fund_compst_key")) &
                   (F.col("return_periods.per_nm") == "1MO") &
                   (F.col("gr.fund_key") == F.col("transformed_gr.fund_key"))&
                   (F.col("index_return.bmk_idx_key") == F.col("transformed_gr.bmk_idx_key")))

        df_calc_inf_ratio = df_calc_inf_ratio_calc\
            .groupBy("fund_key_join", "bmk_idx_key_join", "cal_day")\
            .agg(F.sum(F.log(F.abs(F.col("excess_ret")))).alias("t_retval_t"),
                 F.coalesce(F.stddev_pop("Log_ER"), F.lit(0)).alias("t_std_dev"))

        df_calc_inf_ratio = df_calc_inf_ratio.withColumn("t_retval_t", F.exp(F.col("t_retval_t")))

        df_transformed = df_transformed\
            .join(df_calc_inf_ratio.alias("calc_inf_ratio"),
                  [F.col("calc_inf_ratio.fund_key_join") == F.col("tbl_fund.fund_key"),
                  F.col("calc_inf_ratio.bmk_idx_key_join") == F.col("tbl_fund.bmk_idx_key")], how="left")\
            .drop("fund_key_join", "bmk_idx_key_join")

        df_transformed = df_transformed\
            .withColumn("t_info_ratio",
                        F.when((F.substring(F.col("tbl_per.per_ut"), 1, 2) == F.lit("YR")) | (F.substring(F.col("tbl_per.per_ut"), 1, 2) == F.lit("IN")),
                               calc_t_inf_ratio(F.col("calc_inf_ratio.t_retval_t"),
                                                F.col('calc_inf_ratio.t_std_dev'),
                                                F.col('t_annl'))
                               ).otherwise(None))

        # compare calender to p_end_dt in order to get p_end_dt day_key
        df_day_key = df_calendar \
            .where(F.col('cal_day') == F.lit(p_end_dt))\
            .select(F.col('day_key'), F.col('cal_day'))

        df_day_key = df_day_key \
            .withColumn('t_day_key', F.col('day_key')) \
            .drop('day_key').collect()

        t_day_key = df_day_key[0]['t_day_key']

        df_transformed = df_transformed \
            .withColumn('t_day_key', F.lit(t_day_key))

        # get Index return
        df_get_index_return = df_index_return.alias("index_return")\
            .join(df_calendar.alias("calendar"), F.col("calendar.day_key") == F.col("index_return.day_key"), how="inner")\
            .join(df_return_periods.alias("return_periods"), F.col("return_periods.per_key") == F.col("index_return.per_key"))\
            .select(F.col("index_return.idx_ret").alias("t_idx_ret"), F.col("index_return.bmk_idx_key").alias("bmk_idx_key_join"),
                    F.col("index_return.fund_compst_key").alias("fund_compst_key_join"), F.col("return_periods.per_key").alias("per_key_join"))\
            .where(F.col("calendar.cal_day") == F.lit(p_end_dt))

        df_transformed = df_transformed\
            .join(df_get_index_return.alias("get_index_return"),
                  [F.col("get_index_return.bmk_idx_key_join") == F.col("tbl_fund.bmk_idx_key"),
                  F.col("get_index_return.fund_compst_key_join") == F.col("tbl_fund.fund_compst_key"),
                  F.col("get_index_return.per_key_join") == F.col("tbl_per.per_key")], how="left")\
            .drop("per_key_join", "bmk_idx_key_join", "fund_compst_key_join")

        df_transformed = df_transformed.filter(~F.isnull(F.col("t_return")))

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


# calc_gr_return
@F.udf(T.DoubleType())
def calc_gr_return(t_retval, p_annl_fctr):
    if t_retval is None or p_annl_fctr is None:
        return None
    return pow(t_retval, (1 / p_annl_fctr)) - 1


# calc_t_inf_ratio
@F.udf(T.DoubleType())
def calc_t_inf_ratio(t_retval, p_annl_fctr,t_std_dev):
    if t_retval is None or p_annl_fctr is None or t_std_dev is None or t_std_dev == 0 or t_std_dev == 0.0 or p_annl_fctr == 0 or p_annl_fctr == 0.0:
        return None
    # Decimal module used to avoid the following error: "OverflowError: (34, 'Numerical result out of range')"
    elif (((pow(D.Decimal(t_retval), (1 / D.Decimal(p_annl_fctr)))) - 1) / (D.Decimal(t_std_dev) * D.Decimal(math.sqrt(12)))) / 100 == 0:
        return None
    return (((pow(D.Decimal(t_retval), (1 / D.Decimal(p_annl_fctr)))) - 1) / (D.Decimal(t_std_dev) * D.Decimal(math.sqrt(12)))) / 100