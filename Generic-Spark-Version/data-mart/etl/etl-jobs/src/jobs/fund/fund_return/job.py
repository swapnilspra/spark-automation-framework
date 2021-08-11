from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
import pyspark.sql.types as T
from common.etl_job import ETLJob # must be imported after spark has been set up
import datetime
import calendar
from dateutil.relativedelta import relativedelta


class Job(ETLJob):
    target_table = "fund_return"
    business_key = ["fund_key", "per_key", "day_key"]
    primary_key = {"fund_ret_key": "int"}
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
        "fund_valuation": {
            "type": "table",
            "source": "fund_valuation"
        }
    }

    joins: List[Dict[str, Any]] = []

    target_mappings:List[Dict[str,Any]] = [
        {"source": F.col("fund_key"), "target": "fund_key"},
        {"source": F.col("per_key"), "target": "per_key"},
        {"source": F.col("t_day_key"), "target": "day_key"},
        {"source": F.col("t_beg_dt"), "target": "strt_per"},
        {"source": F.col("t_return"), "target": "fund_ret"},
        {"source": F.col("t_annl"), "target": "annlzn_fctr"},
        {"source": F.col("t_asof_ret"), "target": "asof_fund_ret"},
        {"source": F.col("ret_per_flg"), "target": "ret_per_flg"},
        {"source": F.lit("Y"), "target": "curr_row_flg"},
        {"source": F.lit("N"), "target": "ret_vrfyn_flg"}
    ]

    def join(self,inputs:Dict[str,pyspark.sql.DataFrameReader]) -> pyspark.sql.DataFrame:

        if not self._args.get("p_end_dt",None):
            # default value is current day
            p_end_dt = common.utils.trunc_date(datetime.datetime.today())
        else:
            p_end_dt = datetime.datetime.strptime(self._args["p_end_dt"],"%m/%d/%Y")

        df_joined = super().join(inputs)

        # Create dataframes from tables
        df_fund = inputs["fund"]
        df_fund_composite = inputs["fund_composite"]
        df_return_period = inputs["return_periods"]
        df_fund_manager = inputs["fund_manager"]

        # ----- Start Populate the dataframe df_joined for the etl pipeline
        #       using  fund_composite, fund, return_periods, fund_manager tables

        df_sba = df_fund_composite\
            .join(df_fund_manager, [F.col("fund_composite.fund_compst_key") == F.col("fund_manager.fund_compst_key")], how="inner")\
            .select(F.col("fund_composite.fund_compst_key"), F.col("fund_composite.compst_nm"), F.col("fund_composite.compst_incpn_dt"),
                    F.col("fund_manager.sba_efftv_strt_dt"), F.col("fund_manager.sba_efftv_end_dt"), F.col("fund_manager.sba_actv_flg"),
                    F.col("fund_composite.compst_stat_cd"), F.col("fund_composite.compst_ord"), F.col("fund_composite.compst_incpn_dt"))

        df_sba = df_sba\
            .where((((F.col("compst_stat_cd") == 'O') | (F.col("compst_stat_cd") == 'S')) | F.lit(p_end_dt).between(
                F.col("sba_efftv_strt_dt"), F.coalesce(F.col("sba_efftv_end_dt"), F.lit(p_end_dt)))) &(F.lit(p_end_dt).between(
                       F.col("sba_efftv_strt_dt"), F.coalesce(F.col("sba_efftv_end_dt"), F.lit(p_end_dt)))) &
                   ((F.col("sba_actv_flg") == 'Y') | (F.lit(p_end_dt).between(
                       F.col("sba_efftv_strt_dt"), F.coalesce(F.col("sba_efftv_end_dt"), F.lit(p_end_dt))))))

        df_sba = df_sba\
            .groupBy("fund_compst_key", "compst_nm", "compst_ord", "compst_incpn_dt")\
            .agg(F.to_timestamp(F.max("sba_efftv_strt_dt")).alias("sba_incpn_dt"))

        df_tbl_fund = df_fund_composite\
            .join(df_fund, F.col("fund.fund_compst_key") == F.col("fund_composite.fund_compst_key"), how="inner")\
            .join(df_sba.alias("sba"), F.col("fund_composite.fund_compst_key") == F.col("sba.fund_compst_key"), how="left")\
            .select(F.col("fund.fund_key"), F.col("fund.perf_incpn_dt"), F.col("fund.fund_incpn_dt"),
                    F.col("fund_composite.fund_compst_key"), F.col("fund_composite.compst_nm"),
                    F.col("sba.sba_incpn_dt"))

        df_tbl_fund = df_tbl_fund\
            .groupBy("fund_key", "fund_compst_key", "perf_incpn_dt", "sba_incpn_dt")\
            .agg(F.max(F.greatest("sba_incpn_dt", "perf_incpn_dt")).alias("sba_incpn_dt_t"))

        df_tbl_fund = df_tbl_fund\
            .select(F.col("fund.fund_key"), F.col("fund_composite.fund_compst_key"), F.col("fund.perf_incpn_dt").alias("fund_incpn_dt"), F.col("sba_incpn_dt_t"))

        df_joined = df_tbl_fund.alias("tbl_fund")
        df_joined = df_joined\
            .withColumn("t_incp", F.floor(F.months_between(F.lit(p_end_dt), F.to_timestamp(F.col("tbl_fund.fund_incpn_dt")))/12))

        df_tbl_per = df_return_period
        df_tbl_per = df_tbl_per.alias("tbl_per")\
            .where(((F.col("tbl_per.daily_msr_flg") == 'Y') | (F.col("tbl_per.mo_end_msr_flg") == 'Y')))

        df_joined = df_joined\
            .join(df_tbl_per, ~( 
                (F.col("tbl_per.per_msr") > F.col("t_incp")) & 
                (F.col("tbl_per.per_ut") == 'YR')
            ), how="inner")

        return df_joined

    def transform(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

        df_transformed = self.calc_transformed(df_joined)
        df_transformed = super().transform(df_transformed)
        return df_transformed

    def calc_transformed(self,df_joined:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        if not self._args.get("p_end_dt",None):
            # default value is current day
            p_end_dt = common.utils.trunc_date(datetime.datetime.today())
        else:
            p_end_dt = datetime.datetime.strptime(self._args["p_end_dt"],"%m/%d/%Y")

        df_transformed = df_joined

        df_transformed = df_transformed\
            .withColumn('ret_per_flg',
                        F.when(F.lit(p_end_dt) == F.last_day(F.lit(p_end_dt)), 'M').otherwise('D'))

        df_transformed = df_transformed \
            .withColumn("t_gen_incpn_dt",
                        F.when(F.col('tbl_per.per_nm') == 'SUBI', F.col("tbl_fund.sba_incpn_dt_t")).otherwise(F.col("tbl_fund.fund_incpn_dt")))

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
            .withColumn("t_beg_dt",  F.to_timestamp(get_begin_dt(F.col("tbl_per.per_ut"), F.col("tbl_per.per_msr"), F.col("t_gen_incpn_dt"),
                                                                 F.lit(p_end_dt), F.to_timestamp(F.lit(t_bus_day_month), "MM/dd/yyyy"),
                                                                 F.to_timestamp(F.lit(t_bus_day_quarter), "MM/dd/yyyy")), "MM/dd/yyyy"))

        df_transformed = df_transformed\
            .withColumn('t_annl_yr', F.when(F.col('tbl_per.per_ut') == 'IN',
                                            F.floor(F.months_between(F.date_trunc('day', F.lit(p_end_dt)), F.col('t_gen_incpn_dt')) / 12)).cast(T.IntegerType()))

        df_transformed = df_transformed\
            .withColumn('t_days_month_between', F.when(F.col('tbl_per.per_ut') == 'IN',
                                                       (F.floor(F.months_between(F.lit(p_end_dt), F.col('t_gen_incpn_dt')) / 12) * 12)).cast(T.IntegerType()))

        df_transformed = df_transformed\
            .withColumn('t_days_add_month', F.when(F.col('tbl_per.per_ut') == 'IN',
                                                   F.date_add(F.expr("add_months(t_gen_incpn_dt,t_days_month_between)"), -1))).drop(F.col('t_days_month_between'))

        df_transformed = df_transformed\
            .withColumn('t_days', F.when(F.col('tbl_per.per_ut') == 'IN',
                                         F.datediff(F.to_date(F.lit(p_end_dt), "MM/dd/yyyy"), F.col('t_days_add_month')))).drop(F.col('t_days_add_month'))

        # Count days in current Year
        df_transformed = df_transformed\
            .withColumn('t_curr_yr_days', F.when(F.col('tbl_per.per_ut') == 'IN',
                                                 F.datediff(F.to_date(F.lit(p_end_dt),  "MM/dd/yyyy"), F.add_months(F.to_date(F.lit(p_end_dt), "MM/dd/yyyy"), -12))))

        df_transformed = df_transformed\
            .withColumn('t_annl', F.when(F.col('tbl_per.per_ut') == 'IN',
                                         (F.col('t_annl_yr') + (F.col('t_days') / F.col('t_curr_yr_days')))).otherwise(F.col('tbl_per.annl_fctr')))

        df_transformed = df_transformed \
            .withColumn('t_annl', F.col("t_annl").cast("double"))

        df_transformed = df_transformed\
            .withColumn('t_annl', F.when(F.col('t_annl') < 1, 1).otherwise(F.col('t_annl')))

        #  calc_Return function:
        #  in order to calculate fund_ret and asof_fund_ret columns we need to join fund valuation to the pipeline

        df_fund_valuation = common.utils.read_table_snapshot(
            table_name="fund_valuation",
            env=self._env,
            spark=self._spark)

        df_fund_valuation = df_fund_valuation\
            .select(F.col("fund_key").alias("fund_key_join"), F.col("day_key").alias("day_key_join"), F.col("wlth_idx"), F.col("asof_wlth_idx"))

        # get the start wealth Index
        df_calendar_begin = df_calendar\
            .select(F.col('cal_day'), F.col('day_key'))

        df_join_begin = df_fund_valuation.alias("fund_valuation")\
            .join(df_calendar_begin.alias("calendar"), F.col("fund_valuation.day_key_join") == F.col("calendar.day_key"), how="inner")\
            .select(F.col('fund_valuation.fund_key_join'), F.col('calendar.cal_day'),
                    F.col('fund_valuation.wlth_idx').alias('fund_t_begin_wi').cast(T.DoubleType()),
                    F.col('fund_valuation.asof_wlth_idx').alias('asof_t_begin_wi').cast(T.DoubleType()))

        df_transformed = df_transformed\
            .join(df_join_begin.alias("join_begin"), [F.col("tbl_fund.fund_key") == F.col("join_begin.fund_key_join"),
                                                      F.col("join_begin.cal_day") == F.col("t_beg_dt")], how="left")\
            .drop('fund_key_join', 'cal_day')

        # get the end wealth Index
        df_calendar_end = df_calendar\
            .select(F.col('cal_day'), F.col('day_key'))

        df_join_end = df_fund_valuation.alias("fund_valuation")\
            .join(df_calendar_end.alias("calendar"), [F.col("fund_valuation.day_key_join") == F.col("calendar.day_key"),
                                                      F.col("calendar.cal_day") == F.lit(p_end_dt)], how="inner")\
            .select(F.col('fund_key_join'), F.col('cal_day'),
                    F.col('fund_valuation.wlth_idx').alias('fund_t_end_wi').cast(T.DoubleType()),
                    F.col('fund_valuation.asof_wlth_idx').alias('asof_t_end_wi').cast(T.DoubleType()))

        df_transformed = df_transformed\
            .join(df_join_end.alias("join_end"), F.col("join_end.fund_key_join") == F.col("tbl_fund.fund_key"), how="left")\
            .drop('fund_key_join', 'cal_day')

        # get t_return using calc_return UDF
        df_transformed = df_transformed\
            .withColumn('t_return', calc_return(F.col('fund_t_end_wi'), F.col('fund_t_begin_wi'), F.col('t_annl')))

        # get t_asof_ret using calc_return UDF
        df_transformed = df_transformed\
            .withColumn('t_asof_ret', calc_return(F.col('asof_t_end_wi'), F.col('asof_t_begin_wi'), F.col('t_annl')))

        df_day_key = df_calendar\
            .where(F.col('cal_day') == F.lit(p_end_dt)).select(F.col('day_key'), F.col('cal_day'))

        df_day_key = df_day_key\
            .withColumn('t_day_key', F.col('day_key'))\
            .drop('day_key').collect()

        t_day_key = df_day_key[0]['t_day_key']

        df_transformed = df_transformed\
            .withColumn('t_day_key', F.lit(t_day_key))

        # filter down only records where t_return is not null
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
                t_date = p_end_dt + relativedelta(days=-1)
            else:
                t_date = p_end_dt + relativedelta(days=-1 * p_measure)
        else:
            t_date = p_end_dt + relativedelta(days=-1 * p_measure)
        if t_date < (p_inc_dt + relativedelta(days=-1)):
            t_date = None
    elif p_unit == 'WK':
        t_date = p_end_dt + relativedelta(days=-7 * p_measure)
        if t_date < (p_inc_dt + relativedelta(days=-1)):
            t_date = None
    elif p_unit == 'MT':
        t_date = p_end_dt.replace(day=1) + relativedelta(days=-1)
        if t_date < p_inc_dt and p_end_dt != (
        datetime.datetime(p_end_dt.year, p_end_dt.month, calendar.monthrange(p_end_dt.year, p_end_dt.month)[-1])):
            # Check if it is a month end return calculation (Date is last date of the month)
            t_date = p_inc_dt + relativedelta(days=-1)
    elif p_unit == 'QT':
        t_date = datetime.datetime(p_end_dt.year, 3 * int((p_end_dt.month - 1) / 3 + 1) - 2, 1).replace(day=1)+ relativedelta(days=-1)
#   added to include any fund that started on first business day and not the first calendar day.
        if t_date < p_inc_dt and p_end_dt != (
        datetime.datetime(p_end_dt.year, p_end_dt.month, calendar.monthrange(p_end_dt.year, p_end_dt.month)[-1])):
            t_date = p_inc_dt + relativedelta(days=-1)
        elif t_bus_day_quarter < p_inc_dt:
            t_date = None
    elif p_unit == 'YT':
        t_date = p_end_dt.replace(day=1, month=1)+ relativedelta(days=-1)
#   If fund inception date is current year, YTD should start from Inception date
#   For Month end return, do not calculate YTD if Inception is in current Year.
        if t_date < p_inc_dt and p_end_dt == (
        datetime.datetime(p_end_dt.year, p_end_dt.month, calendar.monthrange(p_end_dt.year, p_end_dt.month)[-1])):
            t_date = None
        elif t_date < p_inc_dt and p_end_dt != (
        datetime.datetime(p_end_dt.year, p_end_dt.month, calendar.monthrange(p_end_dt.year, p_end_dt.month)[-1])):
            # Check if it is a month end return calculation (Date is last date of the month)
            t_date = p_inc_dt + relativedelta(days=-1)
    elif p_unit == 'IN':
        t_date = p_inc_dt + relativedelta(days=-1)

    return t_date


# calc_return
@F.udf(T.DoubleType())
def calc_return(t_end_wi, t_begin_wi, p_annl_fctr):
    if t_end_wi is None or t_begin_wi is None or p_annl_fctr is None:
        return None
    return pow((t_end_wi / t_begin_wi), (1 / p_annl_fctr)) - 1
