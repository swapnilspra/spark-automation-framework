CREATE OR REPLACE PACKAGE HDM.INITIAL_INVESTMENT_PROJECTION
AS

/******************************************************************************
NAME:    Initial Investment Projection
PURPOSE: Calculates the projected return of an intial investment for a fund
         or it's related benchmark.   

REVISIONS:
 Date        Author           Description
----------  ---------  --------------------------------------------------------
06/17/2010   jcorkery  Initial package creation.

                                              
******************************************************************************/
FUNCTION main(
                p_in_ret_type varchar --flag for whether to get a fund return
                                      --or a benchmark return
              , p_in_start_date date --start date of projection
              , p_in_end_date date   --end date of projection
              , p_in_incp_date date  --inception date of relative fund
              , p_in_bm_idx_key number    --benchmark to project growth for
              , p_in_fnd_key number)  --fund to project growth for
RETURN number;                                                  



FUNCTION get_fund_projection(
                                   p_in_start_date_key number --start date of projection
                                 , p_in_end_date_key number   --end date of projection
                                 , p_in_incp_date date  --inception date of relative fund
                                 , p_in_fnd_key number)  --fund to project growth for
RETURN number;                                                  

FUNCTION get_benchmark_projection(
                                   p_in_start_date_key number --start date of projection
                                 , p_in_end_date_key number   --end date of projection
                                 , p_in_incp_date date  --inception date of relative fund
                                 , p_in_bm_idx_key number )   --benchmark to project growth for

RETURN number;                                                  



END INITIAL_INVESTMENT_PROJECTION;

CREATE OR REPLACE PACKAGE BODY HDM.INITIAL_INVESTMENT_PROJECTION
AS

/******************************************************************************
NAME:    Initial Investment Projection
PURPOSE: Calculates the projected return of an intial investment for a fund
         or it's related benchmark. Logic summary:
         1) Get parameters and determine if a fund or benchmark return is to
            be calculated
         2) Call appropriate routine and return projection.     

REVISIONS:
 Date        Author           Description
----------  ---------  --------------------------------------------------------
06/17/2010   jcorkery  Initial package creation.
11/22/2010   jcorkery  Converted to run from HDM.  Main change was to add variables
                       to deal with the fact that integers rathere than dates are
                       now kept in the facts.
                       

******************************************************************************/

FUNCTION main(
                p_in_ret_type varchar --flag for whether to get a fund return
                                      --or a benchmark return
              , p_in_start_date date --start date of projection
              , p_in_end_date date   --end date of projection
              , p_in_incp_date date  --inception date of relative fund
              , p_in_bm_idx_key number    --benchmark to project growth for
              , p_in_fnd_key number)  --fund to project growth for
RETURN number
IS

v_Projection number:= 0;    --return variable for fund growth
v_ProjBase number := 10000; --amount to project
v_WI_Begin number:= 0;      --Wealth index on start date
v_WI_End number := 0;       --Wealth index on end date
v_WI_Test number:=0;        --holder for test of whether fund 

v_StartDateKey number:=0;   --calendar table key for start date
v_EndDateKey number:=0;     --calendar table key for end date

BEGIN

    SELECT C.DAY_KEY INTO v_StartDateKey FROM HDM.CALENDAR C WHERE C.CAL_DAY = p_in_start_date;
    SELECT C.DAY_KEY INTO v_EndDateKey FROM HDM.CALENDAR C  WHERE C.CAL_DAY = p_in_end_date;
    

    IF p_in_ret_type = 'Actual'
    THEN  v_projection := get_fund_projection(
                                               v_StartDateKey
                                             , v_EndDateKey
                                             , p_in_incp_date
                                             , p_in_fnd_key ) ;
    ELSE     
          v_projection := get_benchmark_projection(
                                               v_StartDateKey
                                             , v_EndDateKey 
                                             , p_in_incp_date
                                             , p_in_bm_idx_key ) ;
    END IF;        
        
return v_projection;

END MAIN;
                                                  
FUNCTION get_fund_projection(
                                                    p_in_start_date_key number --start date of projection
                                                  , p_in_end_date_key number   --end date of projection
                                                  , p_in_incp_date date  --inception date of relative fund
                                                  , p_in_fnd_key number)  --fund to project growth for
RETURN number
IS
/******************************************************************************
   NAME: get_fund_projection       
   PURPOSE: Calculate initial investment projection for a fund using the
             wealth index table
   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        06/17/2010  jcorkery         initial creation
******************************************************************************/

v_Projection number:= 0;    --return variable for fund growth
v_ProjBase number := 10000; --amount to project
v_WI_Begin number:= 0;      --Wealth index on start date
v_WI_End number := 0;       --Wealth index on end date
v_WI_Test number:=0;        --holder for test of whether fund 

v_StartDate date;           --calendar table date for start date
v_EndDate date;             --calendar table date for end date


BEGIN

    SELECT C.CAL_DAY INTO v_StartDate FROM HDM.CALENDAR C WHERE C.DAY_KEY = p_in_start_date_key;
    SELECT C.CAL_DAY INTO v_EndDate FROM HDM.CALENDAR C  WHERE C.DAY_KEY = p_in_end_date_key;




        --end wealth index
     SELECT W.ASOF_WLTH_IDX INTO v_WI_End from HDM.FUND_VALUATION W WHERE W.FUND_KEY = p_in_fnd_key and W.DAY_KEY = p_in_end_date_key; 

        --funds incepted before projection start date
    IF  p_in_incp_Date <=      v_StartDate
    THEN
        SELECT W.ASOF_WLTH_IDX INTO v_WI_Begin from HDM.FUND_VALUATION W WHERE W.FUND_KEY = p_in_fnd_key and W.DAY_KEY = p_in_start_date_key; 
        v_projection := ((v_WI_End - v_WI_Begin)/(v_WI_Begin)+1) * v_ProjBase;
    ELSE
        --funds incepted after the projection start date
        v_projection := v_WI_End * v_ProjBase;
    END IF;    
    
RETURN v_projection; 

END get_fund_projection;

FUNCTION get_benchmark_projection(
                                   p_in_start_date_key number --start date of projection
                                 , p_in_end_date_key number   --end date of projection
                                 , p_in_incp_date date  --inception date of relative fund
                                 , p_in_bm_idx_key number)
RETURN number
IS

/******************************************************************************
   NAME: get_benchmark_projection       
   PURPOSE: Calculate initial investment projection for a benchmark using the
             bencmark price table
   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        06/17/2010  jcorkery         initial creation
******************************************************************************/

v_Projection number:= 0;    --return variable for fund growth
v_ProjBase number := 10000; --amount to project
v_Price_Begin number:= 0;   --benchmark price on start date
v_Price_End number := 0;    --benchmark price index on end date
v_CalcStartDate date;       --holder for start date of funds incepted AFTER the
                            --beginning of the projection period.  Inception date - 1 day. 

v_CalcStartDate_key number :=0 ;

v_StartDate date;           --calendar table date for start date
v_EndDate date;             --calendar table date for end date

BEGIN


SELECT C.CAL_DAY INTO v_StartDate FROM HDM.CALENDAR C WHERE C.DAY_KEY = p_in_start_date_key;
SELECT C.CAL_DAY INTO v_EndDate FROM HDM.CALENDAR C  WHERE C.DAY_KEY = p_in_end_date_key;


        --Get begining period price
    IF p_in_incp_date <= v_StartDate
    THEN
        SELECT BP.IDX_VLTN_AMT INTO v_Price_Begin FROM HDM.INDEX_VALUATION BP WHERE BP.DAY_KEY = p_in_start_date_key and BP.BMK_IDX_KEY = p_in_bm_idx_key;

    ELSE  --not enough history for full period calc.  Do inception calc instead
        v_CalcStartDate := p_in_incp_date - 1;
        SELECT C.DAY_KEY INTO v_CalcStartDate_key FROM HDM.CALENDAR C WHERE C.CAL_DAY = v_CalcStartDate;
        
        
        SELECT BP.IDX_VLTN_AMT INTO v_Price_Begin FROM HDM.INDEX_VALUATION BP WHERE BP.DAY_KEY = v_CalcStartDate_key and BP.BMK_IDX_KEY = p_in_bm_idx_key;
    END IF;   

             --Get ending period price
    SELECT BP.IDX_VLTN_AMT INTO v_Price_End FROM HDM.INDEX_VALUATION BP WHERE BP.DAY_KEY = p_in_end_date_key and BP.BMK_IDX_KEY = p_in_bm_idx_key;
     
            --Calculate the projection
     v_projection :=  v_price_end / v_price_begin * v_ProjBase;      

    return v_projection;

END get_benchmark_projection;
                                                  
END INITIAL_INVESTMENT_PROJECTION;

CREATE OR REPLACE PACKAGE HDM.LOAD_DEALER_POSITIONS
AS

/******************************************************************************
NAME:    LOAD_DEALER_POSITIONS
BACKGROUND: 
    Harbor Capital Advisors makes some dealer fee payments based on the dealer’s
    average daily assets under management.  The payment amounts are calculated 
    by DST, and reviewed for accuracy by the Accounting Team at Harbor before 
    they are sent out.  

    Account balances are stored in the data warehouse at the account level.  Due
    to the large volume of records in the Account Position table, the dealer 
    balances cannot be computed on the fly but rather need to be pre-aggregated
    by dealer by day.  Additionally, position records must be manufactured for 
    non-business days because Harbor only receives and loads position records 
    for business days.    

    The dealer / account relationships in effect as of the 3rd business day of 
    the subsequent month should be used.  EG, for July 2010, all July positions 
    would be accumulated using the ownership in effect on August 4th.    
   
    Business Contacts at 08/20/2010 Joanna Neary, Emily Childers.

REVISIONS:
 Date        Author           Description
----------  ---------  --------------------------------------------------------
08/20/2009   jcorkery  Initial package creation.

                                              
******************************************************************************/

PROCEDURE load_dealer_positions_main
    ( p_in_load_month IN varchar2);


PROCEDURE compute_load_month
    ( p_in_load_month IN varchar2);

PROCEDURE compute_third_busn_day;    


PROCEDURE load_business_day_positions;


PROCEDURE fill_non_busn_day_positions;




END LOAD_DEALER_POSITIONS;

CREATE OR REPLACE PACKAGE BODY HDM.LOAD_DEALER_POSITIONS
AS

/******************************************************************************
NAME:    LOAD_DEALER_POSITIONS
PURPOSE: See Package Spec   

REVISIONS:
 Date        Author           Description
----------  ---------  --------------------------------------------------------
08/18/2010   jcorkery  Initial package creation.
08/08/2011   jcorkery  Corrected cartesian product join in fill_non_busn_day_positions
                       to eliminate extra zero-balance records.
                                              
******************************************************************************/

-- Begin Global Declarations
        --Month for which dealer positions are to be loaded
    g_load_month varchar2(6);   
    
        --Day of record for dealer account ownership
    g_third_busn_day_of_month number;

-- End Global Declarations


-------------------------------------------------------------------------------
-- NAME:    load_dealer_positions_main
-- PURPOSE: Controlling procedure for loading the dealer positions table
--
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/19/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------


PROCEDURE load_dealer_positions_main
    ( p_in_load_month IN varchar2)
IS

BEGIN 

    compute_load_month(p_in_load_month);
    compute_third_busn_day;
    load_business_day_positions;    
    fill_non_busn_day_positions;

END load_dealer_positions_main;    


-------------------------------------------------------------------------------
-- NAME:    compute load month
-- PURPOSE: Initialize global variable for month to be processed.  If a specifid
--          month is to be loaded, pass it.  If the most-recently completed
--          month is to be loaded, pass in '999999'.  '999999' will be used in
--          the scheduled tidal job run on the fourth business day of the month.
--          Tidal job will be scheduled on month begin offset 4. 
--          
--
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/19/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------
PROCEDURE compute_load_month
    ( p_in_load_month IN varchar2)
IS

BEGIN
    IF p_in_load_month = '999999'
    THEN
        SELECT
            CAL.MO_CD INTO g_load_month
        FROM
            HDM.calendar CAL
        WHERE
            TRUNC(CAL.CAL_DAY) = ADD_MONTHS(TRUNC(sysdate),-1); 
     ELSE
        g_load_month := p_in_load_month;
     END IF;   
        
END compute_load_month;    


-------------------------------------------------------------------------------
-- NAME:    compute third busn day
-- PURPOSE: Initialize global variable for third business day of current month.
--          This is the day of record for dealer account ownership. 
--          
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/19/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------
PROCEDURE compute_third_busn_day
IS

BEGIN
        SELECT
            ILT.DAY_KEY INTO g_third_busn_day_of_month
        FROM
              (
                SELECT
                      CAL.DAY_KEY
                    , CAL.CAL_DAY
                    , RANK () OVER (PARTITION BY CAL.MO_CD ORDER BY CAL.CAL_DAY ASC) AS DAY_RANK
                    , CAL.MO_CD
                FROM
                    calendar CAL
                WHERE
                    CAL.MO_CD = to_char(add_months(to_date(g_load_month, 'YYYYMM'),1),'YYYYMM' )  
                AND
                    CAL.BUS_DAY_FLG = 'Y'
              )ILT
         WHERE
            ILT.day_rank = 3;
        
END compute_third_busn_day;    


-------------------------------------------------------------------------------
-- NAME:    Load Business Day Positions
-- PURPOSE: Summarize all accounts by dealer, and load their position summary
--          balances by day. 
--          
--
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/19/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------
PROCEDURE load_business_day_positions
IS

   --all date keys to be loaded
CURSOR cur_position_dates
IS
   SELECT
            CAL.DAY_KEY
   FROM   
            HDM.vw_calendar CAL
   WHERE   
            CAL.MO_CD = g_load_month
                                                                                    
  AND       CAL.BUS_DAY_FLG = 'Y' ; --new restriction for efficiency

BEGIN
 FOR rec_position_dates IN cur_position_dates
   LOOP
      --DBMS_OUTPUT.PUT_LINE(rec_position_dates.day_key);
        INSERT INTO HDM.DEALER_POSITION
        (
          DLR_KEY
        , FUND_KEY
        , DAY_KEY
        , MO_CD
        , DEALER_BALANCE
        ) 
        SELECT
           NVL(DLR.DLR_KEY, POS.dlr_key) dlr_key, 
           POS.fund_key,
           POS.DAY_KEY,
           CAL.MO_CD,
           SUM (POS.TOT_BAL_AMT) dealer_balance
        FROM
           HDM.account_position POS
             INNER JOIN HDM.calendar CAL
               ON POS.DAY_KEY = CAL.DAY_KEY
                      --the in-line table below functions simply as a lookup of what dealer owned the account on the 3rd business day of the next month.
                      --the actual balances added are from the previous month and are from the POS alias above
             LEFT JOIN (SELECT PDLR.ACCT_NBR, PDLR.FUND_KEY, PDLR.DLR_KEY FROM HDM.ACCOUNT_POSITION PDLR WHERE PDLR.DAY_KEY = g_third_busn_day_of_month ) DLR
               ON POS.ACCT_NBR = DLR.ACCT_NBR
              and POS.FUND_KEY = DLR.FUND_KEY  
        WHERE
             POS.DAY_KEY = rec_position_dates.day_key
        GROUP
        BY
             POS.fund_key,
             NVL(DLR.DLR_KEY, POS.dlr_key),
             POS.DAY_KEY,
             CAL.MO_CD;

        COMMIT;
        
   END LOOP; 
        
END load_business_day_positions; 


-------------------------------------------------------------------------------
-- NAME:    Fill Non Business Day Positions
-- PURPOSE: Load dealer position balances for non business days.  Use the 
--          most recent PREVIOUS business day balance. 
--          
--
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/20/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------
PROCEDURE fill_non_busn_day_positions
IS

BEGIN

INSERT INTO HDM.DEALER_POSITION 
            (
                DLR_KEY
              , FUND_KEY
              , DAY_KEY
              , MO_CD
              , DEALER_BALANCE
            ) 
SELECT
      JDAYS.dlr_key    
    , JDAYS.fund_key
    , JDAYS.day_key
    , JDAYS.mo_cd
--    , JDAYS.cal_day
--    , JDAYS.BUS_DAY_FLG
--    , JDAYS.day
--    , JDAYS.join_day_key
--    , JDAYS.join_date
--    , JDAYS.join_day
    , NVL( POSO.DEALER_BALANCE ,0) as day_balance
FROM    
(                                                                               --2 start
    --Take inner result set.  If a day was a business day, repeat it, if not, 
    --get the previous business day.  The result of this query is the same as 
    --it's source, with the exception that each calendar day is references to
    --a business day for purposes of substitution a business day TNA for weekends
    --and holidays    
SELECT
      CARP.dlr_key  
    , CARP.fund_key
    , CARP.mo_cd
    , CARP.day_key
    , CARP.cal_day
    , CARP.BUS_DAY_FLG
    , CARP.cal_day_shrt_nm as day
    , (select day_Key from vw_calendar where cal_day = (SELECT max(C2.CAL_DAY ) from vw_calendar C2 WHERE C2.CAL_DAY <= CARP.CAL_DAY and C2.BUS_DAY_FLG = 'Y')) join_day_key
    , (SELECT max(C2.CAL_DAY ) from vw_calendar C2 WHERE C2.CAL_DAY <= CARP.CAL_DAY and C2.BUS_DAY_FLG = 'Y') as join_date
    , (select cal_day_shrt_nm from vw_calendar where cal_day = (SELECT max(C2.CAL_DAY ) from vw_calendar C2 WHERE C2.CAL_DAY <= CARP.CAL_DAY and C2.BUS_DAY_FLG = 'Y')) as join_day 
FROM    
(                                                                               --1 start
    --Get true pos to fund join, but cartesian product for all the calendar
    --dates in the applicable accounting period.  The result of this query
    --is a listing of every dealer / fund / day combination for the entire
    --month.
SELECT
DISTINCT
      POS.DLR_KEY  
    , POS.FUND_KEY
    , CAL.MO_CD
    , CAL.DAY_KEY
    , CAL.CAL_DAY
    , CAL.CAL_DAY_SHRT_NM
    , CAL.BUS_DAY_FLG
FROM
    (
        --dealer / fund combinations from current month 8/8/2011
    SELECT
    DISTINCT
          P.DLR_KEY
        , P.FUND_KEY
    FROM
        HDM.DEALER_POSITION P
            INNER JOIN HDM.CALENDAR C
                ON P.DAY_KEY = C.DAY_KEY
    WHERE
        C.MO_CD =  g_load_month  
    ) POS,
   (
        --days from current month 8/8/2011
    SELECT  
          CA.MO_CD
        , CA.DAY_KEY
        , CA.CAL_DAY
        , CA.CAL_DAY_SHRT_NM
        , CA.BUS_DAY_FLG
    FROM
        HDM.CALENDAR CA
    WHERE
        CA.MO_CD = g_load_month
    ) CAL                                     
)CARP                                                                           --1 end    
)JDAYS                                                                          --2 end
     left
     JOIN HDM.dealer_position POSO
        ON JDAYS.dlr_key = POSO.dlr_key 
       and JDAYS.fund_key = POSO.fund_key
       and JDAYS.join_day_key = POSO.DAY_KEY
WHERE
    JDAYS.BUS_DAY_FLG = 'N';
    
COMMIT;        

END fill_non_busn_day_positions; 




END LOAD_DEALER_POSITIONS;

CREATE OR REPLACE PACKAGE HDM.LOAD_DEALER_POSITIONS_tst
AS

/******************************************************************************
NAME:    LOAD_DEALER_POSITIONS_tst
BACKGROUND: 
    Harbor Capital Advisors makes some dealer fee payments based on the dealer’s
    average daily assets under management.  The payment amounts are calculated 
    by DST, and reviewed for accuracy by the Accounting Team at Harbor before 
    they are sent out.  

    Account balances are stored in the data warehouse at the account level.  Due
    to the large volume of records in the Account Position table, the dealer 
    balances cannot be computed on the fly but rather need to be pre-aggregated
    by dealer by day.  Additionally, position records must be manufactured for 
    non-business days because Harbor only receives and loads position records 
    for business days.    

    The dealer / account relationships in effect as of the 3rd business day of 
    the subsequent month should be used.  EG, for July 2010, all July positions 
    would be accumulated using the ownership in effect on August 4th.    
   
    Business Contacts at 08/20/2010 Joanna Neary, Emily Childers.

REVISIONS:
 Date        Author           Description
----------  ---------  --------------------------------------------------------
08/20/2009   jcorkery  Initial package creation.

                                              
******************************************************************************/

PROCEDURE LOAD_DEALER_POSITIONS_tst_main
    ( p_in_load_month IN varchar2);


PROCEDURE compute_load_month
    ( p_in_load_month IN varchar2);

PROCEDURE compute_third_busn_day;    


PROCEDURE load_business_day_positions;


PROCEDURE fill_non_busn_day_positions;




END LOAD_DEALER_POSITIONS_tst;

CREATE OR REPLACE PACKAGE BODY HDM.LOAD_DEALER_POSITIONS_tst
AS

/******************************************************************************
NAME:    LOAD_DEALER_POSITIONS_tst
PURPOSE: See Package Spec   

REVISIONS:
 Date        Author           Description
----------  ---------  --------------------------------------------------------
08/18/2010   jcorkery  Initial package creation.

                                              
******************************************************************************/

-- Begin Global Declarations
        --Month for which dealer positions are to be loaded
    g_load_month varchar2(6);   
    
        --Day of record for dealer account ownership
    g_third_busn_day_of_month number;

-- End Global Declarations


-------------------------------------------------------------------------------
-- NAME:    LOAD_DEALER_POSITIONS_tst_main
-- PURPOSE: Controlling procedure for loading the dealer positions table
--
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/19/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------


PROCEDURE LOAD_DEALER_POSITIONS_tst_main
    ( p_in_load_month IN varchar2)
IS

BEGIN 

    compute_load_month(p_in_load_month);
    compute_third_busn_day;
    load_business_day_positions;    
    fill_non_busn_day_positions;

END LOAD_DEALER_POSITIONS_tst_main;    


-------------------------------------------------------------------------------
-- NAME:    compute load month
-- PURPOSE: Initialize global variable for month to be processed.  If a specifid
--          month is to be loaded, pass it.  If the most-recently completed
--          month is to be loaded, pass in '999999'.  '999999' will be used in
--          the scheduled tidal job run on the fourth business day of the month.
--          Tidal job will be scheduled on month begin offset 4. 
--          
--
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/19/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------
PROCEDURE compute_load_month
    ( p_in_load_month IN varchar2)
IS

BEGIN
    IF p_in_load_month = '999999'
    THEN
        SELECT
            CAL.MO_CD INTO g_load_month
        FROM
            HDM.calendar CAL
        WHERE
            TRUNC(CAL.CAL_DAY) = ADD_MONTHS(TRUNC(sysdate),-1); 
     ELSE
        g_load_month := p_in_load_month;
     END IF;   
        
END compute_load_month;    


-------------------------------------------------------------------------------
-- NAME:    compute third busn day
-- PURPOSE: Initialize global variable for third business day of current month.
--          This is the day of record for dealer account ownership. 
--          
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/19/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------
PROCEDURE compute_third_busn_day
IS

BEGIN
        SELECT
            ILT.DAY_KEY INTO g_third_busn_day_of_month
        FROM
              (
                SELECT
                      CAL.DAY_KEY
                    , CAL.CAL_DAY
                    , RANK () OVER (PARTITION BY CAL.MO_CD ORDER BY CAL.CAL_DAY ASC) AS DAY_RANK
                    , CAL.MO_CD
                FROM
                    calendar CAL
                WHERE
                    CAL.MO_CD = to_char(add_months(to_date(g_load_month, 'YYYYMM'),1),'YYYYMM' )  
                AND
                    CAL.BUS_DAY_FLG = 'Y'
              )ILT
         WHERE
            ILT.day_rank = 3;
        
END compute_third_busn_day;    


-------------------------------------------------------------------------------
-- NAME:    Load Business Day Positions
-- PURPOSE: Summarize all accounts by dealer, and load their position summary
--          balances by day. 
--          
--
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/19/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------
PROCEDURE load_business_day_positions
IS

   --all date keys to be loaded
CURSOR cur_position_dates
IS
   SELECT
            CAL.DAY_KEY
   FROM   
            HDM.vw_calendar CAL
   WHERE   
            CAL.MO_CD = g_load_month
                                                                                    
  AND       CAL.BUS_DAY_FLG = 'Y' ; --new restriction for efficiency

BEGIN
 FOR rec_position_dates IN cur_position_dates
   LOOP
      --DBMS_OUTPUT.PUT_LINE(rec_position_dates.day_key);
        INSERT INTO HDM.DEALER_POSITION_tst
        (
          DLR_KEY
        , FUND_KEY
        , DAY_KEY
        , MO_CD
        , DEALER_BALANCE
        ) 
        SELECT
           NVL(DLR.DLR_KEY, POS.dlr_key) dlr_key, 
           POS.fund_key,
           POS.DAY_KEY,
           CAL.MO_CD,
           SUM (POS.TOT_BAL_AMT) dealer_balance
        FROM
           HDM.account_position POS
             INNER JOIN HDM.calendar CAL
               ON POS.DAY_KEY = CAL.DAY_KEY
                      --the in-line table below functions simply as a lookup of what dealer owned the account on the 3rd business day of the next month.
                      --the actual balances added are from the previous month and are from the POS alias above
             LEFT JOIN (SELECT PDLR.ACCT_NBR, PDLR.FUND_KEY, PDLR.DLR_KEY FROM HDM.ACCOUNT_POSITION PDLR WHERE PDLR.DAY_KEY = g_third_busn_day_of_month ) DLR
               ON POS.ACCT_NBR = DLR.ACCT_NBR
              and POS.FUND_KEY = DLR.FUND_KEY  
        WHERE
             POS.DAY_KEY = rec_position_dates.day_key
        GROUP
        BY
             POS.fund_key,
             NVL(DLR.DLR_KEY, POS.dlr_key),
             POS.DAY_KEY,
             CAL.MO_CD;

        COMMIT;
        
   END LOOP; 
        
END load_business_day_positions; 


-------------------------------------------------------------------------------
-- NAME:    Fill Non Business Day Positions
-- PURPOSE: Load dealer position balances for non business days.  Use the 
--          most recent PREVIOUS business day balance. 
--          
--
-- REVISIONS:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--08/20/2010   jcorkery  Initial procedure creation

--------------------------------------------------------------------------------
PROCEDURE fill_non_busn_day_positions
IS

BEGIN

INSERT INTO HDM.DEALER_POSITION_tst 
            (
                DLR_KEY
              , FUND_KEY
              , DAY_KEY
              , MO_CD
              , DEALER_BALANCE
            ) 
SELECT
      JDAYS.dlr_key    
    , JDAYS.fund_key
    , JDAYS.day_key
    , JDAYS.mo_cd
--    , JDAYS.cal_day
--    , JDAYS.BUS_DAY_FLG
--    , JDAYS.day
--    , JDAYS.join_day_key
--    , JDAYS.join_date
--    , JDAYS.join_day
    , NVL( POSO.DEALER_BALANCE ,0) as day_balance
FROM    
(                                                                               --2 start
    --Take inner result set.  If a day was a business day, repeat it, if not, 
    --get the previous business day.  The result of this query is the same as 
    --it's source, with the exception that each calendar day is references to
    --a business day for purposes of substitution a business day TNA for weekends
    --and holidays    
SELECT
      CARP.dlr_key  
    , CARP.fund_key
    , CARP.mo_cd
    , CARP.day_key
    , CARP.cal_day
    , CARP.BUS_DAY_FLG
    , CARP.cal_day_shrt_nm as day
    , (select day_Key from vw_calendar where cal_day = (SELECT max(C2.CAL_DAY ) from vw_calendar C2 WHERE C2.CAL_DAY <= CARP.CAL_DAY and C2.BUS_DAY_FLG = 'Y')) join_day_key
    , (SELECT max(C2.CAL_DAY ) from vw_calendar C2 WHERE C2.CAL_DAY <= CARP.CAL_DAY and C2.BUS_DAY_FLG = 'Y') as join_date
    , (select cal_day_shrt_nm from vw_calendar where cal_day = (SELECT max(C2.CAL_DAY ) from vw_calendar C2 WHERE C2.CAL_DAY <= CARP.CAL_DAY and C2.BUS_DAY_FLG = 'Y')) as join_day 
FROM    
(                                                                               --1 start
    --Get true pos to fund join, but cartesian product for all the calendar
    --dates in the applicable accounting period.  The result of this query
    --is a listing of every dealer / fund / day combination for the entire
    --month.
SELECT
DISTINCT
      POS.DLR_KEY  
    , POS.FUND_KEY
    , CAL.MO_CD
    , CAL.DAY_KEY
    , CAL.CAL_DAY
    , CAL.CAL_DAY_SHRT_NM
    , CAL.BUS_DAY_FLG
FROM
    HDM.DEALER_POSITION_tst POS,
    vw_calendar CAL
WHERE
    CAL.MO_CD = g_load_month                                     
)CARP                                                                           --1 end    
)JDAYS                                                                          --2 end
     left
     JOIN HDM.DEALER_POSITION_tst POSO
        ON JDAYS.dlr_key = POSO.dlr_key 
       and JDAYS.fund_key = POSO.fund_key
       and JDAYS.join_day_key = POSO.DAY_KEY
WHERE
    JDAYS.BUS_DAY_FLG = 'N';
    
COMMIT;        

END fill_non_busn_day_positions; 




END LOAD_DEALER_POSITIONS_tst;

CREATE OR REPLACE PACKAGE HDM.PKG_AFTER_TAX AS
    TYPE after_tax_tbl is TABLE OF AFTER_TAX_CALCULATION%ROWTYPE;
    TYPE after_tax_return_tbl IS TABLE OF after_tax_return%ROWTYPE;
    
    PROCEDURE LOAD_AFTER_TAX_WI(P_END_DATE IN date DEFAULT NULL,p_target_flg IN varchar2 default 'Y');
    FUNCTION  GET_AFTER_TAX_CALC(P_END_DATE IN date DEFAULT NULL,p_target_flg IN varchar2 default 'Y') RETURN after_tax_tbl PIPELINED;
    PROCEDURE LOAD_AFTER_TAX_CALC(P_END_DATE IN date DEFAULT NULL,p_target_flg IN varchar2 default 'Y');
    FUNCTION GET_AFTER_TAX_RETURN(P_END_DATE IN DATE DEFAULT NULL) RETURN AFTER_TAX_RETURN_TBL PIPELINED;
    PROCEDURE LOAD_AFTER_TAX_RETURN(P_END_DATE IN DATE DEFAULT NULL);
    
END PKG_AFTER_TAX;

CREATE OR REPLACE PACKAGE body HDM.PKG_AFTER_TAX AS

PROCEDURE LOAD_AFTER_TAX_WI(P_END_DATE IN date DEFAULT NULL,p_target_flg IN varchar2 default 'Y') is
-- This procedure needs to be called immediately after the dividend is loaded into mart tables.  Pass the date of dividend load (reinvestment date) 
-- to the procedure.  If running backdated ,pass  either  the reinvest date or last date of the month.


    CURSOR div(p_date IN DATE) IS
    select cal_day,fund_key,reinvest_day_key,tot_divd,ordn_incm,shrt_term_cap_gains,long_term_cap_gains,forgn_tax_cr,re_invst_pr,div.qdi,div.row_strt_dttm
    from fund_dividend div,calendar cal
    where div.reinvest_day_key= cal.day_key
    and cal_day = p_date;
    
    t_date DATE;
    t_key NUMBER;
BEGIN
    if p_end_date is null then
        begin
            select max(cal_day) into t_date
            from fund_dividend d,calendar c
            where d.reinvest_day_key =c.day_key
            and fund_key in (select fund_key 
                             from fund ,fund_composite cmp 
                             where fund.fund_compst_key=cmp.fund_compst_key 
                             and fund_nbr not in (2015,2215)    -- do not require MM funds;
                             and asset_cls_nm not like (Case When p_target_flg = 'Y' then '1%' else 'Target%' end) ); -- includes or excludes target funds
        end;
    else
        begin
            select max(cal_day) into t_date
            from fund_dividend d,calendar c
            where d.reinvest_day_key =c.day_key
            and fund_key in (select fund_key 
                             from fund ,fund_composite cmp 
                             where fund.fund_compst_key=cmp.fund_compst_key 
                             and fund_nbr not in (2015,2215)    -- do not require MM funds;
                             and asset_cls_nm not like (Case When p_target_flg = 'Y' then '1%' else 'Target%' end) )  -- includes or excludes target funds
            and cal_day <= p_end_date;
        end;
    end if;
    -- GET THE MAX KEY VALUE
    BEGIN
        select nvl(max(atax_wlth_idx_key),0) INTO t_key
        FROM AFTER_TAX_WEALTH_INDEX;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        t_key := 0;
    END;
    
-- Load the first set of data into wealth index table.
    MERGE INTO after_tax_wealth_index TGT
    USING    (
                select rownum + t_key atax_wi_key,fund_key,day_key,reinvest_day_key,cal_day,MO_ENDING_DT,tot_divd,at_oi,at_stcg,at_ltcg,at_ftc,tax_yr, (at_oi + at_stcg + at_ltcg + at_ftc) tot_at_div,cm_nav,pm_nav,re_invst_pr,
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
                              When nvl(qdi,0) > (ordn_incm + shrt_term_cap_gains) then ((forgn_tax_cr - (nvl(qdi,0) - ordn_incm - shrt_term_cap_gains)) * (1-stcg_rt)) + (nvl(qdi,0) - ordn_incm - shrt_term_cap_gains) * (1- ltcg_rt)
                              Else forgn_tax_cr * (1-stcg_rt) end) at_ftc,    
                        --forgn_tax_cr - (forgn_tax_cr * ordn_incm_rt) at_ftc,
                        tax_yr,
                        (select pr_per_shr from fund_valuation a,calendar c where a.day_key=c.day_key and cal_day = cal.mo_ending_dt and fund_key = div.fund_key) cm_nav,
                        (select pr_per_shr from fund_valuation a,calendar c where a.day_key=c.day_key and cal_day = add_months(cal.mo_ending_dt,-1) and fund_key = div.fund_key) pm_nav
                        from fund_dividend div,calendar cal,dividend_tax_rate tax
                        where div.reinvest_day_key= cal.day_key
                        and cal.yr_nbr=tax.tax_yr
                        and cal_day = t_date
                        --and fund_key = (select fund_key from fund where fund_nbr=2011)
                     )      
            ) SRC
    ON (TGT.day_key = SRC.day_key AND TGT.fund_key=SRC.fund_key)
    WHEN NOT MATCHED THEN
        INSERT ( ATAX_WLTH_IDX_KEY,
                 day_key,
                 reinv_day_key,
                 fund_key,
                 atax_ordn_incm,
                 atax_ltcg,
                 atax_stcg,
                 atax_forgn_tax_cr,
                 atax_tot_divd,
                 prir_mo_pr,
                 curr_mo_pr,
                 reinv_pr,
                 btax_ret,
                 atax_ret,
                 atax_reinv_per_shr,
                 curr_row_flg,
                 row_strt_dttm)
        VALUES (SRC.atax_wi_key,
                src.day_key,
                src.reinvest_day_key,
                src.fund_key,
                src.at_oi,
                src.at_ltcg,
                src.at_stcg,
                src.at_ftc,
                src.tot_at_div,
                src.pm_nav,
                src.cm_nav,
                src.re_invst_pr,
                src.bt_ret,
                src.at_ret,
                src.at_share_reinv,
                'Y',
                sysdate
        )
    WHEN MATCHED THEN 
        UPDATE SET
                 tgt.atax_ordn_incm = src.at_oi,
                 tgt.atax_ltcg = src.at_ltcg,
                 tgt.atax_stcg = src.at_stcg,
                 tgt.atax_forgn_tax_cr = src.at_ftc,
                 tgt.atax_tot_divd = src.tot_at_div,
                 tgt.prir_mo_pr = src.pm_nav,
                 tgt.curr_mo_pr = src.cm_nav,
                 tgt.reinv_pr = src.re_invst_pr,
                 tgt.btax_ret = src.bt_ret,
                 tgt.atax_ret = src.at_ret,
                 tgt.atax_reinv_per_shr = src.at_share_reinv,
                 tgt.row_strt_dttm    = sysdate;    
                 
    COMMIT;
end  LOAD_AFTER_TAX_WI;

FUNCTION GET_AFTER_TAX_CALC(P_END_DATE IN date DEFAULT NULL,p_target_flg IN varchar2 default 'Y') RETURN after_tax_tbl PIPELINED is
    CURSOR PERIODS(p_years IN NUMBER) IS
        SELECT PER_KEY,PER_NM,ANNL_FCTR,PER_SORT_ORD
        FROM RETURN_PERIODS
        where per_nm in ('YR1','YR5','YR10','INCP')
        and annl_fctr <= p_years
        ORDER BY PER_SORT_ORD;
        
    CURSOR FUNDS(P_DATE in DATE) IS
        SELECT ATAX.FUND_KEY,perf_incpn_dt FUND_INCPN_DT,fund_sort_ord
        FROM AFTER_TAX_WEALTH_INDEX ATAX,FUND ,CALENDAR CAL
        WHERE ATAX.FUND_KEY=FUND.FUND_KEY
        AND ATAX.DAY_KEY=CAL.DAY_KEY
        AND CAL_DAY = P_DATE
        GROUP BY ATAX.FUND_KEY,perf_INCPN_DT,FUND_SORT_ORD
        ORDER BY FUND_SORT_ORD;
        
    CURSOR DIVIDENDS(p_start_dt IN date,p_end_dt IN DATE,p_fund_key IN NUMBER) IS
        SELECT WI.DAY_KEY,REINV_DAY_KEY,WI.FUND_KEY,ATAX_TOT_DIVD,CURR_MO_PR,ATAX_REINV_PER_SHR,CAL.CAL_DAY
        FROM AFTER_TAX_WEALTH_iNDEX WI,CALENDAR CAL,FUND 
        WHERE WI.FUND_KEY=FUND.FUND_KEY
        AND WI.DAY_KEY=CAL.DAY_KEY
        AND CAL_DAY BETWEEN p_start_dt AND p_end_dt
        AND FUND.FUND_KEY = p_fund_key
        ORDER BY CAL_DAY;
        
    t_date DATE;
    t_init_amt NUMBER;
    t_init_date DATE;
    t_incp_flg VARCHAR2(1);
    t_curr_yr_days NUMBER;
    t_annl_yr NUMBER;
    t_days NUMBER;
    t_annl NUMBER;
    t_loop_yr NUMBER;
    
    t_result after_tax_calculation%ROWTYPE;
    TYPE Fund_Table IS TABLE OF FUNDS%ROWTYPE;
    TYPE Per_Table  IS TABLE OF PERIODS%ROWTYPE;
    TYPE Div_table IS TABLE OF DIVIDENDS%ROWTYPE;
    
    tbl_FUND Fund_Table;
    tbl_Period  Per_Table;    
    tbl_div Div_table;
    
    t_shares         NUMBER;
    t_cost_basis     NUMBER;
    t_init_flg         NUMBER;
    t_yr_nbr         NUMBER;
    t_derived_incp    DATE;
    t_last_pr       NUMBER;
    t_first_div_dt   DATE;
    
    
BEGIN
    if p_end_date is null then
        begin
            select last_day(max(cal_day)),max(YR_NBR) into t_date,t_yr_nbr
            from fund_dividend d,calendar c
            where d.reinvest_day_key =c.day_key
            and fund_key in (select fund_key 
                             from fund ,fund_composite cmp 
                             where fund.fund_compst_key=cmp.fund_compst_key 
                             and fund_nbr not in (2015,2215)    -- do not require MM funds;
                             and asset_cls_nm not like (Case When p_target_flg = 'Y' then '1%' else 'Target%' end) ); -- includes or excludes target funds
        end;
    else
        begin
            select last_day(max(cal_day)),max(YR_NBR) into t_date,t_yr_nbr
            from fund_dividend d,calendar c
            where d.reinvest_day_key =c.day_key
            and fund_key in (select fund_key 
                             from fund ,fund_composite cmp 
                             where fund.fund_compst_key=cmp.fund_compst_key 
                             and fund_nbr not in (2015,2215)    -- do not require MM funds;
                             and asset_cls_nm not like (Case When p_target_flg = 'Y' then '1%' else 'Target%' end) )  -- includes or excludes target funds
            and cal_day < p_end_date;
        end;
    end if;

    t_init_amt := 1000;
    
    -- Count days in current Year.
    t_curr_yr_days := t_date - add_months(t_date,-12);    
    
    OPEN funds(p_end_date);
    FETCH funds BULK COLLECT INTO tbl_fund;
    

    FOR t_fund_cntr IN 1..tbl_fund.COUNT LOOP
    
       -- dbms_output.put_line(tbl_fund(t_fund_cntr).fund_key || ' fund count: ' ||tbl_fund.COUNT );
    -- calculate the first date of dividend.
        BEGIN
            SELECT min(cal_day) INTO t_first_div_dt
            from fund_dividend d,calendar c
                where d.reinvest_day_key =c.day_key
                and fund_key = tbl_fund(t_fund_cntr).fund_key
                and cal_day < p_end_date;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            t_first_div_dt := t_derived_incp;
        WHEN OTHERS THEN
            RAISE;
        END;

    -- calculate annualization factor
        t_derived_incp := add_months(last_day(tbl_fund(t_fund_cntr).fund_incpn_dt),-1) ; -- set to the first day of the month.
        t_annl_yr      := floor(months_between(trunc(t_date),t_derived_incp)/12);
        t_days         := trunc(t_date) - (add_months(t_derived_incp,(floor(months_between(t_date,t_derived_incp)/12)* 12 )) - 1);
        t_annl         := t_annl_yr + (t_days/t_curr_yr_days);
        
        if t_annl_yr < 10 then
            t_incp_flg := 'Y';
        else
            t_incp_flg := 'N';
        end if;
        
        OPEN PERIODS(t_annl_yr);
        FETCH periods BULK COLLECT INTO tbl_period;
        FOR t_period_cntr IN 1..tbl_period.COUNT LOOP
            IF tbl_period(t_period_cntr).per_nm = 'INCP' AND t_incp_flg ='Y' then
                t_init_date := t_derived_incp + 1;
                t_loop_yr   := t_annl_yr;
            ELSE
                t_init_date := add_months(t_date,(12 * tbl_period(t_period_cntr).annl_fctr * -1)) + 1;
                t_loop_yr   := tbl_period(t_period_cntr).annl_fctr;
            END IF;
            
            t_cost_basis := t_init_amt;
            t_init_flg   := 0;
           -- dbms_output.put_line(tbl_period(t_period_cntr).per_nm || ' periods: ' || tbl_period.COUNT);
            -- loop through the number of years
            dbms_output.put_line('init dt: ' || t_init_date || ', t_date = ' || t_date || ', fund_key= ' || tbl_fund(t_fund_cntr).fund_key);
            OPEN DIVIDENDS(t_init_date,t_date,tbl_fund(t_fund_cntr).fund_key);
            FETCH DIVIDENDS BULK COLLECT INTO tbl_div;
            FOR t_div_cntr in 1..tbl_div.COUNT LOOP
               -- DBMS_OUTPUT.PUT_LINE('DIVIDEND RECS: ' || tbl_div.COUNT );
                if t_init_flg = 1 then
                    t_cost_basis := t_cost_basis + (tbl_div(t_div_cntr).atax_tot_divd * t_shares);
                    t_shares     := (tbl_div(t_div_cntr).atax_reinv_per_shr * t_shares) + t_shares;
                    
                elsif t_init_flg = 0 then
                    
                    -- if the year calculation happens to be on the same year of inception (YR5 calc and number of years years of fund = 5)
                    if (t_annl_yr = tbl_period(t_period_cntr).annl_fctr OR tbl_period(t_period_cntr).annl_fctr = 0) AND t_first_div_dt > t_init_date then
                        t_shares := 100;
                    else
                        -- get the previous year's price.
                        Begin
                            SELECT curr_mo_pr INTO t_last_pr 
                            from AFTER_TAX_WEALTH_INDEX A,CALENDAR C
                            WHERE A.DAY_KEY=C.DAY_KEY
                            AND A.FUND_KEY = tbl_fund(t_fund_cntr).fund_key
                            AND CAL_DAY = (select max(cal_day) 
                                           from after_tax_wealth_index a1, calendar c1,fund f1
                                           where a1.day_key=c1.day_key
                                           and a1.fund_key=f1.fund_key
                                           and f1.fund_key = tbl_fund(t_fund_cntr).fund_key
                                           and c1.cal_day < tbl_div(t_div_cntr).cal_day); 
/*                                       
                        begin
                            select pr_per_shr into t_last_pr
                            from fund_valuation a, calendar c
                            WHERE A.DAY_KEY=C.DAY_KEY
                            AND A.FUND_KEY = tbl_fund(t_fund_cntr).fund_key
                            AND CAL_DAY =      (select max(cal_day)
                                                from fund_valuation a1, calendar c1, fund f1
                                                   where a1.day_key=c1.day_key
                                                   and a1.fund_key=f1.fund_key
                                                   and f1.fund_key = tbl_fund(t_fund_cntr).fund_key
                                                   and c1.cal_day < tbl_div(t_div_cntr).cal_day); */
                                                   
                            t_shares     := t_init_amt / t_last_pr;
                            
                        exception
                        when NO_DATA_FOUND Then
                            t_shares := NULL;
                        End;
                        
                    end if;
                    t_cost_basis := t_init_amt + (tbl_div(t_div_cntr).atax_tot_divd * t_shares);
                    t_shares     := (tbl_div(t_div_cntr).atax_reinv_per_shr * t_shares) + t_shares;
                    t_init_flg   := 1;
                end if;
                -- Load the return record
                t_result.reinv_day_key := tbl_div(t_div_cntr).reinv_day_key;
                t_result.day_key := tbl_div(t_div_cntr).day_key;
                t_result.tax_yr := t_yr_nbr;
                t_result.fund_key := tbl_div(t_div_cntr).fund_key;
                t_result.per_key := tbl_period(t_period_CNTR).per_key;
                t_result.cb_amt := t_cost_basis;
                t_result.shrs_calc := t_shares;
                PIPE ROW(t_result);
                
            END LOOP; -- END OF DIVIDEND LOOP
            CLOSE DIVIDENDS;
        END LOOP; -- END OF PERIOD LOOP
        CLOSE PERIODS;
    END LOOP;  -- END OF FUND LOOP;
    CLOSE FUNDS;
    RETURN;
exception
when others then
    RAISE;
END GET_AFTER_TAX_CALC;   

PROCEDURE LOAD_AFTER_TAX_CALC(P_END_DATE IN date DEFAULT NULL,p_target_flg IN varchar2 default 'Y') is
    t_last_key NUMBER;
BEGIN
    
    BEGIN
        SELECT NVL(MAX(ATAX_CALC_KEY),0) INTO t_last_key FROM after_tax_calculation;
    END; 
    
    MERGE INTO AFTER_TAX_CALCULATION TGT
    USING (
            SELECT ROWNUM + t_last_key ATAX_CALC_KEY,TAX_YR,REINV_DAY_KEY,DAY_KEY,FUND_KEY,PER_KEY,CB_AMT,SHRS_CALC
            FROM TABLE(pkg_after_tax.get_after_tax_calc(p_end_date, p_target_flg))
          ) SRC
    ON (tgt.tax_yr=src.tax_yr AND tgt.day_key=src.day_key and tgt.fund_key=src.fund_key and tgt.per_key=src.per_key)
    WHEN NOT MATCHED THEN
    INSERT (
            ATAX_CALC_KEY,
            TAX_YR,
            DAY_KEY,
            REINV_DAY_KEY,
            FUND_KEY,
            PER_KEY,
            CB_AMT,
            SHRS_CALC,
            CURR_ROW_FLG,
            ROW_STRT_DTTM)
    VALUES (
            SRC.ATAX_CALC_KEY,
            SRC.TAX_YR,
            SRC.DAY_KEY,
            SRC.REINV_DAY_KEY,
            SRC.FUND_KEY,
            SRC.PER_KEY,
            SRC.CB_AMT,
            SRC.SHRS_CALC,
            'Y',
            SYSDATE)
    WHEN MATCHED THEN
    UPDATE SET
        TGT.CB_AMT = SRC.CB_AMT,
        TGT.SHRS_CALC = SRC.SHRS_CALC,
        TGT.ROW_STRT_DTTM = SYSDATE,
        CURR_ROW_FLG = 'Y';
        
    COMMIT;
EXCEPTION
WHEN OTHERS THEN
    RAISE;
           
END LOAD_AFTER_TAX_CALC;



FUNCTION GET_AFTER_TAX_RETURN(P_END_DATE IN DATE DEFAULT NULL) RETURN AFTER_TAX_RETURN_TBL PIPELINED IS
    CURSOR DIV(p_date IN DATE,p_year IN NUMBER) IS
    -- 2/3/17 -- PERF_INCPN-DT CHANGE
        SELECT PER.PER_KEY,FUND.FUND_KEY,FUND.PERF_INCPN_DT FUND_INCPN_DT,PER.PER_NM,PER.PER_MSR,CAL.DAY_KEY,CAL_DAY,TAX_YR,ATC.CB_AMT,ATC.SHRS_CALC,WI.CURR_MO_PR,REINV_PR
        FROM AFTER_TAX_CALCULATION ATC, AFTER_TAX_WEALTH_INDEX WI,CALENDAR CAL,FUND FUND,RETURN_PERIODS PER
        WHERE ATC.DAY_KEY = WI.DAY_KEY
        AND ATC.FUND_KEY=WI.FUND_KEY
        AND ATC.DAY_KEY=CAL.DAY_KEY
        AND ATC.FUND_KEY=FUND.FUND_KEY
        AND ATC.PER_KEY=PER.PER_KEY
        AND CAL_DAY=  p_date
        AND TAX_YR=p_year
        ORDER BY FUND.FUND_SORT_ORD,PER.PER_SORT_ORD;
        
   TYPE Div_table IS TABLE OF DIV%ROWTYPE;
   t_div_tbl  div_table;
   t_result after_tax_return%ROWTYPE;
      
   t_year NUMBER;
   t_date DATE;
  -- t_prev_dt DATE; 
  -- t_div_ctr NUMBER; 
   t_annl_yr  NUMBER;
   t_days NUMBER;
   t_annl NUMBER;
   t_init_amt NUMBER;
   t_prev_shrs NUMBER;
   t_stcg_rt NUMBER;
   t_ltcg_rt NUMBER;
   t_prev_cb_amt NUMBER;
   t_curr_yr_days NUMBER;
   
   t_sold_amt NUMBER;
   --t_cb_amt   NUMBER;
   t_gls_amt    NUMBER;
   t_st_tax NUMBER;
   t_lt_tax NUMBER;
   t_net_proc NUMBER;
   t_wo_atax_ret NUMBER;
   t_atax_ret NUMBER;

BEGIN
    
-- GET THE YEAR
    BEGIN
        SELECT yr_nbr INTO t_year
        FROM calendar
        WHERE CAL_DAY = p_end_date;
    END;
    
    if p_end_date IS NULL THEN
        select max(cal_day) INTO t_date
        FROM AFTER_TAX_CALCULATION ATC,CALENDAR CAL
        WHERE ATC.DAY_KEY=CAL.DAY_KEY;
    ELSE
        t_date := p_end_date;
    END IF;
    
    t_init_amt := 1000;
    -- CALCULATE THE DAYS IN CURRENT YEAR.
    t_curr_yr_days := t_date - add_months(t_date,-12); 
    
    OPEN div(t_date,t_year);
    FETCH div BULK COLLECT INTO t_div_tbl;
    
    FOR t_div_ctr IN 1..t_div_tbl.COUNT LOOP
        
        -- get the annualization factor
        IF t_div_tbl(t_div_ctr).per_nm = 'INCP' Then
            t_annl_yr      := floor(months_between(trunc(t_date),t_div_tbl(t_div_ctr).FUND_INCPN_DT)/12);
            t_days         := trunc(t_date) - (add_months(t_div_tbl(t_div_ctr).FUND_INCPN_DT,(floor(months_between(t_date,t_div_tbl(t_div_ctr).FUND_INCPN_DT)/12)* 12 )) - 1);
            t_annl         := t_annl_yr + (t_days/t_curr_yr_days);    
        ELSE
             t_annl :=   t_div_tbl(t_div_ctr).PER_MSR;
        END IF;
        
        t_sold_amt :=   t_div_tbl(t_div_ctr).shrs_calc * t_div_tbl(t_div_ctr).curr_mo_pr;
        t_gls_amt  := t_sold_amt - t_div_tbl(t_div_ctr).cb_amt;
        t_wo_atax_ret := POWER((t_sold_amt / t_init_amt),(1/t_annl)) - 1;
        
        -- get the previous year shares
        BEGIN
            SELECT shrs_calc,cb_amt INTO t_prev_shrs,t_prev_cb_amt
            FROM AFTER_TAX_CALCULATION ATC,CALENDAR cal
            WHERE atc.day_key=cal.day_key
            AND atc.fund_key =   t_div_tbl(t_div_ctr).fund_key
            AND atc.per_key =  t_div_tbl(t_div_ctr).per_key
            AND atc.tax_yr =  t_div_tbl(t_div_ctr).tax_yr
            AND CAL_DAY = add_months(t_date,-12);
            
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            t_prev_shrs := 0;
            t_prev_cb_amt := 0;
            
        WHEN OTHERS THEN
            RAISE;
        END;
        
        -- GET THE TAX RATES
        
        BEGIN
            SELECT ltcg_rt,stcg_rt INTO t_ltcg_rt,t_stcg_rt
            FROM dividend_tax_rate
            WHERE tax_yr = t_year;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE; -- it should be an error.
        WHEN OTHERS THEN
            RAISE;
        END;
        
        if t_prev_shrs > 0 Then
            t_st_tax :=  (((t_div_tbl(t_div_ctr).shrs_calc - t_prev_shrs) * t_div_tbl(t_div_ctr).curr_mo_pr) - (t_div_tbl(t_div_ctr).cb_amt - t_prev_cb_amt)) * t_stcg_rt;
            t_lt_tax := (t_sold_amt - ((t_div_tbl(t_div_ctr).shrs_calc - t_prev_shrs) * t_div_tbl(t_div_ctr).curr_mo_pr) - t_prev_cb_amt) * t_ltcg_rt;
        else
            t_st_tax := t_gls_amt * t_stcg_rt;
            t_lt_tax := 0;
        end if;
        
        
        t_net_proc := t_sold_amt - ( t_st_tax + t_lt_tax);
        t_atax_ret := POWER((t_net_proc / t_init_amt),(1/t_annl)) - 1;
        
        -- fill the pipeline.
        
        t_result.day_key        := t_div_tbl(t_div_ctr).day_key;
        t_result.fund_key       := t_div_tbl(t_div_ctr).fund_key;
        t_result.per_key        := t_div_tbl(t_div_ctr).per_key;
        t_result.sold_amt       := t_sold_amt;
        t_result.cb_amt         := t_div_tbl(t_div_ctr).cb_amt;
        t_result.gls_amt        := t_gls_amt;
        t_result.st_tax_gls     := t_st_tax;
        t_result.lt_tax_gls      := t_lt_tax;
        t_result.tax_amt        := t_st_tax + t_lt_tax;
        t_result.net_prcd_atax  := t_net_proc;
        t_result.atax_ret_wo_sale := t_wo_atax_ret;
        t_result.atax_ret       := t_atax_ret;
        t_result.annlzn_fctr      := t_annl;
        t_result.strt_per := add_months(t_date,(t_annl * -12));
        
        -- send it up
        PIPE ROW(t_result);
        
    END LOOP;
    CLOSE Div;
    
    RETURN;
    
EXCEPTION
WHEN OTHERS THEN
    RAISE;
END  GET_AFTER_TAX_RETURN;  


PROCEDURE LOAD_AFTER_TAX_RETURN(P_END_DATE IN DATE DEFAULT NULL) IS
    t_last_key NUMBER;
    
BEGIN

    -- get the last key value.
    BEGIN
        SELECT nvl(MAX(atax_ret_key),0) INTO t_last_key
        FROM after_tax_return;
    
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        t_last_key := 0;
    END;
    
    MERGE INTO AFTER_TAX_RETURN TGT
    USING
        (SELECT ROWNUM + t_last_key atax_ret_key,DAY_KEY,FUND_KEY,PER_KEY,SOLD_AMT,CB_AMT,GLS_AMT,ST_TAX_GLS,LT_TAX_GLS,
         TAX_AMT,NET_PRCD_ATAX,STRT_PER,ANNLZN_FCTR,ATAX_RET_WO_SALE,ATAX_RET
         FROM TABLE(PKG_AFTER_TAX.GET_AFTER_TAX_RETURN(P_END_DATE))
        ) SRC
    ON (TGT.DAY_KEY=SRC.DAY_KEY AND TGT.FUND_KEY=SRC.FUND_KEY AND TGT.PER_KEY=SRC.PER_KEY)
    WHEN NOT MATCHED THEN
    INSERT ( atax_ret_key,
             day_key,
             fund_key,
             per_key,
             sold_amt,
             cb_amt,
             gls_amt,
             st_tax_gls,
             lt_tax_gls,
             tax_amt,
             net_prcd_atax,
             strt_per,
             annlzn_fctr,
             atax_ret_wo_sale,
             atax_ret,
             curr_row_flg,
             row_strt_dttm)
     VALUES
            ( SRC.atax_ret_key,
              SRC.day_key,
              SRC.fund_key,
              SRC.per_key,
              SRC.sold_amt,
              SRC.cb_amt,
              SRC.gls_amt,
              SRC.st_tax_gls,
              SRC.lt_tax_gls,
              SRC.tax_amt,
              SRC.net_prcd_atax,
              SRC.strt_per,
              SRC.annlzn_fctr,
              SRC.atax_ret_wo_sale,
              SRC.atax_ret,
              'Y',
              sysdate
             )
     WHEN MATCHED THEN
     UPDATE SET
        TGT.sold_amt = SRC.sold_amt,
        TGT.cb_amt = SRC.cb_amt,
        TGT.gls_amt = SRC.gls_amt,
        TGT.st_tax_gls = SRC.st_tax_gls,
        TGT.lt_tax_gls = SRC.lt_tax_gls,
        TGT.tax_amt = SRC.tax_amt,
        TGT.net_prcd_atax = SRC.net_prcd_atax,
        TGT.strt_per = SRC.strt_per,
        TGT.annlzn_fctr = SRC.annlzn_fctr,
        TGT.atax_ret_wo_sale = SRC.atax_ret_wo_sale,
        TGT.atax_ret = SRC.atax_ret,
        TGT.row_strt_dttm = sysdate;
        
        COMMIT;
        
EXCEPTION
WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END  LOAD_AFTER_TAX_RETURN;                        
         

END PKG_AFTER_TAX;

CREATE OR REPLACE PACKAGE HDM.pkg_dealer_analysis AS
    
    procedure fee_analysis(p_end_dt IN DATE) ;
    
    procedure build_monthly_avg_position(p_date IN DATE);
    
    procedure build_asset_summary(p_date IN DATE);
    
    procedure update_fee_eligible;
    procedure build_monthly_avg_draft(p_date IN DATE);
    procedure build_Daily_avg_Account(p_date IN DATE);
    
    -- for fee automation
    -- Step 1
    procedure load_fee_research(p_date IN DATE);
    -- Step 3
    procedure load_avg_nav_check(p_date IN DATE);
    
    -- step 2
  --  procedure load_dealer_changes(p_date IN DATE);
  procedure load_dealer_change_list;
    
END pkg_dealer_analysis;

CREATE OR REPLACE PACKAGE BODY HDM.pkg_dealer_analysis AS


PROCEDURE fee_analysis(p_end_dt IN DATE) 

AS
    t_key number;
    t_prev_dt date;

BEGIN
    SELECT nvl(max(dlr_fee_anlys_key),0) INTO t_key FROM dealer_fee_analysis;
    t_prev_dt := add_months(p_end_dt,-1);

    MERGE INTO dealer_fee_analysis TGT
    USING (
            select t_key + rownum t_row,s1.*,round((tot_compnsn / mo_days_cnt * yr_days_cnt / nullif(curr_eligbl_avg_aum,0)),4) drvvd_rt,
            nvl( tot_compnsn,0) - nvl(pre_tot_compnsn,0) fee_dif,
            nvl(curr_eligbl_avg_aum,0) - nvl(prev_eligbl_avg_aum,0)  asset_diff
            from (
                            select a.dlr_fee_type_key,a.day_key,fund_key,dlr_key,a.ELIGBL_SHR_RT/10000 actl_rt,d.yr_days_cnt,d.mo_days_cnt,
                            sum(a.tot_compnsn) tot_compnsn,
                            sum((select sum(c.tot_compnsn)
                                 from hdm.dealer_payment_summary c,
                                         hdm.calendar b
                                         where c.day_key=b.day_key
                                         and b.cal_day= t_prev_dt
                                         and c.dlr_fee_type_key=a.dlr_fee_type_key 
                                         and c.fund_key=a.fund_key 
                                         and C.TRL_MODE_FLG ='N'
                                         and c.dlr_key=a.dlr_key)) pre_tot_compnsn,
                            avg((select avg(C.ELIGBL_SHR_RT)
                                 from hdm.dealer_payment_summary c,
                                         hdm.calendar b
                                         where c.day_key=b.day_key
                                         and b.cal_day= t_prev_dt
                                         and C.TRL_MODE_FLG ='N'
                                         and c.dlr_fee_type_key=a.dlr_fee_type_key and c.fund_key=a.fund_key and c.dlr_key=a.dlr_key)) pre_actl_rt, 
                            avg((select FEE.DRVVD_RT
                                    from HDM.DEALER_FEE_ANALYSIS fee, hdm.calendar cal
                                    where fee.day_key=cal.day_key
                                    and cal.cal_day = t_prev_dt
                                    and  fee.dlr_fee_type_key=a.dlr_fee_type_key and fee.fund_key=a.fund_key and fee.dlr_key=a.dlr_key)) pre_rt,                                    
                            sum((select sum((Case When f.dlr_fee_type_cd ='12B1' or q.DLR_TPA_FLG='T' Then  tot_eligbl_bal else tot_eligbl_excl_tpa end))
                                    from hdm.monthly_avg_position q
                                    where q.day_key=a.day_key
                                    and q.dlr_key=a.dlr_key
                                    and q.fund_key=a.fund_key )) curr_eligbl_avg_aum,
                            sum((select sum((Case When f.dlr_fee_type_cd ='12B1'  or t.DLR_TPA_FLG='T' Then   tot_eligbl_bal else tot_eligbl_excl_tpa end))
                                    from hdm.monthly_avg_position t,hdm.calendar s
                                    where t.day_key=s.day_key
                                    and t.dlr_key=a.dlr_key
                                    and t.fund_key=a.fund_key
                                    and s.cal_day = t_prev_dt)) prev_eligbl_avg_aum,
                            sum((select sum((Case When f.dlr_fee_type_cd ='12B1' or q.DLR_TPA_FLG='T' Then  tot_bal_amt else tot_amt_excl_tpa end))
                                    from hdm.monthly_avg_position q
                                    where q.day_key=a.day_key
                                    and q.dlr_key=a.dlr_key
                                    and q.fund_key=a.fund_key )) curr_avg_aum,
                            sum((select sum((Case When f.dlr_fee_type_cd ='12B1'  or t.DLR_TPA_FLG='T' Then   tot_bal_amt else tot_amt_excl_tpa end))
                                    from hdm.monthly_avg_position t,hdm.calendar s
                                    where t.day_key=s.day_key
                                    and t.dlr_key=a.dlr_key
                                    and t.fund_key=a.fund_key
                                    and s.cal_day = t_prev_dt)) prev_avg_aum                             
                            from 
                                (select q.day_key,dlr_fee_type_key,dlr_key,fund_key,trl_mode_flg,max(eligbl_shr_rt) eligbl_shr_rt,sum(tot_compnsn) tot_compnsn
                                    from hdm.dealer_payment_summary q,hdm.calendar p
                                    where q.day_key=p.day_key
                                    and cal_day =  p_end_dt
                                    group by q.day_key,dlr_fee_type_key,dlr_key,fund_key,trl_mode_flg
                                    UNION
                                      select  (select day_key from hdm.calendar where cal_day=p_end_dt) day_key,
                                      prev.dlr_fee_type_key,prev.dlr_key,prev.fund_key,prev.trl_mode_flg,null eligbl_shr_rt,null tot_compnsn
                                     from 
                                     ( 
                                          select dlr_fee_type_key,a.day_key,fund_key,dlr_key,tot_compnsn,eligbl_shr_rt,trl_mode_flg
                                          from dealer_payment_summary a,calendar b
                                          where a.day_key=b.day_key
                                          and cal_day = t_prev_dt
                                          and trl_mode_flg='N'
                                    )  prev,
                                     ( 
                                          select dlr_fee_type_key,a.day_key,fund_key,dlr_key,tot_compnsn
                                          from dealer_payment_summary a,calendar b
                                          where a.day_key=b.day_key
                                          and cal_day = p_end_dt
                                          and trl_mode_flg='N'
                                    )   curr
                                    where prev.dlr_fee_type_key=curr.dlr_fee_type_key(+)
                                    and prev.fund_key=curr.fund_key(+)
                                    and prev.dlr_key =curr.dlr_key(+)
                                    and curr.fund_key is null                                    
                                ) a,
                            hdm.calendar d,hdm.dealer_fee_type f
                            where  a.day_key=d.day_key
                            and a.dlr_fee_type_key=f.dlr_fee_type_key
                            and d.cal_day=p_end_dt
                            and a.TRL_MODE_FLG ='N'
                            group by a.dlr_fee_type_key,a.day_key,fund_key,dlr_key,a.ELIGBL_SHR_RT,d.yr_days_cnt,d.mo_days_cnt) s1
          ) SRC
    ON (tgt.dlr_fee_type_key=src.dlr_fee_type_key    AND tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.day_key=src.day_key)
    WHEN NOT MATCHED THEN
        INSERT (
                dlr_fee_anlys_key,
                dlr_key,
                fund_key,
                day_key,
                dlr_fee_type_key,
                actl_rt,
                prir_per_rt,
                actl_fee,
                avg_daily_asset,
                AVG_DAILY_ASSET_PRIR_PER,
                AVG_ELIGBL_DAILY_ASSET,
                AVG_ELIGBL_ASSET_PRIR_PER,
                prir_per_fee,
                fee_diff,
                asset_diff,
                drvvd_rt,
                curr_row_flg,
                row_strt_dttm
            )
        VALUES
            (    src.t_row,
                src.dlr_key,
                src.fund_key,
                src.day_key,
                src.dlr_fee_type_key,
                src.actl_rt,
                src.pre_rt,
                src.tot_compnsn,
                src.curr_avg_aum,
                src.prev_avg_aum,
                src.curr_eligbl_avg_aum,
                src.prev_eligbl_avg_aum,
                src.pre_tot_compnsn,
                nvl(src.tot_compnsn,0) - nvl(src.pre_tot_compnsn,0),
                nvl(src.curr_avg_aum,0) - nvl(src.prev_avg_aum,0),
                src.drvvd_rt,
                'Y',
                sysdate
             )    
    WHEN MATCHED THEN
        UPDATE SET 
                actl_rt                                       = src.actl_rt,
                actl_fee                                     = src.tot_compnsn,
                avg_daily_asset                          = src.curr_avg_aum,
                avg_daily_asset_prir_per             = src.prev_avg_aum,
                AVG_ELIGBL_DAILY_ASSET          = src.curr_eligbl_avg_aum,
                AVG_ELIGBL_ASSET_PRIR_PER    = src.prev_eligbl_avg_aum,
                prir_per_fee                               = src.pre_tot_compnsn,
                fee_diff                                      = nvl(src.tot_compnsn,0) - nvl(src.pre_tot_compnsn,0),
                asset_diff                                   = nvl(src.curr_avg_aum,0) - nvl(src.prev_avg_aum,0),
                prir_per_rt                                 = src.pre_rt,
                drvvd_rt                                     = src.drvvd_rt,
                row_strt_dttm                             = sysdate;                
    COMMIT;
--EXCEPTION
--WHEN OTHERS THEN
 --       RAISE;
END fee_analysis;

    

procedure build_monthly_avg_position(p_date IN DATE) IS
    CURSOR pos(p_date IN Date) is
    select 0 row_key,
           A.DLR_KEY,
           a.fund_key,
           b.day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
           -- exclude assets that have a tpa or tpa  in (707103,707120,709157)
           (CASE WHEN NVL(c.TPA_id,0) = 0 OR (c.tpa_id >= 700000 AND  c.tpa_id <=  799999 and   tpa_id not in (707103,707120,709157) ) THEN Tot_bal_amt ELSE 0 END)  tot_bal_amt_xtpa,   
--            (CASE WHEN NVL(c.TPA_id,0) = 0  then tot_bal_amt else 0 end) tot_bal_amt_xtpa,        
           null fee_eligbl_flg,
           sysdate row_dt,
           'D' dlr_tpa_flg
      from hdm.account_position a, 
           hdm.calendar b, 
           hdm.account c 
     where a.day_key = B.DAY_KEY 
       and A.ACCT_KEY = c.acct_key 
       and mo_cd = to_char(add_months(trunc(p_date),-1),'yyyymm')
       and partition_yr = NVL(to_number(to_char(add_months(trunc(p_date),-1),'yyyy')), UID) 
       and sign(a.tot_shrs) =  1;
       
-- TPA dealers       
    CURSOR pos_tpa(p_date IN Date) is
     select 0 row_key,
           A.DLR_KEY,
           a.fund_key,
           b.day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
--           case when c.tpa_nbr = 0 
--        or substr(to_char(c.tpa_nbr), 1, 1) = 7 then tot_bal_amt 
--                else 0 end tot_bal_amt_xtpa,
           0 tot_bal_amt_xtpa,
           null fee_eligbl_flg,
           sysdate row_dt,
           'T' dlr_tpa_flg
      from hdm.account_position a, 
           hdm.calendar b, 
           hdm.account c 
     where a.day_key = B.DAY_KEY 
       and A.ACCT_KEY = c.acct_key 
       and mo_cd = to_char(add_months(trunc(p_date),-1),'yyyymm')
       and partition_yr = NVL(to_number(to_char(add_months(trunc(p_date),-1),'yyyy')), UID) 
       and c.tpa_id > 0 and (c.tpa_id < 700000 or c.tpa_id >  799999 or c.tpa_id in (707103,707120,709157) )
       and sign(a.tot_shrs) =  1;

       
    CURSOR Hol(p_date IN Date,p_day_key IN Number) is
    select 0 row_key,
           A.DLR_KEY,
           a.fund_key,
           p_day_key day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
           -- exclude assets that have a tpa or tpa  in (707103,707120,709157)
           (CASE WHEN NVL(d.TPA_id,0) = 0 OR (d.tpa_id >= 700000 AND  d.tpa_id <=  799999 and   tpa_id not in (707103,707120,709157) ) THEN Tot_bal_amt ELSE 0 END)  tot_bal_amt_xtpa,  
   --        (CASE WHEN NVL(d.TPA_NBR,0) = 0  then tot_bal_amt else 0 end) tot_bal_amt_xtpa,     
           null fee_eligbl_flg,
           sysdate row_dt,
           'D' dlr_tpa_flg
         from HDM.ACCOUNT_POSITION a,hdm.calendar b,
         hdm.account d
         where a.day_key=b.day_key
         and a.acct_key=d.acct_key
         and b.cal_day = p_date
         and partition_yr= NVL(to_number(to_char(p_date,'yyyy')), UID) 
         and sign(a.tot_shrs) =  1;    
         
    -- TPA dealer for holidays.
    CURSOR Hol_tpa(p_date IN Date,p_day_key IN number) is
    select 0 row_key,
           A.DLR_KEY,
           a.fund_key,
           p_day_key day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
--           case when d.tpa_nbr = 0 
--        or substr(to_char(d.tpa_nbr), 1, 1) = 7 then tot_bal_amt 
--                else 0 end tot_bal_amt_xtpa,
            0  tot_bal_amt_xtpa,
           null fee_eligbl_flg,
           sysdate row_dt,
           'T' dlr_tpa_flg
         from HDM.ACCOUNT_POSITION a,hdm.calendar b,
         hdm.account d
         where a.day_key=b.day_key
         and a.acct_key=d.acct_key
         and b.cal_day = p_date
         and partition_yr=NVL(to_number(to_char(p_date,'yyyy')), UID) 
         and d.tpa_id > 0 and (d.tpa_id < 700000 or d.tpa_id >  799999 or d.tpa_id in (707103,707120,709157))  -- Any tpa in 7 series exception the one in list should be excluded.
         and sign(a.tot_shrs) =  1;    
    
         
    CURSOR holiday(p_date IN Date) is
    select day_key,cal_day,
        (select max(cal_day) 
            from hdm.calendar where bus_day_flg='Y' 
            and cal_day < a.cal_day) last_date
    FROM hdm.calendar a
    WHERE mo_cd = to_char(add_months(trunc(p_date),-1),'yyyymm')
    and bus_day_flg='N'
    and cal_day <> '05-Dec-2018'  -- PResident HW Bush.
    order by cal_day;
    
    TYPE pos_table IS TABLE OF pos%ROWTYPE;
    TYPE Hol_table IS TABLE OF Hol%ROWTYPE;
    
    tbl_pos POS_TABLE;
    tbl_hol HOL_TABLE;
    
    t_row NUMBER;
    t_key NUMBER;
    t_hdm_key NUMBER;
    
    t_prim_key NUMBER;
    
BEGIN
    -- populate the monthly positions;
    --execute immediate 'TRUNCATE TABLE STG.MONTHLY_AVG_POSITION';
    DELETE FROM STG.MONTHLY_AVG_POSITION WHERE 1=1;
    COMMIT;
    dbms_output.put_line('All start :' || to_char(sysdate,'hh:mi:ss AM'));
    
      dbms_output.put_line('1. insert all dealers start :' || to_char(sysdate,'hh:mi:ss AM'));
    OPEN pos(p_date);
    LOOP
        FETCH pos BULK COLLECT INTO tbl_pos LIMIT 10000;
        FORALL t_row IN 1..tbl_pos.COUNT
            INSERT INTO stg.monthly_avg_position --(mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt,tot_amt_excl_tpa,fee_eligbl_flg,dlr_tpa_flg)
            VALUES  tbl_pos(t_row);
            EXIT WHEN pos%NOTFOUND;
            
            COMMIT;
    END LOOP;
    CLOSE pos;
      dbms_output.put_line('1. insert all dealers end :' || to_char(sysdate,'hh:mi:ss AM'));
 
        dbms_output.put_line('2. insert tpa dealers start :' || to_char(sysdate,'hh:mi:ss AM'));
    -- TPA load
    OPEN pos_tpa(p_date);
    LOOP
        FETCH pos_tpa BULK COLLECT INTO tbl_pos LIMIT 10000;
        FORALL t_row IN 1..tbl_pos.COUNT
            INSERT INTO stg.monthly_avg_position -- (mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt,tot_amt_excl_tpa,fee_eligbl_flg,dlr_tpa_flg)
            VALUES  tbl_pos(t_row);
            EXIT WHEN pos_tpa%NOTFOUND;
            
            COMMIT;
    END LOOP;
    CLOSE pos_tpa;    
    
    dbms_output.put_line('2. insert tpa dealers end:' || to_char(sysdate,'hh:mi:ss AM'));
    -- insert holidays.
    
    dbms_output.put_line('3. insert holiday dealers start :' || to_char(sysdate,'hh:mi:ss AM'));
    FOR Holidays IN holiday(p_date) LOOP
       
        OPEN hol(holidays.last_date,holidays.day_key);
        LOOP
            FETCH hol BULK COLLECT INTO tbl_hol LIMIT 10000;
            FORALL t_row IN 1..tbl_hol.COUNT
                INSERT INTO stg.monthly_avg_position -- (mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt,tot_amt_excl_tpa,fee_eligbl_flg,dlr_tpa_flg)
                VALUES tbl_hol(t_row);
            EXIT WHEN hol%NOTFOUND;
            
            COMMIT;
        END LOOP;
        CLOSE hol;    
                
    -- Load TPA holiday records.

        
        OPEN hol_tpa(holidays.last_date,holidays.day_key);
        LOOP
            FETCH hol_tpa BULK COLLECT INTO tbl_hol LIMIT 10000;
            FORALL t_row IN 1..tbl_hol.COUNT
                INSERT INTO stg.monthly_avg_position --(mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt,tot_amt_excl_tpa,fee_eligbl_flg,dlr_tpa_flg)
                VALUES tbl_hol(t_row);
            EXIT WHEN hol_tpa%NOTFOUND;
            
            COMMIT;
        END LOOP;
        CLOSE hol_tpa;
        
    END LOOP;
   
    -- update the 3rd day DEALER
    dbms_output.put_line('5. update dealer key start :' || to_char(sysdate,'hh:mi:ss AM'));
/*    
    MERGE INTO STG.MONTHLY_AVG_POSITION TGT
    USING 
        (    SELECT   rownum t_key,a.day_key,a.fund_key,a.acct_key,a.tot_shrs,a.tot_bal_amt,tot_amt_excl_tpa,dlr_tpa_flg,
             (Case when dlr_tpa_flg = 'T' then tpa.dlr_key else  p1.dlr_key end) dlr_key
            FROM stg.monthly_avg_position a,hdm.account_position p1,
            (select dlr_key ,acct_key
                from hdm.account acct,hdm.dealer tpa1
                where acct.tpa_nbr=tpa1.dlr_id) tpa
            where p1.acct_key=a.acct_key 
            and p1.fund_key=a.fund_key 
            AND a.acct_key=tpa.acct_key(+)
            and p1.partition_yr=NVL(to_number(to_char(add_months(trunc(p_date),0),'yyyy')), UID) 
            and p1.day_key= (select day_key from (
                                            select cal_day,day_key ,bus_day_flg,rank() over (order by cal_day) day_rank
                                            from hdm.calendar
                                            where cal_day between p_date AND p_date + 10
                                            and bus_day_flg='Y') where day_rank=3
                            )       
        ) SRC
    ON (TGT.day_key=src.day_key AND tgt.fund_key=src.fund_key AND  tgt.acct_key=src.acct_key AND tgt.dlr_tpa_flg=src.dlr_tpa_flg)
    WHEN MATCHED THEN
        UPDATE SET
            TGT.MTHLY_AVG_POSN_KEY=src.t_key,
            tgt.dlr_key=src.dlr_key;
*/            
   -- Update all dealers.
   
    MERGE INTO STG.MONTHLY_AVG_POSITION TGT
    USING 
        (    
             SELECT   rownum t_key,p1.fund_key,p1.acct_key,p1.tot_shrs,p1.tot_bal_amt,'D' dlr_tpa_flg,p1.day_key,
                         p1.dlr_key
                        FROM hdm.account_position p1
                        where  p1.partition_yr= NVL(to_number(to_char(trunc(p_date),'yyyy')), UID) 
                        and p1.day_key= (select day_key from (
                                                        select cal_day,day_key ,bus_day_flg,rank() over (order by cal_day) day_rank
                                                        from hdm.calendar
                                                        where cal_day between p_date AND p_date + 10
                                                        and bus_day_flg='Y') where day_rank=3
                                        )                                         
        ) SRC
    ON ( tgt.fund_key=src.fund_key AND  tgt.acct_key=src.acct_key AND tgt.dlr_tpa_flg=src.dlr_tpa_flg)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.dlr_key=src.dlr_key
            WHERE tgt.dlr_tpa_flg = 'D';
                                
    COMMIT;

    
   -- uPDATE ALL TPA DEALERS
   
    MERGE INTO STG.MONTHLY_AVG_POSITION TGT
    USING 
        (    
             SELECT   rownum  t_key,p1.fund_key,p1.acct_key,p1.tot_shrs,p1.tot_bal_amt,'T' dlr_tpa_flg,p1.day_key,
                         tpa.dlr_key
                          FROM hdm.account_position p1,
                           (select tpa1.dlr_key ,acct_key
                                from hdm.account acct,hdm.dealer tpa1
                                where acct.tpa_id=tpa1.dlr_id) tpa
                          where  p1.acct_key=tpa.acct_key
                          and  p1.partition_yr=  NVL(to_number(to_char(trunc(p_date),'yyyy')), UID) 
                          and p1.day_key= (select day_key from (
                                                        select cal_day,day_key ,bus_day_flg,rank() over (order by cal_day) day_rank
                                                        from hdm.calendar
                                                        where cal_day between p_date AND p_date + 10
                                                        and bus_day_flg='Y') where day_rank=3
                                        )                                         
        ) SRC
    ON (tgt.fund_key=src.fund_key AND  tgt.acct_key=src.acct_key AND tgt.dlr_tpa_flg=src.dlr_tpa_flg)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.dlr_key=src.dlr_key
            WHERE tgt.dlr_tpa_flg = 'T';
                                
    COMMIT;    
    
    -- Update the keys
    
    MERGE INTO STG.MONTHLY_AVG_POSITION tgt
    USING
                (
                    SELECT RowNum pk_key,day_key,acct_key,dlr_key,dlr_tpa_flg
                    FROM stg.monthly_avg_position
                ) SRC
    ON (tgt.acct_key=src.acct_key AND tgt.day_key=src.day_key and tgt.dlr_key=src.dlr_key and tgt.dlr_tpa_flg=src.dlr_tpa_flg)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.mthly_avg_posn_key = src.pk_key;
            
    COMMIT;
    
    dbms_output.put_line('5. update dealer key end :' || to_char(sysdate,'hh:mi:ss AM'));
    -- call the update fee_eligible procedure to update the eligibility flags.
    
    dbms_output.put_line('6. update eligibility start :' || to_char(sysdate,'hh:mi:ss AM'));
    hdm.pkg_dealer_analysis.update_fee_eligible;
    
    -- load to the monthly average summary table.
        SELECT NVL(MAX(mthly_avg_posn_key),0) INTO t_hdm_key FROM hdm.monthly_avg_position;
    
    MERGE INTO HDM.MONTHLY_AVG_POSITION tgt
    USING
        (
             select rownum + t_hdm_key rowkey,a.*
             from (
                    select dlr_key,fund_key,dlr_tpa_flg,B.MO_DAYS_CNT,
                    max((select day_key from hdm.calendar where cal_day=B.MO_ENDING_DT)) day_key,
                    sum(tot_shrs) /mo_days_cnt tot_shrs,
                    sum(tot_bal_amt) /mo_days_cnt tot_bal_amt,
                    sum(A.TOT_AMT_EXCL_TPA) /mo_days_cnt tot_amt_excl_tpa,
                    sum((Case When A.FEE_ELIGBL_FLG='Y' Then tot_bal_amt else 0 end)) /mo_days_cnt tot_eligbl_bal,
                    sum((Case When A.FEE_ELIGBL_FLG='Y' Then A.TOT_AMT_EXCL_TPA else 0 end)) /mo_days_cnt tot_eligbl_excl_tpa
                    from stg.monthly_avg_position a,hdm.calendar b
                    where a.day_key=b.day_key
                    group by dlr_key,a.fund_key,dlr_tpa_flg,B.MO_DAYS_CNT
             ) a
        ) SRC
        ON (tgt.dlr_key=src.dlr_key AND tgt.day_key=src.day_key AND tgt.fund_key=src.fund_key)
        WHEN NOT MATCHED THEN
            INSERT (
                            mthly_avg_posn_key,
                            dlr_key,
                            day_key,
                            fund_key,
                            tot_shrs,
                            tot_bal_amt,
                            tot_amt_excl_tpa,
                            tot_eligbl_bal,
                            tot_eligbl_excl_tpa,
                            dlr_tpa_flg,
                            curr_row_flg,
                            row_strt_dttm
                        )
            VALUES
                        (  
                            src.rowkey,
                            src.dlr_key,
                            src.day_key,
                            src.fund_key,
                            src.tot_shrs,
                            src.tot_bal_amt,
                            src.tot_amt_excl_tpa,
                            src.tot_eligbl_bal,
                            src.tot_eligbl_excl_tpa,
                            src.dlr_tpa_flg,
                            'Y',
                            sysdate
                         )
       WHEN MATCHED THEN
            UPDATE SET
                            tgt.tot_shrs =src.tot_shrs,
                            tgt.tot_bal_amt =src.tot_bal_amt,
                            tgt.tot_amt_excl_tpa = src.tot_amt_excl_tpa,
                            tgt.tot_eligbl_bal =src.tot_eligbl_bal,
                            tgt.tot_eligbl_excl_tpa = src.tot_eligbl_excl_tpa,
                            tgt.dlr_tpa_flg = src.dlr_tpa_flg,
                            tgt.row_strt_dttm = sysdate;
                            
       COMMIT;                               
       
  dbms_output.put_line('all end :' || to_char(sysdate,'hh:mi:ss AM'));
--EXCEPTION
--WHEN OTHERS THEN
--       RAISE;    
END build_monthly_avg_position; 

PROCEDURE update_fee_eligible  IS
        CURSOR fee_list IS
            SELECT  dlr_key,inlsn_excln_flg,cum_discnt_nbr,shr_cls_cd
            from HDM.ELIGIBLE_ASSET_LIST
            order by 1,4;
            
BEGIN
       
        FOR c1 IN fee_list LOOP
                CASE 
                WHEN c1.inlsn_excln_flg = 'I' Then
                    
                    -- CONDITION 1.  UPDATE THE FLAG FOR ALL MATCHES.
                    MERGE INTO stg.MONTHLY_AVG_POSITION TGT
                    USING (
                                select mthly_avg_posn_key,dlr_key,fee_eligbl_flg,b.shr_cls_cd,c.cum_discnt_nbr
                                from stg.monthly_avg_position a,hdm.fund b,hdm.account c
                                where a.fund_key=b.fund_key
                                and a.acct_key=c.acct_key
                                and a.dlr_key=c1.dlr_key
                              --  and a.day_key=p_day_key
                                and b.shr_cls_cd = nvl(c1.shr_cls_cd,b.shr_cls_cd)
                                and c.cum_discnt_nbr = c1.cum_discnt_nbr  
                                and fee_eligbl_flg is null 
                     ) SRC
                     ON (TGT.MTHLY_AVG_POSN_KEY = src.MTHLY_AVG_POSN_KEY )
                     WHEN MATCHED THEN
                            UPDATE SET fee_eligbl_flg = 'Y';
                     
                     COMMIT;         
                     
                    -- CONDITION 2:  UPDATE THE FLAG FOR DIFF CUM_DISC IN SAME SHARE CLASS
                    MERGE INTO stg.MONTHLY_AVG_POSITION TGT
                    USING (
                                select mthly_avg_posn_key,dlr_key,fee_eligbl_flg,b.shr_cls_cd,c.cum_discnt_nbr
                                from stg.monthly_avg_position a,hdm.fund b,hdm.account c
                                where a.fund_key=b.fund_key
                                and a.acct_key=c.acct_key
                                and a.dlr_key=c1.dlr_key
                              --  and a.day_key=p_day_key
                                and b.shr_cls_cd = nvl(c1.shr_cls_cd,b.shr_cls_cd)
                                and c.cum_discnt_nbr <> c1.cum_discnt_nbr  
                                and fee_eligbl_flg is null 
                     ) SRC
                     ON (TGT.MTHLY_AVG_POSN_KEY = src.MTHLY_AVG_POSN_KEY )
                     WHEN MATCHED THEN
                            UPDATE SET fee_eligbl_flg = 'N';
                     
                     COMMIT;        
                WHEN c1.inlsn_excln_flg = 'E' Then                     
                    -- CONDITION 1.  UPDATE THE FLAG FOR ALL MATCHES. -- EXCLUSION.
                    MERGE INTO stg.MONTHLY_AVG_POSITION TGT
                    USING (
                                select mthly_avg_posn_key,dlr_key,fee_eligbl_flg,b.shr_cls_cd,c.cum_discnt_nbr
                                from stg.monthly_avg_position a,hdm.fund b,hdm.account c
                                where a.fund_key=b.fund_key
                                and a.acct_key=c.acct_key
                                and a.dlr_key=c1.dlr_key
                             --   and a.day_key=p_day_key
                                and b.shr_cls_cd = nvl(c1.shr_cls_cd,b.shr_cls_cd)
                                and c.cum_discnt_nbr = c1.cum_discnt_nbr  
                                and fee_eligbl_flg is null 
                     ) SRC
                     ON (TGT.MTHLY_AVG_POSN_KEY = src.MTHLY_AVG_POSN_KEY )
                     WHEN MATCHED THEN
                            UPDATE SET fee_eligbl_flg = 'N';
                     
                     COMMIT;   
                END CASE;
        END LOOP;
        
        -- UDATE THE NULL VALUES WITH 'Y'.   THEY SHOULD BE INCLUDED.
        
        UPDATE stg.MONTHLY_AVG_POSITION
        SET fee_eligbl_flg = 'Y'
        WHERE  fee_eligbl_flg IS NULL;
        
        COMMIT;

EXCEPTION
WHEN OTHERS THEN
        RAISE;        

END update_fee_eligible;

procedure build_asset_summary(p_date IN DATE) IS

        t_key NUMBER;
        t_prev_dt DATE;
        
BEGIN
        -- Load the current month end assets.
        
        SELECT NVL(MAX(dlr_fee_sum_key),9) INTO t_key
        FROM HDM.DEALER_ASSET_SUMMARY;
        
        MERGE INTO HDM.DEALER_ASSET_SUMMARY tgt
        USING
            (   
                select ROWNUM + t_key dlr_sum_key,dlr_key,fund_key,day_key,total_aum,total_ta_aum,eligble_aum,ineligble_aum,dlr_tpa_flg,eligble_ta_aum,ineligble_ta_aum
                FROM (
                        select
                        a.dlr_key,a.day_key,a.fund_key,dlr_tpa_flg,sum(A.TOT_BAL_AMT) total_aum,
                        sum (Case When dlr_tpa_flg = 'D' then tot_amt_excl_tpa else tot_bal_amt end)  total_ta_aum,
                        sum((Case When fee_eligbl_flg='Y' then 
                                         (Case When dlr_tpa_flg = 'D' then tot_amt_excl_tpa else tot_bal_amt end)  else 0 end)) eligble_TA_aum,
                        sum((Case When fee_eligbl_flg='N' then 
                                         (Case When dlr_tpa_flg = 'D' then tot_amt_excl_tpa else tot_bal_amt end)  else 0 end)) ineligble_ta_aum,
                        sum((Case When fee_eligbl_flg ='Y' then tot_bal_amt else 0 end)) eligble_aum,
                         sum((Case When fee_eligbl_flg ='N' then tot_bal_amt else 0 end)) ineligble_aum
                        from stg.monthly_avg_position a,hdm.vw_calendar c,hdm.vw_fund b
                        where a.fund_key=b.fund_key
                        and a.day_key=c.day_key
                        and cal_day=p_date
                        group by a.dlr_key,a.day_key,a.fund_key ,dlr_tpa_flg
                )
           ) src
           ON (tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.day_key=src.day_key)
           WHEN NOT MATCHED THEN
           INSERT (
                            dlr_fee_sum_key,
                            dlr_key,
                            fund_key,
                            day_key,
                            CURR_PER_ASSET,
                            CURR_PER_TA_ASSET,
                            CURR_PER_ELIGBL_ASSET,
                            CURR_PER_INELIG_ASSET,
                            CURR_PER_ta_ELIGBL_ASSET,
                            CURR_PER_ta_INELIG_ASSET,
                            dlr_tpa_flg,
                            curr_row_flg,
                            row_strt_dttm)
           VALUES
                        (
                            src.dlr_sum_key,
                            src.dlr_key,
                            src.fund_key,
                            src.day_key,
                            src.total_aum,
                            src.total_ta_aum,
                            src.eligble_aum,
                            src.ineligble_aum,
                            src.eligble_ta_aum,
                            src.ineligble_ta_aum,
                            src.dlr_tpa_flg,
                            'Y',
                            sysdate
                         )
         WHEN MATCHED THEN
                UPDATE SET 
                            tgt.CURR_PER_ASSET = src.total_aum,
                            tgt.curr_per_ta_asset = src.total_ta_aum,
                            tgt.CURR_PER_ELIGBL_ASSET = src.eligble_aum,
                            tgt.CURR_PER_INELIG_ASSET = src.ineligble_aum,
                            tgt.CURR_PER_ta_ELIGBL_ASSET = src.eligble_ta_aum,
                            tgt.CURR_PER_ta_INELIG_ASSET = src.ineligble_ta_aum,                            
                            tgt.dlr_tpa_flg = src.dlr_tpa_flg;
                            
         COMMIT;
        -- Load current Month  Average cashflow
        
    -- recalc the key
        SELECT NVL(MAX(dlr_fee_sum_key),9) INTO t_key
        FROM HDM.DEALER_ASSET_SUMMARY;
                
        MERGE INTO HDM.DEALER_ASSET_SUMMARY tgt
        USING
            (    
                SELECT t_key + rownum  dlr_sum_key,srz.*
                FROM (      
                        select dlr_key,fund_key,me_day_key day_key,
                        sum(sub * avgdays) subs, sum(red * avgdays) reds,sum(net *avgdays ) net
                        from (
                        select a.dlr_key,a.fund_key,mo_ending_dt month_dt,cal_day,sum(sub_amt) sub,sum(redmpn_amt) red ,sum(net_cashflow_amt) net,mo_days_cnt,(MO_DAYS_CNT - to_number(to_char(cal_day,'dd'))+1)   / mo_days_cnt avgdays,
                        max((select day_key from hdm.calendar where cal_day=c.mo_ending_dt)) me_day_key
                        from hdm.sdcm_cashflow a,hdm.calendar c
                        where A.SPR_SHEET_DT_KEY=c.day_key
                        and  c.mo_ending_dt = p_date
                        group by dlr_key,fund_key,cal_day,mo_ending_dt,mo_days_cnt ,(MO_DAYS_CNT - to_number(to_char(cal_day,'dd'))+1)   / mo_days_cnt 
                        ) a
                        group by dlr_key,fund_key  ,me_day_key  
                 ) SRZ
             ) src
        ON (tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.day_key=src.day_key)
        WHEN MATCHED THEN
                UPDATE SET
                        TGT.AVG_SUB_AMT=src.subs,
                        tgt.avg_redmpn_amt=src.reds,
                        tgt.avg_net_amt=src.net
           WHEN NOT MATCHED THEN
           INSERT (
                            dlr_fee_sum_key,
                            dlr_key,
                            fund_key,
                            day_key,
                            avg_sub_amt,
                            avg_redmpn_amt,
                            avg_net_amt,
                            curr_row_flg,
                            row_strt_dttm)
           VALUES
                        (
                            src.dlr_sum_key,
                            src.dlr_key,
                            src.fund_key,
                            src.day_key,
                            src.subs,
                            src.reds,
                            src.net,
                            'Y',
                            sysdate
                         );
                        
                        
        COMMIT;
        
    -- recalc the key
        SELECT NVL(MAX(dlr_fee_sum_key),9) INTO t_key
        FROM HDM.DEALER_ASSET_SUMMARY;
                
  -- Load full Month cashflow
  
        MERGE INTO HDM.DEALER_ASSET_SUMMARY tgt
        USING
            (           
               SELECT t_key + rownum  dlr_sum_key,srz.*
               FROM (
                        select a.dlr_key,a.fund_key,mo_ending_dt month_dt,sum(sub_amt) sub,sum(redmpn_amt) red ,sum(net_cashflow_amt) net,
                        max((select day_key from hdm.calendar where cal_day=c.mo_ending_dt)) day_key
                        from hdm.sdcm_cashflow a,hdm.calendar c
                        where A.SPR_SHEET_DT_KEY=c.day_key
                        and  c.mo_ending_dt = p_date
                        group by  a.dlr_key,a.fund_key,mo_ending_dt
                ) SRZ
             ) src
        ON (tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.day_key=src.day_key)
        WHEN MATCHED THEN
                UPDATE SET
                        TGT.SUB_AMT=src.sub,
                        tgt.redmpn_amt=src.red,
                        tgt.net_cashflow_amt=src.net
           WHEN NOT MATCHED THEN
           INSERT (
                            dlr_fee_sum_key,
                            dlr_key,
                            fund_key,
                            day_key,
                            sub_amt,
                            redmpn_amt,
                            net_cashflow_amt,
                            curr_row_flg,
                            row_strt_dttm)
           VALUES
                        (
                            src.dlr_sum_key,
                            src.dlr_key,
                            src.fund_key,
                            src.day_key,
                            src.sub,
                            src.red,
                            src.net,
                            'Y',
                            sysdate
                         );                   
                        
       COMMIT;  
        
  -- Load current month  end cashflow  for TPA
  -- Not used at this time.
  /*
         MERGE INTO HDM.DEALER_ASSET_SUMMARY tgt
         USING
            (           
                   select d.dlr_key,  a.fund_key, d.day_key, 
                       sum((Case when shr_bal_effct_flg = 'A' then net_amt  else 0 end)) subs, 
                       sum((Case when shr_bal_effct_flg = 'S' then net_amt  else 0 end)) reds ,
                       sum((Case when shr_bal_effct_flg = 'A' then net_amt  else 0 end))  - 
                       sum((Case when shr_bal_effct_flg = 'S' then net_amt  else 0 end))  Net                                  
                  from HDM.FUND_TRANS_HIST a, 
                       hdm.calendar b, 
                       hdm.account c, 
                       (select a.* 
                          from stg.monthly_avg_position a 
                         where A.DLR_TPA_FLG = 'T' 
                           and day_key = (select day_key 
                                            from hdm.calendar 
                                           where cal_day = p_date)) d 
                 where A.EFFTV_DAY_KEY = b.day_key 
                   and a.acct_key = c.acct_key 
                   and a.acct_key = d.acct_key 
                   and mo_cd = (select mo_cd from hdm.calendar where cal_day=p_date)
                 group by d.dlr_key, a.fund_key, d.day_key       
             ) src
        ON (tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.day_key=src.day_key)
        WHEN MATCHED THEN
                UPDATE SET
                        TGT.SUB_AMT=src.subs,
                        tgt.redmpn_amt=src.reds,
                        tgt.net_cashflow_amt=src.net;
                        
        COMMIT; 
*/

    -- recalc the key
        SELECT NVL(MAX(dlr_fee_sum_key),9) INTO t_key
        FROM HDM.DEALER_ASSET_SUMMARY;
        
    -- Get previous month assets
      MERGE INTO HDM.DEALER_ASSET_SUMMARY tgt
        USING
            (   
               SELECT t_key + rownum  dlr_sum_key,srz.*
               FROM (            
                            SELECT  dlr_key,fund_key,
                            (select day_key from hdm.calendar where cal_day=p_date) day_key, 
                            CURR_PER_ASSET,CURR_PER_ELIGBL_ASSET,CURR_PER_INELIG_ASSET ,CURR_PER_AVG_ASSET,CURR_PER_AVG_ELIGBL_ASSET,
                            curr_per_ta_eligbl_asset,curr_per_ta_inelig_asset,curr_avg_ta_eligbl_asset,curr_avg_ta_inelig_asset,curr_per_ta_asset,curr_per_avg_ta_asset           
                            from HDM.DEALER_ASSET_SUMMARY a,hdm.calendar c
                            where  a.day_key=c.day_key
                            and cal_day=add_months(p_date,-1)
                ) SRZ
           ) src
           ON (tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.day_key=src.day_key)
           WHEN MATCHED THEN
                UPDATE SET 
                            TGT.PRIR_PER_ASSET = src.CURR_PER_ASSET,
                            TGT.PRIR_PER_ELIGBL_ASSET = src.CURR_PER_ELIGBL_ASSET,
                            TGT.PRIR_PER_INELIG_ASSET = src.CURR_PER_INELIG_ASSET,
                            TGT.PRIR_PER_AVG_ASSET     = src.curr_per_avg_asset,
                            TGT.PRIR_PER_AVG_ELIGBL_ASSET=src.CURR_PER_AVG_ELIGBL_ASSET,
                            TGT.PRIR_PER_ta_ELIGBL_ASSET=SRC.CURR_PER_ta_ELIGBL_ASSET,
                            TGT.PRIR_PER_ta_INELIG_ASSET=SRC.CURR_PER_ta_INELIG_ASSET,
                            TGT.PRIR_AVG_ta_ELIGBL_ASSET=SRC.CURR_AVG_ta_ELIGBL_ASSET,
                            TGT.PRIR_AVG_ta_INELIG_ASSET=SRC.CURR_AVG_ta_INELIG_ASSET,
                            tgt.prir_per_avg_ta_asset = src.curr_per_avg_ta_asset,
                            tgt.prir_per_ta_asset = src.curr_per_ta_asset
           WHEN NOT MATCHED THEN
           INSERT (
                            dlr_fee_sum_key,
                            dlr_key,
                            fund_key,
                            day_key,
                            prir_per_asset,
                            prir_per_eligbl_asset,
                            prir_per_inelig_asset,
                            prir_per_avg_asset,
                            prir_per_avg_eligbl_asset,
                            prir_per_ta_eligbl_asset,
                            prir_per_ta_inelig_asset,
                            prir_avg_ta_eligbl_asset,
                            prir_avg_ta_inelig_asset,
                            prir_per_avg_ta_asset,
                            prir_per_ta_asset,
                            curr_row_flg,
                            row_strt_dttm)
           VALUES
                        (
                            src.dlr_sum_key,
                            src.dlr_key,
                            src.fund_key,
                            src.day_key,
                            src.CURR_PER_ASSET,
                            src.CURR_PER_ELIGBL_ASSET,
                            src.CURR_PER_INELIG_ASSET,
                            src.curr_per_avg_asset,
                            src.CURR_PER_AVG_ELIGBL_ASSET,
                            src.curr_per_ta_eligbl_asset,
                            src.curr_per_ta_inelig_asset,
                            src.curr_avg_ta_eligbl_asset,
                            src.curr_avg_ta_inelig_asset,
                            src.curr_per_avg_ta_asset,
                            src.curr_per_ta_asset,                            
                            'Y',
                            sysdate
                         ) ;                           
           
           COMMIT;
                          
    -- recalc the key
        SELECT NVL(MAX(dlr_fee_sum_key),9) INTO t_key
        FROM HDM.DEALER_ASSET_SUMMARY;
                   
   -- Get current month average assets
      MERGE INTO HDM.DEALER_ASSET_SUMMARY tgt
        USING
            (   
               SELECT t_key + rownum  dlr_sum_key,srz.*
               FROM (                   
                         select a.dlr_key,a.fund_key,a.day_key,
                          (Case When dlr_tpa_flg = 'D' Then A.TOT_AMT_EXCL_TPA ELSE  tot_bal_amt END) TOT_ta_AMT,
                         (Case When dlr_tpa_flg = 'D' Then A.TOT_ELIGBL_EXCL_TPA else  a.TOT_ELIGBL_BAL end) TOT_ta_ELIGBL ,
                         (Case When dlr_tpa_flg = 'D' Then A.TOT_AMT_EXCL_TPA ELSE  tot_bal_amt END) - (Case When dlr_tpa_flg = 'D' Then A.TOT_ELIGBL_EXCL_TPA else  a.TOT_ELIGBL_BAL end) tot_ta_inelig,
                         tot_bal_amt,
                         tot_eligbl_bal ,
                         tot_bal_amt - tot_eligbl_bal tot_inelig,
                         a.dlr_tpa_flg   
                         from HDM.MONTHLY_AVG_POSITION a,hdm.calendar b
                         where a.day_key=b.day_key
                         and b.cal_day=p_date
               ) SRZ
           ) src
           ON (tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.day_key=src.day_key)
           WHEN MATCHED THEN
                UPDATE SET 
                            TGT.curr_per_avg_asset  = src.tot_bal_amt,
                            TGT.CURR_PER_AVG_ELIGBL_ASSET=src.TOT_ELIGBL_BAL,
                            tgt.curr_per_avg_inelig_asset = src.tot_inelig,
                            tgt.curr_per_avg_ta_asset = src.tot_ta_amt,
                            TGT.CURR_AVG_TA_ELIGBL_ASSET=src.tot_ta_eligbl,
                            TGT.CURR_AVG_TA_INELIG_ASSET=src.tot_ta_inelig,
                            tgt.dlr_tpa_flg = src.dlr_tpa_flg
           WHEN NOT MATCHED THEN
           INSERT (
                            dlr_fee_sum_key,
                            dlr_key,
                            fund_key,
                            day_key,
                            curr_per_avg_asset,
                            CURR_PER_AVG_ELIGBL_ASSET,
                            curr_per_avg_inelig_asset,
                            curr_per_ta_asset,
                            curr_avg_ta_eligbl_asset,
                            curr_avg_ta_inelig_asset,
                            dlr_tpa_flg,
                            curr_row_flg,
                            row_strt_dttm)
           VALUES
                        (
                            src.dlr_sum_key,
                            src.dlr_key,
                            src.fund_key,
                            src.day_key,
                            src.tot_bal_amt,
                            src.TOT_ELIGBL_BAL,
                            src.tot_inelig,
                            src.tot_ta_amt,
                            src.tot_ta_eligbl,
                            src.tot_ta_inelig,
                            src.dlr_tpa_flg,
                            'Y',
                            sysdate
                         ) ;                                 
           
           COMMIT;

              
   END build_asset_summary;          
   
procedure build_monthly_avg_draft(p_date IN DATE) IS
    CURSOR pos(p_date IN Date) is
    select 0 row_key,
           A.DLR_KEY,
           a.fund_key,
           b.day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
           -- exclude assets that have a tpa or tpa  in (707103,707120,709157)
           (CASE WHEN NVL(c.TPA_id,0) = 0 OR (c.tpa_id >= 700000 AND  c.tpa_id <=  799999 and   tpa_id not in (707103,707120,709157) ) THEN Tot_bal_amt ELSE 0 END)  tot_bal_amt_xtpa,   
--            (CASE WHEN NVL(c.TPA_id,0) = 0  then tot_bal_amt else 0 end) tot_bal_amt_xtpa,        
           null fee_eligbl_flg,
           sysdate row_dt,
           'D' dlr_tpa_flg
      from hdm.account_position a, 
           hdm.calendar b, 
           hdm.account c 
     where a.day_key = B.DAY_KEY 
       and A.ACCT_KEY = c.acct_key 
       and mo_cd = to_char(add_months(trunc(p_date),-1),'yyyymm')
       and partition_yr = NVL(to_number(to_char(add_months(trunc(p_date),-1),'yyyy')), UID) 
       and sign(a.tot_shrs) =  1;
       
-- TPA dealers       
    CURSOR pos_tpa(p_date IN Date) is
     select 0 row_key,
           A.DLR_KEY,
           a.fund_key,
           b.day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
--           case when c.tpa_nbr = 0 
--        or substr(to_char(c.tpa_nbr), 1, 1) = 7 then tot_bal_amt 
--                else 0 end tot_bal_amt_xtpa,
           0 tot_bal_amt_xtpa,
           null fee_eligbl_flg,
           sysdate row_dt,
           'T' dlr_tpa_flg
      from hdm.account_position a, 
           hdm.calendar b, 
           hdm.account c 
     where a.day_key = B.DAY_KEY 
       and A.ACCT_KEY = c.acct_key 
       and mo_cd = to_char(add_months(trunc(p_date),-1),'yyyymm')
       and partition_yr = NVL(to_number(to_char(add_months(trunc(p_date),-1),'yyyy')), UID) 
       and c.tpa_id > 0 and (c.tpa_id < 700000 or c.tpa_id >  799999 or c.tpa_id in (707103,707120,709157) )
       and sign(a.tot_shrs) =  1;

       
    CURSOR Hol(p_date IN Date,p_day_key IN Number) is
    select 0 row_key,
           A.DLR_KEY,
           a.fund_key,
           p_day_key day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
           -- exclude assets that have a tpa or tpa  in (707103,707120,709157)
           (CASE WHEN NVL(d.TPA_id,0) = 0 OR (d.tpa_id >= 700000 AND  d.tpa_id <=  799999 and   tpa_id not in (707103,707120,709157) ) THEN Tot_bal_amt ELSE 0 END)  tot_bal_amt_xtpa,  
   --        (CASE WHEN NVL(d.TPA_NBR,0) = 0  then tot_bal_amt else 0 end) tot_bal_amt_xtpa,     
           null fee_eligbl_flg,
           sysdate row_dt,
           'D' dlr_tpa_flg
         from HDM.ACCOUNT_POSITION a,hdm.calendar b,
         hdm.account d
         where a.day_key=b.day_key
         and a.acct_key=d.acct_key
         and b.cal_day = p_date
         and partition_yr= NVL(to_number(to_char(p_date,'yyyy')), UID) 
         and sign(a.tot_shrs) =  1;    
         
    -- TPA dealer for holidays.
    CURSOR Hol_tpa(p_date IN Date,p_day_key IN number) is
    select 0 row_key,
           A.DLR_KEY,
           a.fund_key,
           p_day_key day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
--           case when d.tpa_nbr = 0 
--        or substr(to_char(d.tpa_nbr), 1, 1) = 7 then tot_bal_amt 
--                else 0 end tot_bal_amt_xtpa,
            0  tot_bal_amt_xtpa,
           null fee_eligbl_flg,
           sysdate row_dt,
           'T' dlr_tpa_flg
         from HDM.ACCOUNT_POSITION a,hdm.calendar b,
         hdm.account d
         where a.day_key=b.day_key
         and a.acct_key=d.acct_key
         and b.cal_day = p_date
         and partition_yr=NVL(to_number(to_char(p_date,'yyyy')), UID) 
         and d.tpa_id > 0 and (d.tpa_id < 700000 or d.tpa_id >  799999 or d.tpa_id in (707103,707120,709157))  -- Any tpa in 7 series exception the one in list should be excluded.
         and sign(a.tot_shrs) =  1;    
    
         
    CURSOR holiday(p_date IN Date) is
    select day_key,cal_day,
        (select max(cal_day) 
            from hdm.calendar where bus_day_flg='Y' 
            and cal_day < a.cal_day) last_date
    FROM hdm.calendar a
    WHERE mo_cd = to_char(add_months(trunc(p_date),-1),'yyyymm')
    and bus_day_flg='N'
    and cal_day <> '05-Dec-2018'  -- PResident HW Bush.
    order by cal_day;
    
    TYPE pos_table IS TABLE OF pos%ROWTYPE;
    TYPE Hol_table IS TABLE OF Hol%ROWTYPE;
    
    tbl_pos POS_TABLE;
    tbl_hol HOL_TABLE;
    
    t_row NUMBER;
    t_key NUMBER;
    t_hdm_key NUMBER;
    
    t_prim_key NUMBER;
    
BEGIN
    -- populate the monthly positions;
    execute immediate 'DELETE FROM STG.MONTHLY_AVG_POSITION';
    COMMIT;
    dbms_output.put_line('All start :' || to_char(sysdate,'hh:mi:ss AM'));
    
      dbms_output.put_line('1. insert all dealers start :' || to_char(sysdate,'hh:mi:ss AM'));
    OPEN pos(p_date);
    LOOP
        FETCH pos BULK COLLECT INTO tbl_pos LIMIT 10000;
        FORALL t_row IN 1..tbl_pos.COUNT
            INSERT INTO stg.monthly_avg_position --(mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt,tot_amt_excl_tpa,fee_eligbl_flg,dlr_tpa_flg)
            VALUES  tbl_pos(t_row);
            EXIT WHEN pos%NOTFOUND;
            
            COMMIT;
    END LOOP;
    CLOSE pos;
      dbms_output.put_line('1. insert all dealers end :' || to_char(sysdate,'hh:mi:ss AM'));
 
        dbms_output.put_line('2. insert tpa dealers start :' || to_char(sysdate,'hh:mi:ss AM'));
    -- TPA load
    OPEN pos_tpa(p_date);
    LOOP
        FETCH pos_tpa BULK COLLECT INTO tbl_pos LIMIT 10000;
        FORALL t_row IN 1..tbl_pos.COUNT
            INSERT INTO stg.monthly_avg_position -- (mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt,tot_amt_excl_tpa,fee_eligbl_flg,dlr_tpa_flg)
            VALUES  tbl_pos(t_row);
            EXIT WHEN pos_tpa%NOTFOUND;
            
            COMMIT;
    END LOOP;
    CLOSE pos_tpa;    
    
    dbms_output.put_line('2. insert tpa dealers end:' || to_char(sysdate,'hh:mi:ss AM'));
    -- insert holidays.
    
    dbms_output.put_line('3. insert holiday dealers start :' || to_char(sysdate,'hh:mi:ss AM'));
    FOR Holidays IN holiday(p_date) LOOP
       
        OPEN hol(holidays.last_date,holidays.day_key);
        LOOP
            FETCH hol BULK COLLECT INTO tbl_hol LIMIT 10000;
            FORALL t_row IN 1..tbl_hol.COUNT
                INSERT INTO stg.monthly_avg_position -- (mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt,tot_amt_excl_tpa,fee_eligbl_flg,dlr_tpa_flg)
                VALUES tbl_hol(t_row);
            EXIT WHEN hol%NOTFOUND;
            
            COMMIT;
        END LOOP;
        CLOSE hol;    
                
    -- Load TPA holiday records.

        
        OPEN hol_tpa(holidays.last_date,holidays.day_key);
        LOOP
            FETCH hol_tpa BULK COLLECT INTO tbl_hol LIMIT 10000;
            FORALL t_row IN 1..tbl_hol.COUNT
                INSERT INTO stg.monthly_avg_position --(mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt,tot_amt_excl_tpa,fee_eligbl_flg,dlr_tpa_flg)
                VALUES tbl_hol(t_row);
            EXIT WHEN hol_tpa%NOTFOUND;
            
            COMMIT;
        END LOOP;
        CLOSE hol_tpa;
        
    END LOOP;
   
    MERGE INTO STG.MONTHLY_AVG_POSITION TGT
    USING 
        (    
             SELECT   rownum t_key,p1.fund_key,p1.acct_key,p1.tot_shrs,p1.tot_bal_amt,'D' dlr_tpa_flg,p1.day_key,
                         p1.dlr_key
                        FROM hdm.account_position p1
                        where  p1.partition_yr= NVL(to_number(to_char(add_months(trunc(p_date),-1),'yyyy')), UID) 
                        and p1.day_key= (select day_key from (
                                                        select cal_day,day_key ,bus_day_flg,rank() over (order by cal_day desc) day_rank
                                                        from hdm.calendar
                                                        where cal_day  between trunc(p_date-1,'mm') and p_date-1
                                                        and bus_day_flg='Y') where day_rank=1
                                        )                                         
        ) SRC
    ON ( tgt.fund_key=src.fund_key AND  tgt.acct_key=src.acct_key AND tgt.dlr_tpa_flg=src.dlr_tpa_flg)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.dlr_key=src.dlr_key
            WHERE tgt.dlr_tpa_flg = 'D';
                                
    COMMIT;

    
   -- uPDATE ALL TPA DEALERS
   
    MERGE INTO STG.MONTHLY_AVG_POSITION TGT
    USING 
        (    
             SELECT   rownum  t_key,p1.fund_key,p1.acct_key,p1.tot_shrs,p1.tot_bal_amt,'T' dlr_tpa_flg,p1.day_key,
                         tpa.dlr_key
                          FROM hdm.account_position p1,
                           (select tpa1.dlr_key ,acct_key
                                from hdm.account acct,hdm.dealer tpa1
                                where acct.tpa_id=tpa1.dlr_id) tpa
                          where  p1.acct_key=tpa.acct_key
                          and  p1.partition_yr= NVL(to_number(to_char(add_months(trunc(p_date),-1),'yyyy')), UID) 
                          and p1.day_key= (select day_key from (
                                                       select cal_day,day_key ,bus_day_flg,rank() over (order by cal_day desc) day_rank
                                                        from hdm.calendar
                                                        where cal_day  between trunc(p_date-1,'mm') and p_date-1
                                                        and bus_day_flg='Y') where day_rank=1
                                        )                                         
        ) SRC
    ON (tgt.fund_key=src.fund_key AND  tgt.acct_key=src.acct_key AND tgt.dlr_tpa_flg=src.dlr_tpa_flg)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.dlr_key=src.dlr_key
            WHERE tgt.dlr_tpa_flg = 'T';
                                
    COMMIT;    
    -- Update the keys
    
    MERGE INTO STG.MONTHLY_AVG_POSITION tgt
    USING
                (
                    SELECT RowNum pk_key,day_key,acct_key,dlr_key,dlr_tpa_flg
                    FROM stg.monthly_avg_position
                ) SRC
    ON (tgt.acct_key=src.acct_key AND tgt.day_key=src.day_key and tgt.dlr_key=src.dlr_key and tgt.dlr_tpa_flg=src.dlr_tpa_flg)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.mthly_avg_posn_key = src.pk_key;
            
    COMMIT;
    
    dbms_output.put_line('5. update dealer key end :' || to_char(sysdate,'hh:mi:ss AM'));
    -- call the update fee_eligible procedure to update the eligibility flags.
    
    dbms_output.put_line('6. update eligibility start :' || to_char(sysdate,'hh:mi:ss AM'));
    hdm.pkg_dealer_analysis.update_fee_eligible;
    
    -- load to the monthly average summary table.
        SELECT NVL(MAX(MTHLY_AVG_POSN_DRFT_KEY),0) INTO t_hdm_key FROM hdm.MONTHLY_AVG_POSN_DRAFT;
    
    MERGE INTO HDM.MONTHLY_AVG_POSN_DRAFT tgt
    USING
        (
             select rownum + t_hdm_key rowkey,a.*
             from (
                    select dlr_key,fund_key,dlr_tpa_flg,B.MO_DAYS_CNT,
                    max((select day_key from hdm.calendar where cal_day=B.MO_ENDING_DT)) day_key,
                    sum(tot_shrs) /mo_days_cnt tot_shrs,
                    sum(tot_bal_amt) /mo_days_cnt tot_bal_amt,
                    sum(A.TOT_AMT_EXCL_TPA) /mo_days_cnt tot_amt_excl_tpa,
                    sum((Case When A.FEE_ELIGBL_FLG='Y' Then tot_bal_amt else 0 end)) /mo_days_cnt tot_eligbl_bal,
                    sum((Case When A.FEE_ELIGBL_FLG='Y' Then A.TOT_AMT_EXCL_TPA else 0 end)) /mo_days_cnt tot_eligbl_excl_tpa
                    from stg.monthly_avg_position a,hdm.calendar b
                    where a.day_key=b.day_key
                    group by dlr_key,a.fund_key,dlr_tpa_flg,B.MO_DAYS_CNT
             ) a
        ) SRC
        ON (tgt.dlr_key=src.dlr_key AND tgt.day_key=src.day_key AND tgt.fund_key=src.fund_key)
        WHEN NOT MATCHED THEN
            INSERT (
                            mthly_avg_posn_drft_key,
                            dlr_key,
                            day_key,
                            fund_key,
                            tot_shrs,
                            tot_bal_amt,
                            tot_amt_excl_tpa,
                            tot_eligbl_bal,
                            tot_eligbl_excl_tpa,
                            dlr_tpa_flg,
                            curr_row_flg,
                            row_strt_dttm
                        )
            VALUES
                        (  
                            src.rowkey,
                            src.dlr_key,
                            src.day_key,
                            src.fund_key,
                            src.tot_shrs,
                            src.tot_bal_amt,
                            src.tot_amt_excl_tpa,
                            src.tot_eligbl_bal,
                            src.tot_eligbl_excl_tpa,
                            src.dlr_tpa_flg,
                            'Y',
                            sysdate
                         )
       WHEN MATCHED THEN
            UPDATE SET
                            tgt.tot_shrs =src.tot_shrs,
                            tgt.tot_bal_amt =src.tot_bal_amt,
                            tgt.tot_amt_excl_tpa = src.tot_amt_excl_tpa,
                            tgt.tot_eligbl_bal =src.tot_eligbl_bal,
                            tgt.tot_eligbl_excl_tpa = src.tot_eligbl_excl_tpa,
                            tgt.dlr_tpa_flg = src.dlr_tpa_flg,
                            tgt.row_strt_dttm = sysdate;
                            
       COMMIT;                               
       
  dbms_output.put_line('all end :' || to_char(sysdate,'hh:mi:ss AM'));
--EXCEPTION
--WHEN OTHERS THEN
--       RAISE;    
END build_monthly_avg_draft;                      

procedure build_Daily_avg_Account(p_date IN DATE) IS  --p_date is first day after the month end.
    CURSOR pos(p_in_date IN Date) is
    select 0 row_key,
           d.DLR_KEY,
           a.fund_key,
           b.day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
           sysdate row_dt,
           tpa.dlr_Key tpa_key
      from hdm.account_position a, 
           hdm.calendar b, 
           hdm.account c,
           hdm.Dealer d,
           hdm.dealer tpa   
     where a.day_key = B.DAY_KEY 
       and A.ACCT_KEY = c.acct_key 
	   and c.dlr_id = d.dlr_id
	   and c.TPA_ID = tpa.dlr_id(+)
       and mo_cd = to_char(add_months(trunc(p_in_date),-1),'yyyymm')
       and partition_yr = NVL(to_number(to_char(add_months(trunc(p_in_date),-1),'yyyy')), UID) 
       and sign(a.tot_shrs) =  1;
       
      
    CURSOR Hol(p_in_date IN Date,p_day_key IN Number) is
    select 0 row_key,
           A.DLR_KEY,
           a.fund_key,
           p_day_key day_key, 
           a.acct_key, 
           a.tot_shrs, 
           a.tot_bal_amt, 
           sysdate row_dt,
           tpa.dlr_Key tpa_key
         from HDM.ACCOUNT_POSITION a,hdm.calendar b,
         hdm.account d,hdm.dealer tpa 
         where a.day_key=b.day_key
         and a.acct_key=d.acct_key
         and d.TPA_ID = tpa.dlr_id(+)
         and b.cal_day = p_in_date
         and partition_yr= NVL(to_number(to_char(p_in_date,'yyyy')), UID) 
         and sign(a.tot_shrs) =  1;    
         
    CURSOR holiday(p_in_date IN Date) is
    select day_key,cal_day,
        (select max(cal_day) 
            from hdm.calendar where bus_day_flg='Y' 
            and cal_day < a.cal_day) last_date
    FROM hdm.calendar a
    WHERE mo_cd = to_char(add_months(trunc(p_in_date),-1),'yyyymm')
    and bus_day_flg='N'
    and cal_day <> '05-Dec-2018'  -- PResident HW Bush.
    order by cal_day;
    
    TYPE pos_table IS TABLE OF pos%ROWTYPE;
    TYPE Hol_table IS TABLE OF Hol%ROWTYPE;
    
    tbl_pos POS_TABLE;
    tbl_hol HOL_TABLE;
    
    t_row NUMBER;
    t_hdm_key NUMBER;

    
BEGIN
    -- populate the monthly positions;

   -- DELETE FROM STG.Daily_avg_acct_position WHERE 1=1;
    STG.TRUNCATE_TABLE('STG.Daily_avg_acct_position');
  --  COMMIT;
    dbms_output.put_line('All start :' || to_char(sysdate,'hh:mi:ss AM'));
    
      dbms_output.put_line('1. insert all dealers start :' || to_char(sysdate,'hh:mi:ss AM'));
    OPEN pos(p_date);
    LOOP
        FETCH pos BULK COLLECT INTO tbl_pos LIMIT 10000;
        FORALL t_row IN 1..tbl_pos.COUNT
            INSERT INTO stg.Daily_avg_acct_position --(mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt)
            VALUES  tbl_pos(t_row);
            EXIT WHEN pos%NOTFOUND;
            
            COMMIT;
    END LOOP;
    CLOSE pos;
    
    dbms_output.put_line('3. insert holiday dealers start :' || to_char(sysdate,'hh:mi:ss AM'));
    FOR Holidays IN holiday(p_date) LOOP
       
        OPEN hol(holidays.last_date,holidays.day_key);
        LOOP
            FETCH hol BULK COLLECT INTO tbl_hol LIMIT 10000;
            FORALL t_row IN 1..tbl_hol.COUNT
                INSERT INTO stg.Daily_avg_acct_position -- (mthly_avg_posn_key,dlr_key,fund_key,day_key,acct_key,tot_shrs,tot_bal_amt)
                VALUES tbl_hol(t_row);
            EXIT WHEN hol%NOTFOUND;
            
            COMMIT;
        END LOOP;
        CLOSE hol;    
                
       
    END LOOP;
    
    -- Update the keys
     dbms_output.put_line('5. update dealer key Start :' || to_char(sysdate,'hh:mi:ss AM'));
    
    MERGE INTO STG.Daily_avg_acct_position tgt
    USING
                (
                    SELECT RowNum pk_key,day_key,acct_key,dlr_key,fund_key
                    FROM stg.Daily_avg_acct_position
                ) SRC
    ON (tgt.acct_key=src.acct_key AND tgt.day_key=src.day_key and tgt.dlr_key=src.dlr_key and tgt.fund_key=src.fund_key)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.DAILY_AVG_POSN_KEY = src.pk_key;
            
    COMMIT;
    
    dbms_output.put_line('5. update dealer key end :' || to_char(sysdate,'hh:mi:ss AM'));
    -- Update all dealers.
   
    MERGE INTO STG.Daily_avg_acct_position  TGT
    USING 
        (    
             SELECT   rownum t_key,p1.fund_key,p1.acct_key,p1.tot_shrs,p1.tot_bal_amt,'D' dlr_tpa_flg,p1.day_key,
                         p1.dlr_key
                        FROM hdm.account_position p1
                        where  p1.partition_yr= NVL(to_number(to_char(trunc(p_date),'yyyy')), UID) 
                        and p1.day_key= (select day_key from (
                                                        select cal_day,day_key ,bus_day_flg,rank() over (order by cal_day) day_rank
                                                        from hdm.calendar
                                                        where cal_day between p_date AND p_date + 10
                                                        and bus_day_flg='Y') where day_rank=3
                                        )                                         
        ) SRC
    ON ( tgt.fund_key=src.fund_key AND  tgt.acct_key=src.acct_key)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.dlr_key=src.dlr_key;
                                
    COMMIT;    
    
    
    -- load to the Daily average summary table.
        SELECT NVL(MAX(DAILY_AVG_ACCT_POSN_Key),0) INTO t_hdm_key FROM hdm.Daily_avg_acct_position;
    
    MERGE INTO HDM.Daily_avg_acct_position tgt
    USING
        (
             select rownum + t_hdm_key rowkey,a.*
             from (
                    select dlr_key,tpa_key,fund_key,acct_key,B.MO_DAYS_CNT,
                    max((select day_key from hdm.calendar where cal_day=B.MO_ENDING_DT)) day_key,
                    sum(tot_shrs) /mo_days_cnt  avg_daily_shrs,
                    sum(tot_bal_amt) /mo_days_cnt  avg_daily_bal
                    from stg.Daily_avg_acct_position a,hdm.calendar b
                    where a.day_key=b.day_key
                    group by dlr_key,tpa_key,a.fund_key,acct_key,B.MO_DAYS_CNT
             ) a
        ) SRC
        ON (tgt.dlr_key=src.dlr_key AND tgt.day_key=src.day_key AND tgt.fund_key=src.fund_key AND tgt.acct_key=src.acct_key and tgt.tpa_key=src.tpa_key)
        WHEN NOT MATCHED THEN
            INSERT (
                            DAILY_AVG_ACCT_POSN_Key,
                            dlr_key,
                            tpa_key,
                            day_key,
                            fund_key,
							acct_key,
                            avg_daily_shrs,
                            avg_daily_bal,
                            curr_row_flg,
                            row_strt_dttm
                        )
            VALUES
                        (  
                            src.rowkey,
                            src.dlr_key,
                            src.tpa_key,
                            src.day_key,
                            src.fund_key,
							src.acct_key,
                            src.avg_daily_shrs,
                            src.avg_daily_bal,
                            'Y',
                            sysdate
                         )
       WHEN MATCHED THEN
            UPDATE SET
                            tgt.avg_daily_shrs =src.avg_daily_shrs,
                            tgt.avg_daily_bal =src.avg_daily_bal,
                            tgt.row_strt_dttm = sysdate;
                            
       COMMIT;                               
       
  dbms_output.put_line('all end :' || to_char(sysdate,'hh:mi:ss AM'));
EXCEPTION
WHEN OTHERS THEN
       RAISE;    
END build_Daily_avg_Account; 

-- for fee automation

procedure load_fee_research(p_date IN DATE) IS
-- Step 1:  Transaction Research
    t_key NUMBER;
    t_date  DATE;
    pmt_dt DATE;
    
    -- Input is Payment Date.  this cursor finds all Invoice Dates belongs to that Payment DAte.
    CURSOR Dt(p_Dt IN Date) IS
        SELECT pmt_dt,invc_dt,pmt_day_key
        FROM (
            select pCal.cal_day pmt_dt,iCal.Cal_day invc_dt,pCal.Day_key pmt_day_key
            from dealer_invoice_trial INVC INNER JOIN calendar iCal ON (invc.invc_day_key=iCal.day_key)
            INNER JOIN calendar pCal ON (INVC.PMT_DAY_KEY=pCal.day_key)
            WHERE pCal.cal_day=p_Dt
            group by pCal.cal_day,iCal.Cal_day,pCal.Day_key
            UNION ALL
            select pCal.cal_day,iCal.Cal_day,pCal.Day_key pmt_day_key
            from dealer_PAYMENT_trial INVC INNER JOIN calendar iCal ON (invc.invc_day_key=iCal.day_key)
            INNER JOIN calendar pCal ON (INVC.PMT_DAY_KEY=pCal.day_key)
            WHERE pCal.cal_day=p_Dt
        ) GROUP BY pmt_dt,invc_dt,pmt_Day_key
        ORDER BY invc_dt;
    
BEGIN
        
    pmt_dt := p_date;
    
    -- Start the Cursor to list
    FOR c1 IN Dt(pmt_dt) LOop

        -- Get the max Key value after each cycle
        SELECT nvl(MAX(dlr_fee_rsrh_key),0) INTO t_key
        FROM Dealer_Fee_Research;

        t_date := c1.invc_dt;
        
     -- Step 1:  Identify all Records from Average Account Position and insert into the table:

         MERGE INTO dealer_Fee_research TGT
            USING
            (
                SELECT t_key + rownum FEE_RSRH_KEY, pos.DLR_KEY,pos.DAY_KEY,c1.pmt_day_key pmt_day_key,pos.FUND_KEY,pos.ACCT_KEY,schd.DLR_FEE_TYPE_KEY,avg_daily_bal,fee_rt,avg_daily_shrs,
                        ROUND((avg_daily_bal * (fee_rt / 10000)) * ( MO_DAYS_CNT / yr_days_cnt),2) calc_fee
                         FROM HDM.DAILY_AVG_ACCT_POSITION pos INNER join 
                        (select dlr_key,fund_key,dlr_fee_type_key, avg(fee_rt) fee_rt from dealer_fee_schedule group by  dlr_key,fund_key,dlr_fee_type_key) schd on (pos.dlr_key=schd.dlr_key and pos.fund_key=schd.fund_key)
                        INNER JOIN calendar cal on (pos.day_key=cal.day_key)
                        INNER JOIN dealer_fee_type ft ON (schd.DLR_FEE_TYPE_KEY=ft.dlr_fee_type_key)
                        WHERE cal.cal_day = t_date
            ) SRC
            ON (TGT.DLR_KEY=SRC.DLR_KEY AND TGT.invc_DAY_KEY=SRC.DAY_KEY and TGT.PMT_DAY_KEY= SRC.PMT_DAY_KEY AND TGT.FUND_KEY=SRC.FUND_KEY AND TGT.ACCT_KEY=SRC.ACCT_KEY AND TGT.DLR_FEE_TYPE_KEY=SRC.DLR_FEE_TYPE_KEY)
            WHEN NOT MATCHED THEN
            INSERT (DLR_FEE_RSRH_KEY,
                    DLR_KEY,
                    invc_DAY_KEY,
                    PMT_DAY_KEY,
                    FUND_KEY,
                    ACCT_KEY,
                    DLR_FEE_TYPE_KEY,
                    HBR_AVG_AUM,
                    HBR_AVG_SHRS,
                    FEE_BP,
                    FEE_CALC,
                    FEE_CHRGD_FLG,
                    CURR_ROW_FLG,
                    ROW_STRT_DTTM
            )
            VALUES (SRC.FEE_RSRH_KEY,
                    SRC.dlr_key,
                    SRC.day_key,
                    SRC.pmt_day_key,
                    SRC.fund_key,
                    SRC.acct_key,
                    SRC.dlr_fee_type_key,
                    SRC.avg_daily_bal,
                    SRC.avg_daily_shrs,
                    SRC.fee_rt,
                    SRC.calc_fee,
                    'N',
                    'Y',
                    sysdate
                )
            WHEN MATCHED THEN
            UPDATE SET
                    HBR_AVG_AUM = SRC.AVG_DAILY_BAL,
                    HBR_AVG_SHRS = SRC.Avg_daily_shrs,
                    FEE_BP = src.fee_Rt,
                    FEE_CALC = src.calc_fee,
                    CUSIP_LVL_AUM_DIFF_FLG = NULL,
                    CUSIP_LVL_FEE_DIFF_FLG = null,
                    CUSIP_CURR_AUM_DIFF_FLG = null,
                    ACCT_CURR_AUM_DIFF_FLG = null,
                    FEE_DIFF_FLG = null,
                    AUM_DIFF_FLG = null,
                    ROW_STop_DTTM  = sysdate;     

        COMMIT;
        
        dbms_output.put_line('Step 1 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
        
        SELECT nvl(MAX(dlr_fee_rsrh_key),0) INTO t_key
        FROM Dealer_Fee_Research;

        
     -- Step 2:  Identify all Records from Average Account Position and insert into the table for TPAs:

         MERGE INTO dealer_Fee_research TGT
            USING
            (
                SELECT t_key + rownum FEE_RSRH_KEY, pos.tpa_KEY dlr_key,pos.DAY_KEY,c1.pmt_day_key pmt_day_key,pos.FUND_KEY,pos.ACCT_KEY,schd.DLR_FEE_TYPE_KEY,avg_daily_bal,fee_rt,avg_daily_shrs,
                        ROUND((avg_daily_bal * (fee_rt / 10000)) * ( MO_DAYS_CNT / yr_days_cnt),2) calc_fee
                         FROM HDM.DAILY_AVG_ACCT_POSITION pos INNER join 
                        (select dlr_key,fund_key,dlr_fee_type_key, avg(fee_rt) fee_rt from dealer_fee_schedule group by  dlr_key,fund_key,dlr_fee_type_key) schd on (pos.tpa_key=schd.dlr_key and pos.fund_key=schd.fund_key)
                        INNER JOIN calendar cal on (pos.day_key=cal.day_key)
                        INNER JOIN dealer_fee_type ft ON (schd.DLR_FEE_TYPE_KEY=ft.dlr_fee_type_key)
                        WHERE cal.cal_day = t_date and pos.tpa_key is not null
            ) SRC
            ON (TGT.DLR_KEY=SRC.DLR_KEY AND TGT.invc_DAY_KEY=SRC.DAY_KEY and TGT.PMT_DAY_KEY= SRC.PMT_DAY_KEY AND TGT.FUND_KEY=SRC.FUND_KEY AND TGT.ACCT_KEY=SRC.ACCT_KEY AND TGT.DLR_FEE_TYPE_KEY=SRC.DLR_FEE_TYPE_KEY)
            WHEN NOT MATCHED THEN
            INSERT (DLR_FEE_RSRH_KEY,
                    DLR_KEY,
                    invc_DAY_KEY,
                    PMT_DAY_KEY,
                    FUND_KEY,
                    ACCT_KEY,
                    DLR_FEE_TYPE_KEY,
                    HBR_AVG_AUM,
                    HBR_AVG_SHRS,
                    FEE_BP,
                    FEE_CALC,
                    FEE_CHRGD_FLG,
                    CURR_ROW_FLG,
                    ROW_STRT_DTTM
            )
            VALUES (SRC.FEE_RSRH_KEY,
                    SRC.dlr_key,
                    SRC.day_key,
                    SRC.pmt_day_key,
                    SRC.fund_key,
                    SRC.acct_key,
                    SRC.dlr_fee_type_key,
                    SRC.avg_daily_bal,
                    SRC.avg_daily_shrs,
                    SRC.fee_rt,
                    SRC.calc_fee,
                    'N',
                    'Y',
                    sysdate
                )
            WHEN MATCHED THEN
            UPDATE SET
                    HBR_AVG_AUM = SRC.AVG_DAILY_BAL,
                    HBR_AVG_SHRS = SRC.Avg_daily_shrs,
                    FEE_BP = src.fee_Rt,
                    FEE_CALC = src.calc_fee,
                    CUSIP_LVL_AUM_DIFF_FLG = NULL,
                    CUSIP_LVL_FEE_DIFF_FLG = null,
                    CUSIP_CURR_AUM_DIFF_FLG = null,
                    ACCT_CURR_AUM_DIFF_FLG = null,
                    FEE_DIFF_FLG = null,
                    AUM_DIFF_FLG = null,                    
                    ROW_STop_DTTM  = sysdate;     

        COMMIT;        
        
        dbms_output.put_line('Step 2 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
        
    -- Step 3:  Merge MTF with Previous values and Invoice with current Fee values

         MERGE INTO dealer_Fee_research TGT
            USING
            (
                SELECT cal_day,a.invc_day_key day_key,a.pmt_day_key,a.dlr_key,a.fund_key,a.dlr_fee_type_key,a.acct_key, 'M' invc_freq_flg,'MTF' btype,
                sum(a.tot_compnsn) fees,sum(a.avg_eligbl_asset) avg_aum,avg(a.avg_eligbl_bp) avg_rt,
                sum(prev_mtf.tot_compnsn) Prev_fees,sum(prev_mtf.avg_eligbl_asset) prev_avg_aum,avg(prev_mtf.avg_eligbl_bp) prev_avg_rt,
                C.MO_DAYS_CNT  curr_per_days 
                from dealer_payment_trial a INNER JOIN calendar c ON a.invc_DAY_KEY=c.day_key
                LEFT OUTER JOIN 
                (select pmtf.* from dealer_payment_trial pmtf INNER JOIN calendar pcal ON pmtf.invc_day_key=pcal.day_key WHERE cal_day = add_months(t_date,-1)) prev_mtf  -- Raymond James not included.. Need to add an exception.
                ON a.ACCT_KEY = prev_mtf.ACCT_KEY
                and a.DLR_KEY = prev_mtf.DLR_KEY
                and a.FUND_KEY = prev_mtf.FUND_KEY
                and a.DLR_FEE_TYPE_KEY = prev_mtf.DLR_FEE_TYPE_KEY
                WHERE cal_day = t_date
                group by cal_day,a.invc_day_key,a.pmt_day_key,a.dlr_key,a.fund_key,a.dlr_fee_type_key,a.acct_key,c.mo_days_cnt
                UNION ALL
                SELECT a.cal_day,a.invc_day_key,a.pmt_day_key,a.dlr_key,a.fund_key,a.dlr_fee_type_key,a.acct_key,a.invc_freq_flg,'Invoice' btype,
                a.fees,a.avg_aum ,a.fee_rt,prev_inv.tot_Fee_amt prev_fees,prev_inv.invc_avg_asset prev_avg_aum ,prev_inv.fee_Rt  prev_fee_rt ,a.curr_per_days
                FROM 
                (   select c.cal_day,a.invc_day_key,a.pmt_day_key,a.dlr_key,a.fund_key,a.dlr_fee_type_key,a.acct_key,a.invc_freq_flg,'Invoice' btype,
                    sum(a.tot_fee_amt) fees,sum(a.invc_avg_asset) avg_aum ,avg(a.fee_rt) fee_rt,
                    max((Case When INVC_FREQ_FLG='M' then C.MO_DAYS_CNT When INVC_FREQ_FLG='Q' then C.QTR_DAYS_CNT When INVC_FREQ_FLG='Y' then c.yr_days_cnt end)) curr_per_days            
                     from dealer_invoice_trial a INNER JOIN calendar c ON a.invc_DAY_KEY = c.DAY_KEY INNER JOIN calendar pc on a.PMT_DAY_KEY=pc.day_key
                    WHERE c.cal_day = t_date
                    AND pc.cal_day = pmt_dt
                    AND a.INVC_FREQ_FLG IN ('M','Q','Y','H') 
                    group by c.cal_day,a.invc_day_key,a.pmt_day_key,a.dlr_key,a.fund_key,a.dlr_fee_type_key,a.acct_key,a.invc_freq_flg 
                ) A
                LEFT OUTER JOIN
                (select dlr_Fee_type_key,dlr_key,fund_key,invc_day_key,acct_key,invc_Freq_flg,pcal.cal_day prev_dt ,
                    sum(invc_avg_asset) invc_avg_asset,avg(Fee_Rt) fee_Rt,sum(tot_Fee_amt) tot_Fee_amt
                    from dealer_invoice_trial pinv INNER JOIN calendar pcal ON pinv.invc_day_key=pcal.day_key  
                    WHERE cal_day = add_months(t_date,(Case When INVC_FREQ_FLG in ('M','H') Then -1 When INVC_FREQ_FLG = 'Q' Then -3 When INVC_FREQ_FLG = 'Y' Then -12 end))
                    and INVC_FREQ_FLG IN ('M','Q','Y','H')
                    group by dlr_Fee_type_key,dlr_key,fund_key,invc_day_key,acct_key,invc_Freq_flg,pcal.cal_day ) prev_inv
                ON a.dlr_key=prev_inv.dlr_key 
                and a.fund_key=prev_inv.fund_key
                and a.DLR_FEE_TYPE_KEY=prev_inv.dlr_fee_type_key 
                and a.acct_key=prev_inv.acct_key 
                and a.invc_freq_flg=prev_inv.invc_freq_flg
                -- for H, Fees are same as M, but AUM needs to be calculated differently (prior 3 months average)
            ) SRC 
            ON (TGT.DLR_KEY=SRC.DLR_KEY AND TGT.invc_DAY_KEY=SRC.DAY_KEY AND TGT.FUND_KEY=SRC.FUND_KEY AND TGT.ACCT_KEY=SRC.ACCT_KEY AND TGT.DLR_FEE_TYPE_KEY=SRC.DLR_FEE_TYPE_KEY AND TGT.PMT_DAY_KEY=SRC.PMT_DAY_KEY) 
            WHEN MATCHED THEN
            UPDATE SET
            INVC_FREQ_FLG        = SRC.invc_freq_flg,
            BILLG_TYPE           = SRC.btype,
            FEE_CHRGD            = SRC.fees,
            AVG_AUM              = SRC.avg_aum,
            AVG_BP               = SRC.avg_rt,
            FEE_CHRGD_PREV       = SRC.prev_fees,
            AVG_AUM_PREV         = SRC.prev_avg_aum,
            AVG_BP_PREV          = SRC.prev_avg_rt,
            CURR_PER_DAYs        = SRC.CURR_PER_DAYS,
            FEE_CHRGD_FLG        = 'Y';   
            
            commit;
            
            dbms_output.put_line('Step 3 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
            
    -- Step 4:  Calculate and add the AUM for Hybrid . Current period & previous Period
            MERGE INTO dealer_Fee_research TGT
            USING
            (
                select (select day_Key from calendar where cal_day = t_date)  invc_day_key,A.PMT_DAY_KEY,a.dlr_key,a.fund_key,a.dlr_fee_type_key,a.acct_key,a.invc_freq_flg,a.BType,a.avg_aum,
                pinv.invc_avg_asset prev_avg_aum 
                FROM
                ( SELECT a.dlr_key,a.fund_key,a.pmt_day_key,a.dlr_fee_type_key,a.acct_key,a.invc_freq_flg,'Invoice' btype,
                    sum(a.INVC_AVG_ASSET)/3  avg_aum  ,count(1) recs
                    from dealer_invoice_trial a INNER JOIN calendar c ON a.invc_DAY_KEY = c.DAY_KEY INNER JOIN calendar pc on a.PMT_DAY_KEY=pc.day_key
                    WHERE c.cal_day BETWEEN ADD_MONTHS(t_date,-2) AND t_date
                    AND a.INVC_FREQ_FLG = 'H'
                    AND pc.cal_day = pmt_dt
                    group by a.dlr_key,a.fund_key,a.dlr_fee_type_key,a.acct_key,a.invc_freq_flg ,a.pmt_day_key
                ) A
                LEFT OUTER JOIN
                (select dlr_Fee_type_key,dlr_key,fund_key,acct_key,invc_Freq_flg,
                    sum(invc_avg_asset)/3 invc_avg_asset
                    from dealer_invoice_trial pinv INNER JOIN calendar pcal ON pinv.invc_day_key=pcal.day_key  
                    WHERE cal_day BETWEEN ADD_MONTHS(t_date,-5) AND ADD_MONTHS(t_date,-3)
                    and INVC_FREQ_FLG = 'H'
                    group by dlr_Fee_type_key,dlr_key,fund_key,acct_key,invc_Freq_flg ) pinv
                ON a.dlr_key=pinv.dlr_key 
                and a.fund_key=pinv.fund_key
                and a.DLR_FEE_TYPE_KEY=pinv.dlr_fee_type_key 
                and a.acct_key=pinv.acct_key 
            ) SRC
            ON (tgt.dlr_key=src.dlr_key AND tgt.invc_day_key= src.invc_day_key AND tgt.fund_key= src.fund_key AND tgt.dlr_fee_type_key=src.dlr_fee_type_key 
                AND tgt.acct_key=src.acct_key AND tgt.invc_freq_flg=src.invc_freq_flg AND tgt.pmt_day_key=src.pmt_day_key)
            WHEN MATCHED THEN
            UPDATE SET               
                TGT.AVG_AUM=SRC.AVG_AUM,
                TGT.AVG_AUM_PREV=SRC.PREV_AVG_AUM;
                
            COMMIT;
            dbms_output.put_line('Step 4 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
            
    -- Step 5a:  Calculate the Harbor AUM for Monthly, Qtrly,Yearly and Hybrid for current period.  Only available for accounts with a fee payment.
    --           for qtrly,hybrid getting by calculating 3 month average.
            MERGE INTO dealer_Fee_research TGT
            USING
            (    
                SELECT fr.dlr_fee_rsrh_key,fr.dlr_key,CAL.day_key,fr.fund_key,fr.acct_key,fr.dlr_fee_type_key,fr.INVC_FREQ_FLG,
                SUM(pos.avg_daily_bal)/mth_cnt AVG_DAILY_BAL,sum(avg_daily_shrs)/mth_cnt avg_daily_shrs, MAX(cal.cal_day) DT,
                (Case When INVC_FREQ_FLG='M' then CAL.MO_DAYS_CNT When INVC_FREQ_FLG in ('Q','H') then CAL.QTR_DAYS_CNT When INVC_FREQ_FLG='Y' then cal.yr_days_cnt end) curr_per_days,count(1) recs
                FROM DEALER_FEE_RESEARCH FR
                INNER JOIN CALENDAR CAL ON FR.invc_DAY_KEY=CAL.DAY_KEY
                INNER JOIN CALENDAR pCAL ON FR.pmt_DAY_KEY=pCAL.DAY_KEY
                inner JOIN HDM.DAILY_AVG_ACCT_POSITION POS 
                ON FR.DLR_KEY=POS.DLR_KEY
                    AND FR.FUND_KEY=POS.FUND_KEY
                    AND FR.ACCT_KEY=POS.ACCT_KEY
                INNER JOIN CALENDAR cc ON POS.DAY_KEY=cc.DAY_KEY
                INNER JOIN (
                            SELECT 'M' invc_freq,1 mth_cnt
                            from dual
                            union
                            SELECT 'Q' invc_freq,3 mth_cnt
                            from dual
                            union
                            SELECT 'H' invc_freq,3 mth_cnt
                            from dual 
                            union       
                            SELECT 'Y' invc_freq,12 mth_cnt
                            from dual  
                            ) FQ ON FR.INVC_FREQ_flg=FQ.INVC_FREQ                                             
                WHERE cc.CAL_DAY   BETWEEN ADD_MONTHS(CAL.CAL_DAY,(-1 * (fq.mth_cnt-1))) AND CAL.CAL_DAY
                AND fee_chrgd_FLG ='Y'
                AND cal.CAL_DAY = t_date
                AND pcal.cal_day = pmt_dt
                GROUP BY fr.dlr_fee_rsrh_key,fr.dlr_key,CAL.day_key,fr.fund_key,fr.acct_key,fr.dlr_fee_type_key,fr.INVC_FREQ_FLG ,mth_cnt,
                (Case When INVC_FREQ_FLG='M' then CAL.MO_DAYS_CNT When INVC_FREQ_FLG in ('Q','H') then CAL.QTR_DAYS_CNT When INVC_FREQ_FLG='Y' then cal.yr_days_cnt end) 
            ) SRC
            ON (TGT.DLR_FEE_RSRH_KEY=SRC.DLR_FEE_RSRH_KEY)
            WHEN MATCHED THEN
            UPDATE SET
                TGT.HBR_AVG_AUM   = SRC.AVG_DAILY_BAL,
                TGT.HBR_AVG_SHRS  = SRC.AVG_DAILY_SHRS,
                TGT.CURR_PER_DAYs = SRC.CURR_PER_DAYS;                  
                
            COMMIT;
            dbms_output.put_line('Step 5A completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);    
                        
    -- Step 5B:  Get the Previous Period Asset values for dealers.  It's only available for accounts with a fee payment.  Needed 

            MERGE INTO dealer_Fee_research TGT
            USING
            (
                SELECT fr.dlr_fee_rsrh_key,fr.dlr_key,CAL.day_key,fr.fund_key,fr.acct_key,fr.dlr_fee_type_key,fr.INVC_FREQ_FLG,
                SUM(pos.avg_daily_bal)/mth_cnt AVG_DAILY_BAL,sum(avg_daily_shrs)/mth_cnt avg_daily_shrs, MAX(CC.cal_day) DT,(ADD_MONTHS(CAL.CAL_DAY,(-2 * fq.mth_cnt)+1)) STRTDT,ADD_MONTHS(CAL.CAL_DAY,(-1 * fq.mth_cnt)) ENDDT,
                (Case When INVC_FREQ_FLG='M' then prevcal.MO_DAYS_CNT When INVC_FREQ_FLG in ('Q','H') then prevcal.QTR_DAYS_CNT When INVC_FREQ_FLG='Y' then prevcal.yr_days_cnt end) PR_per_days,count(1) recs
                FROM DEALER_FEE_RESEARCH FR
                INNER JOIN CALENDAR CAL ON FR.invc_DAY_KEY=CAL.DAY_KEY
                INNER JOIN CALENDAR pCAL ON FR.pmt_DAY_KEY=pCAL.DAY_KEY
                inner JOIN HDM.DAILY_AVG_ACCT_POSITION POS 
                ON FR.DLR_KEY=POS.DLR_KEY
                    AND FR.FUND_KEY=POS.FUND_KEY
                    AND FR.ACCT_KEY=POS.ACCT_KEY
                INNER JOIN CALENDAR cc ON POS.DAY_KEY=cc.DAY_KEY
                INNER JOIN (
                            SELECT 'M' invc_freq,1 mth_cnt
                            from dual
                            union
                            SELECT 'Q' invc_freq,3 mth_cnt
                            from dual
                            union
                            SELECT 'H' invc_freq,3 mth_cnt
                            from dual 
                            union       
                            SELECT 'Y' invc_freq,12 mth_cnt
                            from dual  
                            ) FQ ON FR.INVC_FREQ_flg=FQ.INVC_FREQ    
                INNER JOIN HDM.CALENDAR prevcal   on prevcal.cal_day = ADD_MONTHS(CAL.CAL_DAY,(-1 * (fq.mth_cnt)))                                          
                WHERE cc.CAL_DAY   BETWEEN ADD_MONTHS(CAL.CAL_DAY,(-2 * fq.mth_cnt)+1) AND ADD_MONTHS(CAL.CAL_DAY,(-1 * (fq.mth_cnt)))
                AND fee_chrgd_FLG ='Y'
                AND cal.CAL_DAY = t_date
                AND pcal.cal_day = pmt_dt
                GROUP BY fr.dlr_fee_rsrh_key,fr.dlr_key,CAL.day_key,fr.fund_key,fr.acct_key,fr.dlr_fee_type_key,fr.INVC_FREQ_FLG ,mth_cnt,
                (Case When INVC_FREQ_FLG='M' then prevcal.MO_DAYS_CNT When INVC_FREQ_FLG in ('Q','H') then prevcal.QTR_DAYS_CNT When INVC_FREQ_FLG='Y' then prevcal.yr_days_cnt end) ,
                ADD_MONTHS(CAL.CAL_DAY,(-2 * fq.mth_cnt)+1) ,ADD_MONTHS(CAL.CAL_DAY,(-1 * (fq.mth_cnt))) 
            ) SRC
            ON (TGT.DLR_FEE_RSRH_KEY=SRC.DLR_FEE_RSRH_KEY)
            WHEN MATCHED THEN
            UPDATE SET
                HBR_AVG_AUM_PREV   = SRC.AVG_DAILY_BAL,
                HBR_AVG_SHRS_PREV  = SRC.AVG_DAILY_SHRS,
                PRIR_PER_DAYS      = SRC.PR_PER_DAYS;                    
                
            COMMIT;
            dbms_output.put_line('Step 5b completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
            
    -- Step 6a:  Calculate the Harbor AUM for Monthly, Qtrly,Yearly and Hybrid for current period TPAs.  Only available for accounts with a fee payment.
    --           for qtrly,hybrid getting by calculating 3 month average.
            MERGE INTO dealer_Fee_research TGT
            USING
            (    
                SELECT fr.dlr_fee_rsrh_key,fr.dlr_key,CAL.day_key,fr.fund_key,fr.acct_key,fr.dlr_fee_type_key,fr.INVC_FREQ_FLG,
                SUM(pos.avg_daily_bal)/mth_cnt AVG_DAILY_BAL,sum(avg_daily_shrs)/mth_cnt avg_daily_shrs, MAX(cal.cal_day) DT,
                (Case When INVC_FREQ_FLG='M' then CAL.MO_DAYS_CNT When INVC_FREQ_FLG in ('Q','H') then CAL.QTR_DAYS_CNT When INVC_FREQ_FLG='Y' then cal.yr_days_cnt end) curr_per_days,count(1) recs
                FROM DEALER_FEE_RESEARCH FR
                INNER JOIN CALENDAR CAL ON FR.invc_DAY_KEY=CAL.DAY_KEY
                INNER JOIN CALENDAR pCAL ON FR.pmt_DAY_KEY=pCAL.DAY_KEY
                inner JOIN HDM.DAILY_AVG_ACCT_POSITION POS 
                ON FR.DLR_KEY=POS.TPA_KEY
                    AND FR.FUND_KEY=POS.FUND_KEY
                    AND FR.ACCT_KEY=POS.ACCT_KEY
                INNER JOIN CALENDAR cc ON POS.DAY_KEY=cc.DAY_KEY
                INNER JOIN (
                            SELECT 'M' invc_freq,1 mth_cnt
                            from dual
                            union
                            SELECT 'Q' invc_freq,3 mth_cnt
                            from dual
                            union
                            SELECT 'H' invc_freq,3 mth_cnt
                            from dual 
                            union       
                            SELECT 'Y' invc_freq,12 mth_cnt
                            from dual  
                            ) FQ ON FR.INVC_FREQ_flg=FQ.INVC_FREQ                                             
                WHERE cc.CAL_DAY   BETWEEN ADD_MONTHS(CAL.CAL_DAY,(-1 * (fq.mth_cnt-1))) AND CAL.CAL_DAY
                AND fee_chrgd_FLG ='Y'
                AND cal.CAL_DAY = t_date
                AND pcal.cal_day = pmt_dt
                GROUP BY fr.dlr_fee_rsrh_key,fr.dlr_key,CAL.day_key,fr.fund_key,fr.acct_key,fr.dlr_fee_type_key,fr.INVC_FREQ_FLG ,mth_cnt,
                (Case When INVC_FREQ_FLG='M' then CAL.MO_DAYS_CNT When INVC_FREQ_FLG in ('Q','H') then CAL.QTR_DAYS_CNT When INVC_FREQ_FLG='Y' then cal.yr_days_cnt end) 
            ) SRC
            ON (TGT.DLR_FEE_RSRH_KEY=SRC.DLR_FEE_RSRH_KEY)
            WHEN MATCHED THEN
            UPDATE SET
                TGT.HBR_AVG_AUM   = SRC.AVG_DAILY_BAL,
                TGT.HBR_AVG_SHRS  = SRC.AVG_DAILY_SHRS,
                TGT.CURR_PER_DAYs = SRC.CURR_PER_DAYS;                  
                
            COMMIT;
            dbms_output.put_line('Step 6A completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);    
                        
    -- Step 6B:  Get the Previous Period Asset values for TPA dealers.  It's only available for accounts with a fee payment.  Needed 

            MERGE INTO dealer_Fee_research TGT
            USING
            (
                SELECT fr.dlr_fee_rsrh_key,fr.dlr_key,CAL.day_key,fr.fund_key,fr.acct_key,fr.dlr_fee_type_key,fr.INVC_FREQ_FLG,
                SUM(pos.avg_daily_bal)/mth_cnt AVG_DAILY_BAL,sum(avg_daily_shrs)/mth_cnt avg_daily_shrs, MAX(CC.cal_day) DT,(ADD_MONTHS(CAL.CAL_DAY,(-2 * fq.mth_cnt)+1)) STRTDT,ADD_MONTHS(CAL.CAL_DAY,(-1 * fq.mth_cnt)) ENDDT,
                (Case When INVC_FREQ_FLG='M' then prevcal.MO_DAYS_CNT When INVC_FREQ_FLG in ('Q','H') then prevcal.QTR_DAYS_CNT When INVC_FREQ_FLG='Y' then prevcal.yr_days_cnt end) PR_per_days,count(1) recs
                FROM DEALER_FEE_RESEARCH FR
                INNER JOIN CALENDAR CAL ON FR.invc_DAY_KEY=CAL.DAY_KEY
                INNER JOIN CALENDAR pCAL ON FR.pmt_DAY_KEY=pCAL.DAY_KEY
                inner JOIN HDM.DAILY_AVG_ACCT_POSITION POS 
                ON FR.DLR_KEY=POS.TPA_KEY
                    AND FR.FUND_KEY=POS.FUND_KEY
                    AND FR.ACCT_KEY=POS.ACCT_KEY
                INNER JOIN CALENDAR cc ON POS.DAY_KEY=cc.DAY_KEY
                INNER JOIN (
                            SELECT 'M' invc_freq,1 mth_cnt
                            from dual
                            union
                            SELECT 'Q' invc_freq,3 mth_cnt
                            from dual
                            union
                            SELECT 'H' invc_freq,3 mth_cnt
                            from dual 
                            union       
                            SELECT 'Y' invc_freq,12 mth_cnt
                            from dual  
                            ) FQ ON FR.INVC_FREQ_flg=FQ.INVC_FREQ   
                INNER JOIN HDM.CALENDAR prevcal   on prevcal.cal_day = ADD_MONTHS(CAL.CAL_DAY,(-1 * (fq.mth_cnt)))                                                         
                WHERE cc.CAL_DAY   BETWEEN ADD_MONTHS(CAL.CAL_DAY,(-2 * fq.mth_cnt)+1) AND ADD_MONTHS(CAL.CAL_DAY,(-1 * (fq.mth_cnt)))
                AND fee_chrgd_FLG ='Y'
                AND cal.CAL_DAY = t_date
                AND pcal.cal_day = pmt_dt
                GROUP BY fr.dlr_fee_rsrh_key,fr.dlr_key,CAL.day_key,fr.fund_key,fr.acct_key,fr.dlr_fee_type_key,fr.INVC_FREQ_FLG ,mth_cnt,
                (Case When INVC_FREQ_FLG='M' then prevcal.MO_DAYS_CNT When INVC_FREQ_FLG in ('Q','H') then prevcal.QTR_DAYS_CNT When INVC_FREQ_FLG='Y' then prevcal.yr_days_cnt end) ,
                ADD_MONTHS(CAL.CAL_DAY,(-2 * fq.mth_cnt)+1) ,ADD_MONTHS(CAL.CAL_DAY,(-1 * (fq.mth_cnt))) 
            ) SRC
            ON (TGT.DLR_FEE_RSRH_KEY=SRC.DLR_FEE_RSRH_KEY)
            WHEN MATCHED THEN
            UPDATE SET
                HBR_AVG_AUM_PREV   = SRC.AVG_DAILY_BAL,
                HBR_AVG_SHRS_PREV  = SRC.AVG_DAILY_SHRS,
                PRIR_PER_DAYS      = SRC.PR_PER_DAYS;                    
                
            COMMIT;
            dbms_output.put_line('Step 6b completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date); 
                
    -- Step 7 :  Check the AUM difference with previous period at CUSIP level.  Update flag if diff is > 1%

            MERGE INTO dealer_Fee_research TGT
            USING
            (
                SELECT dlr_key,rsr.invc_day_key day_key,rsr.pmt_day_key,fund_key,rsr.dlr_fee_type_key,
                (Case When round(abs(((Case When sum(nvl(avg_aum,0))=0 then -1 
                                            When sum(nvl(hbr_avg_aum,0)) = 0 then 1
                                            Else sum(avg_aum) / sum(hbr_avg_aum) - 1 end)) - 
                                       (Case When sum(nvl(avg_aum_prev,0)) = 0 then -1
                                            When sum(nvl(hbr_avg_aum_prev,0))= 0 then 1
                                            Else (sum(avg_aum_prev) / (sum(hbr_avg_aum_prev)) - 1) end)),4)> 0.01 Then 'Y' Else 'N' end) pct_aum_diff_flg 
                FROM DEALER_FEE_RESEARCH rsr INNER JOIN calendar cal ON rsr.invc_day_key=cal.day_key INNER JOIN calendar pc on rsr.PMT_DAY_KEY=pc.day_key
                WHERE fee_chrgd_FLG ='Y'
                and cal.cal_day=t_date
                and pc.cal_day = pmt_dt
                group by dlr_key,rsr.invc_day_key,fund_key ,rsr.pmt_day_key ,rsr.DLR_FEE_TYPE_KEY
            ) SRC
            ON ( tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.invc_day_key=src.day_key AND tgt.pmt_day_key=src.pmt_day_key and tgt.dlr_fee_type_key=src.dlr_fee_type_key)
            WHEN MATCHED THEN
            UPDATE SET
                CUSIP_LVL_AUM_DIFF_FLG = src.pct_aum_diff_flg
                WHERE fee_chrgd_FLG ='Y';
                
            COMMIT;
            dbms_output.put_line('Step 7 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
    -- Step 8 :  Check the FEE difference with previous period at CUSIP level.  Update flag if diff is > 10%
    --           Adjust the number of days in current and prior periods.
            MERGE INTO dealer_Fee_research TGT
            USING
            (
                SELECT dlr_key,cal.day_key,rsr.pmt_day_key,fund_key,dlr_fee_type_key,
                (Case When sum(nvl(fee_chrgd,0)) = 0 and sum(nvl(fee_chrgd_prev,0)) = 0 Then 'N'
                      When  least(sum(nvl(fee_chrgd_prev,0)), sum(nvl(fee_chrgd,0))) = 0 Then 'Y'
                      When abs(sum(fee_chrgd) / (sum(fee_chrgd_prev) * max(curr_per_days) / max(prir_per_days)) - 1)> 0.1 Then 'Y' Else 'N' end) pct_fee_diff_flg
                FROM DEALER_FEE_RESEARCH rsr INNER JOIN calendar cal ON rsr.invc_day_key=cal.day_key INNER JOIN calendar pc on rsr.PMT_DAY_KEY=pc.day_key
                WHERE fee_chrgd_FLG ='Y'
                and cal.cal_day=t_date
                and pc.cal_day = pmt_dt
                group by dlr_key,cal.day_key,fund_key ,dlr_fee_type_key,rsr.pmt_day_key
            ) SRC
            ON ( tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.invc_day_key=src.day_key AND tgt.dlr_fee_type_key=src.dlr_fee_type_key and tgt.pmt_day_key=src.pmt_day_key)
            WHEN MATCHED THEN
            UPDATE SET
                CUSIP_LVL_FEE_DIFF_FLG = src.pct_fee_diff_flg
                WHERE fee_chrgd_FLG ='Y';
                
            COMMIT;        
            dbms_output.put_line('Step 8 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
    -- Step 9 :  Calculate the account level fee and aum difference flag only for those records where cusip level aum diff > 1% 
            MERGE INTO dealer_Fee_research TGT
            USING
            (         
                SELECT dlr_fee_rsrh_key,dlr_key,cal.day_key,fund_key,dlr_fee_type_key,acct_key,
                (Case When round(abs(((Case When sum(nvl(avg_aum,0))=0 then -1 
                                            When sum(nvl(hbr_avg_aum,0)) = 0 then 1
                                            Else sum(avg_aum) / sum(hbr_avg_aum) - 1 end)) - 
                                       (Case When sum(nvl(avg_aum_prev,0)) = 0 then -1
                                            When sum(nvl(hbr_avg_aum_prev,0))= 0 then 1
                                            Else (sum(avg_aum_prev) / (sum(hbr_avg_aum_prev) )- 1) end)),4)> 0.01 Then 'Y' Else 'N' end) pct_aum_diff_flg                
                FROM DEALER_FEE_RESEARCH rsr INNER JOIN calendar cal ON rsr.invc_day_key=cal.day_key INNER JOIN calendar pc on rsr.PMT_DAY_KEY=pc.day_key
                WHERE fee_chrgd_FLG ='Y'
                and cusip_lvl_aum_diff_flg='Y' 
                and cal.cal_day=t_date
                and pc.cal_day = pmt_dt
                group by dlr_fee_rsrh_key,dlr_key,cal.day_key,fund_key,dlr_fee_type_key,acct_key,curr_per_days , prir_per_days
            ) SRC
            ON (TGT.DLR_FEE_RSRH_KEY = SRC.DLR_FEE_RSRH_KEY)
            WHEN MATCHED THEN
            UPDATE SET
                TGT.AUM_DIFF_FLG = SRC.PCT_AUM_DIFF_FLG
                WHERE fee_chrgd_FLG ='Y';
                
            COMMIT;
            dbms_output.put_line('Step 9 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
    -- Step 10 :  Calculate the account level fee and aum difference flag only for those records where cusip level  fee diff > 10%
            MERGE INTO dealer_Fee_research TGT
            USING
            (         
                SELECT dlr_fee_rsrh_key,dlr_key,rsr.invc_day_key day_key,fund_key,dlr_fee_type_key,acct_key,
                (Case when sum(nvl(fee_chrgd,0)) = 0 and sum(nvl(fee_chrgd_prev,0)) = 0 Then 'N'   
                      when least(sum(nvl(fee_chrgd,0)),sum(nvl(fee_chrgd_prev,0))) = 0 then 'Y'
                      when  abs(sum(fee_chrgd) / (sum(fee_chrgd_prev) * curr_per_days / prir_per_days) - 1)> 0.1 Then 'Y' Else 'N' end)  pct_fee_diff_flg
                FROM DEALER_FEE_RESEARCH rsr INNER JOIN calendar cal ON rsr.invc_day_key=cal.day_key INNER JOIN calendar pc on rsr.PMT_DAY_KEY=pc.day_key
                WHERE fee_chrgd_FLG ='Y'
                and cusip_lvl_fee_diff_flg='Y'  
                and cal.cal_day=t_date
                and pc.cal_day = pmt_dt
                group by dlr_fee_rsrh_key,dlr_key,rsr.invc_day_key,fund_key,dlr_fee_type_key,acct_key, curr_per_days , prir_per_days
            ) SRC
            ON (TGT.DLR_FEE_RSRH_KEY = SRC.DLR_FEE_RSRH_KEY)
            WHEN MATCHED THEN
            UPDATE SET
                TGT.FEE_DIFF_FLG = SRC.PCT_FEE_DIFF_FLG;
                
            COMMIT;      
            dbms_output.put_line('Step 10 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
    -- Step 11 :  Check the AUM difference CURRENT period between Invoiced/MTF AUM and DWH AUM at CUSIP level.  Update flag if diff is > 1%
            MERGE INTO dealer_Fee_research TGT
            USING
            (

                SELECT dlr_key,rsr.invc_day_key,rsr.pmt_day_key,fund_key,rsr.dlr_fee_type_key,
                (Case when sum(nvl(avg_aum,0)) = 0 and sum(nvl(hbr_avg_aum,0)) =0 Then 'N'
                      when least(sum(nvl(hbr_avg_aum,0)),sum(nvl(avg_aum,0))) = 0 then 'Y'
                      When abs((sum(avg_aum) / sum(hbr_avg_aum) - 1)) > 0.01 Then 'Y' Else 'N' end) pct_aum_diff_flg
                FROM DEALER_FEE_RESEARCH rsr INNER JOIN calendar cal ON rsr.invc_day_key=cal.day_key INNER JOIN calendar pc on rsr.PMT_DAY_KEY=pc.day_key
                WHERE fee_chrgd_FLG ='Y'
                and cal.cal_day = t_date
                and pc.cal_day = pmt_dt
                group by dlr_key,rsr.invc_day_key,fund_key  ,rsr.pmt_day_key,rsr.dlr_fee_type_key
            ) SRC
            ON ( tgt.dlr_key=src.dlr_key AND tgt.fund_key=src.fund_key AND tgt.invc_day_key=src.invc_day_key AND tgt.pmt_day_key=src.pmt_day_key and tgt.dlr_fee_type_key=src.dlr_fee_type_key)
            WHEN MATCHED THEN
            UPDATE SET
                CUSIP_CURR_AUM_DIFF_FLG = src.pct_aum_diff_flg
                WHERE fee_chrgd_FLG ='Y';
                
            COMMIT;       
            dbms_output.put_line('Step 11 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
    -- Step 12 :  Calculate the account level  aum difference flag only for those records where cusip level CURR aum diff > 1% 
            MERGE INTO dealer_Fee_research TGT
            USING
            (         
                SELECT dlr_fee_rsrh_key,dlr_key,rsr.invc_day_key,rsr.pmt_day_key,fund_key,dlr_fee_type_key,acct_key,
                (Case When sum(nvl(avg_aum,0)) =0  and sum(nvl(hbr_avg_aum,0)) = 0 then 'N'
                      When least(sum(nvl(hbr_avg_aum,0)),sum(nvl(avg_aum,0)) ) = 0 then 'Y'
                      When abs((sum(avg_aum) / sum(hbr_avg_aum) - 1)) > 0.01 Then 'Y' Else 'N' end) pct_aum_diff_flg
                FROM DEALER_FEE_RESEARCH rsr INNER JOIN calendar cal ON rsr.invc_day_key=cal.day_key INNER JOIN calendar pc on rsr.PMT_DAY_KEY=pc.day_key
                WHERE fee_chrgd_FLG ='Y'
                and cusip_curr_aum_diff_flg='Y' 
                and cal.cal_day=t_date
                and pc.cal_day = pmt_dt
                group by dlr_fee_rsrh_key,dlr_key,rsr.invc_day_key,fund_key,dlr_fee_type_key,acct_key,rsr.pmt_day_key
            ) SRC
            ON (TGT.DLR_FEE_RSRH_KEY = SRC.DLR_FEE_RSRH_KEY)
            WHEN MATCHED THEN
            UPDATE SET
                TGT.ACCT_CURR_AUM_DIFF_FLG = SRC.PCT_AUM_DIFF_FLG;
                
            COMMIT;  
            
            dbms_output.put_line('Step 12 completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);   
            
     -- Call Load avg nav check to calculate average nav for invoice date
     
            PKG_DEALER_ANALYSIS.LOAD_AVG_NAV_CHECK(t_date);
            dbms_output.put_line('Average NAV calc step completed, payment date = ' || pmt_dt || ' , invoice date = ' || t_date);
        END LOOP;
        
EXCEPTION
WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;
END  load_fee_research;

 
procedure load_avg_nav_check(p_date IN DATE) IS
-- STEP 3:  Compare Average NAV and Average Share Balance differences.
    t_date  DATE;
    
BEGIN
        t_date := p_date;

        -- Task 1:  Calculate and update Average NAV values for current period.

        MERGE INTO dealer_Fee_research TGT
        USING    
        (
            SELECT val.fund_key,fr.invc_day_key,invc_freq_flg,avg(val.pr_per_shr) avg_monthly_nav ,dlr_key,pmt_day_key
            FROM 
            (
                select dfr.invc_day_key,fund_key,invc_freq_flg,cal_day,dfr.dlr_key,DFR.PMT_DAY_KEY,
                (Case When invc_freq_flg in ('M','H') Then mo_days_cnt When invc_freq_flg='Q' then qtr_days_cnt When invc_freq_flg='Y' then yr_days_cnt end) day_cnt
                from HDM.DEALER_FEE_RESEARCH DFR INNER JOIN  CALENDAR CAL ON (DFR.invc_DAY_KEY=CAL.DAY_KEY)
                where fee_chrgd_flg='Y'
                and invc_freq_flg in ('M','H','Q')
                and cal.cal_day = t_date
                group by dfr.invc_day_key,fund_key,invc_freq_flg,cal_day,dfr.dlr_key,DFR.PMT_DAY_KEY,
                (Case When invc_freq_flg in ('M','H') Then mo_days_cnt When invc_freq_flg='Q' then qtr_days_cnt When invc_freq_flg='Y' then yr_days_cnt end) 
            ) FR INNER JOIN FUND_VALUATION VAL ON fr.fund_key=val.fund_key
            INNER JOIN calendar cal ON val.day_key=cal.day_key
            WHERE cal.cal_day between fr.cal_day - day_cnt + 1 AND fr.cal_day
            group by val.fund_key,fr.invc_day_key,invc_freq_flg,dlr_key,pmt_day_key
        ) SRC 
        ON (TGT.fund_key=SRC.fund_key AND tgt.invc_day_key=src.invc_day_key AND tgt.invc_Freq_flg=src.invc_freq_flg AND tgt.dlr_key=src.dlr_key and tgt.pmt_day_key=src.pmt_day_key)  
        WHEN MATCHED THEN
        UPDATE SET
            tgt.nav_mo_avg = src.avg_monthly_nav;
            
        COMMIT;

        -- Task 2:  Calculate and update Average NAV values for Previous period.

        MERGE INTO dealer_Fee_research TGT
        USING    
        (
            SELECT val.fund_key,fr.invc_day_key,invc_freq_flg,avg(val.pr_per_shr) avg_monthly_nav ,dlr_key,pmt_day_key
            FROM 
            (
                select dfr.invc_day_key,fund_key,invc_freq_flg,cal_day,dfr.dlr_key,DFR.PMT_DAY_KEY,
                (Case When invc_freq_flg in ('M','H') Then mo_days_cnt When invc_freq_flg='Q' then qtr_days_cnt When invc_freq_flg='Y' then yr_days_cnt end) day_cnt,
                (Case When invc_freq_flg in ('M','H') Then 1 When invc_freq_flg='Q' then 3 When invc_freq_flg='Y' then 12 end) mo_cnt
                from HDM.DEALER_FEE_RESEARCH DFR INNER JOIN  CALENDAR CAL ON (DFR.invc_DAY_KEY=CAL.DAY_KEY)
                where fee_chrgd_flg='Y'
                and invc_freq_flg in ('M','H','Q')
                and cal.cal_day = t_date
                group by dfr.invc_day_key,fund_key,invc_freq_flg,mo_days_cnt,qtr_days_cnt,yr_days_cnt,cal_day,dfr.dlr_key,DFR.PMT_DAY_KEY
            ) FR INNER JOIN FUND_VALUATION VAL ON fr.fund_key=val.fund_key
            INNER JOIN calendar cal ON val.day_key=cal.day_key
            WHERE cal.cal_day between add_months(fr.cal_day,mo_cnt * -2) + 1 AND add_months(fr.cal_day,mo_cnt * -1)
            group by val.fund_key,fr.invc_day_key,invc_freq_flg,dlr_key,pmt_day_key
        ) SRC 
        ON (TGT.fund_key=SRC.fund_key AND tgt.invc_day_key=src.invc_day_key AND tgt.invc_Freq_flg=src.invc_freq_flg AND tgt.dlr_key=src.dlr_key and tgt.pmt_day_key=src.pmt_day_key)  
        WHEN MATCHED THEN
        UPDATE SET
            tgt.nav_mo_avg_prev = src.avg_monthly_nav;
            
        COMMIT;        
        
    
EXCEPTION
WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;
end load_avg_nav_check;    

 procedure load_dealer_change_list IS
    t_curr_key NUMBER;
 begin
    SELECT NVL(max(DLR_CHG_LIST_Key),0) INTO t_curr_key
    FROM Dealer_Change_List;
    
   MERGE INTO Dealer_Change_List TGT
   USING (
            select ROWNUM + t_curr_key dlr_chg_key,day_key,acct_key,fund_key,change_type,acct_mnt_seq_id,batch_number,orig_value,new_value,dst.acct_nbr,dst.fund_nbr,dst.change_dt
            from vw_dealer_dst_change_list dst 
            INNER JOIN account ON dst.ACCT_NBR = account.ACCT_NBR and dst.FUND_NBR = account.FUND_NBR
            INNER JOIN fund fnd ON dst.FUND_NBR = fnd.FUND_NBR
            INNER JOIN calendar cal ON dst.CHANGE_DT=cal.cal_day
        ) SRC
    ON ( tgt.day_key=src.day_key AND tgt.fund_key=src.fund_key AND tgt.acct_key=src.acct_key and tgt.dlr_chg_type=src.change_type and tgt.acct_maint_seq_id=src.acct_mnt_seq_id)
    WHEN NOT MATCHED THEN INSERT
    ( dlr_chg_list_key,
       day_key,
       fund_key,
       acct_key,
       dlr_chg_type,
       acct_maint_seq_id,
       dlr_orig_val,
       dlr_new_val,
       batch_nbr,
       curr_row_flg,
       row_strt_dttm
     )
    VALUES (
       src.dlr_chg_key,
       src.day_key,
       src.fund_key,
       src.acct_key,
       src.change_type,
       src.acct_mnt_seq_id,
       src.orig_value,
       src.new_value,
       src.batch_number,
       'Y',
       sysdate
    )
    WHEN MATCHED THEN UPDATE SET
        tgt.dlr_orig_val=src.orig_value,
        tgt.dlr_new_val = src.new_value,
        tgt.row_stop_dttm = sysdate
        where tgt.dlr_orig_val <> src.orig_value or tgt.dlr_new_val <> src.new_value;

    COMMIT;
EXCEPTION
WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;      
end load_dealer_change_list;               
END pkg_dealer_analysis;

CREATE OR REPLACE PACKAGE HDM.PKG_DEALER_ASSET_FEE AS
    /*    created by Li H. on 10/22/12    */
    
    /*
    procedure CALC_ALL_ASSET_FEE calculates assets and fees for all cases, no need to call direct or indirect procedures 
    p_in_dlr : input dealer number
    p_in_date: input date for dealer_invoice.row_strt_dttm
    */
    Procedure CALC_ALL_ASSET_FEE (p_in_dlr number,p_in_date date);
    
    /*
    procedure CALC_DIRECT_ASSET_FEE calculates assets and fees for those business lines need no conversion,ie: source BL is equal to target BL 
    p_in_dlr : input dealer number
    p_in_date: input date for dealer_invoice.row_strt_dttm
    */
    Procedure CALC_DIRECT_ASSET_FEE (p_in_dlr number,p_in_date date);
    /*
    procedure CALC_INDIRECT_ASSET_FEE calculates assets and fees for all cases
    p_in_dlr : input dealer number
    p_in_date: input date for dealer_invoice.row_strt_dttm
    */    
    Procedure CALC_INDIRECT_ASSET_FEE (p_in_dlr number,p_in_date date);
    
    Procedure CALC_ALL_ASSET_FEE_tmp (p_in_dlr number,p_in_date date);
   
end PKG_DEALER_ASSET_FEE;

CREATE OR REPLACE package body HDM.PKG_DEALER_ASSET_FEE AS

/*
package body created by Li H. on 10/24/12
*/

/*
procedure CALC_DIRECT_ASSET_FEE calculates assets and fees for those business lines need no conversion,ie: source BL is equal to target BL 
p_in_dlr : input dealer number
p_in_date: input date for row_strt_dttm
*/

PROCEDURE CALC_DIRECT_ASSET_FEE(p_in_dlr number,p_in_date date) IS   
BEGIN
    merge into HDM.DEALER_INVOICE_MODEL tgt
    using
        (/*inner table src gets only the rows whose business lines are null, or those whose mapping to target BL can't be found in dealer_invoice_rule table, or those having one-to-one mapping in dealer_invoice_rule table
         src rows are holding simple mapping as target BL=Source BL, target Assets= sum(Source assets), target fees=sum(Source fees)+ BP*(days_in_month/Days_in_year)*sum(assets)
        */
        ---------------------
            select   
                dlr_fee_type_key,a.dlr_key,invc_day_key,a.fund_key,bus_line,INVC_AVG_ASSET_sum,
                a.tot_fee_amt_sum+a.INVC_AVG_ASSET_sum*((trunc(last_day(C.CAL_DAY),'DDD')-trunc(C.CAL_DAY,'MM')+1)/(add_months(trunc(C.CAL_DAY,'YEAR')-1,12)-trunc(C.CAL_DAY,'YEAR')+1))*NVL(R.BP_VALUE,0)/10000 Tot_fee_amt_final
            from
                (/* inner table get all rows by the given date and dealer, but with all bus_line */
                    select 
                        dlr_fee_type_key,dlr_key,invc_day_key,fund_key,bus_line,nvl(sum(B.INVC_AVG_ASSET),0) INVC_AVG_ASSET_sum,nvl(sum(B.TOT_FEE_AMT),0) TOT_FEE_AMT_sum
                    from 
                        HDM.DEALER_INVOICE b
                    where
                        /*
                         get rows needs to be updated or inserted. First get invc_day_key,dlr_fee_type_key,fund_key,bus_line from those new rows based on row_strt_dttm input, then using these keys to 
                            get all possible new and old rows. since bus_line could be null, need to use NVL function to grep null bus_line rows too
                       */
                        (B.INVC_day_key,fund_key,dlr_fee_type_key,NVL(bus_line,'NULL')) in (select invc_day_key,fund_key,dlr_fee_type_key,NVL(bus_line,'NULL') from HDM.DEALER_INVOICE where ROW_STRT_DTTM > p_in_date)
                    group by 
                        dlr_key,invc_day_key,fund_key,dlr_fee_type_key,bus_line
                    having 
                        B.DLR_KEY=p_in_dlr        
                )a
            inner join hdm.fund f on F.FUND_KEY=a.fund_key
            inner join HDM.CALENDAR c on C.DAY_KEY=A.INVC_DAY_KEY
            left outer join HDM.DEALER_INVOICE_RULE r on R.DLR_KEY=a.dlr_key and R.SRC_BUS_LINE=a.bus_line and f.shr_cls_cd=R.SHR_CLS_CD
            where 
                /* the first condition is to get one-one SRC_BL to TGT_BL mapping,the second condition is for those not defined in dealer_invoice_rule table, or those whose BL is null */
                ((R.INVC_DLR_RULE_KEY is not null) and R.MUL_SRC_BL_FLG='N') or (R.INVC_DLR_RULE_KEY is null)
            --------------------- 
        )src
    on (tgt.dlr_fee_type_key=src.dlr_fee_type_key and tgt.dlr_key=src.dlr_key and tgt.invc_day_key=src.invc_day_key and tgt.fund_key=src.fund_key and nvl(tgt.bus_line,'NULL')=NVL(src.bus_line,'NULL'))
    when matched then
        update set TGT.INVC_AVG_ASSET=src.INVC_AVG_ASSET_sum, TGT.TOT_FEE_AMT=src.tot_fee_amt_final, TGT.ROW_STRT_DTTM=sysdate
    when not matched then
        insert (DLR_INVC_MDL_KEY,dlr_fee_type_key,dlr_key,invc_day_key,fund_key,bus_line,INVC_AVG_ASSET,TOT_FEE_AMT,curr_row_flg,row_strt_dttm)
        values (HDM.SEQ_DEALER_INVOICE_MODEL.nextval,src.dlr_fee_type_key,src.dlr_key,src.invc_day_key,src.fund_key,src.bus_line,src.INVC_AVG_ASSET_sum,src.TOT_FEE_AMT_final,'Y',sysdate);
    
    commit;
end CALC_DIRECT_ASSET_FEE;

/*
procedure CALC_INDIRECT_ASSET_FEE calculates assets and fees for multiple source BLs mapped to one single target BL 
p_in_dlr : input dealer number
p_in_date: input date for row_strt_dttm
*/
PROCEDURE CALC_INDIRECT_ASSET_FEE(p_in_dlr number,p_in_date date) IS   
BEGIN
    merge into HDM.DEALER_INVOICE_MODEL tgt
    using
        (
        select 
            i.dlr_fee_type_key,i.dlr_key,i.invc_day_key,i.fund_key,i.trgt_bus_line bus_line,sum(i.invc_avg_asset_final) invc_avg_asset_sum,sum(i.tot_fee_amt_final) tot_fee_amt_final
        from  
            (
            /*--------------- table i ---------------*/
                select  
                    a.dlr_fee_type_key,a.dlr_key,a.invc_day_key,a.fund_key,
                    --bus_line,R.BP_VALUE,R.CALC_FLG,R.FEE_USED_IN_FEE_FLG,F.SHR_CLS_CD,C.CAL_DAY,R.SRC_BUS_LINE,
                    R.TRGT_BUS_LINE,
                    /* if CALC_FLG is A, add the assets; if CALC_FLG is S, subtract the assets */
                    decode(R.CALC_FLG,'A',a.INVC_AVG_ASSET_sum,'S',-a.INVC_AVG_ASSET_sum) INVC_AVG_ASSET_final,
                    /* fees contains two parts:the first is fee part:if FEE_USED_IN_FEE_FLG is Y, need to add this fee. If it is N,no need to count in this fee
                    the second part is assets*bp*days_in_month/days_in_year. BP column is positive or negative in dealer_invoice_rule table.So no need to process the addition or subtraction of BP part */
                    decode(R.FEE_USED_IN_FEE_FLG,'Y',A.TOT_FEE_AMT_sum,'N',0)+ a.invc_avg_asset_sum*((trunc(last_day(C.CAL_DAY),'DDD')-trunc(C.CAL_DAY,'MM')+1)/(add_months(trunc(C.CAL_DAY,'YEAR')-1,12)-trunc(C.CAL_DAY,'YEAR')+1))*NVL(R.BP_VALUE,0)/10000 Tot_fee_AMT_Final
                from
                    ( /* inner table get all rows by the given date and dealer, but with all bus_line */
                        select 
                            dlr_fee_type_key,dlr_key,invc_day_key,fund_key,bus_line,nvl(sum(B.INVC_AVG_ASSET),0) INVC_AVG_ASSET_sum,nvl(sum(B.TOT_FEE_AMT),0) TOT_FEE_AMT_sum
                        from 
                            HDM.DEALER_INVOICE b
                        where
                            /*
                             get rows needs to be updated or inserted. First get invc_day_key,dlr_fee_type_key,fund_key,bus_line from those new rows based on row_strt_dttm input, then using these keys to 
                                get all possible new and old rows. since bus_line could be null, need to use NVL function to grep null bus_line rows too
                           */
                            (B.INVC_day_key,fund_key,dlr_fee_type_key,bus_line) in (select invc_day_key,fund_key,dlr_fee_type_key,bus_line from HDM.DEALER_INVOICE where ROW_STRT_DTTM > p_in_date)
                        group by 
                            dlr_key,invc_day_key,fund_key,dlr_fee_type_key,bus_line
                        having 
                            B.DLR_KEY=p_in_dlr        
                    )a
                inner join hdm.fund f on F.FUND_KEY=a.fund_key
                inner join HDM.CALENDAR c on C.DAY_KEY=A.INVC_DAY_KEY
                inner join HDM.DEALER_INVOICE_RULE r on R.DLR_KEY=a.dlr_key and R.SRC_BUS_LINE=a.bus_line and f.shr_cls_cd=R.SHR_CLS_CD
                where 
                    (R.INVC_DLR_RULE_KEY is not null) and R.MUL_SRC_BL_FLG='Y' 
             /*------------------------------*/
             )i
        group by 
            i.dlr_fee_type_key,i.dlr_key,i.invc_day_key,i.fund_key,i.trgt_bus_line
        ) src
    on (tgt.dlr_fee_type_key=src.dlr_fee_type_key and tgt.dlr_key=src.dlr_key and tgt.invc_day_key=src.invc_day_key and tgt.fund_key=src.fund_key and nvl(tgt.bus_line,'NULL')=NVL(src.bus_line,'NULL'))
    when matched then
        update set TGT.INVC_AVG_ASSET=src.INVC_AVG_ASSET_sum, TGT.TOT_FEE_AMT=src.tot_fee_amt_final, TGT.ROW_STRT_DTTM=sysdate
    when not matched then
        insert (DLR_INVC_MDL_KEY,dlr_fee_type_key,dlr_key,invc_day_key,fund_key,bus_line,INVC_AVG_ASSET,TOT_FEE_AMT,curr_row_flg,row_strt_dttm)
        values (HDM.SEQ_DEALER_INVOICE_MODEL.nextval,src.dlr_fee_type_key,src.dlr_key,src.invc_day_key,src.fund_key,src.bus_line,src.INVC_AVG_ASSET_sum,src.TOT_FEE_AMT_final,'Y',sysdate);
        
    commit;
end CALC_INDIRECT_ASSET_FEE;


/*
procedure CALC_ALL_ASSET_FEE_tmp calculates assets and fees for all cases, can't carry over the INVC_FREQ_FLG flag.
p_in_dlr : input dealer number
p_in_date: input date for row_strt_dttm
*/
PROCEDURE CALC_ALL_ASSET_FEE_tmp(p_in_dlr number,p_in_date date) IS   
BEGIN
    merge into HDM.DEALER_INVOICE_MODEL tgt
    using
        (
        select 
            i.dlr_fee_type_key,i.dlr_key,i.invc_day_key,i.fund_key,i.trgt_bus_line bus_line,sum(i.invc_avg_asset_final) invc_avg_asset_sum,sum(i.tot_fee_amt_final) tot_fee_amt_final
        from  
            (
            /*--------------- table i ---------------*/
                select  
                    a.dlr_fee_type_key,a.dlr_key,a.invc_day_key,a.fund_key,
                    a.bus_line,R.BP_VALUE,R.CALC_FLG,R.FEE_USED_IN_FEE_FLG,F.SHR_CLS_CD,C.CAL_DAY,R.SRC_BUS_LINE,
                    NVL(R.TRGT_BUS_LINE,a.bus_line) trgt_bus_line,
                    /* if CALC_FLG is A, add the assets; if CALC_FLG is S, subtract the assets */
                    decode(R.CALC_FLG,'A',a.INVC_AVG_ASSET_sum,'S',-a.INVC_AVG_ASSET_sum,null,A.INVC_AVG_ASSET_sum) INVC_AVG_ASSET_final,
                    /* fees contains two parts:the first is fee part:if FEE_USED_IN_FEE_FLG is Y, need to add this fee. If it is N,no need to count in this fee, if it is null should add this fee
                    the second part is assets*bp*days_in_month/days_in_year. BP column is positive or negative in dealer_invoice_rule table.So no need to process the addition or subtraction of BP part */
                    decode(R.FEE_USED_IN_FEE_FLG,'Y',A.TOT_FEE_AMT_sum,'N',0,null,A.TOT_FEE_AMT_sum)+ a.invc_avg_asset_sum*((trunc(last_day(C.CAL_DAY),'DDD')-trunc(C.CAL_DAY,'MM')+1)/(add_months(trunc(C.CAL_DAY,'YEAR')-1,12)-trunc(C.CAL_DAY,'YEAR')+1))*NVL(R.BP_VALUE,0)/10000 Tot_fee_AMT_Final
                    /*  by filtering sum(cnt_me_only_me_flg) can delete those rows which counted as brokerage but only contain Retirement Fundsnetowrk or MandI*/
                    ,NVL(r.cnt_me_only_me_flg,1) CNT_ME_ONLY_ME_FLG
                from
                    ( /* inner table a get all rows by the given date and dealer, with all bus_line */
                        select 
                            dlr_fee_type_key,dlr_key,invc_day_key,fund_key,bus_line,nvl(sum(B.INVC_AVG_ASSET),0) INVC_AVG_ASSET_sum,nvl(sum(B.TOT_FEE_AMT),0) TOT_FEE_AMT_sum
                        from 
                            HDM.DEALER_INVOICE b
                        where
                            /*
                             get rows needs to be updated or inserted. First get invc_day_key,dlr_fee_type_key,fund_key from those new rows based on row_strt_dttm input, then using these keys to 
                                get all possible new and old rows. since some bus_lines could affect different bus_line asset or fees calculation, so don't grep bus_line here
                           */
                            (B.INVC_day_key,fund_key,dlr_fee_type_key) in (select invc_day_key,fund_key,dlr_fee_type_key from HDM.DEALER_INVOICE where ROW_STRT_DTTM > p_in_date)
                        group by 
                            dlr_key,invc_day_key,fund_key,dlr_fee_type_key,bus_line
                        having 
                            B.DLR_KEY= p_in_dlr        
                    )a
                inner join hdm.fund f on F.FUND_KEY=a.fund_key
                inner join HDM.CALENDAR c on C.DAY_KEY=A.INVC_DAY_KEY
                left outer join HDM.DEALER_INVOICE_RULE r on R.DLR_KEY=a.dlr_key and R.SRC_BUS_LINE=a.bus_line and f.shr_cls_cd=R.SHR_CLS_CD
             /*------------------------------*/
             )i
        group by 
            i.dlr_fee_type_key,i.dlr_key,i.invc_day_key,i.fund_key,i.trgt_bus_line
        having      
            mod(sum( i.cnt_me_only_me_flg),1000)!=0
        ) src
    on (tgt.dlr_fee_type_key=src.dlr_fee_type_key and tgt.dlr_key=src.dlr_key and tgt.invc_day_key=src.invc_day_key and tgt.fund_key=src.fund_key and nvl(tgt.bus_line,'NULL')=NVL(src.bus_line,'NULL'))
    when matched then
        update set TGT.INVC_AVG_ASSET=src.INVC_AVG_ASSET_sum, TGT.TOT_FEE_AMT=src.tot_fee_amt_final, TGT.ROW_STRT_DTTM=sysdate
    when not matched then
        insert (DLR_INVC_MDL_KEY,dlr_fee_type_key,dlr_key,invc_day_key,fund_key,bus_line,INVC_AVG_ASSET,TOT_FEE_AMT,curr_row_flg,row_strt_dttm)
        values (HDM.SEQ_DEALER_INVOICE_MODEL.nextval,src.dlr_fee_type_key,src.dlr_key,src.invc_day_key,src.fund_key,src.bus_line,src.INVC_AVG_ASSET_sum,src.TOT_FEE_AMT_final,'Y',sysdate);
        
    commit;
end CALC_ALL_ASSET_FEE_tmp;


/*
procedure CALC_ALL_ASSET_FEE calculates assets and fees for all cases 
p_in_dlr : input dealer number
p_in_date: input date for row_strt_dttm
*/
PROCEDURE CALC_ALL_ASSET_FEE(p_in_dlr number,p_in_date date) IS   
BEGIN
    merge into HDM.DEALER_INVOICE_MODEL tgt
    using
        (
        select 
            i.dlr_fee_type_key,i.dlr_key,i.invc_day_key,i.fund_key,i.trgt_bus_line bus_line,sum(i.invc_avg_asset_final) invc_avg_asset_sum,sum(i.tot_fee_amt_final) tot_fee_amt_final, i.INVC_FREQ_FLG
        from  
            (
            /*--------------- table i ---------------*/
                select  
                    a.dlr_fee_type_key,a.dlr_key,a.invc_day_key,a.fund_key,
                    a.bus_line,R.BP_VALUE,R.CALC_FLG,R.FEE_USED_IN_FEE_FLG,F.SHR_CLS_CD,C.CAL_DAY,R.SRC_BUS_LINE,
                    NVL(R.TRGT_BUS_LINE,a.bus_line) trgt_bus_line,
                    /* if CALC_FLG is A, add the assets; if CALC_FLG is S, subtract the assets */
                    decode(R.CALC_FLG,'A',a.INVC_AVG_ASSET_sum,'S',-a.INVC_AVG_ASSET_sum,null,A.INVC_AVG_ASSET_sum) INVC_AVG_ASSET_final,
                    /* fees contains two parts:the first is fee part:if FEE_USED_IN_FEE_FLG is Y, need to add this fee. If it is N,no need to count in this fee, if it is null should add this fee
                    the second part is assets*bp*days_in_month/days_in_year. BP column is positive or negative in dealer_invoice_rule table.So no need to process the addition or subtraction of BP part */
                    decode(R.FEE_USED_IN_FEE_FLG,'Y',A.TOT_FEE_AMT_sum,'N',0,null,A.TOT_FEE_AMT_sum)+ a.invc_avg_asset_sum*((trunc(last_day(C.CAL_DAY),'DDD')-trunc(C.CAL_DAY,'MM')+1)/(add_months(trunc(C.CAL_DAY,'YEAR')-1,12)-trunc(C.CAL_DAY,'YEAR')+1))*NVL(R.BP_VALUE,0)/10000 Tot_fee_AMT_Final
                    /*  by filtering sum(cnt_me_only_me_flg) can delete those rows which counted as brokerage but only contain Retirement Fundsnetowrk or MandI*/
                    ,NVL(r.cnt_me_only_me_flg,1) CNT_ME_ONLY_ME_FLG, a.INVC_FREQ_FLG
                from
                    ( /* inner table a get all rows by the given date and dealer, with all bus_line */
                        select 
                            dlr_fee_type_key,dlr_key,invc_day_key,fund_key,bus_line,nvl(sum(B.INVC_AVG_ASSET),0) INVC_AVG_ASSET_sum,nvl(sum(B.TOT_FEE_AMT),0) TOT_FEE_AMT_sum, B.INVC_FREQ_FLG
                        from 
                            HDM.DEALER_INVOICE b
                        where
                            /*
                             get rows needs to be updated or inserted. First get invc_day_key,dlr_fee_type_key,fund_key from those new rows based on row_strt_dttm input, then using these keys to 
                                get all possible new and old rows. since some bus_lines could affect different bus_line asset or fees calculation, so don't grep bus_line here
                           */
                            (B.INVC_day_key,fund_key,dlr_fee_type_key) in (select invc_day_key,fund_key,dlr_fee_type_key from HDM.DEALER_INVOICE where ROW_STRT_DTTM > p_in_date)
                        group by 
                            dlr_key,invc_day_key,fund_key,dlr_fee_type_key,bus_line, B.INVC_FREQ_FLG
                        having 
                            B.DLR_KEY= p_in_dlr        
                    )a
                inner join hdm.fund f on F.FUND_KEY=a.fund_key and F.CURR_ROW_FLG='Y'
                inner join HDM.CALENDAR c on C.DAY_KEY=A.INVC_DAY_KEY and C.CURR_ROW_FLG='Y'
                --add effective start date and end date for dealer rule, so that more than one mapping(source business line---target business line mapping) can exist in the system
                left outer join HDM.DEALER_INVOICE_RULE r on R.DLR_KEY=a.dlr_key and R.SRC_BUS_LINE=a.bus_line and f.shr_cls_cd=R.SHR_CLS_CD and R.CURR_ROW_FLG='Y' and (c.cal_day between R.EFFTV_STRT_DT and R.EFFTV_END_DT)
             /*------------------------------*/
             )i
        group by 
            i.dlr_fee_type_key,i.dlr_key,i.invc_day_key,i.fund_key,i.trgt_bus_line, i.INVC_FREQ_FLG
        having      
            mod(sum( i.cnt_me_only_me_flg),1000)!=0
        ) src
    on (tgt.dlr_fee_type_key=src.dlr_fee_type_key and tgt.dlr_key=src.dlr_key and tgt.invc_day_key=src.invc_day_key and tgt.fund_key=src.fund_key and nvl(tgt.bus_line,'NULL')=NVL(src.bus_line,'NULL') and tgt.invc_freq_flg=src.invc_freq_flg)
    when matched then
        update set TGT.INVC_AVG_ASSET=src.INVC_AVG_ASSET_sum, TGT.TOT_FEE_AMT=src.tot_fee_amt_final,TGT.ROW_STRT_DTTM=sysdate
    when not matched then
        insert (DLR_INVC_MDL_KEY,dlr_fee_type_key,dlr_key,invc_day_key,fund_key,bus_line,INVC_AVG_ASSET,TOT_FEE_AMT,curr_row_flg,row_strt_dttm, invc_freq_flg)
        values (HDM.SEQ_DEALER_INVOICE_MODEL.nextval,src.dlr_fee_type_key,src.dlr_key,src.invc_day_key,src.fund_key,src.bus_line,src.INVC_AVG_ASSET_sum,src.TOT_FEE_AMT_final,'Y',sysdate,src.invc_freq_flg);
        
    commit;
end CALC_ALL_ASSET_FEE;

end PKG_DEALER_ASSET_FEE;

CREATE OR REPLACE package HDM.pkg_fiduciary_load as 

procedure main;
function nav_val (in_day_key in number, in_fund_key in number) return number;
function prior_mktval_fund(in_day_key in number, in_fid_cust_key in number, in_fund_key in number) return number;
function prior_mktval_mix(in_day_key in number, in_fid_cust_key in number, in_fid_asset_cls_key in number) return number;
function one_month_return(in_day_key in number, in_fund_key in number) return number;
function incptn_date ( in_fid_cust_key number, in_fid_asset_cls_key number) return date;

end pkg_fiduciary_load;

CREATE OR REPLACE PACKAGE BODY HDM.pkg_fiduciary_load is
/***********************************************************
PACKAGE:  pkg_fiduciary_load
WRITTEN BY: Igor Rakhman 

OVERVIEW: This package is designed to process custom performance reports for 
several clients of Harbor.  This data includes external account data for the clients,
in order to provide comprehensive client's performance values.

Business Unit: Financial Accounting Department

-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
***********************************************************/

procedure main is

load_date_key number;
v_perf_from_dt date;
v_perf_to_dt date;
v_perf_to_dt_key number;
v_rec_count number;
v_months number;
v_err_msg varchar2(250);
v_from_dt date;
v_fund_incp_dt date;
v_day_count number;
-- yes
NEW_INCP_DT_MIX EXCEPTION;

begin

-----------------------------------------------------------------------------------------------
--  FUND MIX PERFORMANCE CALCULATION --------------------------------------------
-----------------------------------------------------------------------------------------------


-- Determine one month fund return and fund's NAV value 

v_err_msg := 'Update monthly return and NAV values for funds';

update hdm.fiduciary_position fp
     set fp.nav = nav_val(day_key, fund_key),  
          fp.mo_ret =  one_month_return(day_key, fund_key)
where fp.curr_row_flg = 'Y'
    and fp.mo_ret is null;
    
v_err_msg := 'Update Market Value value for fund positions';
    
-- Calculate Market Value 
update hdm.fiduciary_position fp
     set fp.mkt_val = round(fp.nav * fp.shrs, 2)
where fp.curr_row_flg = 'Y'
    and fp.mkt_val is null;

v_err_msg := 'Create asset class records for funds';
    
-- Create mix records along with combined Market Value for Wealth Index calculation 
insert into hdm.fid_fund_wealth_index
(fid_wlth_idx_key, day_key, fid_asset_cls_key, fid_cust_key, mkt_val, curr_row_flg)
select seq_fid_fund_wi.nextval, day_key, fid_asset_cls_key, fid_cust_key, mkt_val, curr_row_flg from (
        select  fp.day_key, ac.fid_asset_cls_key, fp.fid_cust_key, sum(fp.mkt_val) mkt_val, 'Y' curr_row_flg
          from hdm.fiduciary_position fp, 
                  hdm.vw_fiduciary_fund f,
                  hdm.vw_fiduciary_asset_link al, 
                  hdm.vw_fiduciary_asset_class ac
         where ac.fid_asset_cls_key = al.fid_asset_cls_key
            and al.fund_key = fp.fund_key
            and f.fund_key = fp.fund_key
            and f.asset_cls_nm <> 'Other Investments'
            and fp.curr_row_flg = 'Y'
         group by fp.day_key, ac.fid_asset_cls_key, fp.fid_cust_key);
         
delete from hdm.fid_wealth_index_temp;

v_err_msg := 'Calculate wealth index values for asset classes bbb';

-- Determine mix performance and wealth index values by first creating the 
-- records in a temp table, and then updating the wealth index table records with
-- return and wealth index values          
insert into hdm.fid_wealth_index_temp
( fid_asset_cls_key, fid_cust_key, day_key, asset_ret, wlth_idx)
select al.fid_asset_cls_key,
                     fp.fid_cust_key,
                     fp.day_key,
                     sum(mo_ret * (prior_mktval_fund(day_key, fid_cust_key, fp.fund_key)/
                                            prior_mktval_mix(day_key, fid_cust_key, al.fid_asset_cls_key)
                                            )) , 
                     sum(mo_ret * (prior_mktval_fund(day_key, fid_cust_key, fp.fund_key)/
                                            prior_mktval_mix(day_key, fid_cust_key, al.fid_asset_cls_key)
                                            )) + 1 
                      from hdm.vw_fiduciary_position fp,
                              hdm.vw_fiduciary_asset_link al
                    where fp.fund_key = al.fund_key
                        and curr_row_flg = 'Y'
                 group by al.fid_asset_cls_key,
                              fp.fid_cust_key,
                              fp.day_key;                                   

v_err_msg := 'Update wealth index asset class values with calculated wealth index values';
        
update hdm.fid_fund_wealth_index wi
     set (asset_ret, wlth_idx) = 
          (select asset_ret, wlth_idx from hdm.fid_wealth_index_temp wi_temp
           where wi_temp.day_key = wi.day_key
               and wi_temp.fid_cust_key = wi.fid_cust_key
               and wi_temp.fid_asset_cls_key = wi.fid_asset_cls_key)
where wi.curr_row_flg = 'Y';  
 
    
--  Calculate mix performance ------------------------------------------------------------

v_err_msg := 'Check to see if there are breaks in the history for perf calculation';

-- Check to see if a mix was discontinued (assets sold off) or if a "break" was
-- intriduced within past three months 
select count(*) 
   into v_rec_count
  from fid_client_asset_mix am
where (fid_cust_key, fid_asset_cls_key) not in (
                        select fid_cust_key, fid_asset_cls_key from (
                        select fid_cust_key, fid_asset_cls_key, count(*) rec_count
                          from hdm.vw_fid_fund_wealth_index wi
                        where curr_row_flg = 'Y'
                        group by fid_cust_key, fid_asset_cls_key
                        having count(*) = 3));
                       
-- If there are records, stop processing and raise an error 
if v_rec_count > 0 then 
 
       v_err_msg := v_rec_count || ' Stop processing because the gap was found';
       
       RAISE NEW_INCP_DT_MIX;
       
end if;

-- Determine the performance "to" date 
select max(c.cal_day)
  into v_perf_to_dt
  from hdm.vw_fiduciary_position fp, hdm.vw_calendar c
where fp.day_key = c.day_key
    and fp.curr_row_flg = 'Y';
    
-- Determine the performance "to" date key value (used to insert into perf table)     
select day_key
into v_perf_to_dt_key
from hdm.vw_calendar 
where cal_day = v_perf_to_dt;
    
-- Loop through return type records, and create performance records for the mix. 
-- During the calculation, determine "from" dates, and check to make sure 
-- there are no breaks from the "inception" date.  

v_err_msg := 'Calculate performance values for funds';

for return_rec in (
       select * from hdm.vw_return_periods
       where per_nm in ('QTR','YTD','YR1','YR2','YR3','YR5','YR10','INCP')
       )
loop 
   
    case return_rec.per_nm
        when 'QTR' then v_months := 3;
        when 'YTD' then select months_between(v_perf_to_dt + 1, trunc(v_perf_to_dt, 'YEAR')) 
                                    into v_months
                                   from dual;
        when 'YR1' then v_months := 12;
        when 'YR2' then v_months := 24;
        when 'YR3' then v_months := 36;
        when 'YR5' then v_months := 60;
        when 'YR10' then v_months := 120;
        else  v_months := 0;
    end case;               
                          
   if (return_rec.per_nm <> 'INCP') then

    select last_day(add_months(v_perf_to_dt, - v_months) + 1)
       into v_perf_from_dt
      from dual;
                        
      -- Calculate performance and insert records into the performance table.  
       insert into hdm.fid_fund_performance
                      (fid_fund_perf_key, day_key, fid_asset_cls_key, fid_cust_key, per_key, annlzn_fctr, strt_dt, fund_ret, curr_row_flg)
       select seq_fid_fund_perf.nextval,  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt, perf, 'Y' 
         from (
                      select  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt, 
                                  power(exp(sum(ln(wlth_idx))), 1/return_rec.annl_fctr) - 1 perf
                          from hdm.vw_fid_fund_wealth_index wi, hdm.vw_calendar c
                        where c.day_key = wi.day_key
                            and c.cal_day between v_perf_from_dt and v_perf_to_dt
                            and (wi.fid_asset_cls_key, wi.fid_cust_key) not in (
                                              select fid_asset_cls_key, fid_cust_key from (
                                                          select wi2.fid_asset_cls_key, wi2.fid_cust_key, count(*) 
                                                            from hdm.vw_fid_fund_wealth_index wi2, hdm.vw_calendar c2
                                                           where wi2.day_key = c2.day_key
                                                             and c2.cal_day between v_perf_from_dt and v_perf_to_dt
                                                            group by wi2.fid_asset_cls_key, wi2.fid_cust_key
                                                            having count(*) < v_months))
                        group by  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt
                  );
        
   else

                       insert into hdm.fid_fund_performance
                                      (fid_fund_perf_key, day_key, fid_asset_cls_key, fid_cust_key, per_key, annlzn_fctr, strt_dt, fund_ret, curr_row_flg)
                       select seq_fid_fund_perf.nextval,  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, incpt_dt, perf, 'Y' 
                         from (
                                      select  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, months_between(v_perf_to_dt, incptn_date(wi.fid_cust_key, wi.fid_asset_cls_key)) / 12,  incptn_date(wi.fid_cust_key, wi.fid_asset_cls_key) incpt_dt, 
                                                  power(exp(sum(ln(wlth_idx))), 1/((months_between(v_perf_to_dt, incptn_date(wi.fid_cust_key, wi.fid_asset_cls_key))+1) / 12)) - 1 perf
                                          from hdm.vw_fid_fund_wealth_index wi, hdm.vw_calendar c
                                        where c.day_key = wi.day_key
                                            and c.cal_day between incptn_date(wi.fid_cust_key, wi.fid_asset_cls_key) and v_perf_to_dt
                                        group by  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt
                                  );
                                  
   end if;
                                                
end loop;

-----------------------------------------------------------------------------------------------
--  BENCHMARK MIX PERFORMANCE CALCULATION ------------------------------------
-----------------------------------------------------------------------------------------------

v_err_msg := 'Create benchmark asset class records';

-- Create benchmark position records.  Using Merge to avoid inserting external 
-- benchmark records which have been loaded via external ETL load (data 
-- provided by the business.
merge into hdm.fid_benchmark_position bp
using (
            select fp.fid_cust_key, fp.day_key, fp.fund_key, ir.bmk_idx_key
              from hdm.fiduciary_position fp,
                      hdm.vw_fid_fund_index_rltn ir
            where fp.curr_row_flg = 'Y'
                and fp.fund_key = ir.fund_key
                and ir.bmk_idx_key <> 10000001
                ) fp
on  (       bp.day_key = fp.day_key
        and bp.fund_key = fp.fund_key
        and bp.fid_cust_key = bp.fid_cust_key)
when not matched then 
          insert (bp.bmk_posn_key, bp.fid_cust_key, bp.day_key, bp.fund_key, bp.bmk_idx_key, curr_row_flg)
          values (seq_fid_bm_pos.nextval, fp.fid_cust_key, fp.day_key, fp.fund_key, fp.bmk_idx_key, 'Y');

v_err_msg := 'Determine monthly return for benchmark records';

-- Update monthly return for the loaded benchmark records.
update hdm.fid_benchmark_position bp
set mo_ret = (
select  distinct ir.idx_ret
  from hdm.vw_index_return ir,
          hdm.vw_return_periods rp
where rp.per_key = ir.per_key
    and rp.per_nm = '1MO'
    and ir.bmk_idx_key = bp.bmk_idx_key
    and ir.day_key = bp.day_key
)
where bp.curr_row_flg = 'Y'
    and mo_ret is null;
    
-- Determine benchmark's mix wealth index 
-- Because in the difference in calculation, this is done separately for each client.

-- TBP ----

-- Create mix records with mix return and their wealth index.  
insert into hdm.fid_bm_wealth_index (
  BMK_WLTH_IDX_KEY,
  DAY_KEY,
  FID_ASSET_CLS_KEY,
  FID_CUST_KEY,
  ASSET_RET,
  WLTH_IDX,
  CURR_ROW_FLG)
select seq_fid_bm_wi.nextval, day_key, fid_asset_cls_key, fid_cust_key, asset_ret, wi, 'Y'
from (
select  benchmark_data.day_key,
          benchmark_data.fid_asset_cls_key ,
          benchmark_data.fid_cust_key,
          sum(benchmark_data.mo_ret * bw.bmk_wgt) asset_ret,
          sum(benchmark_data.mo_ret * bw.bmk_wgt) + 1 wi
from
(
select bp.day_key, f.fund_key, ac.fid_asset_cls_key, bp.fid_cust_key, bi.ext_bmk_idx_key, c.cal_day, f.fund_nm, bi.idx_nm, ac.fid_asset_cls_desc, mo_ret
from hdm.vw_fid_benchmark_position bp,
        hdm.vw_fiduciary_benchmark_index bi,
        hdm.vw_calendar c,
        hdm.vw_fiduciary_fund f,
        hdm.vw_fiduciary_asset_link al,
        hdm.vw_fiduciary_asset_class ac
where 1=1
    and bp.day_key = c.day_key
    and bp.fund_key = al.fund_key
    and bp.fund_key = f.fund_key
    and bp.bmk_idx_key = bi.ext_bmk_idx_key
    and ac.fid_asset_cls_key = al.fid_asset_cls_key
    and bp.fid_cust_key = 1
    and bp.curr_row_flg = 'Y'
) benchmark_data,
hdm.vw_fid_benchmark_weight bw
where benchmark_data.fund_key = bw.fund_key
    and benchmark_data.ext_bmk_idx_key = bw.bmk_idx_key
    and benchmark_data.fid_cust_key = bw.fid_cust_key
    and benchmark_data.cal_day between bw.efftv_strt_dt and nvl(efftv_end_dt, '31-DEC-9999')
    and benchmark_data.fid_asset_cls_key = bw.fid_asset_cls_key
group by benchmark_data.day_key,
             benchmark_data.fid_asset_cls_key,
             benchmark_data.fid_cust_key);
             
-- GERSHEIMER ---------------

-- Create mix records with mix return and their wealth index.  
insert into hdm.fid_bm_wealth_index (
  BMK_WLTH_IDX_KEY,
  DAY_KEY,
  FID_ASSET_CLS_KEY,
  FID_CUST_KEY,
  ASSET_RET,
  WLTH_IDX,
  CURR_ROW_FLG)
select seq_fid_bm_wi.nextval, day_key, fid_asset_cls_key, fid_cust_key, mix_ret, mix_wi, 'Y'
from(
select bp.day_key,  al.fid_asset_cls_key, bp.fid_cust_key,
         sum( bp.mo_ret * (pkg_fiduciary_load.prior_mktval_fund(bp.day_key, bp.fid_cust_key, bp.fund_key) / 
                                     pkg_fiduciary_load.prior_mktval_mix(bp.day_key, bp.fid_cust_key, al.fid_asset_cls_key))) mix_ret,
         sum( bp.mo_ret * (pkg_fiduciary_load.prior_mktval_fund(bp.day_key, bp.fid_cust_key, bp.fund_key) / 
                                     pkg_fiduciary_load.prior_mktval_mix(bp.day_key, bp.fid_cust_key, al.fid_asset_cls_key))) + 1 mix_wi
  from hdm.vw_fid_benchmark_position bp,
          hdm.vw_fiduciary_fund f,
          hdm.vw_fiduciary_benchmark_index bi,
          hdm.vw_fiduciary_asset_link al,
          hdm.vw_fiduciary_asset_class ac
where 1=1
    and f.fund_key = bp.fund_key
    and bi.ext_bmk_idx_key = bp.bmk_idx_key
    and al.fund_key = bp.fund_key
    and ac.fid_asset_cls_key = al.fid_asset_cls_key
    and fid_cust_key = 2
    and bp.curr_row_flg = 'Y'
    group by bp.day_key,  al.fid_asset_cls_key, bp.fid_cust_key
    );          

-- Calculate Benchmark Performance --------------------------------------------------

v_err_msg := 'Calculate performance values for benchmarks';

for return_rec in (
       select * from hdm.vw_return_periods
       where per_nm in ('QTR','YTD','YR1','YR2','YR3','YR5','YR10','INCP')
       )
loop 
 
    case return_rec.per_nm
        when 'QTR' then v_months := 3;
        when 'YTD' then select months_between(v_perf_to_dt + 1, trunc(v_perf_to_dt, 'YEAR')) 
                                    into v_months
                                   from dual;
        when 'YR1' then v_months := 12;
        when 'YR2' then v_months := 24;
        when 'YR3' then v_months := 36;
        when 'YR5' then v_months := 60;
        when 'YR10' then v_months := 120;
        else  v_months := 0;
    end case;               

                             
   if (return_rec.per_nm <> 'INCP') then

    select last_day(add_months(v_perf_to_dt, - v_months) + 1)
       into v_perf_from_dt
      from dual;
                       
      -- Calculate performance and insert records into the performance table.  
       insert into hdm.fid_benchmark_performance
                      (fid_bmk_perf_key, day_key, fid_asset_cls_key, fid_cust_key, per_key, annlzn_fctr, strt_dt, bmk_ret, curr_row_flg)
       select seq_fid_bm_perf.nextval,  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt, perf, 'Y' 
         from (
                      select  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt, 
                                  power(exp(sum(ln(wlth_idx))), 1/return_rec.annl_fctr) - 1 perf
                          from hdm.vw_fid_bm_wealth_index wi, hdm.vw_calendar c
                        where c.day_key = wi.day_key
                            and c.cal_day between v_perf_from_dt and v_perf_to_dt
                            and (wi.fid_asset_cls_key, wi.fid_cust_key) not in (
                                              select fid_asset_cls_key, fid_cust_key from (
                                                         select wi2.fid_asset_cls_key, wi2.fid_cust_key, count(*) 
                                                            from hdm.vw_fid_bm_wealth_index wi2, hdm.vw_calendar c2
                                                           where wi2.day_key = c2.day_key
                                                              and c2.cal_day between v_perf_from_dt and v_perf_to_dt
                                                            group by wi2.fid_asset_cls_key, wi2.fid_cust_key
                                                            having count(*) < v_months))
                        group by  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt
                  );
        
   else

                       insert into hdm.fid_benchmark_performance
                                     (fid_bmk_perf_key, day_key, fid_asset_cls_key, fid_cust_key, per_key, annlzn_fctr, strt_dt, bmk_ret, curr_row_flg)
                         select seq_fid_bm_perf.nextval,  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, incpt_dt, perf, 'Y' 
                         from (
                                      select  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, months_between(v_perf_to_dt, incptn_date(wi.fid_cust_key, wi.fid_asset_cls_key)) / 12,  incptn_date(wi.fid_cust_key, wi.fid_asset_cls_key) incpt_dt, 
                                                  power(exp(sum(ln(wlth_idx))), 1/((months_between(v_perf_to_dt, incptn_date(wi.fid_cust_key, wi.fid_asset_cls_key))+1) / 12)) - 1 perf
                                          from hdm.vw_fid_bm_wealth_index wi, hdm.vw_calendar c
                                        where c.day_key = wi.day_key
                                            and c.cal_day between incptn_date(wi.fid_cust_key, wi.fid_asset_cls_key) and v_perf_to_dt
                                        group by  v_perf_to_dt_key, fid_asset_cls_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt
                                  );
                                  
   end if;
                                                
end loop;


-------- Calculate individual fund performance ------------------------------------------

-- Create wealth index records for "external funds" records that were added.
insert into hdm.fid_fund_level_wealth_index
(
  fund_lvl_wlth_idx_key,
  day_key,
  fund_key,
  fid_cust_key,
  wlth_idx,
  curr_row_flg
)
select seq_fid_fund_level_wi.nextval, day_key, fund_key, fid_cust_key, wi, curr_row_flg from (
select day_key,
         fund_key,
         fid_cust_key, 
         mo_ret + 1 wi,
         'Y' curr_row_flg
from hdm.fiduciary_position fp, hdm.vw_external_fund f
where fp.curr_row_flg = 'Y'
   and f.ext_fund_key = fp.fund_key
   and f.asset_cls_nm <> 'Other Investments'
   and mo_ret is not null
    and fp.fund_key in (select ext_fund_key from hdm.fid_external_fund)
    );
    
-- Create wealth index records for external funds benchmarks

insert into hdm.fid_bm_level_wealth_index
(
  bm_lvl_wlth_idx_key,
  day_key,
  fund_key,
  bmk_idx_key,
  fid_cust_key,
  wlth_idx,
  curr_row_flg
)
select seq_fid_bm_level_wi.nextval, day_key, fund_key, bmk_idx_key, fid_cust_key, wi, curr_row_flg from (
select day_key,
         fund_key,
         bmk_idx_key,
         fid_cust_key, 
         mo_ret + 1 wi,
         'Y' curr_row_flg
from hdm.fid_benchmark_position fp, hdm.vw_external_fund f
where fp.curr_row_flg = 'Y'
   and f.ext_fund_key = fp.fund_key
   and f.asset_cls_nm <> 'Other Investments'
   and mo_ret is not null
    and fp.fund_key in (select ext_fund_key from hdm.fid_external_fund)
    );
    

-- Create performance records for individual funds -------------------------------------

-- Loop through customer / fund combinations with active records to calculate 
-- returns.

for cust_fund_rec in (
     select distinct fid_cust_key, fund_key
       from hdm.fid_fund_level_wealth_index wi
     where curr_row_flg = 'Y'
     )
loop

    -- What is the earliest data available for this fund / customer data set?  
    select min(c.cal_day) 
       into v_from_dt
    from hdm.vw_calendar c, 
            hdm.fid_fund_level_wealth_index wi
   where wi.day_key = c.day_key
       and wi.fid_cust_key = cust_fund_rec.fid_cust_key
       and wi.fund_key = cust_fund_rec.fund_key;
       
       dbms_output.put_line('v_from_dt : ' || v_from_dt);
       
       -- Check to make sure there are no gaps 
       select max(cal_day)
       into v_fund_incp_dt
        from (
                  select c3.cal_day, wi_data.cal_day wi_day
                    from                                                    
                       (select c4.cal_day from hdm.fid_fund_level_wealth_index wi3, hdm.vw_calendar c4
                        where c4.day_key = wi3.day_key) wi_data,
                        hdm.vw_calendar c3
                  where wi_data.cal_day (+) = c3.cal_day 
                     and c3.cal_mo_end_flg = 'Y'
                     and c3.cal_day between v_from_dt and  v_perf_to_dt  
                     )
                     where wi_day is null;
                  
       if v_fund_incp_dt is not null 
       then v_from_dt := v_fund_incp_dt;
       end if;

    -- How many months worth of data does the customer have for this fund?
    select count(wi.day_key) 
        into v_day_count
      from hdm.fid_fund_level_wealth_index wi, 
              hdm.vw_calendar c
    where wi.day_key = c.day_key
        and wi.fid_cust_key = cust_fund_rec.fid_cust_key
        and wi.fund_key = cust_fund_rec.fund_key
        and c.cal_day between v_from_dt and v_perf_to_dt;
                          
     -- Calculate performance 
     
     v_err_msg := 'Calculate performance values for individual funds';

        for return_rec in (
               select * from hdm.vw_return_periods
               where per_nm in ('QTR','YTD','YR1','YR2','YR3','YR5','YR10','INCP')
               )
        loop 
         
            case return_rec.per_nm
                when 'QTR' then v_months := 3;
                when 'YTD' then select months_between(v_perf_to_dt + 1, trunc(v_perf_to_dt, 'YEAR')) 
                                            into v_months
                                           from dual;
                when 'YR1' then v_months := 12;
                when 'YR2' then v_months := 24;
                when 'YR3' then v_months := 36;
                when 'YR5' then v_months := 60;
                when 'YR10' then v_months := 120;
                else  v_months := 0;
            end case;               

                                     
           if (return_rec.per_nm <> 'INCP' and (v_months <= v_day_count)) then

            select last_day(add_months(v_perf_to_dt, - v_months) + 1)
               into v_perf_from_dt
              from dual;
                              
              -- Calculate performance and insert records into the performance table.  
              insert into hdm.fid_fund_level_performance
                             (fund_lvl_perf_key, day_key, fund_key, fid_cust_key, per_key,  annlzn_fctr,  strt_dt, incpn_dt, curr_row_flg, fund_ret)
              select seq_fid_fund_level_perf.nextval, v_perf_to_dt_key, fund_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt, v_from_dt, 'Y', perf
                from 
                        (
                             select v_perf_to_dt_key, fund_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt,  
                                      power(exp(sum(ln(wlth_idx))), 1/return_rec.annl_fctr) - 1 perf
                               from hdm.fid_fund_level_wealth_index wi, hdm.vw_calendar c
                                where c.day_key = wi.day_key
                                    and c.cal_day between v_perf_from_dt and v_perf_to_dt
                                    and wi.fid_cust_key = cust_fund_rec.fid_cust_key
                                    and wi.fund_key = cust_fund_rec.fund_key
                              group by v_perf_to_dt_key, fund_key, fid_cust_key, return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt
                        );
                  
               -- Calculate benchmark performance 
               insert into hdm.fid_bm_level_performance perf
                              (bm_lvl_perf_key, fund_key, bmk_idx_key, day_key, fid_cust_key, per_key, annlzn_fctr, strt_dt, incpn_dt, curr_row_flg, bmk_ret)
              select seq_fid_bm_level_perf.nextval, fund_key, bmk_idx_key, v_perf_to_dt_key, fid_cust_key, per_key, annl_fctr, v_perf_from_dt, v_from_dt, 'Y', perf
                 from 
                        (
                            select fund_key, bmk_idx_key, v_perf_to_dt_key, fid_cust_key,  return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt,
                                     power(exp(sum(ln(wlth_idx))), 1/return_rec.annl_fctr) - 1 perf
                              from hdm.fid_bm_level_wealth_index wi, hdm.vw_calendar c
                                where c.day_key = wi.day_key
                                    and c.cal_day between v_perf_from_dt and v_perf_to_dt
                                    and wi.fid_cust_key = cust_fund_rec.fid_cust_key
                                    and wi.fund_key = cust_fund_rec.fund_key
                              group by fund_key, bmk_idx_key, v_perf_to_dt_key, fid_cust_key,  return_rec.per_key, return_rec.annl_fctr, v_perf_from_dt
                        );

           else 
                   if (return_rec.per_nm = 'INCP' ) then 
                   
                   -- Calculate fund since inception performance
                   insert into hdm.fid_fund_level_performance
                             (fund_lvl_perf_key, day_key, fund_key, fid_cust_key, per_key,  annlzn_fctr,  strt_dt, incpn_dt, curr_row_flg, fund_ret)
                   select seq_fid_fund_level_perf.nextval, v_perf_to_dt_key, fund_key, fid_cust_key, return_rec.per_key, v_day_count/12, v_fund_incp_dt, v_from_dt, 'Y', perf        
                    from 
                        (
                             select v_perf_to_dt_key, fund_key, fid_cust_key, return_rec.per_key, v_day_count/12, v_perf_from_dt,  
                                      power(exp(sum(ln(wlth_idx))), 1/(v_day_count/12)) - 1 perf
                               from hdm.fid_fund_level_wealth_index wi, hdm.vw_calendar c
                                where c.day_key = wi.day_key
                                    and c.cal_day between v_from_dt and v_perf_to_dt
                                    and wi.fid_cust_key = cust_fund_rec.fid_cust_key
                                    and wi.fund_key = cust_fund_rec.fund_key
                              group by v_perf_to_dt_key, fund_key, fid_cust_key, return_rec.per_key, v_day_count/12, v_perf_from_dt
                        );

                   -- Calculate benchmark since inception performance 
                   insert into hdm.fid_bm_level_performance perf
                              (bm_lvl_perf_key, fund_key, bmk_idx_key, day_key, fid_cust_key, per_key, annlzn_fctr, strt_dt, incpn_dt, curr_row_flg, bmk_ret)
                   select seq_fid_bm_level_perf.nextval, fund_key, bmk_idx_key, v_perf_to_dt_key, fid_cust_key, per_key, v_day_count/12, v_perf_from_dt, v_from_dt, 'Y', perf
                    from 
                        (
                            select fund_key, bmk_idx_key, v_perf_to_dt_key, fid_cust_key,  return_rec.per_key, v_day_count/12, v_perf_from_dt,
                                     power(exp(sum(ln(wlth_idx))), 1/(v_day_count/12)) - 1 perf
                              from hdm.fid_bm_level_wealth_index wi, hdm.vw_calendar c
                                where c.day_key = wi.day_key
                                    and c.cal_day between v_from_dt and v_perf_to_dt
                                    and wi.fid_cust_key = cust_fund_rec.fid_cust_key
                                    and wi.fund_key = cust_fund_rec.fund_key
                              group by fund_key, bmk_idx_key, v_perf_to_dt_key, fid_cust_key,  return_rec.per_key, v_day_count/12, v_perf_from_dt
                        );
                        
                  end if;
                                          
           end if;
                                                        
        end loop;

end loop;    
                         

commit;
     
EXCEPTION

        when NEW_INCP_DT_MIX then 
                dbms_output.put_line('Error Occurred here: ' || v_err_msg);
                ROLLBACK;
                RAISE;
        when OTHERS then
                dbms_output.put_line('Error Occurred here: ' || v_err_msg);
                ROLLBACK;
                RAISE; 

end main;


function incptn_date ( in_fid_cust_key number, in_fid_asset_cls_key number) return date is
/*---------------------------------------------------------------------------------------------
FUNCTION: incptn_date
WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns inception date for a given client for a given 
                  mix.

Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment               
-----------------------------------------------------------------------------------------------*/

v_inception_date date;

begin

select incpn_dt 
  into v_inception_date
  from hdm.fid_client_asset_mix 
where fid_asset_cls_key = in_fid_asset_cls_key
    and fid_cust_key = in_fid_cust_key;
    
return v_inception_date;

end incptn_date;

function nav_val (in_day_key in number, in_fund_key in number) return number IS
/*---------------------------------------------------------------------------------------------
FUNCTION: nav_val
WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns NAV value for a given day for a given fund.

Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment               
-----------------------------------------------------------------------------------------------*/

v_nav_val number;

begin

select hdm.vw_fund_valuation.pr_per_shr
   into v_nav_val
  from hdm.vw_fund_valuation
where hdm.vw_fund_valuation.fund_key = in_fund_key
    and hdm.vw_fund_valuation.day_key = in_day_key;
    
return v_nav_val;


end nav_val;


function one_month_return(in_day_key in number, in_fund_key in number) return number IS
/*---------------------------------------------------------------------------------------------
FUNCTION: one_month_return
WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns one month return value for a given day and fund. 
 
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment               
-----------------------------------------------------------------------------------------------*/

v_one_month_return number;

begin

-- Determine market value of the prior day 
select fr.fund_ret
   into v_one_month_return
  from hdm.vw_fund_return fr
where fr.fund_key = in_fund_key
    and fr.day_key = in_day_key
    and fr.per_key = (select per_key from hdm.vw_return_periods where per_nm = '1MO');

return v_one_month_return;

end one_month_return;


function prior_mktval_fund(in_day_key in number, in_fid_cust_key in number, in_fund_key in number) return number IS
/*---------------------------------------------------------------------------------------------
FUNCTION: prior_mktval_fund 
WRITTEN BY: Igor Rakhman 

OVERVIEW: This function looks up prior date's market value for a given client,
for a given fund.  Used in the wealth index calculation.

Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment               
-----------------------------------------------------------------------------------------------*/

v_mrkt_val number;

begin

-- Determine market value of the prior day 
select mkt_val
  into v_mrkt_val
from hdm.vw_fiduciary_position fp, hdm.vw_calendar c
where fp.day_key = c.day_key
    and fund_key = in_fund_key
    and fid_cust_key = in_fid_cust_key
    and c.cal_day = (                                                                       
            select max(cal_day) from hdm.vw_fiduciary_position fp2, hdm.vw_calendar c2
            where fp2.day_key = c2.day_key
                and c2.cal_day < (select cal_day from hdm.vw_calendar
                                         where day_key = in_day_key));

return v_mrkt_val;

end prior_mktval_fund;

function prior_mktval_mix(in_day_key in number, in_fid_cust_key in number, in_fid_asset_cls_key in number) return number IS
/*---------------------------------------------------------------------------------------------
FUNCTION: prior_mktval_mix
WRITTEN BY: Igor Rakhman 

OVERVIEW: This function looks up prior date's market value for a given client,
for a given mix.  Used in the wealth index calculation.

Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment               
-----------------------------------------------------------------------------------------------*/

v_mrkt_val number;

begin

-- Determine market value of the prior day 
select mkt_val + nvl(mkt_val_adj, 0)
    into v_mrkt_val
  from hdm.vw_fid_fund_wealth_index wi, hdm.vw_calendar c
where wi.day_key = c.day_key
    and wi.fid_asset_cls_key = in_fid_asset_cls_key
    and wi.fid_cust_key = in_fid_cust_key
    and c.cal_day = (                                                                       
            select max(cal_day) from hdm.vw_fid_fund_wealth_index wi2, hdm.vw_calendar c2
            where wi2.day_key = c2.day_key
                and c2.cal_day < (select cal_day from hdm.vw_calendar
                                         where day_key = in_day_key)); 
                                         
return v_mrkt_val;

end prior_mktval_mix;


END pkg_fiduciary_load;

CREATE OR REPLACE package HDM.pkg_foreign_ownership as 

function threshold_breach_ind (prct_value in number, threshold_crty_key in number, int_ext_ind char) return char;
function threshold_country_type(in_thrs_crty_key in number)  return number;
function threshold_interval (in_thrs_crty_key in number)  return number;
function prior_day_breach_ind(in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_fund_compst_key in number, in_secr_key in number, in_prior_day_key number) return char;
function threshold_country_name (in_thrs_crty_key in number)  return varchar2;
function prior_day_percent(in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_fund_compst_key in number, in_secr_key in number, in_prior_day_key number)  return number;
function  threshold_country(inc_crty_key in number, iss_crty_key in number, trde_crty_key in number) return number;
function threshold_value_breached(prct_value in number, threshold_crty_key in number ) return number;
function new_breach(in_rec_type in char, in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_thrs_crty_key in number, in_fund_compst_key in number, in_secr_key in number, in_prc_shrs in number ,
                               in_prior_day_key number, in_breach_ind in char, in_thrsh_br in number, load_date_key in number) return char;
function country_trigger (threshold_crty_key in number, trigger_type in char)  return number;
procedure main(p_date_load in date);

end pkg_foreign_ownership;

CREATE OR REPLACE PACKAGE BODY HDM.pkg_foreign_ownership is
/***********************************************************
PACKAGE:  Foreign Ownership 
WRITTEN BY: Igor Rakhman 

OVERVIEW: This package is designed to process Harbor's holdings data to 
help determine percent of ownership, and daily change of percent of ownership
of various securities as required for foreign filings if the percent breaches a certain 
threshold level.  

Business Unit: Legal and Financial Accounting Departments 
 
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
***********************************************************/

procedure main (p_date_load in date) is
 
   load_date date := p_date_load; -- latest date loaded
   load_date_key number; 
   prior_day date; -- day prior to last loaded 
   prior_day_key number; 
      
   holding_pos_key number;
   threshold_ctry_key number;
   
   rec_count number;
   DATE_LOADED EXCEPTION;
   err_msg varchar2(200);
   status_msg varchar2(300);
    
   spec_alert_text varchar2(250);
   
begin

-- Get last date loaded  
select day_key into load_date_key from hdm.calendar where cal_day = load_date;

-- Get date prior to last load 
select max(c.cal_day) into prior_day  from hdm.holdings_foreign_ownership hfo, hdm.calendar c where hfo.day_key = c.day_key and c.cal_day <> load_date;

select day_key into prior_day_key from hdm.calendar where cal_day = prior_day;

-- Check if data for this date has already been loaded.  If so, raise an error
-- and stop processing to avoid re-inserting same records twice.
select count(1) into rec_count  
  from hdm.holdings_foreign_ownership hfo
where hfo.day_key = load_date_key;

status_msg := 'Raise error if data has already been loaded';
  
case 
    when rec_count > 0 then RAISE DATE_LOADED;
       else rec_count := 0;
end case;

status_msg := 'Insert raw records into the foreign ownership table';
  
-- Insert raw records for the load date into the threshold foreign ownership table 
insert into hdm.holdings_foreign_ownership
( holdg_open_posn_key,
  fund_compst_key,
  secr_key,
  inc_crty_key,
  iss_crty_key,
  trde_crty_key,
  day_key,
  tot_long_posn,
  actl_cstdy_posn,
  pend_sale_qty,
  pend_buy_qty,
  free_qty,
  oshr_qty,
  par_shr_qty,
  tot_shrt_posn,
  on_loan_qty,
  new_secr_flg,
  all_cls_oshr_qty,
  all_voting_oshr_qty,
  all_non_voting_oshr_qty,
  voting_shr_ind,
  curr_row_flg,
  row_strt_dttm,
  row_stop_dttm,
  etl_load_cyc_key,
  src_sys_id,
  adr_spsr_ship_type
  )
  select holdg_open_posn_key,
           fund_compst_key,
           secr_key,
           inc_crty_key,
           iss_crty_key,
           trde_crty_key,
           day_key,
           tot_long_posn,
           actl_cstdy_posn,
           pend_sale_qty,
           pend_buy_qty,
           free_qty,
           oshr_qty,
           par_shr_qty,
           tot_shrt_posn,
           on_loan_qty,
           new_secr_flg,
           all_cls_oshr_qty * 1000000,
           all_voting_oshr_qty,
           all_non_voting_oshr_qty,
           voting_shr_ind,
           curr_row_flg,
           row_strt_dttm,
           row_stop_dttm,
           etl_load_cyc_key,
           src_sys_id,
           adr_spsr_ship_type
   from hdm.holdings_open_position op
 where op.day_key = load_date_key
    and  op.fund_compst_key in (select fund_compst_key from hdm.fund_composite
                                             where asset_cls_nm in ('Domestic','International and Global') )
     and op.secr_key in 
     (select secr_key from ( SELECT DISTINCT s."SECR_KEY",
                   s."SEDOL_ID",
                   s."ISIN_ID",
                   s."TCKR_SYM",
                   s."SECR_NM",
                   s."ISSUER_NM",
                   s."GLBL_ISSUER_NM",
                   s."INVMT_TYPE_CD",
                   s."INVMT_TYPE_DESC",
                   s."DTC_CUSIP",
                   s."ASSET_ID",
                   s."MUNCPL_SECR_ST_CD",
                   s."MUNCPL_SECR_ST",
                   s."CURR_ROW_FLG",
                   sa.asset_grp_cd,
                   sa.asset_grp_desc
     FROM hdm.vw_security s,
          hdm.vw_security_attributes sa,
          hdm.vw_fund_holdings h,
          hdm.vw_calendar c
    WHERE     s.secr_key = h.secr_key
          AND h.secr_attr_key = sa.secr_attr_key
          AND h.day_key = c.day_key
          AND c.cal_day = load_date
                            ) 
             where asset_grp_desc = 'Stock');

status_msg := 'Update threshold country value and issuer name';
  
-- Determine Threshold Country value and issuer name 
update hdm.holdings_foreign_ownership hfo 
     set thrs_crty_key = threshold_country(inc_crty_key, iss_crty_key, trde_crty_key),
          issuer_nm = (select issuer_nm 
                                from hdm.vw_security sl
                              where sl.secr_key = hfo.secr_key)
 where hfo.day_key = load_date_key;

status_msg := 'Update issuer name for records where issuer name was not determined';
  
-- Update issuer name for records with no Issuer name found 
update hdm.holdings_foreign_ownership hfo
set issuer_nm = (select 'zNO_ISSUER ' || SL.SECR_NM 
                           from hdm.vw_security sl
                         where sl.secr_key = hfo.secr_key)
 where hfo.day_key = load_date_key
     and hfo.issuer_nm is null; 

------------------------------------------------------------------------------------------------                                              
-- Create Issuer summary records 
delete from hdm.holdings_position_temp;

status_msg := 'Insert records into the temp position table';
  
insert into hdm.holdings_position_temp
        (day_key, issuer_nm, secr_key, thrs_crty_key, par_shr_qty, oshr_qty, all_cls_oshr_qty, all_voting_oshr_qty, all_non_voting_oshr_qty, voting_shr_ind ) 
select day_key, issuer_nm, secr_key, thrs_crty_key, par_shr_qty, oshr_qty, all_cls_oshr_qty, all_voting_oshr_qty, all_non_voting_oshr_qty, voting_shr_ind
 from (
            -- Total shares held for a given issuer. 
            select hfo.day_key, 
                     hfo.issuer_nm, 
                     null secr_key,
                     hfo.thrs_crty_key, 
                     sum(hfo.par_shr_qty) par_shr_qty, 
                     null oshr_qty,
                     min(all_cls_oshr_qty) all_cls_oshr_qty, 
                     null all_voting_oshr_qty, 
                     null all_non_voting_oshr_qty,
                     null voting_shr_ind
              from hdm.holdings_foreign_ownership hfo
            where hfo.day_key = load_date_key
                and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES'
                and (hfo.adr_spsr_ship_type <> 'U' or hfo.adr_spsr_ship_type is null)
        group by hfo.day_key, hfo.ISSUER_NM, hfo.thrs_crty_key
        union
            -- Voting rights Shares Total 
            select hfo.day_key, 
                     hfo.issuer_nm, 
                     null secr_key,
                     hfo.thrs_crty_key, 
                     sum(hfo.par_shr_qty)  op_par_shr_qty, 
                     null,
                     min(all_cls_oshr_qty) all_cls_oshr_qty, 
                     min(all_voting_oshr_qty) all_voting_oshr_qty, 
                     null all_non_voting_oshr_qty,
                     max(voting_shr_ind) voting_shr_ind
              from hdm.holdings_foreign_ownership hfo
            where hfo.day_key = load_date_key
                and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES'
                and (hfo.adr_spsr_ship_type <> 'U' or hfo.adr_spsr_ship_type is null)
                and hfo.voting_shr_ind > 0
        group by hfo.day_key, hfo.ISSUER_NM, hfo.thrs_crty_key
        union
            -- NON-Voting rights Shares Total 
            select hfo.day_key, 
                     hfo.issuer_nm, 
                     null secr_key,
                     hfo.thrs_crty_key, 
                     sum(hfo.par_shr_qty) op_par_shr_qty, 
                     null,
                     min(all_cls_oshr_qty) all_cls_oshr_qty, 
                     null all_voting_oshr_qty, 
                     min(all_non_voting_oshr_qty) all_non_voting_oshr_qty,
                     0 voting_shr_ind
              from hdm.holdings_foreign_ownership hfo
            where hfo.day_key = load_date_key
                and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES'
                and (hfo.adr_spsr_ship_type <> 'U' or hfo.adr_spsr_ship_type is null)
                and hfo.voting_shr_ind = 0
        group by hfo.day_key, hfo.ISSUER_NM, hfo.thrs_crty_key
        union
            -- Class Level Summary Records 
            select hfo.day_key, 
                     hfo.issuer_nm, 
                     hfo.secr_key,
                     hfo.thrs_crty_key, 
                     sum(hfo.par_shr_qty) par_shr_qty, 
                     min(oshr_qty) oshr_qty,
                     min(all_cls_oshr_qty) all_cls_oshr_qty, 
                     min(all_voting_oshr_qty) all_voting_oshr_qty, 
                     min(all_non_voting_oshr_qty) all_non_voting_oshr_qty,
                     min(voting_shr_ind) voting_shr_ind
              from hdm.holdings_foreign_ownership hfo
            where hfo.day_key = load_date_key
                and hfo.secr_key in (select secr_key from (
                                               select hfo.secr_key, count(*)
                                                from hdm.holdings_foreign_ownership hfo
                                                where day_key = load_date_key
                                                    and hfo.secr_key is not null
                                                    and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES'     
                                                    and (hfo.adr_spsr_ship_type <> 'U' or hfo.adr_spsr_ship_type is null)                                                    
                                                group by  hfo.secr_key
                                                having count(*) > 1))                
        group by hfo.day_key, 
                     hfo.issuer_nm, 
                     hfo.secr_key,
                     hfo.thrs_crty_key
        );

status_msg := 'Update % of shares for Issuer level records';
          
-- Update % of shares for Issuer level records 
update hdm.holdings_position_temp pt
     set prc_shrs_cls          = decode(secr_key, null, null, par_shr_qty / oshr_qty),
          prc_shrs_acls         = decode(secr_key, null, (decode(voting_shr_ind, null, par_shr_qty / all_cls_oshr_qty, null)),
                                                                   par_shr_qty / all_cls_oshr_qty),
          prc_shrs_voting      = decode(voting_shr_ind, 0, null, null, null, (par_shr_qty * voting_shr_ind) / all_voting_oshr_qty),
          prc_shrs_nonvoting = decode(voting_shr_ind, 0, par_shr_qty / all_non_voting_oshr_qty, null);

status_msg := 'Insert issuer level records into the main table';
  
-- Insert issuer level records into the main table 
insert into hdm.holdings_foreign_ownership
       (holdg_open_posn_key,
         day_key,
         issuer_nm,
         thrs_crty_key,
         secr_key,
         par_shr_qty, 
         oshr_qty,
         all_cls_oshr_qty,
         all_voting_oshr_qty,
         all_non_voting_oshr_qty,
         prc_shrs_cls,
         prc_shrs_acls,
         prc_shrs_voting,
         prc_shrs_nonvoting,
         voting_shr_ind)
select hdm.seq_holdings_open_position.NEXTVAL,
         day_key,
         issuer_nm,
         thrs_crty_key,
         secr_key,
         par_shr_qty,
         oshr_qty,
         all_cls_oshr_qty,
         all_voting_oshr_qty,
         all_non_voting_oshr_qty,       
         prc_shrs_cls,  
         prc_shrs_acls,
         prc_shrs_voting,
         prc_shrs_nonvoting,
         voting_shr_ind  
 from (       
            select day_key,
                     issuer_nm,
                     thrs_crty_key,
                     secr_key,
                     decode(secr_key, null, null, max(par_shr_qty)) par_shr_qty,
                     decode(secr_key, null, null, max(oshr_qty)) oshr_qty,
                     decode(secr_key, null, null, max(all_cls_oshr_qty)) all_cls_oshr_qty,
                     decode(secr_key, null, null, max(all_voting_oshr_qty)) all_voting_oshr_qty,
                     decode(secr_key, null, null, max(all_non_voting_oshr_qty)) all_non_voting_oshr_qty,
                     decode(secr_key, null, null, max(prc_shrs_cls)) prc_shrs_cls,
                     max(prc_shrs_acls) prc_shrs_acls,
                     max(prc_shrs_voting) prc_shrs_voting,
                     max(prc_shrs_nonvoting) prc_shrs_nonvoting,
                     max(voting_shr_ind) voting_shr_ind
             from hdm.holdings_position_temp
            group by day_key,
                     issuer_nm,
                     secr_key,
                     thrs_crty_key);                                      

commit;

status_msg := 'Populate temporary table with prior day records';
  
-- Populate temporary table with prior day records 
delete from hdm.holdings_position_temp;

insert into hdm.holdings_position_temp
select * from hdm.holdings_foreign_ownership
where day_key = prior_day_key;
 
status_msg := 'Calculate percentage of holdings';
  
-- Calculate percentages of holdings
update hdm.holdings_foreign_ownership hfo
     set prc_shrs_cls = par_shr_qty / oshr_qty,
          prc_shrs_acls = par_shr_qty / all_cls_oshr_qty,
          prc_shrs_voting = decode(voting_shr_ind, 0, null, null, null, par_shr_qty * voting_shr_ind / all_voting_oshr_qty),
          prc_shrs_nonvoting = decode(voting_shr_ind, 0, par_shr_qty / all_non_voting_oshr_qty, null)
where hfo.day_key = load_date_key
    and hfo.fund_compst_key is not null
    and threshold_country_name(hfo.thrs_crty_key) <> 'UNITED STATES'
    and hfo.secr_key is not null;

status_msg := 'Set indicators if security is breaching any of the minimum thresholds as well as proximity';
                                               
-- Set indicator if a security is breaching any of the mininum thresholds as well 
-- as proximity to the internal and official triggers 
update hdm.holdings_foreign_ownership hfo
     set breach_ind_cls = decode(prc_shrs_cls, null, null, decode(hfo.secr_key, null, null, threshold_breach_ind (hfo.prc_shrs_cls, hfo.thrs_crty_key, 'O'))),
          breach_ind_acls = decode(prc_shrs_acls, null, null, threshold_breach_ind (hfo.prc_shrs_acls, hfo.thrs_crty_key, 'O')),
          breach_ind_voting = decode(prc_shrs_voting, null, null, decode(hfo.voting_shr_ind, 0, null, threshold_breach_ind (hfo.prc_shrs_voting, hfo.thrs_crty_key, 'O'))),
          breach_ind_nonvoting = decode(prc_shrs_nonvoting, null, null, decode(hfo.voting_shr_ind, 0, threshold_breach_ind (hfo.prc_shrs_nonvoting, hfo.thrs_crty_key, 'O'), null)),
          breach_ind_int_cls = decode(prc_shrs_cls, null, null, decode(hfo.secr_key, null, null, threshold_breach_ind (hfo.prc_shrs_cls, hfo.thrs_crty_key,'I'))),
          breach_ind_int_acls = decode(prc_shrs_acls, null, null, threshold_breach_ind (hfo.prc_shrs_acls, hfo.thrs_crty_key,'I')),
          breach_ind_int_voting = decode(prc_shrs_voting, null, null, decode(hfo.voting_shr_ind, 0, null, threshold_breach_ind (hfo.prc_shrs_voting, hfo.thrs_crty_key, 'I'))),
          breach_ind_int_nonvoting = decode(prc_shrs_nonvoting, null, null, decode(hfo.voting_shr_ind, 0, threshold_breach_ind (hfo.prc_shrs_nonvoting, hfo.thrs_crty_key, 'I'), null)),
          prox_ext_br_cls           = decode (hfo.secr_key, null, null, country_trigger(hfo.thrs_crty_key, 'O') - prc_shrs_cls),
          prox_ext_br_acls         =  country_trigger (hfo.thrs_crty_key, 'O') - prc_shrs_acls,
          prox_ext_br_voting      = decode (hfo.prc_shrs_voting, null, null, country_trigger(hfo.thrs_crty_key, 'O') - prc_shrs_voting),
          prox_ext_br_nonvoting = decode (hfo.prc_shrs_nonvoting, null, null, country_trigger(hfo.thrs_crty_key, 'O') - prc_shrs_nonvoting),
          prox_int_br_cls            = decode (hfo.secr_key, null, null, country_trigger(hfo.thrs_crty_key, 'I') - prc_shrs_cls),
          prox_int_br_acls          = country_trigger(hfo.thrs_crty_key, 'I') - prc_shrs_acls,     
          prox_int_br_voting       = decode (hfo.prc_shrs_voting, null, null, country_trigger(hfo.thrs_crty_key, 'I') - prc_shrs_voting),
          prox_int_br_nonvoting  = decode (hfo.prc_shrs_nonvoting, null, null, country_trigger(hfo.thrs_crty_key, 'I') - prc_shrs_nonvoting) 
where hfo.day_key = load_date_key
    and threshold_country_name(hfo.thrs_crty_key)   <> 'UNITED STATES';        
    
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TYPE 1 COUNTRIES 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 
status_msg := 'Type 1: which threshold is being breached?';
  
-- Which threshold is being breached? 
update hdm.holdings_foreign_ownership hfo
     set thrsh_br_cls = decode(hfo.breach_ind_cls, 'Y', threshold_value_breached(hfo.prc_shrs_cls, hfo.thrs_crty_key), null),
          thrsh_br_acls = decode(hfo.breach_ind_acls, 'Y',  threshold_value_breached(hfo.prc_shrs_acls, hfo.thrs_crty_key), null),
          thrsh_br_voting = decode(hfo.breach_ind_voting, 'Y',  threshold_value_breached(hfo.prc_shrs_voting, hfo.thrs_crty_key), null),
          thrsh_br_nonvoting = decode(hfo.breach_ind_nonvoting, 'Y',  threshold_value_breached(hfo.prc_shrs_nonvoting, hfo.thrs_crty_key), null)
where hfo.day_key = load_date_key
    and threshold_country_type(thrs_crty_key) = 1
    and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES';                           
    
-- For Canada, create a special alert 12% milestone is crossed: ----------------------
-----------------------------------------------------------------------------------------------

status_msg := 'Process Canada';
  
spec_alert_text := '12% Threshold Has Been Breached!';

status_msg := 'Canada: check for special alert for class records';
  
-- For Canada, determine if special alert is needed CLASS      
update hdm.holdings_foreign_ownership hfo set spec_alert_cls = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) = 'CANADA'  
    and prc_shrs_cls >= 0.12 and prior_day_percent('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.12;

status_msg := 'Canada: check for special alert for Issuer records';
  
-- For Canada, determine if special alert is needed ISSUER      
update hdm.holdings_foreign_ownership hfo set spec_alert_acls = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) = 'CANADA'  
    and prc_shrs_acls >= 0.12 and prior_day_percent('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.12;

status_msg := 'Canada: check for special alert for Voting records';
      
-- For Canada, determine if special alert is needed VOTING      
update hdm.holdings_foreign_ownership hfo set spec_alert_voting = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) = 'CANADA'  
    and prc_shrs_voting >= 0.12 and prior_day_percent('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.12;

status_msg := 'Canada: check for special alert for NonVoting records';
      
-- For Canada, determine if special alert is needed NONVOTING       
update hdm.holdings_foreign_ownership hfo set spec_alert_nonvoting = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) = 'CANADA'  
    and prc_shrs_nonvoting >= 0.12 and prior_day_percent('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.12;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TYPE 2 COUNTRIES 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- If this is a new breach, update breach value to that of the current percent 

status_msg := 'Type 2: class';
  
update hdm.holdings_foreign_ownership hfo set thrsh_br_cls = prc_shrs_cls
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_cls = 'Y' and hfo.secr_key is not null and day_key = load_date_key
     and (    prior_day_breach_ind('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'N'
            or prior_day_breach_ind('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) is null);  

status_msg := 'Type 2: all classes';
  
update hdm.holdings_foreign_ownership hfo set thrsh_br_acls = prc_shrs_acls
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_acls = 'Y' and day_key = load_date_key
     and (    prior_day_breach_ind('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'N'
            or prior_day_breach_ind('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) is null);  

status_msg := 'Type 2: Voting';
              
update hdm.holdings_foreign_ownership hfo set thrsh_br_voting = prc_shrs_voting
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_voting = 'Y' and day_key = load_date_key
     and (    prior_day_breach_ind('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'N'
            or prior_day_breach_ind('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) is null);  

status_msg := 'Type 2: NonVoting';
              
update hdm.holdings_foreign_ownership hfo set thrsh_br_nonvoting = prc_shrs_nonvoting
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_nonvoting = 'Y' and day_key = load_date_key
     and (    prior_day_breach_ind('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'N'
            or prior_day_breach_ind('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) is null);  
            
-- Y/Y: Which threshold is being breached?  

status_msg := 'Type 2: Which Treshold - class';
  
update hdm.holdings_foreign_ownership hfo set thrsh_br_cls = prc_shrs_cls
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_cls = 'Y' and hfo.secr_key is not null and day_key = load_date_key
     and prior_day_breach_ind('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'Y'
     and abs(prc_shrs_cls - prior_day_percent('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)) >= threshold_interval(thrs_crty_key);          

status_msg := 'Type 2: Which Treshold - all classes';
       
update hdm.holdings_foreign_ownership hfo set thrsh_br_acls = prc_shrs_acls
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_acls = 'Y' and day_key = load_date_key
     and prior_day_breach_ind('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'Y'
     and abs(prc_shrs_acls - prior_day_percent('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)) >= threshold_interval(thrs_crty_key);   

status_msg := 'Type 2: Which Threshold - voting';
       
update hdm.holdings_foreign_ownership hfo set thrsh_br_voting = prc_shrs_voting
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_voting = 'Y' and day_key = load_date_key
     and prior_day_breach_ind('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'Y'
     and abs(prc_shrs_voting - prior_day_percent('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)) >= threshold_interval(thrs_crty_key);   

status_msg := 'Type 2: Which Treshold - nonvoting';
       
update hdm.holdings_foreign_ownership hfo set thrsh_br_nonvoting = prc_shrs_nonvoting 
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_nonvoting = 'Y' and day_key = load_date_key
     and prior_day_breach_ind('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'Y'
     and abs(prc_shrs_nonvoting - prior_day_percent('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)) >= threshold_interval(thrs_crty_key);  
     
     

status_msg := 'Type 2: Prior day percent - class';
       
update hdm.holdings_foreign_ownership hfo set thrsh_br_cls = prior_day_percent('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_cls = 'Y' and hfo.secr_key is not null and day_key = load_date_key
     and prior_day_breach_ind('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'Y'
     and abs(prc_shrs_cls - prior_day_percent('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)) < threshold_interval(thrs_crty_key);      
         
status_msg := 'Type 2: Prior day percent - all classes';     

update hdm.holdings_foreign_ownership hfo set thrsh_br_acls = prior_day_percent('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_acls = 'Y' and day_key = load_date_key
     and prior_day_breach_ind('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'Y'
     and abs(prc_shrs_acls - prior_day_percent('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)) < threshold_interval(thrs_crty_key);   
          
status_msg := 'Type 2: Prior day percent - voting';     
    
update hdm.holdings_foreign_ownership hfo set thrsh_br_voting = prior_day_percent('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_voting = 'Y' and day_key = load_date_key
     and prior_day_breach_ind('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'Y'
     and abs(prc_shrs_voting - prior_day_percent('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)) < threshold_interval(thrs_crty_key);   
         
status_msg := 'Type 2: Prior day percent - non-voting';     
     
update hdm.holdings_foreign_ownership hfo set thrsh_br_nonvoting = prior_day_percent('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)
 where threshold_country_type(thrs_crty_key) = 2 and breach_ind_nonvoting = 'Y' and day_key = load_date_key
     and prior_day_breach_ind('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key, prior_day_key) = 'Y'
     and abs(prc_shrs_nonvoting - prior_day_percent('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,prior_day_key)) < threshold_interval(thrs_crty_key);   
               

-- For South Korea, create a special alert 12% milestone is crossed: ----------------
-----------------------------------------------------------------------------------------------

spec_alert_text := '10% Threshold Has Been Breached!';

status_msg := 'South Korea: special alert check - class';

-- For South Korea, determine if special alert is needed CLASS      
update hdm.holdings_foreign_ownership hfo set spec_alert_cls = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) = 'KOREA, SOUTH'  
    and prc_shrs_cls >= 0.1 and prior_day_percent('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.1;
    
status_msg := 'South Korea: special alert check - all classes';

-- For South Korea, determine if special alert is needed ISSUER      
update hdm.holdings_foreign_ownership hfo set spec_alert_acls = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) = 'KOREA, SOUTH'  
    and prc_shrs_acls >= 0.1 and prior_day_percent('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.1;

status_msg := 'South Korea: special alert check - voting';
    
-- For South Korea, determine if special alert is needed VOTING      
update hdm.holdings_foreign_ownership hfo set spec_alert_voting = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) = 'KOREA, SOUTH'  
    and prc_shrs_voting >= 0.1 and prior_day_percent('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.1;

status_msg := 'South Korea: special alert check - nonvoting';
    
-- For South Korea, determine if special alert is needed NONVOTING       
update hdm.holdings_foreign_ownership hfo set spec_alert_nonvoting = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) = 'KOREA, SOUTH'  
    and prc_shrs_nonvoting >= 0.1 and prior_day_percent('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TYPE 4 COUNTRIES 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

spec_alert_text := 'Both 5% and 10% thresholds breached simultaneously!';

status_msg := 'Type 4: special alert check - class';

-- For Mexico and Philippines, determine if special alert is needed CLASS      
update hdm.holdings_foreign_ownership hfo set spec_alert_cls = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) in ('MEXICO','PHILIPPINES') 
    and prc_shrs_cls >= 0.1 and prior_day_percent('class', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.05;

status_msg := 'Type 4: special alert check - all classes';

-- For Mexico and Philippines, determine if special alert is needed ISSUER      
update hdm.holdings_foreign_ownership hfo set spec_alert_acls = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) in ('MEXICO','PHILIPPINES')  
    and prc_shrs_acls >= 0.1 and prior_day_percent('all classes', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.05;

status_msg := 'Type 4: special alert check - voting';
    
-- For Mexico and Philippines, determine if special alert is needed VOTING      
update hdm.holdings_foreign_ownership hfo set spec_alert_voting = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) in ('MEXICO','PHILIPPINES') 
    and prc_shrs_voting >= 0.1 and prior_day_percent('voting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.05;

status_msg := 'Type 4: special alert check - nonvoting';
    
-- For Mexico and Philippines, determine if special alert is needed NONVOTING       
update hdm.holdings_foreign_ownership hfo set spec_alert_nonvoting = spec_alert_text 
 where day_key = load_date_key and threshold_country_name(hfo.thrs_crty_key) in ('MEXICO','PHILIPPINES') 
    and prc_shrs_nonvoting >= 0.1 and prior_day_percent('nonvoting', hfo.issuer_nm, hfo.fund_compst_key, hfo.secr_key,  prior_day_key) < 0.05;

status_msg := 'Type 4: is this a new breach? Security level';

-- Is this is a new breach? Security Level    
update hdm.holdings_foreign_ownership hfo
     set          new_br_ind_cls = decode(breach_ind_cls, null, null, new_breach('S', 'class',         hfo.issuer_nm, hfo.thrs_crty_key, hfo.fund_compst_key, hfo.secr_key, hfo.prc_shrs_cls, prior_day_key, breach_ind_cls, thrsh_br_cls, load_date_key)),
                  new_br_ind_acls = decode(breach_ind_acls, null, null,new_breach('S', 'all classes', hfo.issuer_nm, hfo.thrs_crty_key, hfo.fund_compst_key, hfo.secr_key, hfo.prc_shrs_acls, prior_day_key,breach_ind_acls, thrsh_br_acls, load_date_key)),
               new_br_ind_voting = decode(breach_ind_voting, null, null,new_breach('S', 'voting',       hfo.issuer_nm, hfo.thrs_crty_key, hfo.fund_compst_key, hfo.secr_key,hfo.prc_shrs_voting, prior_day_key,breach_ind_voting, thrsh_br_voting, load_date_key)),
          new_br_ind_nonvoting = decode(breach_ind_nonvoting, null, null, new_breach('S', 'nonvoting', hfo.issuer_nm, hfo.thrs_crty_key, hfo.fund_compst_key, hfo.secr_key,hfo.prc_shrs_nonvoting, prior_day_key,breach_ind_nonvoting, thrsh_br_nonvoting, load_date_key))
where day_key = load_date_key                           
    and threshold_country_name(hfo.thrs_crty_key) <> 'UNITED STATES'
    and hfo.secr_key is not null;      

status_msg := 'Type 4: special alert check - Issuer level';

-- Is this is a new breach? Issuer Level    
update hdm.holdings_foreign_ownership hfo
     set         new_br_ind_acls = decode(breach_ind_acls, null, null, new_breach('I', 'all classes', hfo.issuer_nm, hfo.thrs_crty_key, hfo.fund_compst_key, hfo.secr_key, hfo.prc_shrs_acls, prior_day_key,breach_ind_acls, thrsh_br_acls, load_date_key)),
               new_br_ind_voting = decode(breach_ind_voting, null, null, new_breach('I', 'voting',       hfo.issuer_nm, hfo.thrs_crty_key, hfo.fund_compst_key, hfo.secr_key,hfo.prc_shrs_voting, prior_day_key,breach_ind_voting, thrsh_br_voting, load_date_key)),
          new_br_ind_nonvoting = decode(breach_ind_nonvoting, null, null, new_breach('I', 'nonvoting', hfo.issuer_nm, hfo.thrs_crty_key, hfo.fund_compst_key, hfo.secr_key,hfo.prc_shrs_nonvoting, prior_day_key,breach_ind_nonvoting, thrsh_br_nonvoting, load_date_key))
where day_key = load_date_key
    and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES'
    and hfo.secr_key is null;    

----------------------------------------------------------------------------------------------
-- Update type 3 country records with New Breach indicator equal 'Y' with a note
-- alerting that there is movement in amount of security ownership.

spec_alert_text := 'Change in ownership level while breaching official threshold detected!';

status_msg := 'Type 3: alert message';

update hdm.holdings_foreign_ownership hfo
     set          spec_alert_cls = decode(new_br_ind_cls, 'Y', spec_alert_text, null),
                  spec_alert_acls = decode(new_br_ind_acls, 'Y', spec_alert_text, null),
               spec_alert_voting = decode(new_br_ind_voting, 'Y', spec_alert_text, null),
          spec_alert_nonvoting = decode(new_br_ind_nonvoting, 'Y', spec_alert_text, null)
where 1=1
    and threshold_country_type(thrs_crty_key) = 3
    and day_key = load_date_key
    and threshold_country_name(hfo.thrs_crty_key) <> 'UNITED STATES';   
                                                                                                  
commit;

EXCEPTION
        when DATE_LOADED then
                dbms_output.put_line('ERROR: Data for ' || p_date_load || ' is already present in the HDM.HOLDINGS_FOREIGN_OWNERSHIP table!');
                ROLLBACK;
                RAISE;           
        when OTHERS then
                dbms_output.put_line('Last step executed:  ' || status_msg);
                ROLLBACK;
                RAISE; 

end main;

function prior_day_breach_ind(in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_fund_compst_key in number, in_secr_key in number, in_prior_day_key number) return char IS
/*---------------------------------------------------------------------------------------------
FUNCTION: prior_day_percent

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns prior day's breach indicator 
 
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

prior_indicator char;
curr_day_sedol varchar2(20);

begin

        if in_secr_key is null then        

             select decode(in_thrsh_type, 'class', breach_ind_cls, 'all classes', breach_ind_acls, 'voting', breach_ind_voting, 'nonvoting', breach_ind_nonvoting)
                into prior_indicator
              from hdm.holdings_position_temp
            where secr_key is null
                and issuer_nm = in_issuer_nm;
                
         else 
         
            select sedol_id 
               into curr_day_sedol   
              from hdm.vw_security
            where secr_key = in_secr_key;
                
            select decode(in_thrsh_type, 'class', breach_ind_cls, 'all classes', breach_ind_acls, 'voting', breach_ind_voting, 'nonvoting', breach_ind_nonvoting)
               into prior_indicator
              from hdm.holdings_position_temp pt, hdm.vw_security s
            where pt.secr_key = s.secr_key
                and s.sedol_id = curr_day_sedol
                and fund_compst_key = in_fund_compst_key;
                
         end if; 

        return prior_indicator;        
        

end prior_day_breach_ind;
function prior_day_percent(in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_fund_compst_key in number, in_secr_key in number, in_prior_day_key number)  return number IS
/*---------------------------------------------------------------------------------------------
FUNCTION: prior_day_percent

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns percentage of shares based on a list of arguments
from a day prior to last day loaded.
 
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

prior_percentage number;
curr_day_sedol varchar2(20);

begin

        if in_secr_key is null then

            select decode(in_thrsh_type, 'class', prc_shrs_cls, 'all classes', prc_shrs_acls, 'voting', prc_shrs_voting, 'nonvoting', prc_shrs_nonvoting)
              into prior_percentage
              from hdm.holdings_position_temp
            where secr_key is null
                and issuer_nm = in_issuer_nm;

         else 
         
            select sedol_id 
               into curr_day_sedol   
              from hdm.security
            where secr_key = in_secr_key;
                
            select decode(in_thrsh_type, 'class', prc_shrs_cls, 'all classes', prc_shrs_acls, 'voting', prc_shrs_voting, 'nonvoting', prc_shrs_nonvoting)
               into prior_percentage
              from hdm.holdings_position_temp pt, hdm.security s
            where pt.secr_key = s.secr_key
                and s.sedol_id = curr_day_sedol
                and fund_compst_key = in_fund_compst_key;
                
         end if; 

        return prior_percentage;        

end prior_day_percent;


function threshold_country_type (in_thrs_crty_key in number)  return number  IS
/*---------------------------------------------------------------------------------------------
FUNCTION: threshold_country_type

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns country type for foreign ownership report.  There 
are three:  1,2 and 3.  These types denote the type of calculation required to 
determine if a security breached a particular level in the breach hierarchy.
 
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

country_type number;

begin

select min(threshold_type) into country_type
from hdm.country_threshold
where crty_key = in_thrs_crty_key;

return country_type;

end threshold_country_type;

function threshold_interval (in_thrs_crty_key in number)  return number  IS
/*---------------------------------------------------------------------------------------------
FUNCTION: threshold_interval

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns Type 2 country threshold interval 
 
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

threshold_interval number;

begin

select periodic_thrsld_disp into threshold_interval
from HDM.COUNTRY_THRESHOLD
where crty_key = in_thrs_crty_key;

return threshold_interval;

end threshold_interval;

function threshold_country_name (in_thrs_crty_key in number)  return varchar2 IS
/*
FUNCTION: threshold_country_name 

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns country name for foreign ownership report given
the country key value 
 
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

country_name varchar2(100);

begin

select crty_nm into country_name
from hdm.vw_country
where crty_key = in_thrs_crty_key;

return country_name;

end threshold_country_name;


function country_trigger (threshold_crty_key in number, trigger_type in char)  return number  IS
/***********************************************************
FUNCTION: country_trigger

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns threshold minimum triger value, either 
internal or official (external) depending on the trigger_type value in the 
parameter.

-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment           
---------------------------------------------------------------------------------------------*/

trigger_value number;

begin

select min(decode(trigger_type, 'I', intrl_trig, 'O', ofcl_trig)) into trigger_value
from hdm.country_threshold
where crty_key = threshold_crty_key;

return trigger_value;

end country_trigger;

function  threshold_country(inc_crty_key in number, iss_crty_key in number, trde_crty_key in number) return number IS
/***********************************************************
FUNCTION: threshold_country

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function will determine the "Threshold Country" key based on 
logic provided by the business.  Used in threshold reporting, proximity to internal 
and official thresholds will be based on the country of threshold.  
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment     
---------------------------------------------------------------------------------------------*/

threshold_country_key number;

begin

threshold_country_key := inc_crty_key;

if  threshold_country_name(iss_crty_key) in ('BERMUDA','COLOMBIA','GUERNSEY','JERSEY','CAYMAN ISLANDS','KAZAKHSTAN','PERU','QATAR','URUGUAY','VENEZUELA') then 
        if  threshold_country_name(trde_crty_key) = 'UNITED STATES' then threshold_country_key :=  iss_crty_key;
         else
                threshold_country_key :=  trde_crty_key;
         end if;
else 
        if threshold_country_name(iss_crty_key) = 'UNITED STATES' then 
                if (threshold_country_name(inc_crty_key) in ('BERMUDA','COLOMBIA','GUERNSEY','JERSEY','CAYMAN ISLANDS','KAZAKHSTAN','PERU','QATAR','URUGUAY','VENEZUELA','EURO CURRENCY')) then
                        if threshold_country_name(trde_crty_key) = 'UNITED STATES' then threshold_country_key :=  inc_crty_key;
                         else threshold_country_key :=  trde_crty_key;
                         end if;
                 else
                        threshold_country_key :=  inc_crty_key;
                 end if;
         else 
                 threshold_country_key :=  inc_crty_key;
         end if;
end if;
            
return threshold_country_key;
                  
            
end threshold_country;

function threshold_breach_ind (prct_value in number, threshold_crty_key in number, int_ext_ind char) return char IS
/***********************************************************
FUNCTION: threshold_breach_ind

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function will look up minimum threshold value for a given country,
be it internal or official trigger, and will return wether or not a percentage passed
in via the parameter is breaching the given threshold.
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment     
---------------------------------------------------------------------------------------------*/

breach_ind char(1);
threshold_value number;

begin

        select min(decode(int_ext_ind, 'I', intrl_trig, ofcl_trig)) 
            into threshold_value
          from hdm.vw_country_threshold
         where crty_key = threshold_crty_key;
         
         if prct_value > threshold_value then breach_ind := 'Y';
         else  breach_ind := 'N';
         end if;
         
         return breach_ind;

end threshold_breach_ind;

function threshold_value_breached(prct_value in number, threshold_crty_key in number ) return number IS
/***********************************************************
FUNCTION: threshold_value_breached

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function will look up the threshold value for type 1 countries 
(milestones) using max fuction and choosing largest value from COUNTRY_THRESHOLD
table.  
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment     
---------------------------------------------------------------------------------------------*/


v_periodic_value number;
v_min_threshold number;

threshold_value number;

begin
       
         select max(thrshld_mlstn_displ) into threshold_value
           from hdm.vw_country_threshold
         where crty_key = threshold_crty_key
             and thrshld_mlstn_displ < prct_value;
 
        return threshold_value;

end threshold_value_breached;

function new_breach(in_rec_type in char, in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_thrs_crty_key in number, in_fund_compst_key in number, in_secr_key in number, in_prc_shrs in number ,
                               in_prior_day_key number, in_breach_ind in char, in_thrsh_br in number, load_date_key in number) return char IS                                                                 
/***********************************************************
FUNCTION: new_breach

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function will determine if a given record is a new breach or not.
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment     
---------------------------------------------------------------------------------------------*/

curr_day_sedol varchar2(20);
curr_day_issuer varchar2(250);

prior_threshold_ind char(1);
prior_threshold_breached number;
prior_prc number;
spec_alert_text varchar2(250);

threshold_interval number;
                    
prior_day_record hdm.holdings_position_temp%ROWTYPE;

begin

  dbms_output.put_line('in_rec_type: ' || in_rec_type || ', in_compst_key: ' || in_fund_compst_key || ',Issuer name: ' || in_issuer_nm || ', crty_key: ' || in_thrs_crty_key || ', threshold type: ' || in_thrsh_type); 
  
        if in_rec_type = 'S' then 
                         
                select sedol_id 
                   into curr_day_sedol   
                  from hdm.security
                where secr_key = in_secr_key;
         
                -- Populate prior day record 
                
                case 
                when in_fund_compst_key is not null then

                        select p.*  
                           into prior_day_record
                          from hdm.holdings_position_temp p, 
                                  hdm.security s
                        where day_key = in_prior_day_key
                            and s.secr_key = p.secr_key
                            and p.fund_compst_key = in_fund_compst_key
                            and s.sedol_id = curr_day_sedol;
                  
                  else
                  
                        select p.*  
                           into prior_day_record
                          from hdm.holdings_position_temp p, 
                                  hdm.security s
                        where day_key = in_prior_day_key
                            and s.secr_key = p.secr_key
                            and p.fund_compst_key is null
                            and s.sedol_id = curr_day_sedol;
                         
                  end case; 
                  
        else
            
                select p.*  
                   into prior_day_record
                  from hdm.holdings_position_temp p
                where day_key = in_prior_day_key
                    and p.issuer_nm = in_issuer_nm
                    and p.secr_key is null
                    and p.thrs_crty_key = in_thrs_crty_key;
                        
        end if;

 --     dbms_output.put_line('COUNTRY TYPE: ' ||  threshold_country_type(in_thrs_crty_key) ); 
      
       -- Determine which column to look at and remember it's value
        case in_thrsh_type when 'class' then prior_threshold_ind := prior_day_record.breach_ind_cls; 
                                                             prior_prc := prior_day_record.prc_shrs_cls;
                                                             prior_threshold_breached := prior_day_record.thrsh_br_cls;
                                    when 'all classes' then  prior_threshold_ind := prior_day_record.breach_ind_acls;
                                                                       prior_prc := prior_day_record.prc_shrs_acls;
                                                                       prior_threshold_breached := prior_day_record.thrsh_br_acls;
                                    when 'voting' then  prior_threshold_ind := prior_day_record.breach_ind_voting;
                                                                prior_prc := prior_day_record.prc_shrs_voting;
                                                                prior_threshold_breached := prior_day_record.thrsh_br_voting;
                                    when 'nonvoting' then  prior_threshold_ind := prior_day_record.breach_ind_nonvoting;
                                                                     prior_prc := prior_day_record.prc_shrs_nonvoting;
                                                                     prior_threshold_breached := prior_day_record.thrsh_br_nonvoting;
        end case;
                
      if  threshold_country_type(in_thrs_crty_key) = 1 then                                   
                                                                                                            
                               if in_breach_ind = 'N' and (prior_threshold_ind = 'N' or prior_threshold_ind is null)  then return 'N';
                        else if in_breach_ind = 'N' and prior_threshold_ind = 'Y' then return 'Y';
                        else if in_breach_ind = 'Y' and (prior_threshold_ind = 'N' or prior_threshold_ind is null)   then   return 'Y';
                        else if in_breach_ind = 'Y' and prior_threshold_ind = 'Y' then
                                if in_thrsh_br = prior_threshold_breached then return 'N';
                                else return 'Y';
                                end if;
                        end if;  end if;  end if;   end if;     
                            
      elsif threshold_country_type(in_thrs_crty_key) = 2 then
       
                       select periodic_thrsld_disp
                          into threshold_interval  
                         from hdm.country_threshold
                       where crty_key = in_thrs_crty_key;
                      
                       if in_breach_ind = 'Y' and (prior_threshold_ind = 'N' or prior_threshold_ind is null) then return 'Y';                                
                       elsif in_breach_ind = 'Y' and prior_threshold_ind = 'Y' then       
                               if abs(in_prc_shrs - prior_threshold_breached) < threshold_interval then return 'N'; else return 'Y'; end if;                         
                       elsif in_breach_ind = 'N' and prior_threshold_ind = 'Y' then
                                return 'Y';
                       elsif in_breach_ind = 'N' and (prior_threshold_ind = 'N' or prior_threshold_ind is null)  then 
                                return 'N';
                       end if;
                       
      elsif threshold_country_type(in_thrs_crty_key) = 3 then

                      if in_breach_ind = 'Y' and (prior_threshold_ind = 'N' or prior_threshold_ind is null) then return 'Y';
                      elsif in_breach_ind = 'Y' and prior_threshold_ind = 'Y' then
                                if in_prc_shrs <> prior_prc then return 'Y'; else return 'N'; end if;
                      elsif in_breach_ind = 'N' and prior_threshold_ind = 'Y' then return 'Y';
                      elsif in_breach_ind = 'N' and (prior_threshold_ind = 'N' or prior_threshold_ind is null) then return 'N';                      
                      end if;
                    
      elsif threshold_country_type(in_thrs_crty_key) = 4 then
            
                if threshold_country_name(in_thrs_crty_key) in ('MEXICO','PHILIPPINES') then 
                                        
                        if     in_breach_ind = 'Y' and (prior_threshold_ind = 'N' or prior_threshold_ind is null) then return 'Y';
                        elsif (in_breach_ind = 'Y' and prior_threshold_ind = 'Y') and (in_prc_shrs < 0.1 and prior_prc > 0.05) then return 'N';
                        elsif (in_breach_ind = 'Y' and prior_threshold_ind = 'Y') and (in_prc_shrs > 0.1 and prior_prc < 0.1) then return 'Y';
                        elsif (in_breach_ind = 'Y' and prior_threshold_ind = 'Y') and (in_prc_shrs > 0.1 and prior_prc >= 0.1) then
                               if in_prc_shrs = prior_prc then return 'N';
                               else return 'Y';
                               end if;
                        elsif (in_breach_ind = 'N' and prior_threshold_ind = 'Y') then return 'Y';
                        elsif (in_breach_ind = 'N' and prior_threshold_ind = 'N') then return 'N';                                                        
                        end if;

                end if;
       end if;
   
end new_breach;

END pkg_foreign_ownership;

CREATE OR REPLACE package HDM.pkg_foreign_ownership_init as 

function threshold_breach_ind (prct_value in number, threshold_crty_key in number, int_ext_ind char) return char;
function threshold_country_type(in_thrs_crty_key in number)  return number;
function threshold_interval (in_thrs_crty_key in number)  return number;
function prior_day_breach_ind(in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_fund_compst_key in number, in_secr_key in number, in_prior_day_key number) return char;
function threshold_country_name (in_thrs_crty_key in number)  return varchar2;
function prior_day_percent(in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_fund_compst_key in number, in_secr_key in number, in_prior_day_key number)  return number;
function  threshold_country(inc_crty_key in number, iss_crty_key in number, trde_crty_key in number) return number;
function threshold_value_breached(prct_value in number, threshold_crty_key in number ) return number;
function new_breach(in_rec_type in char, in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_thrs_crty_key in number, in_fund_compst_key in number, in_secr_key in number, in_prc_shrs in number ,
                               in_prior_day_key number, in_breach_ind in char, in_thrsh_br in number, load_date_key in number) return char;
function country_trigger (threshold_crty_key in number, trigger_type in char)  return number;
procedure main(p_date_load in date);

end pkg_foreign_ownership_init;

CREATE OR REPLACE PACKAGE BODY HDM.pkg_foreign_ownership_init is

procedure main (p_date_load in date) is

   load_date date := p_date_load; -- latest date loaded
   load_date_key number; 
   prior_day date; -- day prior to last loaded 
   prior_day_key number; 
      
   holding_pos_key number;
   threshold_ctry_key number;
   
   rec_count number;
   DATE_LOADED EXCEPTION;
   err_msg varchar2(200);
    
   spec_alert_text varchar2(250);
   
begin

dbms_output.put_line('I AM IN!!!!');

-- Get last date loaded  
select day_key into load_date_key from hdm.calendar where cal_day = load_date;

dbms_output.put_line('day_key = ' || load_date_key);

-- Check if data for this date has already been loaded.  If so, raise an error
-- and stop processing to avoid re-inserting same records twice.
select count(*) into rec_count  
  from hdm.holdings_foreign_ownership hfo
where hfo.day_key = load_date_key;
  
case 
    when rec_count > 0 then RAISE DATE_LOADED;
    else rec_count := 0;
end case;

dbms_output.put_line('rec count: ' || rec_count);

-- Insert raw records for the load date into the threshold foreign ownership table 
insert into hdm.holdings_foreign_ownership
( holdg_open_posn_key,
  fund_compst_key,
  secr_key,
  inc_crty_key,
  iss_crty_key,
  trde_crty_key,
  day_key,
  tot_long_posn,
  actl_cstdy_posn,
  pend_sale_qty,
  pend_buy_qty,
  free_qty,
  oshr_qty,
  par_shr_qty,
  tot_shrt_posn,
  on_loan_qty,
  new_secr_flg,
  all_cls_oshr_qty,
  all_voting_oshr_qty,
  all_non_voting_oshr_qty,
  voting_shr_ind,
  curr_row_flg,
  row_strt_dttm,
  row_stop_dttm,
  etl_load_cyc_key,
  src_sys_id,
  adr_spsr_ship_type
  )
  select holdg_open_posn_key,
           fund_compst_key,
           secr_key,
           inc_crty_key,
           iss_crty_key,
           trde_crty_key,
           day_key,
           tot_long_posn,
           actl_cstdy_posn,
           pend_sale_qty,
           pend_buy_qty,
           free_qty,
           oshr_qty,
           par_shr_qty,
           tot_shrt_posn,
           on_loan_qty,
           new_secr_flg,
           all_cls_oshr_qty * 1000000,
           all_voting_oshr_qty,
           all_non_voting_oshr_qty,
           voting_shr_ind,
           curr_row_flg,
           row_strt_dttm,
           row_stop_dttm,
           etl_load_cyc_key,
           src_sys_id,
           adr_spsr_ship_type
   from hdm.holdings_open_position op
where op.day_key = load_date_key
    and  op.fund_compst_key in (select fund_compst_key from hdm.fund_composite
                                             where asset_cls_nm in ('Domestic','International and Global') )
     and op.secr_key in 
     (select secr_key from ( SELECT DISTINCT s."SECR_KEY",
                   s."SEDOL_ID",
                   s."ISIN_ID",
                   s."TCKR_SYM",
                   s."SECR_NM",
                   s."ISSUER_NM",
                   s."GLBL_ISSUER_NM",
                   s."INVMT_TYPE_CD",
                   s."INVMT_TYPE_DESC",
                   s."DTC_CUSIP",
                   s."ASSET_ID",
                   s."MUNCPL_SECR_ST_CD",
                   s."MUNCPL_SECR_ST",
                   s."CURR_ROW_FLG",
                   sa.asset_grp_cd,
                   sa.asset_grp_desc
     FROM hdm.vw_security s,
          hdm.vw_security_attributes sa,
          hdm.vw_fund_holdings h,
          hdm.vw_calendar c
    WHERE     s.secr_key = h.secr_key
          AND h.secr_attr_key = sa.secr_attr_key
          AND h.day_key = c.day_key
          AND c.cal_day = load_date
                            ) 
             where asset_grp_desc = 'Stock');

commit;

-- Determine Threshold Country value and issuer name 
update hdm.holdings_foreign_ownership hfo 
     set thrs_crty_key = threshold_country(inc_crty_key, iss_crty_key, trde_crty_key),
          issuer_nm = (select issuer_nm 
                                from hdm.vw_security sl
                              where sl.secr_key = hfo.secr_key)
where hfo.day_key = load_date_key;

-- Update issuer name for records with no Issuer name found 
update hdm.holdings_foreign_ownership hfo
set issuer_nm = (select 'zNO_ISSUER ' || SL.SECR_NM 
                           from hdm.vw_security sl
                         where sl.secr_key = hfo.secr_key)
where hfo.day_key = load_date_key
     and hfo.issuer_nm is null; 

------------------------------------------------------------------------------------------------                                              
-- Create Issuer summary records 
delete from hdm.holdings_position_temp;

insert into hdm.holdings_position_temp
        (day_key, issuer_nm, secr_key, thrs_crty_key, par_shr_qty, oshr_qty, all_cls_oshr_qty, all_voting_oshr_qty, all_non_voting_oshr_qty, voting_shr_ind ) 
select day_key, issuer_nm, secr_key, thrs_crty_key, par_shr_qty, oshr_qty, all_cls_oshr_qty, all_voting_oshr_qty, all_non_voting_oshr_qty, voting_shr_ind
from (
            -- Total shares held for a given issuer. 
            select hfo.day_key, 
                     hfo.issuer_nm, 
                     null secr_key,
                     hfo.thrs_crty_key, 
                     sum(hfo.par_shr_qty) par_shr_qty, 
                     null oshr_qty,
                     min(all_cls_oshr_qty) all_cls_oshr_qty, 
                     null all_voting_oshr_qty, 
                     null all_non_voting_oshr_qty,
                     null voting_shr_ind
              from hdm.holdings_foreign_ownership hfo
            where hfo.day_key = load_date_key
                and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES'
                and (hfo.adr_spsr_ship_type <> 'U' or hfo.adr_spsr_ship_type is null)
        group by hfo.day_key, hfo.ISSUER_NM, hfo.thrs_crty_key
        union
            -- Voting rights Shares Total 
            select hfo.day_key, 
                     hfo.issuer_nm, 
                     null secr_key,
                     hfo.thrs_crty_key, 
                     sum(hfo.par_shr_qty)  op_par_shr_qty, 
                     null,
                     min(all_cls_oshr_qty) all_cls_oshr_qty, 
                     min(all_voting_oshr_qty) all_voting_oshr_qty, 
                     null all_non_voting_oshr_qty,
                     max(voting_shr_ind) voting_shr_ind
              from hdm.holdings_foreign_ownership hfo
            where hfo.day_key = load_date_key
                and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES'
                and (hfo.adr_spsr_ship_type <> 'U' or hfo.adr_spsr_ship_type is null)
                and hfo.voting_shr_ind > 0
        group by hfo.day_key, hfo.ISSUER_NM, hfo.thrs_crty_key
        union
            -- NON-Voting rights Shares Total 
            select hfo.day_key, 
                     hfo.issuer_nm, 
                     null secr_key,
                     hfo.thrs_crty_key, 
                     sum(hfo.par_shr_qty) op_par_shr_qty, 
                     null,
                     min(all_cls_oshr_qty) all_cls_oshr_qty, 
                     null all_voting_oshr_qty, 
                     min(all_non_voting_oshr_qty) all_non_voting_oshr_qty,
                     0 voting_shr_ind
              from hdm.holdings_foreign_ownership hfo
            where hfo.day_key = load_date_key
                and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES'
                and (hfo.adr_spsr_ship_type <> 'U' or hfo.adr_spsr_ship_type is null)
                and hfo.voting_shr_ind = 0
        group by hfo.day_key, hfo.ISSUER_NM, hfo.thrs_crty_key
        union
            -- Class Level Summary Records 
            select hfo.day_key, 
                     hfo.issuer_nm, 
                     hfo.secr_key,
                     hfo.thrs_crty_key, 
                     sum(hfo.par_shr_qty) par_shr_qty, 
                     min(oshr_qty) oshr_qty,
                     min(all_cls_oshr_qty) all_cls_oshr_qty, 
                     min(all_voting_oshr_qty) all_voting_oshr_qty, 
                     min(all_non_voting_oshr_qty) all_non_voting_oshr_qty,
                     min(voting_shr_ind) voting_shr_ind
              from hdm.holdings_foreign_ownership hfo
            where hfo.day_key = load_date_key
                and hfo.secr_key in (select secr_key from (
                                               select hfo.secr_key, count(*)
                                                from hdm.holdings_foreign_ownership hfo
                                                where day_key = load_date_key
                                                    and hfo.secr_key is not null
                                                    and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES'     
                                                    and (hfo.adr_spsr_ship_type <> 'U' or hfo.adr_spsr_ship_type is null)                                                    
                                                group by  hfo.secr_key
                                                having count(*) > 1))                
        group by hfo.day_key, 
                     hfo.issuer_nm, 
                     hfo.secr_key,
                     hfo.thrs_crty_key
        );
        
-- Update % of shares for Issuer level records 
update hdm.holdings_position_temp pt
     set prc_shrs_cls          = decode(secr_key, null, null, par_shr_qty / oshr_qty),
          prc_shrs_acls         = decode(secr_key, null, (decode(voting_shr_ind, null, par_shr_qty / all_cls_oshr_qty, null)),
                                                                   par_shr_qty / all_cls_oshr_qty),
          prc_shrs_voting      = decode(voting_shr_ind, 0, null, null, null, (par_shr_qty * voting_shr_ind) / all_voting_oshr_qty),
          prc_shrs_nonvoting = decode(voting_shr_ind, 0, par_shr_qty / all_non_voting_oshr_qty, null);

-- Insert issuer level records into the main table 
insert into hdm.holdings_foreign_ownership
       (holdg_open_posn_key,
         day_key,
         issuer_nm,
         thrs_crty_key,
         secr_key,
         par_shr_qty, 
         oshr_qty,
         all_cls_oshr_qty,
         all_voting_oshr_qty,
         all_non_voting_oshr_qty,
         prc_shrs_cls,
         prc_shrs_acls,
         prc_shrs_voting,
         prc_shrs_nonvoting,
         voting_shr_ind)
select hdm.seq_holdings_open_position.NEXTVAL,
         day_key,
         issuer_nm,
         thrs_crty_key,
         secr_key,
         par_shr_qty,
         oshr_qty,
         all_cls_oshr_qty,
         all_voting_oshr_qty,
         all_non_voting_oshr_qty,       
         prc_shrs_cls,  
         prc_shrs_acls,
         prc_shrs_voting,
         prc_shrs_nonvoting,
         voting_shr_ind  
 from (       
            select day_key,
                     issuer_nm,
                     thrs_crty_key,
                     secr_key,
                     decode(secr_key, null, null, max(par_shr_qty)) par_shr_qty,
                     decode(secr_key, null, null, max(oshr_qty)) oshr_qty,
                     decode(secr_key, null, null, max(all_cls_oshr_qty)) all_cls_oshr_qty,
                     decode(secr_key, null, null, max(all_voting_oshr_qty)) all_voting_oshr_qty,
                     decode(secr_key, null, null, max(all_non_voting_oshr_qty)) all_non_voting_oshr_qty,
                     decode(secr_key, null, null, max(prc_shrs_cls)) prc_shrs_cls,
                     max(prc_shrs_acls) prc_shrs_acls,
                     max(prc_shrs_voting) prc_shrs_voting,
                     max(prc_shrs_nonvoting) prc_shrs_nonvoting,
                     max(voting_shr_ind) voting_shr_ind
             from hdm.holdings_position_temp
           group by day_key,
                     issuer_nm,
                     secr_key,
                     thrs_crty_key);                                      
 
-- Calculate percentages of holdings
update hdm.holdings_foreign_ownership hfo
     set prc_shrs_cls = par_shr_qty / oshr_qty,
          prc_shrs_acls = par_shr_qty / all_cls_oshr_qty,
          prc_shrs_voting = decode(voting_shr_ind, 0, null, null, null, par_shr_qty * voting_shr_ind / all_voting_oshr_qty),
          prc_shrs_nonvoting = decode(voting_shr_ind, 0, par_shr_qty / all_non_voting_oshr_qty, null)
where hfo.day_key = load_date_key
    and hfo.fund_compst_key is not null
    and threshold_country_name(hfo.thrs_crty_key) <> 'UNITED STATES'
    and hfo.secr_key is not null;
                                             
-- Set indicator if a security is breaching any of the mininum thresholds as well 
-- as proximity to the internal and official triggers 
update hdm.holdings_foreign_ownership hfo
     set breach_ind_cls = decode(prc_shrs_cls, null, null, decode(hfo.secr_key, null, null, threshold_breach_ind (hfo.prc_shrs_cls, hfo.thrs_crty_key, 'O'))),
          breach_ind_acls = decode(prc_shrs_acls, null, null, threshold_breach_ind (hfo.prc_shrs_acls, hfo.thrs_crty_key, 'O')),
          breach_ind_voting = decode(prc_shrs_voting, null, null, decode(hfo.voting_shr_ind, 0, null, threshold_breach_ind (hfo.prc_shrs_voting, hfo.thrs_crty_key, 'O'))),
          breach_ind_nonvoting = decode(prc_shrs_nonvoting, null, null, decode(hfo.voting_shr_ind, 0, threshold_breach_ind (hfo.prc_shrs_nonvoting, hfo.thrs_crty_key, 'O'), null)),
          breach_ind_int_cls = decode(prc_shrs_cls, null, null, decode(hfo.secr_key, null, null, threshold_breach_ind (hfo.prc_shrs_cls, hfo.thrs_crty_key,'I'))),
          breach_ind_int_acls = decode(prc_shrs_acls, null, null, threshold_breach_ind (hfo.prc_shrs_acls, hfo.thrs_crty_key,'I')),
          breach_ind_int_voting = decode(prc_shrs_voting, null, null, decode(hfo.voting_shr_ind, 0, null, threshold_breach_ind (hfo.prc_shrs_voting, hfo.thrs_crty_key, 'I'))),
          breach_ind_int_nonvoting = decode(prc_shrs_nonvoting, null, null, decode(hfo.voting_shr_ind, 0, threshold_breach_ind (hfo.prc_shrs_nonvoting, hfo.thrs_crty_key, 'I'), null)),
          prox_ext_br_cls           = decode (hfo.secr_key, null, null, country_trigger(hfo.thrs_crty_key, 'O') - prc_shrs_cls),
          prox_ext_br_acls         =  country_trigger (hfo.thrs_crty_key, 'O') - prc_shrs_acls,
          prox_ext_br_voting      = decode (hfo.prc_shrs_voting, null, null, country_trigger(hfo.thrs_crty_key, 'O') - prc_shrs_voting),
          prox_ext_br_nonvoting = decode (hfo.prc_shrs_nonvoting, null, null, country_trigger(hfo.thrs_crty_key, 'O') - prc_shrs_nonvoting),
          prox_int_br_cls            = decode (hfo.secr_key, null, null, country_trigger(hfo.thrs_crty_key, 'I') - prc_shrs_cls),
          prox_int_br_acls          = country_trigger(hfo.thrs_crty_key, 'I') - prc_shrs_acls,     
          prox_int_br_voting       = decode (hfo.prc_shrs_voting, null, null, country_trigger(hfo.thrs_crty_key, 'I') - prc_shrs_voting),
          prox_int_br_nonvoting  = decode (hfo.prc_shrs_nonvoting, null, null, country_trigger(hfo.thrs_crty_key, 'I') - prc_shrs_nonvoting) 
where hfo.day_key = load_date_key
    and threshold_country_name(hfo.thrs_crty_key)   <> 'UNITED STATES';        
    
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TYPE 1 COUNTRIES 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Which threshold is being breached? 
update hdm.holdings_foreign_ownership hfo
     set thrsh_br_cls = decode(hfo.breach_ind_cls, 'Y', threshold_value_breached(hfo.prc_shrs_cls, hfo.thrs_crty_key), null),
          thrsh_br_acls = decode(hfo.breach_ind_acls, 'Y',  threshold_value_breached(hfo.prc_shrs_acls, hfo.thrs_crty_key), null),
          thrsh_br_voting = decode(hfo.breach_ind_voting, 'Y',  threshold_value_breached(hfo.prc_shrs_voting, hfo.thrs_crty_key), null),
          thrsh_br_nonvoting = decode(hfo.breach_ind_nonvoting, 'Y',  threshold_value_breached(hfo.prc_shrs_nonvoting, hfo.thrs_crty_key), null)
where hfo.day_key = load_date_key
    and threshold_country_type(thrs_crty_key) = 1
    and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES';                           
    
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- TYPE 2 COUNTRIES 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     
-- Which threshold is being breached? 
update hdm.holdings_foreign_ownership hfo
     set thrsh_br_cls = decode(hfo.breach_ind_cls, 'Y', hfo.prc_shrs_cls, hfo.thrs_crty_key, null),
          thrsh_br_acls = decode(hfo.breach_ind_acls, 'Y',  hfo.prc_shrs_acls, hfo.thrs_crty_key, null),
          thrsh_br_voting = decode(hfo.breach_ind_voting, 'Y', hfo.prc_shrs_voting, hfo.thrs_crty_key, null),
          thrsh_br_nonvoting = decode(hfo.breach_ind_nonvoting, 'Y',  hfo.prc_shrs_nonvoting, hfo.thrs_crty_key, null)
where hfo.day_key = load_date_key
    and threshold_country_type(thrs_crty_key) = 2
    and threshold_country_name(hfo.thrs_crty_key)  <> 'UNITED STATES';  
                                                                                                  
commit;

EXCEPTION
        when DATE_LOADED then
                dbms_output.put_line('ERROR: Data for ' || p_date_load || ' is already present in the HDM.HOLDINGS_FOREIGN_OWNERSHIP table!');
                RAISE;            

end main;

function prior_day_breach_ind(in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_fund_compst_key in number, in_secr_key in number, in_prior_day_key number) return char IS
/*---------------------------------------------------------------------------------------------
FUNCTION: prior_day_percent

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns prior day's breach indicator 
 
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

prior_indicator char;
curr_day_sedol varchar2(20);

begin

        if in_secr_key is null then        

             select decode(in_thrsh_type, 'class', breach_ind_cls, 'all classes', breach_ind_acls, 'voting', breach_ind_voting, 'nonvoting', breach_ind_nonvoting)
                into prior_indicator
              from hdm.holdings_position_temp
            where secr_key is null
                and issuer_nm = in_issuer_nm;
                
         else 
         
            select sedol_id 
               into curr_day_sedol   
              from hdm.vw_security
            where secr_key = in_secr_key;
                
            select decode(in_thrsh_type, 'class', breach_ind_cls, 'all classes', breach_ind_acls, 'voting', breach_ind_voting, 'nonvoting', breach_ind_nonvoting)
               into prior_indicator
              from hdm.holdings_position_temp pt, hdm.vw_security s
            where pt.secr_key = s.secr_key
                and s.sedol_id = curr_day_sedol
                and fund_compst_key = in_fund_compst_key;
                
         end if; 

        return prior_indicator;        
        

end prior_day_breach_ind;
function prior_day_percent(in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_fund_compst_key in number, in_secr_key in number, in_prior_day_key number)  return number IS
/*---------------------------------------------------------------------------------------------
FUNCTION: prior_day_percent

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns percentage of shares based on a list of arguments
from a day prior to last day loaded.

-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

prior_percentage number;
curr_day_sedol varchar2(20);

begin

        if in_secr_key is null then
        
            select decode(in_thrsh_type, 'class', prc_shrs_cls, 'all classes', prc_shrs_acls, 'voting', prc_shrs_voting, 'nonvoting', prc_shrs_nonvoting)
              into prior_percentage
              from hdm.holdings_position_temp
            where secr_key is null
                and issuer_nm = in_issuer_nm;

         else 
         
            select sedol_id 
               into curr_day_sedol   
              from hdm.security
            where secr_key = in_secr_key;
                
            select decode(in_thrsh_type, 'class', prc_shrs_cls, 'all classes', prc_shrs_acls, 'voting', prc_shrs_voting, 'nonvoting', prc_shrs_nonvoting)
               into prior_percentage
              from hdm.holdings_position_temp pt, hdm.security s
            where pt.secr_key = s.secr_key
                and s.sedol_id = curr_day_sedol
                and fund_compst_key = in_fund_compst_key;
                
         end if; 

        return prior_percentage;        

end prior_day_percent;


function threshold_country_type (in_thrs_crty_key in number)  return number  IS
/*---------------------------------------------------------------------------------------------
FUNCTION: threshold_country_type

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns country type for foreign ownership report.  There 
are three:  1,2 and 3.  These types denote the type of calculation required to 
determine if a security breached a particular level in the breach hierarchy.

-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

country_type number;

begin

select min(threshold_type) into country_type
from hdm.country_threshold
where crty_key = in_thrs_crty_key;

return country_type;

end threshold_country_type;

function threshold_interval (in_thrs_crty_key in number)  return number  IS
/*---------------------------------------------------------------------------------------------
FUNCTION: threshold_interval

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns Type 2 country threshold interval 
 
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

threshold_interval number;

begin

select periodic_thrsld_disp into threshold_interval
from HDM.COUNTRY_THRESHOLD
where crty_key = in_thrs_crty_key;

return threshold_interval;

end threshold_interval;

function threshold_country_name (in_thrs_crty_key in number)  return varchar2 IS
/*
FUNCTION: threshold_country_name 

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns country name for foreign ownership report given
the country key value 
 
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment 
                  
-----------------------------------------------------------------------------------------------*/

country_name varchar2(100);

begin

select crty_nm into country_name
from hdm.vw_country
where crty_key = in_thrs_crty_key;

return country_name;

end threshold_country_name;


function country_trigger (threshold_crty_key in number, trigger_type in char)  return number  IS
/***********************************************************
FUNCTION: country_trigger

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function returns threshold minimum triger value, either 
internal or official (external) depending on the trigger_type value in the 
parameter.

-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment           
---------------------------------------------------------------------------------------------*/

trigger_value number;

begin

select min(decode(trigger_type, 'I', intrl_trig, 'O', ofcl_trig)) into trigger_value
from hdm.country_threshold
where crty_key = threshold_crty_key;

return trigger_value;

end country_trigger;

function  threshold_country(inc_crty_key in number, iss_crty_key in number, trde_crty_key in number) return number IS
/***********************************************************
FUNCTION: threshold_country

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function will determine the "Threshold Country" key based on 
logic provided by the business.  Used in threshold reporting, proximity to internal 
and official thresholds will be based on the country of threshold.  
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment     
---------------------------------------------------------------------------------------------*/

threshold_country_key number;

begin

threshold_country_key := inc_crty_key;

if  threshold_country_name(iss_crty_key) in ('BERMUDA','COLOMBIA','GUERNSEY','JERSEY','CAYMAN ISLANDS','KAZAKHSTAN','PERU','QATAR','URUGUAY','VENEZUELA') then 
        if  threshold_country_name(trde_crty_key) = 'UNITED STATES' then threshold_country_key :=  iss_crty_key;
         else
                threshold_country_key :=  trde_crty_key;
         end if;
else 
        if threshold_country_name(iss_crty_key) = 'UNITED STATES' then 
                if (threshold_country_name(inc_crty_key) in ('BERMUDA','COLOMBIA','GUERNSEY','JERSEY','CAYMAN ISLANDS','KAZAKHSTAN','PERU','QATAR','URUGUAY','VENEZUELA','EURO CURRENCY')) then
                        if threshold_country_name(trde_crty_key) = 'UNITED STATES' then threshold_country_key :=  inc_crty_key;
                         else threshold_country_key :=  trde_crty_key;
                         end if;
                 else
                        threshold_country_key :=  inc_crty_key;
                 end if;
         else 
                 threshold_country_key :=  inc_crty_key;
         end if;
end if;
            
return threshold_country_key;
                  
            
end threshold_country;

function threshold_breach_ind (prct_value in number, threshold_crty_key in number, int_ext_ind char) return char IS
/***********************************************************
FUNCTION: threshold_breach_ind

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function will look up minimum threshold value for a given country,
be it internal or official trigger, and will return wether or not a percentage passed
in via the parameter is breaching the given threshold.
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment     
---------------------------------------------------------------------------------------------*/

breach_ind char(1);
threshold_value number;

begin

        select min(decode(int_ext_ind, 'I', intrl_trig, ofcl_trig)) 
            into threshold_value
          from hdm.vw_country_threshold
         where crty_key = threshold_crty_key;
         
         if prct_value > threshold_value then breach_ind := 'Y';
         else  breach_ind := 'N';
         end if;
         
         return breach_ind;

end threshold_breach_ind;

function threshold_value_breached(prct_value in number, threshold_crty_key in number ) return number IS
/***********************************************************
FUNCTION: threshold_value_breached

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function will look up the threshold value for type 1 countries 
(milestones) using max fuction and choosing largest value from COUNTRY_THRESHOLD
table.  
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment     
---------------------------------------------------------------------------------------------*/


v_periodic_value number;
v_min_threshold number;

threshold_value number;

begin
       
         select max(thrshld_mlstn_displ) into threshold_value
           from hdm.vw_country_threshold
         where crty_key = threshold_crty_key
             and thrshld_mlstn_displ < prct_value;

        return threshold_value;

end threshold_value_breached;

function new_breach(in_rec_type in char, in_thrsh_type in varchar2, in_issuer_nm in varchar2, in_thrs_crty_key in number, in_fund_compst_key in number, in_secr_key in number, in_prc_shrs in number ,
                               in_prior_day_key number, in_breach_ind in char, in_thrsh_br in number, load_date_key in number) return char IS                                                                 
/***********************************************************
FUNCTION: new_breach

WRITTEN BY: Igor Rakhman 

OVERVIEW: This function will determine if a given record is a new breach or not.
-----------------------------------------------------------------------------------------------
Mod Date    Mod By    Mod Description
-----------    --------    ----------------------------------------------------------------------
                  IR           Initial Deployment     
---------------------------------------------------------------------------------------------*/

curr_day_sedol varchar2(20);
curr_day_issuer varchar2(250);

prior_threshold_ind char(1);
prior_threshold_breached number;
prior_prc number;
spec_alert_text varchar2(250);

threshold_interval number;
                    
prior_day_record hdm.holdings_position_temp%ROWTYPE;

begin

        if in_rec_type = 'S' then 
                
                select sedol_id 
                   into curr_day_sedol   
                  from hdm.security
                where secr_key = in_secr_key;
         
                -- Populate prior day record 
                
                case 
                when in_fund_compst_key is not null then

                        select p.*  
                           into prior_day_record
                          from hdm.holdings_position_temp p, 
                                  hdm.security s
                        where day_key = in_prior_day_key
                            and s.secr_key = p.secr_key
                            and p.fund_compst_key = in_fund_compst_key
                            and s.sedol_id = curr_day_sedol;
                  
                  else
                  
                        select p.*  
                           into prior_day_record
                          from hdm.holdings_position_temp p, 
                                  hdm.security s
                        where day_key = in_prior_day_key
                            and s.secr_key = p.secr_key
                            and p.fund_compst_key is null
                            and s.sedol_id = curr_day_sedol;
                         
                  end case; 
                  
        else
            
                select p.*  
                   into prior_day_record
                  from hdm.holdings_position_temp p
                where day_key = in_prior_day_key
                    and p.issuer_nm = in_issuer_nm
                    and p.secr_key is null;
                        
        end if;

       -- Determine which column to look at and remember it's value
        case in_thrsh_type when 'class' then prior_threshold_ind := prior_day_record.breach_ind_cls; 
                                                             prior_prc := prior_day_record.prc_shrs_cls;
                                                             prior_threshold_breached := prior_day_record.thrsh_br_cls;
                                    when 'all classes' then  prior_threshold_ind := prior_day_record.breach_ind_acls;
                                                                       prior_prc := prior_day_record.prc_shrs_acls;
                                                                       prior_threshold_breached := prior_day_record.thrsh_br_acls;
                                    when 'voting' then  prior_threshold_ind := prior_day_record.breach_ind_voting;
                                                                prior_prc := prior_day_record.prc_shrs_voting;
                                                                prior_threshold_breached := prior_day_record.thrsh_br_voting;
                                    when 'nonvoting' then  prior_threshold_ind := prior_day_record.breach_ind_nonvoting;
                                                                     prior_prc := prior_day_record.prc_shrs_nonvoting;
                                                                     prior_threshold_breached := prior_day_record.thrsh_br_nonvoting;
        end case;
          
      if  threshold_country_type(in_thrs_crty_key) = 1 then                                       
                                                                                                          
                               if in_breach_ind = 'N' and (prior_threshold_ind = 'N' or prior_threshold_ind is null)  then return 'N';
                        else if in_breach_ind = 'N' and prior_threshold_ind = 'Y' then return 'Y';
                        else if in_breach_ind = 'Y' and (prior_threshold_ind = 'N' or prior_threshold_ind is null)   then   return 'Y';
                        else if in_breach_ind = 'Y' and prior_threshold_ind = 'Y' then
                                if in_thrsh_br = prior_threshold_breached then return 'N';
                                else return 'Y';
                                end if;
                        end if;  end if;  end if;   end if;     
                       
      elsif threshold_country_type(in_thrs_crty_key) = 2 then
       
                       select periodic_thrsld_disp
                          into threshold_interval  
                         from hdm.country_threshold
                       where crty_key = in_thrs_crty_key;
                      
                       if in_breach_ind = 'Y' and (prior_threshold_ind = 'N' or prior_threshold_ind is null) then return 'Y';                                
                       elsif in_breach_ind = 'Y' and prior_threshold_ind = 'Y' then       
                               if abs(in_prc_shrs - prior_threshold_breached) < threshold_interval then return 'N'; else return 'Y'; end if;                         
                       elsif in_breach_ind = 'N' and prior_threshold_ind = 'Y' then
                                return 'Y';
                       elsif in_breach_ind = 'N' and (prior_threshold_ind = 'N' or prior_threshold_ind is null)  then 
                                return 'N';
                       end if;
                       
      elsif threshold_country_type(in_thrs_crty_key) = 3 then

                      if in_breach_ind = 'Y' and (prior_threshold_ind = 'N' or prior_threshold_ind is null) then return 'Y';
                      elsif in_breach_ind = 'Y' and prior_threshold_ind = 'Y' then
                                if in_prc_shrs <> prior_prc then return 'Y'; else return 'N'; end if;
                      elsif in_breach_ind = 'N' and prior_threshold_ind = 'Y' then return 'Y';
                      elsif in_breach_ind = 'N' and (prior_threshold_ind = 'N' or prior_threshold_ind is null) then return 'N';                      
                      end if;
                    
      elsif threshold_country_type(in_thrs_crty_key) = 4 then
            
                if threshold_country_name(in_thrs_crty_key) in ('MEXICO','PHILIPPINES') then 
                                        
                        if     in_breach_ind = 'Y' and (prior_threshold_ind = 'N' or prior_threshold_ind is null) then return 'Y';
                        elsif (in_breach_ind = 'Y' and prior_threshold_ind = 'Y') and (in_prc_shrs < 10 and prior_prc > 5) then return 'N';
                        elsif (in_breach_ind = 'Y' and prior_threshold_ind = 'Y') and (in_prc_shrs > 10 and prior_prc < 10) then return 'Y';
                        elsif (in_breach_ind = 'Y' and prior_threshold_ind = 'Y') and (in_prc_shrs > 10 and prior_prc >= 10) then
                               if in_prc_shrs = prior_prc then return 'N';
                               else return 'Y';
                               end if;
                        elsif (in_breach_ind = 'N' and prior_threshold_ind = 'Y') then return 'Y';
                        elsif (in_breach_ind = 'N' and prior_threshold_ind = 'N') then return 'N';                                                        
                        end if;

                end if;
       end if;
   
end new_breach;



END pkg_foreign_ownership_init;

CREATE OR REPLACE package HDM.pkg_fund_perf as

    TYPE  FUND_PERF_TABLE is TABLE OF FUND_RETURN%ROWTYPE;
    TYPE  INDEX_PERF_TABLE is TABLE OF INDEX_RETURN%ROWTYPE;
    TYPE  GROSS_RETURN_TABLE IS TABLE OF gross_return%ROWTYPE;
    
    FUNCTION fund_perf(p_end_dt IN DATE,p_run_mode NUMBER default 0 ) return FUND_PERF_TABLE PIPELINED;
    FUNCTION get_begin_dt ( 
                            p_unit         IN hdm.return_periods.per_ut%TYPE,
                            p_measure     IN hdm.return_periods.per_msr%TYPE,
                            p_inc_dt    IN HDM.FUND.fund_incpn_dt%TYPE,
                            p_end_dt       IN DATE
                           ) return DATE;
                           
    -- VALID CALC_TYPES ARE FUND,ASOF,INDEX                        
    FUNCTION calc_Return(
                          p_end_dt      IN DATE,
                          p_beg_dt        IN DATE,
                          p_Dim_key     IN NUMBER,
                          p_annl_fctr    IN hdm.return_periods.annl_fctr%TYPE,
                          p_calc_type    IN VARCHAR2
                        ) RETURN NUMBER;
                        
FUNCTION  Index_perf(p_end_dt IN DATE,p_run_mode NUMBER default 0) return INDEX_PERF_TABLE PIPELINED   ;   

PROCEDURE load_fund_perf(p_end_dt in DATE);          

PROCEDURE load_Index_perf(p_end_dt in DATE);         

PROCEDURE verify_fund_return(p_end_dt in DATE);     

PROCEDURE  load_asofnav_return;

PROCEDURE load_gross_wealth_Index(p_end_dt IN DATE);

PROCEDURE load_gross_return(p_end_dt IN DATE);

FUNCTION  calc_gr_Return(p_end_dt IN DATE,p_beg_dt IN DATE,p_fund_key IN NUMBER,p_annl_fctr IN NUMBER) RETURN NUMBER;

FUNCTION  gross_fund_perf(p_end_dt IN DATE,p_run_mode NUMBER DEFAULT 0) return GROSS_RETURN_TABLE PIPELINED;

FUNCTION  calc_inf_ratio(p_end_dt IN DATE,p_beg_dt IN DATE,p_fund_key IN NUMBER,p_bmk_idx_key IN NUMBER,p_annl_fctr IN NUMBER) RETURN NUMBER ;
    
end pkg_fund_perf;

CREATE OR REPLACE PACKAGE BODY HDM.pkg_fund_perf 
AS

FUNCTION get_begin_dt ( 
                        p_unit          IN hdm.return_periods.per_ut%TYPE,
                        p_measure       IN hdm.return_periods.per_msr%TYPE,
                        p_inc_dt        IN HDM.FUND.fund_incpn_dt%TYPE,
                        p_end_dt        IN DATE) return DATE
IS
        t_date         DATE;
        t_last_day      DATE;
        t_bus_day       DATE;
BEGIN

        
        CASE
        WHEN p_unit    = 'YR' THEN
             t_date:= add_months(p_end_dt,0 - (12 * p_measure));
        WHEN p_unit = 'MO' THEN
             t_date:= add_months(p_end_dt,0 - p_measure);
             
             -- added to include any fund that started on first business day and not the first calendar day.  11/30
             select min(cal_day) INTO t_bus_day
             from calendar
             where cal_day between   trunc(p_end_dt,'mm') and p_end_dt
             and bus_day_flg = 'Y';
             --IF t_date < (p_inc_dt - 1) then
             -- added to include any fund that started on first business day and not the first calendar day.  11/30
             IF t_bus_day < p_inc_dt then
                t_date := NULL;
             end if;             
        WHEN p_unit = 'DY' THEN
             IF p_measure = 1 THEN
                -- Consider if Month end DAte is a holiday
                -- if calculating on the inception date, go with the last day.  There will be a WI. 2/8/16 
                if p_end_dt = p_inc_dt then
                    t_date := p_end_dt - 1;
                else
                /*
                    BEGIN
                       SELECT MAX(CAL_DAY) into t_last_day
                        FROM HDM.CALENDAR
                        WHERE CAL_DAY <= p_end_dt
                        AND BUS_DAY_FLG = 'Y';
                        
                        SELECT MAX(CAL_DAY) into t_date
                        FROM HDM.CALENDAR
                        WHERE CAL_DAY < t_last_day
                        AND BUS_DAY_FLG = 'Y';    
                         
                        --t_date:= p_end_dt - p_measure;              
                    END;
*/
                    t_date:= p_end_dt - p_measure;
                end if;
             ELSE       
                t_date:= p_end_dt - p_measure;
             END IF;
             IF t_date < (p_inc_dt - 1) then
                t_date := NULL;
              end if;             
        WHEN p_unit = 'WK' THEN
             t_date:= p_end_dt - (p_measure * 7);    
             IF t_date < (p_inc_dt - 1) then
                t_date := NULL;
              end if;                          
        WHEN p_unit = 'MT' THEN
             t_date:= to_date(trunc(p_end_dt,'mm') - 1);
             IF t_date < p_inc_dt AND p_end_dt <> last_day(p_end_dt) THEN
             -- Check if it is a monthend return calculation (Date is last date of the month)
                    t_date := p_inc_dt - 1;
             end if;             
        WHEN p_unit = 'QT' THEN
             t_date:= to_date(trunc(p_end_dt,'Q') - 1);    

             -- added to include any fund that started on first business day and not the first calendar day.  11/30
             select min(cal_day) INTO t_bus_day
             from calendar
             where cal_day between   t_date and p_end_dt
             and bus_day_flg = 'Y';
             
             -- Check if it is a monthend return calculation (Date is last date of the month)
             IF t_date < p_inc_dt AND p_end_dt <> last_day(p_end_dt) THEN
             
                    t_date := p_inc_dt - 1;
             elsif t_bus_day < p_inc_dt then
                t_date := NULL;
             end if;             
                         
        WHEN p_unit = 'YT' THEN
             t_date:= to_date(trunc(p_end_dt,'Y') - 1);
             -- If fund inception date is current year, YTD should start from Inception date
             -- For Month end return, do not calculate YTD if Inception is in current Year.
             IF t_date < p_inc_dt AND p_end_dt = last_day(p_end_dt) THEN
             -- Check if it is a monthend return calculation (Date is last date of the month)
                    t_date := NULL;
             end if;
             IF t_date < p_inc_dt AND p_end_dt <> last_day(p_end_dt) THEN
             -- Check if it is a monthend return calculation (Date is last date of the month)
                    t_date := p_inc_dt - 1;
             end if;
        WHEN p_unit = 'IN' THEN
             t_date := p_inc_dt - 1;  -- Dummy WI Date.
        END CASE;
        RETURN t_date;
END get_begin_dt;

FUNCTION calc_Return(
                      p_end_dt      IN DATE,
                      p_beg_dt        IN DATE,
                      p_Dim_key     IN NUMBER,
                      p_annl_fctr    IN hdm.return_periods.annl_fctr%TYPE,
                      p_calc_type    IN VARCHAR2
                    ) RETURN NUMBER
IS
        t_return    NUMBER;
        t_begin_wi    NUMBER;
        t_end_wi    NUMBER;
        
BEGIN
        CASE p_calc_type
        WHEN 'FUND' THEN
            -- GET THE start wealth Index.
            BEGIN
                SELECT WLTH_IDX into t_begin_wi
                FROM HDM.FUND_VALUATION,HDM.CALENDAR
                WHERE FUND_VALUATION.DAY_KEY=CALENDAR.DAY_KEY
                AND FUND_VALUATION.FUND_KEY = p_dim_key
                AND CALENDAR.cal_day = p_beg_dt;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                    t_begin_wi := NULL;
            WHEN OTHERS THEN
                RAISE;
            END;
            -- GET THE end wealth Index.
            BEGIN
                SELECT WLTH_IDX into t_end_wi
                FROM HDM.FUND_VALUATION,HDM.CALENDAR
                WHERE FUND_VALUATION.DAY_KEY=CALENDAR.DAY_KEY
                AND FUND_VALUATION.FUND_KEY = p_dim_key
                AND CALENDAR.cal_day = p_end_dt;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                    t_end_wi := NULL;
            WHEN OTHERS THEN
                RAISE;
            END;
        WHEN 'ASOF' THEN
            BEGIN
                SELECT ASOF_WLTH_IDX into t_begin_wi
                FROM HDM.FUND_VALUATION,HDM.CALENDAR
                WHERE FUND_VALUATION.DAY_KEY=CALENDAR.DAY_KEY
                AND FUND_VALUATION.FUND_KEY = p_dim_key
                AND CALENDAR.cal_day = p_beg_dt;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                    t_begin_wi := NULL;
            WHEN OTHERS THEN
                RAISE;
            END;
            -- GET THE end wealth Index.
            BEGIN
                SELECT ASOF_WLTH_IDX into t_end_wi
                FROM HDM.FUND_VALUATION,HDM.CALENDAR
                WHERE FUND_VALUATION.DAY_KEY=CALENDAR.DAY_KEY
                AND FUND_VALUATION.FUND_KEY = p_dim_key
                AND CALENDAR.cal_day = p_end_dt;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                    t_end_wi := NULL;
            WHEN OTHERS THEN
                RAISE;
            END;    
        WHEN 'INDEX' THEN
            BEGIN
                SELECT IDX_VLTN_AMT into t_begin_wi
                FROM HDM.INDEX_VALUATION,HDM.CALENDAR
                WHERE INDEX_VALUATION.DAY_KEY=CALENDAR.DAY_KEY
                AND INDEX_VALUATION.BMK_IDX_KEY = p_dim_key
                AND CALENDAR.cal_day = p_beg_dt;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                    t_begin_wi := NULL;
            WHEN OTHERS THEN
                RAISE;
            END;
            -- GET THE end wealth Index.
            BEGIN
                SELECT IDX_VLTN_AMT into t_end_wi
                FROM HDM.INDEX_VALUATION,HDM.CALENDAR
                WHERE INDEX_VALUATION.DAY_KEY=CALENDAR.DAY_KEY
                AND INDEX_VALUATION.BMK_IDX_KEY = p_dim_key
                AND CALENDAR.cal_day = p_end_dt;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                    t_end_wi := NULL;
            WHEN OTHERS THEN
                RAISE;
            END;            
        END CASE;
        
        -- calculate Annualized Return
        t_return := (POWER((t_end_wi/t_begin_wi),(1/p_annl_fctr)) - 1);
        
        RETURN t_return;
END calc_return;
        
FUNCTION  fund_perf(p_end_dt IN DATE,p_run_mode NUMBER DEFAULT 0) return FUND_PERF_TABLE PIPELINED
IS
--    CURSOR cfund IS
--    SELECT FUND_KEY,FUND_INCPN_DT,FUND_COMPST_KEY
--    FROM HDM.FUND
--    WHERE ( FUND_STAT_CD IN (1,5) or nvl(fund_clos_dt,p_end_dt) >= p_end_dt) 
--    AND FUND_INCPN_DT <= p_end_dt
--    ORDER BY FUND_SORT_ORD;
-- Changed on 3/1/16 to enable retirement class performance calculation.
    CURSOR cfund IS
    SELECT fund_key,
--  changed on 2/3/17 to use Performance Inception Date instead of fund inception date.      
--    (Case When shr_cls_desc = 'Retirement Class' Then compst_incpn_dt else fund_incpn_dt end) fund_incpn_dt,
    fund.perf_incpn_dt fund_incpn_dt,
    cmp.fund_compst_key
    FROM HDM.fund,HDM.FUND_COMPOSITE cmp
    WHERE fund.fund_compst_key=cmp.fund_compst_key;

    
    CURSOR cPeriod (p_yr IN NUMBER) IS
    SELECT PER_KEY,PER_UT,PER_MSR,ANNL_FCTR,PER_SORT_ORD
    FROM HDM.RETURN_PERIODS
    WHERE (DAILY_MSR_FLG = 'Y' OR MO_END_MSR_FLG = 'Y')
    AND NOT (PER_UT = 'YR' AND PER_MSR > p_yr)
    ORDER BY PER_SORT_ORD;
    
    t_fund_incpn_dt     HDM.FUND.FUND_INCPN_DT%TYPE;
    t_incp              NUMBER;
    t_return            HDM.FUND_RETURN.FUND_RET%TYPE;
    t_asof_ret          HDM.FUND_RETURN.ASOF_FUND_RET%TYPE;
    t_curr_yr_days      NUMBER;
    t_fnd               NUMBER;
    t_per               NUMBER;
    t_beg_dt            HDM.FUND_RETURN.STRT_PER%TYPE;
    t_annl              NUMBER;
    t_annl_yr           NUMBER;
    t_day_key           HDM.CALENDAR.DAY_KEY%TYPE;
    
    t_ret_key           HDM.FUND_RETURN.fund_ret_key%TYPE;
    t_days              NUMBER;
    
    TYPE Fund_Table IS TABLE OF cFUND%ROWTYPE;
    TYPE Per_Table  IS TABLE OF cPeriod%ROWTYPE;
    
    tbl_FUND Fund_Table;
    tbl_PER  Per_Table;
    
    t_result HDM.FUND_RETURN%ROWTYPE;
    
BEGIN
        -- Count days in current Year.
        t_curr_yr_days := p_end_dt - add_months(p_end_dt,-12);
        

        -- main Loop thru Funds
        OPEN cFund;
        FETCH cFund      BULK COLLECT INTO tbl_fund;
        
    
        FOR t_fnd IN 1..tbl_fund.COUNT LOOP
            -- Gets number of  Years between return calculation date and fund inception date.
            t_fund_incpn_dt := tbl_fund(t_fnd).fund_incpn_dt;
            t_incp    := floor(months_between(trunc(p_end_dt)+1,t_fund_incpn_dt)/12) ;
            
            OPEN cPeriod(t_incp);
            FETCH cPeriod BULK COLLECT INTO tbl_Per;
            
            -- Loop thru all return Periods
            FOR t_Per IN 1..tbl_Per.COUNT LOOP
                t_beg_dt := PKG_FUND_PERF.get_begin_dt(tbl_per(t_per).PER_UT,tbl_per(t_per).PER_MSR,tbl_fund(t_fnd).fund_incpn_dt,p_end_dt);
                
                -- Annualization factor for Inception
                IF tbl_per(t_per).PER_UT = 'IN' THEN
                    t_annl_yr     := floor(months_between(trunc(p_end_dt),tbl_fund(t_fnd).fund_incpn_dt)/12);
                    t_days        := trunc(p_end_dt) - (add_months(tbl_fund(t_fnd).fund_incpn_dt,(floor(months_between(p_end_dt,tbl_fund(t_fnd).fund_incpn_dt)/12)* 12 )) - 1);
                    t_annl        := t_annl_yr + (t_days/t_curr_yr_days);
                ELSE
                    t_annl        := tbl_per(t_per).ANNL_FCTR;
                END IF;
                t_annl            := (Case When t_annl < 1 then 1 else t_annl end);
                t_return          := PKG_FUND_PERF.calc_Return(p_end_dt,t_beg_dt,tbl_fund(t_fnd).fund_key,t_annl,'FUND');
                t_asof_ret        := PKG_FUND_PERF.calc_Return(p_end_dt,t_beg_dt,tbl_fund(t_fnd).fund_key,t_annl,'ASOF');
                BEGIN
                    SELECT day_key into t_day_key
                    FROM HDM.CALENDAR
                    WHERE cal_day = p_end_dt;
                END;
                
                IF t_return IS NOT NULL THEN
                    IF p_run_mode = 1 then
                        SELECT HDM.SEQ_FUND_RETURN.nextval INTO t_ret_key FROM dual;
                    END IF;

                    t_result.FUND_RET_KEY           := t_ret_key;
                    t_result.FUND_KEY               := tbl_fund(t_fnd).fund_key;
                    t_result.PER_KEY                := tbl_per(t_per).per_key;
                    t_result.DAY_KEY                := t_day_key;
                    t_result.STRT_PER               := t_beg_dt;
                    t_result.FUND_RET               := t_return;
                    t_result.ANNLZN_FCTR            := t_annl;
                    t_result.ASOF_FUND_RET          := t_asof_ret;
                    t_result.RET_PER_FLG            := (Case When p_end_dt = last_day(p_end_dt) Then 'M' else 'D' end);
                    t_result.CURR_ROW_FLG           := 'Y';
                    t_result.ROW_STRT_DTTM          := SYSDATE;
                    t_result.ROW_STOP_DTTM          := NULL;
                    t_result.ETL_LOAD_CYC_KEY       := NULL;
                    t_result.SRC_SYS_ID             := NULL;
                    t_result.RET_VRFYN_FLG       := 'N';
                    
                    PIPE ROW(t_result);
                END IF;
            END LOOP;
            Close cPeriod;
        END LOOP;
        close cFund;
        RETURN;
END fund_perf;


FUNCTION  Index_perf(p_end_dt IN DATE,p_run_mode NUMBER default 0) return INDEX_PERF_TABLE PIPELINED
IS
    CURSOR cIndex IS
     SELECT  fnd.FUND_INCPN_DT,
    sum( (case when run_type_cd = 'REPORTING' then 4 else 0 end ) + (case when run_type_cd = 'DAILY_PERF' then 2 else 0 end ) + (case when run_type_cd = 'TARGET_FUNDS' then 1 else 0 end ) )  report_Type,
    min(rel.rpt_sort_ord) rpt_sort_ord,
    COMP.FUND_COMPST_KEY,INDX.BMK_IDX_KEY
    FROM HDM.FUND_COMPOSITE comp,HDM.FUND_INDeX_RLTN rel,hdm.benchmark_index indx,HDM.PERF_RUN_TYPE typ,
    -- changed fund_incpn_dt to perf_incpn_dt 2/3/17
    (select fund_compst_key,min(perf_Incpn_dt) fund_incpn_dt,min(fund_clos_dt) fund_clos_dt from hdm.fund group by fund_compst_key) fnd
    WHERE comp.fund_compst_key=REL.FUND_COMPST_KEY
    and comp.fund_compst_key = fnd.fund_compst_key
    and REL.PERF_RUN_TYPE_KEY=typ.perf_run_type_key
    AND REL.BMK_IDX_KEY=INDX.BMK_IDX_KEY
    and (COMPST_STAT_CD IN ('O','S') or nvl(fund_clos_dt,p_end_dt) >= p_end_dt)  -- if runs historically, this should run for closed funds..
    AND fnd.fund_incpn_dt   <= p_end_dt
    group by  COMP.FUND_COMPST_KEY,INDX.BMK_IDX_KEY,fnd.fund_incpn_dt
    ORDER BY rpt_sort_ord;

    
    CURSOR cPeriod (p_yr IN NUMBER) IS
    SELECT PER_KEY,PER_UT,PER_MSR,ANNL_FCTR,PER_SORT_ORD
    FROM HDM.RETURN_PERIODS
    WHERE (DAILY_MSR_FLG = 'Y' OR MO_END_MSR_FLG = 'Y')
    AND NOT (PER_UT = 'YR' AND PER_MSR > p_yr)
    ORDER BY PER_SORT_ORD;
    
    t_fund_incpn_dt     HDM.FUND.FUND_INCPN_DT%TYPE;
    t_incp              NUMBER;
    t_return            HDM.INDEX_RETURN.IDX_RET%TYPE;
    t_curr_yr_days      NUMBER;
--    t_idx               NUMBER;
--    t_per               NUMBER;
    t_beg_dt            HDM.INDEX_RETURN.STRT_PER%TYPE;
    t_annl              NUMBER;
    t_annl_yr           NUMBER;
    t_day_key           HDM.CALENDAR.DAY_KEY%TYPE;
    
    t_ret_key           HDM.INDEX_RETURN.idx_ret_key%TYPE;
    t_days              NUMBER;
    t_sort              NUMBER;
    
    TYPE Index_Table IS TABLE OF cIndex%ROWTYPE;
    TYPE Per_Table  IS TABLE OF cPeriod%ROWTYPE;
    
    tbl_Index Index_Table;
    tbl_PER  Per_Table;
    
    t_result HDM.INDEX_RETURN%ROWTYPE;
    
BEGIN
        -- Count days in current Year.
        t_curr_yr_days := p_end_dt - add_months(p_end_dt,-12);
        t_sort := 0;
        
        -- main Loop thru Funds
        OPEN cIndex;
        FETCH cIndex      BULK COLLECT INTO tbl_Index;
        
    
        FOR t_idx IN 1..tbl_Index.COUNT LOOP
            -- Gets number of  Years between return calculation date and fund inception date.
            t_fund_incpn_dt := tbl_Index(t_idx).fund_incpn_dt;
            t_incp    := floor(months_between(trunc(p_end_dt)+1,t_fund_incpn_dt)/12)  ;
            
            OPEN cPeriod(t_incp);
            FETCH cPeriod BULK COLLECT INTO tbl_Per;
            
            -- Loop thru all return Periods
            FOR t_Per IN 1..tbl_Per.COUNT LOOP
                t_beg_dt := PKG_FUND_PERF.get_begin_dt(tbl_per(t_per).PER_UT,tbl_per(t_per).PER_MSR,tbl_Index(t_idx).fund_incpn_dt,p_end_dt);
                
                -- Annualization factor for Inception
                IF tbl_per(t_per).PER_UT = 'IN' THEN
                    t_annl_yr     := floor(months_between(trunc(p_end_dt),tbl_Index(t_idx).fund_incpn_dt)/12);
                    t_days        := trunc(p_end_dt) - (add_months(tbl_Index(t_idx).fund_incpn_dt,(floor(months_between(p_end_dt,tbl_Index(t_idx).fund_incpn_dt)/12)* 12 )) - 1);
                    t_annl        := t_annl_yr + (t_days/t_curr_yr_days);
                ELSE
                    t_annl        := tbl_per(t_per).ANNL_FCTR;
                END IF;
                t_annl            := (Case When t_annl < 1 then 1 else t_annl end);
                t_return          := PKG_FUND_PERF.calc_Return(p_end_dt,t_beg_dt,tbl_Index(t_idx).bmk_idx_key,t_annl,'INDEX');
                BEGIN
                    SELECT day_key into t_day_key
                    FROM HDM.CALENDAR
                    WHERE cal_day = p_end_dt;
                END;
                -- Determine if Index is used for Daily, Regular or Both
                
                IF t_return IS NOT NULL THEN
                    If p_run_Mode = 1 Then
                        SELECT HDM.SEQ_INDEX_RETURN.NEXTVAL into t_ret_key FROM dual;
                    end if;
                    t_sort                                   := t_sort + 1;
                    
                    t_result.IDX_RET_KEY           := t_ret_key;
                    t_result.FUND_COMPST_KEY       := tbl_Index(t_idx).fund_compst_key;
                    t_result.PER_KEY               := tbl_per(t_per).per_key;
                    t_result.bmk_idx_key           := tbl_Index(t_idx).bmk_idx_key;
                    t_result.perf_displ_ord        := t_sort;
                    t_result.DAY_KEY               := t_day_key;
                    t_result.STRT_PER              := t_beg_dt;
                    t_result.IDX_RET               := t_return;
                    t_result.ANNLZN_FCTR           := t_annl;
                    t_result.RET_PER_FLG           := (Case When p_end_dt = last_day(p_end_dt) Then 'M' else 'D' end);
                    t_result.gen_rptg_flg          := (Case when  tbl_index(t_idx).report_type in (4,6) then 'Y' else 'N' end);
                    t_result.daily_rptg_flg        := (Case when  tbl_index(t_idx).report_type in (2,6) then 'Y' else 'N' end);
                    t_result.trgt_rptg_flg         := (Case when  tbl_index(t_idx).report_type in (1) then 'Y' else 'N' end);
                    t_result.CURR_ROW_FLG          := 'Y';
                    t_result.ROW_STRT_DTTM         := SYSDATE;
                    t_result.ROW_STOP_DTTM         := NULL;
                    t_result.ETL_LOAD_CYC_KEY      := NULL;
                    t_result.SRC_SYS_ID            := NULL;
                    
                    PIPE ROW(t_result);
                END IF;
            END LOOP;
            Close cPeriod;
        END LOOP;
        close cIndex;
        RETURN;
END Index_perf;


PROCEDURE load_fund_perf(p_end_dt in DATE)
IS

BEGIN
    MERGE INTO HDM.FUND_RETURN tgt
    USING (SELECT FUND_RET_KEY,FUND_KEY,PER_KEY,DAY_KEY,STRT_PER,FUND_RET,ANNLZN_FCTR,ASOF_FUND_RET,RET_PER_FLG,CURR_ROW_FLG,ROW_STRT_DTTM,RET_VRFYN_FLG
    FROM TABLE(PKG_FUND_PERF.FUND_PERF(p_end_dt,1))) src
    ON (tgt.fund_key=src.fund_key AND TGT.DAY_KEY=src.day_key AND tgt.per_key=src.per_key)
    WHEN NOT MATCHED THEN 
        INSERT (FUND_RET_KEY,
                    FUND_KEY,
                    PER_KEY,
                    DAY_KEY,
                    STRT_PER,
                    FUND_RET,
                    ANNLZN_FCTR,
                    ASOF_FUND_RET,
                    RET_PER_FLG,
                    CURR_ROW_FLG,
                    ROW_STRT_DTTM,
                    RET_VRFYN_FLG)
         VALUES (src.FUND_RET_KEY,
                    src.FUND_KEY,
                    src.PER_KEY,
                    src.DAY_KEY,
                    src.STRT_PER,
                    src.FUND_RET,
                    src.ANNLZN_FCTR,
                    src.ASOF_FUND_RET,
                    src.RET_PER_FLG,
                    src.CURR_ROW_FLG,
                    src.ROW_STRT_DTTM,
                    src.RET_VRFYN_FLG)
     WHEN MATCHED THEN
          UPDATE SET
                    tgt.STRT_PER=src.STRT_PER,
                    tgt.FUND_RET=src.FUND_RET,
                    tgt.ANNLZN_FCTR=src.ANNLZN_FCTR,
                    tgt.ASOF_FUND_RET=src.ASOF_FUND_RET,
                    tgt.RET_PER_FLG=src.RET_PER_FLG,
                    tgt.CURR_ROW_FLG=src.CURR_ROW_FLG,
                    tgt.ROW_STRT_DTTM=src.ROW_STRT_DTTM,
                    tgt.RET_VRFYN_FLG=src.RET_VRFYN_FLG;
      
     COMMIT;
EXCEPTION
        when others then
        dbms_output.put_line('Fund return load failed for ' || p_end_dt );
        RAISE;                 

END load_fund_perf;

PROCEDURE load_Index_perf(p_end_dt in DATE) 
IS

BEGIN
        MERGE INTO HDM.INDEX_RETURN tgt
        USING (SELECT IDX_RET_KEY,PER_KEY ,BMK_IDX_KEY,FUND_COMPST_KEY ,DAY_KEY ,IDX_RET ,STRT_PER,ANNLZN_FCTR,RET_PER_FLG,PERF_DISPL_ORD,GEN_RPTG_FLG ,DAILY_RPTG_FLG,
                                TRGT_RPTG_FLG,CURR_ROW_FLG,ROW_STRT_DTTM,ROW_STOP_DTTM, ETL_LOAD_CYC_KEY,SRC_SYS_ID
                                FROM TABLE(PKG_FUND_PERF.INDEX_PERF(p_end_dt,1)) )  src
        ON (TGT.BMK_IDX_KEY=src.bmk_idx_key AND tgt.fund_compst_key=src.fund_compst_key AND tgt.per_key=src.per_key AND tgt.day_key=src.day_key)
        WHEN NOT MATCHED THEN
            INSERT (IDX_RET_KEY,
                         PER_KEY ,
                         BMK_IDX_KEY,
                         FUND_COMPST_KEY ,
                         DAY_KEY ,
                         IDX_RET ,
                         STRT_PER,
                         ANNLZN_FCTR,
                         RET_PER_FLG,
                         PERF_DISPL_ORD,
                         GEN_RPTG_FLG ,
                         DAILY_RPTG_FLG,
                         TRGT_RPTG_FLG,
                         CURR_ROW_FLG,
                         ROW_STRT_DTTM,
                         ROW_STOP_DTTM, 
                         ETL_LOAD_CYC_KEY,
                         SRC_SYS_ID)
                 VALUES
                        (src.IDX_RET_KEY,
                         src.PER_KEY ,
                         src.BMK_IDX_KEY,
                         src.FUND_COMPST_KEY ,
                         src.DAY_KEY ,IDX_RET ,
                         src.STRT_PER,
                         src.ANNLZN_FCTR,
                         src.RET_PER_FLG,
                         src.PERF_DISPL_ORD,
                         src.GEN_RPTG_FLG ,
                         src.DAILY_RPTG_FLG,
                         src.TRGT_RPTG_FLG,
                         src.CURR_ROW_FLG,
                         src.ROW_STRT_DTTM,
                         src.ROW_STOP_DTTM, 
                         src.ETL_LOAD_CYC_KEY,
                         src.SRC_SYS_ID)
        WHEN MATCHED THEN
            UPDATE SET                
                         IDX_RET                = src.IDX_RET,
                         STRT_PER              = src.STRT_PER,
                         ANNLZN_FCTR        = src.ANNLZN_FCTR,
                         RET_PER_FLG         = src.RET_PER_FLG,
                         PERF_DISPL_ORD     = src.PERF_DISPL_ORD,
                         GEN_RPTG_FLG        = src.GEN_RPTG_FLG,
                         DAILY_RPTG_FLG      =src.DAILY_RPTG_FLG,
                         TRGT_RPTG_FLG      =src.TRGT_RPTG_FLG;
        COMMIT;
EXCEPTION
        when others then
        dbms_output.put_line('Index return load failed for ' || p_end_dt );
        RAISE;                
END load_index_perf;               

PROCEDURE verify_fund_return(p_end_dt in DATE)
IS
    
BEGIN
 -- UPDATE THE FUND_RETURN TABLE VERIFICATION FLAG FOR THE SUPPLIED MONTH END.  THIS PROCEDURE IS RUN AFTER
 -- ACCOUNTING CONFIRMS THE MONTH END RETURN VALUES.
 
 UPDATE hdm.FUND_RETURN
    SET RET_VRFYN_FLG = 'Y'
    WHERE DAY_KEY=(SELECT DAY_KEY FROM HDM.CALENDAR WHERE CAL_DAY=p_end_dt);
    
    commit;
    
END verify_fund_return;  

PROCEDURE  load_asofnav_return IS
        CURSOR AON IS
        SELECT cal_day
        FROM hdm.fund_valuation val,hdm.calendar cal
        WHERE VAL.DAY_KEY=cal.day_key
        AND val.row_strt_dttm > trunc(sysdate)
        group by cal_day;
        
        
    
BEGIN

        FOR cur IN aon loop
                    MERGE INTO HDM.FUND_RETURN tgt
                    USING (SELECT FUND_RET_KEY,FUND_KEY,PER_KEY,DAY_KEY,STRT_PER,FUND_RET,ANNLZN_FCTR,ASOF_FUND_RET,RET_PER_FLG,CURR_ROW_FLG,ROW_STRT_DTTM,RET_VRFYN_FLG
                    FROM TABLE(PKG_FUND_PERF.FUND_PERF(cur.cal_day,1))) src
                    ON (tgt.fund_key=src.fund_key AND TGT.DAY_KEY=src.day_key AND tgt.per_key=src.per_key)
                    WHEN MATCHED THEN
                          UPDATE SET
                                    tgt.ASOF_FUND_RET=src.ASOF_FUND_RET,
                                    tgt.ROW_STRT_DTTM=src.ROW_STRT_DTTM;
       END LOOP;

        COMMIT;
        
EXCEPTION
        when others then
        dbms_output.put_line('as OF LOAD FAILED' );
        RAISE;    

END  load_asofnav_return;

PROCEDURE load_gross_wealth_Index(p_end_dt IN DATE) IS
    t_gr_Key NUMBER;
BEGIN
    
    t_gr_key := 0;
    
    BEGIN
        SELECT nvl(MAX(gr_wlth_idx_key),0) INTO t_gr_key
        FROM Gross_Wealth_index;
    end;
    
    MERGE INTO GROSS_WEALTH_INDEX TGT
    USING (
            SELECT rownum + t_gr_key t_key,ret.day_key,ret.fund_key,1 + fund_ret Net_1R, (1+fund_ret)*(1+ net_expns_ratio/12)  Gross_1R -- changed from  (fund_ret +1+( net_expns_ratio/12) )
            FROM fund_return ret,fund,return_periods per,calendar cal,vw_expense_Ratio ex
            WHERE ret.fund_key=fund.fund_key
            AND ret.per_key=per.per_key
            AND ret.day_key=cal.day_key
            AND ret.fund_key=ex.fund_key
            AND ret.day_key=ex.day_key
            AND per_nm='1MO'
            AND cal_day = p_end_dt
          ) SRC
    ON     (tgt.day_key = src.day_key AND tgt.fund_key= src.fund_key)
    WHEN MATCHED THEN
    UPDATE SET
            TGT.gr_wlth_idx  = src.Gross_1R,
            TGT.net_wlth_idx = src.Net_1R,
            TGT.row_strt_dttm = sysdate
    WHEN NOT MATCHED THEN
        INSERT 
            (
                GR_wlth_idx_key,
                day_key,
                fund_key,
                gr_wlth_idx,
                net_wlth_idx,
                curr_row_flg,
                row_strt_dttm
            )
        VALUES
            (
                src.t_key,
                src.day_key,
                src.fund_key,
                src.Gross_1R,
                src.Net_1R,
                'Y',
                sysdate
            );
    
    COMMIT;

END load_gross_wealth_Index;    

FUNCTION  gross_fund_perf(p_end_dt IN DATE,p_run_mode NUMBER DEFAULT 0) return GROSS_RETURN_TABLE PIPELINED
IS
    CURSOR cfund IS
    -- changed fund_incpn_dt to perf_incpn_dt on 2/3/17
    SELECT FUND_KEY,perf_incpn_dt FUND_INCPN_DT,FUND.FUND_COMPST_KEY,REL.BMK_IDX_KEY
    FROM HDM.FUND FUND,HDM.FUND_INDEX_RLTN REL,HDM.BENCHMARK_INDEX IDX
    WHERE FUND.FUND_COMPST_KEY=REL.FUND_COMPST_KEY
    AND REL.BMK_IDX_KEY=IDX.BMK_IDX_KEY
    AND (LINK_PRIO = 1 OR PRIM_BMK_FLG = 'z')  -- include gross return benchmarks for target funds
    AND PRIM_BMK_FLG in ( 'Y','z')
    AND PERF_RUN_TYPE_KEY IN (1)
    AND FUND_STAT_CD IN (1,5)
    AND PERF_INCPN_DT <= p_end_dt
    ORDER BY FUND_SORT_ORD;
    
    CURSOR cPeriod (p_yr IN NUMBER) IS
    SELECT PER_KEY,PER_UT,PER_MSR,ANNL_FCTR,PER_SORT_ORD
    FROM HDM.RETURN_PERIODS
    WHERE (DAILY_MSR_FLG = 'Y' OR MO_END_MSR_FLG = 'Y')
    AND NOT (PER_UT = 'YR' AND PER_MSR > p_yr)
    ORDER BY PER_SORT_ORD;
    
    t_fund_incpn_dt     HDM.FUND.FUND_INCPN_DT%TYPE;
    t_incp              NUMBER;
    t_return            HDM.GROSS_RETURN.gr_RET%TYPE;
    t_curr_yr_days      NUMBER;
    t_fnd               NUMBER;
    t_per               NUMBER;
    t_beg_dt            HDM.FUND_RETURN.STRT_PER%TYPE;
    t_annl              NUMBER;
    t_annl_yr           NUMBER;
    t_day_key           HDM.CALENDAR.DAY_KEY%TYPE;
    t_begin_key            HDM.CALENDAR.DAY_KEY%TYPE;
    
    t_ret_key           HDM.gross_RETURN.gr_ret_key%TYPE;
    t_days              NUMBER;
    t_info_ratio        NUMBER;
    t_idx_ret           NUMBER;
    
    TYPE Fund_Table IS TABLE OF cFUND%ROWTYPE;
    TYPE Per_Table  IS TABLE OF cPeriod%ROWTYPE;
    
    tbl_FUND Fund_Table;
    tbl_PER  Per_Table;
    
    t_result HDM.GROSS_RETURN%ROWTYPE;
    
BEGIN
        -- Count days in current Year.
        t_curr_yr_days := p_end_dt - add_months(p_end_dt,-12);
        

        -- main Loop thru Funds
        OPEN cFund;
        FETCH cFund      BULK COLLECT INTO tbl_fund;
        
    
        FOR t_fnd IN 1..tbl_fund.COUNT LOOP
            -- Gets number of  Years between return calculation date and fund inception date.
            t_fund_incpn_dt := tbl_fund(t_fnd).fund_incpn_dt;
            t_incp    := floor(months_between(trunc(p_end_dt)+1,t_fund_incpn_dt)/12) ;
            
            OPEN cPeriod(t_incp);
            FETCH cPeriod BULK COLLECT INTO tbl_Per;
            
            -- Loop thru all return Periods
            FOR t_Per IN 1..tbl_Per.COUNT LOOP
                t_beg_dt := PKG_FUND_PERF.get_begin_dt(tbl_per(t_per).PER_UT,tbl_per(t_per).PER_MSR,tbl_fund(t_fnd).fund_incpn_dt,p_end_dt);
                
                -- Annualization factor for Inception
                IF tbl_per(t_per).PER_UT = 'IN' THEN
                    t_annl_yr     := floor(months_between(trunc(p_end_dt),tbl_fund(t_fnd).fund_incpn_dt)/12);
                    t_days        := trunc(p_end_dt) - (add_months(tbl_fund(t_fnd).fund_incpn_dt,(floor(months_between(p_end_dt,tbl_fund(t_fnd).fund_incpn_dt)/12)* 12 )) - 1);
                    t_annl        := t_annl_yr + (t_days/t_curr_yr_days);
                ELSE
                    t_annl        := tbl_per(t_per).ANNL_FCTR;
                END IF;
                t_annl            := (Case When t_annl < 1 then 1 else t_annl end);
 
                BEGIN
                    SELECT day_key into t_day_key
                    FROM HDM.CALENDAR
                    WHERE cal_day = p_end_dt;
                END;
            
                --dbms_output.put_line('UT = ' ||  tbl_per(t_per).PER_UT  || ', MSR= ' || tbl_per(t_per).PER_MSR);
                t_return          := PKG_FUND_PERF.calc_gr_Return(p_end_dt,t_beg_dt,tbl_fund(t_fnd).fund_key,t_annl);
                -- Just calculate Inf Ratio for YR and INCP measures
                t_info_ratio := NULL;
                If INSTR('YR,IN',SUBSTR(tbl_per(t_per).PER_UT ,1,2)) > 0 Then 
                    t_info_ratio      := PKG_FUND_PERF.calc_inf_ratio(p_end_dt,t_beg_dt,tbl_fund(t_fnd).fund_key,tbl_fund(t_fnd).bmk_idx_key,t_annl);
                End If;
                
                -- get Index return
                BEGIN
                    SELECT  idx_ret INTO t_idx_ret
                    FROM index_return ret,calendar cal,return_periods per
                    WHERE ret.day_key=cal.day_key
                    AND ret.per_key=per.per_key
                    AND cal_day = p_end_dt
                    AND ret.bmk_idx_key = tbl_fund(t_fnd).bmk_idx_key
                    AND RET.FUND_COMPST_KEY = tbl_fund(t_fnd).fund_compst_key
                    AND per.per_key = tbl_per(t_per).per_key;
                EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    DBMS_OUTPUT.PUT_LINE('NO DATA FOR ' || tbl_fund(t_fnd).bmk_idx_key ||',' || tbl_fund(t_fnd).fund_compst_key);
                    t_idx_ret := NULL;
                WHEN OTHERS THEN
                    RAISE;
                END;
                
                IF t_return IS NOT NULL THEN
                    IF p_run_mode = 1 then
                        SELECT HDM.SEQ_GROSS_RETURN.nextval INTO t_ret_key FROM dual;
                    END IF;

                    t_result.GR_RET_KEY             := t_ret_key;
                    t_result.FUND_KEY               := tbl_fund(t_fnd).fund_key;
                    t_result.PER_KEY                := tbl_per(t_per).per_key;
                    t_result.DAY_KEY                := t_day_key;
                    t_result.BMK_IDX_KEY            := tbl_fund(t_fnd).bmk_idx_key;
                    t_result.GR_RET                 := t_return;
                    t_result.info_ratio             := t_info_ratio;
                    t_result.idx_ret                := t_idx_ret;
                    t_result.CURR_ROW_FLG           := 'Y';
                    t_result.ROW_STRT_DTTM          := SYSDATE;
                    t_result.ROW_STOP_DTTM          := NULL;
                    t_result.ETL_LOAD_CYC_KEY       := NULL;
                    t_result.SRC_SYS_ID             := NULL;
                    
                    PIPE ROW(t_result);
                END IF;
            END LOOP;
            Close cPeriod;
        END LOOP;
        close cFund;
        RETURN;
EXCEPTION
   WHEN OTHERS THEN
       RAISE;
END gross_fund_perf;

FUNCTION calc_gr_Return(p_end_dt IN DATE,p_beg_dt IN DATE,p_fund_key IN NUMBER,p_annl_fctr IN NUMBER) RETURN NUMBER IS
        t_retval NUMBER;
BEGIN
            t_retval := NULL;
            SELECT exp(sum(ln(gr_wlth_idx)))   INTO t_retval
            FROM GROSS_WEALTH_INDEX a,calendar b
            WHERE a.day_key=b.day_key
            AND fund_key= p_fund_key
            AND cal_day > p_beg_dt 
            AND cal_day <= p_end_dt;
            
            t_retval := POWER(t_retval,1/p_annl_fctr) - 1;
            --dbms_output.put_line('Fund = ' || p_fund_key || ' , start dt= ' || to_char(p_beg_dt) || ', end dt= ' || to_char(p_end_dt) || ', ret= ' || t_retval);
            
            RETURN t_retval;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        t_retval := NULL;
        RETURN t_retval;
    WHEN OTHERS THEN
        RAISE;
END calc_gr_return;

            
PROCEDURE load_gross_return(p_end_dt IN DATE) IS

BEGIN
    MERGE INTO HDM.GROSS_RETURN tgt
    USING (SELECT GR_RET_KEY,FUND_KEY,PER_KEY,DAY_KEY,BMK_IDX_KEY,GR_RET,info_ratio,IDX_RET,CURR_ROW_FLG,ROW_STRT_DTTM
    FROM TABLE(PKG_FUND_PERF.GROSS_FUND_PERF(p_end_dt,1))) src
    ON (tgt.fund_key=src.fund_key AND TGT.DAY_KEY=src.day_key AND tgt.per_key=src.per_key AND tgt.bmk_idx_key=src.bmk_idx_key)
    WHEN NOT MATCHED THEN 
        INSERT (GR_RET_KEY,
                    FUND_KEY,
                    PER_KEY,
                    DAY_KEY,
                    BMK_IDX_KEY,
                    GR_RET,
                    INFO_RATIO,
                    IDX_RET,
                    CURR_ROW_FLG,
                    ROW_STRT_DTTM)
         VALUES (src.GR_RET_KEY,
                    src.FUND_KEY,
                    src.PER_KEY,
                    src.DAY_KEY,
                    src.BMK_IDX_KEY,
                    src.GR_RET,
                    src.INFO_RATIO,
                    src.IDX_RET,
                    src.CURR_ROW_FLG,
                    src.ROW_STRT_DTTM)
     WHEN MATCHED THEN
          UPDATE SET
                    tgt.GR_RET=src.GR_RET,
                    tgt.INFO_RATIO=src.INFO_RATIO,
                    tgt.IDX_RET = src.IDX_RET,
                    tgt.CURR_ROW_FLG=src.CURR_ROW_FLG,
                    tgt.ROW_STRT_DTTM=src.ROW_STRT_DTTM;
      
     COMMIT;
EXCEPTION
        when others then
        dbms_output.put_line('Fund return load failed for ' || p_end_dt );
        RAISE;                 

END load_GROSS_RETURN; 

FUNCTION  calc_inf_ratio(p_end_dt IN DATE,p_beg_dt IN DATE,p_fund_key IN NUMBER,p_bmk_idx_key IN NUMBER,p_annl_fctr IN NUMBER) RETURN NUMBER 
IS

        t_inf_ratio NUMBER;
        t_std_dev   NUMBER;
        t_retval    NUMBER;
    
BEGIN
        t_retval := NULL;
        
/*        SELECT exp(sum(ln(abs(gr.gr_ret - ir.idx_ret))))  * (Case when mod(sum(Case When (gr.gr_ret - ir.idx_ret) < 0 Then 1 Else 0 end),2) = 1 Then -1 Else 1 end),
        nullif(STDDEV_POP(gr.gr_ret - ir.idx_ret),0)  
        INTO t_retval,t_std_dev
        from gross_return gr,calendar cal,index_return ir,fund fn,return_periods pr
        where gr.day_key=cal.day_key
        and gr.fund_key=fn.fund_key
        and gr.day_key=ir.day_key
        and gr.per_key=ir.per_key
        and gr.bmk_idx_key=ir.bmk_idx_key
        and gr.per_key=pr.per_key
        and fn.fund_compst_key=IR.FUND_COMPST_KEY
        and cal_day between p_beg_dt and p_end_dt
        and pr.per_nm='1MO'
        and gr.fund_key=p_fund_key
        and ir.bmk_idx_key=p_bmk_idx_key;
        
*/
 --     CALCULATE PRODUCT OF EXCESS RETURN AND STANDARD DEVIATION.
        
        SELECT  exp(sum(ln(abs(EXCESS_RET)))) ,   nullif(STDDEV_POP(LOG_ER),0)   INTO t_retval,t_std_dev  
        from (
                select pr.per_nm,cal_day, gr_wlth_idx,ir.idx_ret, 1 + idx_ret idx_rtn_1,gr_wlth_idx / (1 + idx_ret) excess_ret,ln(gr_wlth_idx / (1 + idx_ret)) Log_ER
                from (  SELECT FUND_KEY,FUND_INCPN_DT,FUND.FUND_COMPST_KEY,REL.BMK_IDX_KEY
                        FROM HDM.FUND FUND,HDM.FUND_INDEX_RLTN REL,HDM.BENCHMARK_INDEX IDX
                        WHERE FUND.FUND_COMPST_KEY=REL.FUND_COMPST_KEY
                        AND REL.BMK_IDX_KEY=IDX.BMK_IDX_KEY
                        AND (LINK_PRIO = 1 OR PRIM_BMK_FLG = 'z')  -- include gross return benchmarks for target funds
                        AND PRIM_BMK_FLG in ( 'Y','z')
                        AND PERF_RUN_TYPE_KEY IN (1)
                        AND FUND_STAT_CD IN (1,5)
                        AND FUND_INCPN_DT <= p_end_dt
                        ORDER BY FUND_SORT_ORD) gr,
                calendar cal,index_return ir,fund fn,return_periods pr,gross_wealth_index gw
                where gw.day_key=cal.day_key
                and gw.fund_key=fn.fund_key
                and gw.day_key=ir.day_key
                and gr.bmk_idx_key=ir.bmk_idx_key
                and ir.per_key=pr.per_key
                and fn.fund_compst_key=IR.FUND_COMPST_KEY
                and gr.fund_key=gw.fund_key
                and ir.day_key=gw.day_key
                and cal_day between add_months(p_beg_dt,1) and  p_end_dt
                and pr.per_nm='1MO'
                and gr.fund_key=p_fund_key
                and ir.bmk_idx_key=p_bmk_idx_key
                order by 2
        );        
        
        
        t_inf_ratio := nullif(((((POWER(t_retval,1/p_annl_fctr))-1))/ (t_std_dev * sqrt(12))  ) / 100,0);
            
        return t_inf_ratio;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(p_fund_key || ',' || p_bmk_idx_key);
        dbms_output.put_line(p_beg_dt|| ',' || p_annl_fctr);
        dbms_output.put_line(sqlerrm);
        RAISE;
END calc_inf_ratio;     
                
end pkg_fund_perf;

CREATE OR REPLACE PACKAGE HDM.PKG_MANAGER_SEARCH AS
    Procedure Load_Manager_Data;
    Procedure Load_Score_Data;
    procedure Load_Manager_info;
    Procedure Load_Final_Score;
    Procedure Load_Univ_name;
    Procedure Cleanup;

end pkg_manager_search;

CREATE OR REPLACE package body HDM.pkg_manager_search AS
        
PROCEDURE Load_Score_data IS
    t_key NUMBER;
BEGIN
    select nvl(max(MGR_RANK_DATA_KEY),0) INTO t_key FROM HDM.MANAGER_RANK_DATA;

    MERGE INTO MANAGER_RANK_DATA TGT
    USING
        (
            SELECT t_key + rownum rank_key, firm,product,return_type,msr_list_key,f.univ_key,
            (Case when score= -99999 then null else score end) score,
            (Case when rank= -99999 then null else rank end) rank,
            day_key,mgr_list_key,B.MSR_NM,A.FIELD_NM,a.fld_id,last_day(a.ret_dt) ret_dt
            from stg.manager_score a,hdm.measure_list b,hdm.calendar c,hdm.manager_list d,hdm.universe f
            where a.fld_id=b.msr_id
            and A.FIRM=d.mgr_nm
            and a.product=d.prod_nm
            and a.univ_nm = f.univ_nm
            and last_day(a.srch_dt)=C.CAL_DAY
            --and a.score <> 0
           -- and (firm,product) Not in (select firm,product from stg.manager_score where  field_nm = 'Excess Rtn - 3 Years'  and score = 0)
        ) SRC
    ON (tgt.mgr_list_key=src.mgr_list_key AND tgt.msr_list_key=src.msr_list_key AND tgt.day_key=src.day_key and tgt.univ_key=src.univ_key)
    WHEN NOT MATCHED THEN
        INSERT (mgr_rank_data_key
                ,mgr_list_key
                ,msr_list_key
                ,univ_key
                ,day_key
                ,ret_set
                ,ret_val
                ,ret_rank
                ,ret_dt
                ,curr_row_flg
                ,row_strt_dttm
        )
        VALUES
            (    src.rank_key
                ,src.mgr_list_key
                ,src.msr_list_key
                ,src.univ_Key
                ,src.day_key
                ,src.return_type
                ,src.score
                ,src.rank
                ,src.ret_dt
                ,'Y'
                ,sysdate
        )
    WHEN MATCHED THEN
        UPDATE SET
            ret_set = src.return_type,
            ret_val = src.score,
            ret_rank = src.rank;
    COMMIT;
    
    -- calculate the Percentile
    
     MERGE INTO MANAGER_RANK_DATA TGT
     USING
        (
/*             select  mgr_rank_data_key,msr_list_key,sum(ret_val) score,ntile(100) over (partition by msr_list_key order by sum(ret_val) desc) as Ranking
             from manager_rank_data
             where day_key = (select day_key from hdm.calendar where cal_day = (select last_day(max(srch_dt)) from STG.MANAGER_SCORE))
             group by mgr_rank_data_key,msr_list_key
*/
               select mgr_rank_data_key,msr_list_key,score,(Case When rank_ord = 'O' then ranking else (Case When total_list < 100 then total_list + 1 else 101 end)  -ranking end) ranking,rank_ord
               FROM (         
                         select  mgr_rank_data_key,b.msr_list_key,sum(ret_val) score,
                         ntile(100) over (partition by b.msr_list_key order by nvl(sum( ret_val),(Case When rank_ord = 'O' then -99999 else 99999 end )) desc) as Ranking,
                         B.MSR_NM,msr_ord,rank_ord,
                         count(mgr_rank_data_key) over (partition by b.msr_list_key) total_list
                         from manager_rank_data a,measure_list b
                         where  A.MsR_LIST_KEY=b.msr_List_key
                         and day_key = (select day_key from hdm.calendar where cal_day = (select last_day(max(srch_dt)) from STG.MANAGER_SCORE))
                         and ret_val is not null
                         group by mgr_rank_data_key,b.msr_list_key      ,B.MSR_NM,msr_ord,rank_ord      
               )             
        )   SRC
        ON (tgt.mgr_rank_data_key=src.mgr_rank_data_key)
        WHEN MATCHED THEN
            UPDATE SET ret_Rank = src.ranking;
            
        COMMIT;
    
EXCEPTION
WHEN OTHERS THEN
        RAISE;
End load_score_data;

PROCEDURE Load_Manager_DATA IS
    t_row NUMBEr;
BEGIN
    SELECT nvl(MAX(mgr_list_key) ,0) INTO t_row
    FROM hdm.manager_list;
    
    MERGE INTO HDM.MANAGER_LIST TGT
    USING (
            select t_row + rownum myrow,firm,product,indx_flg
            from (
                        select  firm_nm firm,prod_nm product,indx_flg
                        from STG.MANAGER_info a
                        group by  firm_nm,prod_nm,indx_flg
                        order by 1,2
                )      
        ) src
    ON (tgt.mgr_nm=src.firm AND tgt.prod_nm=src.product)
    WHEN NOT MATCHED THEN
        INSERT (mgr_list_key,mgr_nm,prod_nm,idx_flg,curr_row_flg,row_strt_dttm)
        VALUES (src.myrow,src.firm,src.product,src.indx_flg,'Y',sysdate);
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        RAISE;
end load_manager_data;        


procedure Load_Manager_info IS

        t_row NUMBER;
       
BEGIN
        
        SELECT NVL(MAX(mgr_info_key),0) INTO t_row
                FROM manager_information;
                
        MERGE INTO manager_information TGT
        USING (
                    SELECT rownum + t_row info_key ,cal.day_key,univ.univ_key,mgr.mgr_list_key,firm_tot_aum,firm_yr,prod_tot,pref_bm,prim_univ,prod_stat,hldg_cnt,pct_hldg_top_ten,
                    annl_ltm,cash_posn,wgtd_avg_mkt,med_mkt ,info.prim_eq_cap,info.prim_eq_style
                    FROM stg.manager_info info,hdm.manager_list mgr,hdm.calendar cal,hdm.universe univ
                    WHERE last_day(info.info_dt)=cal.cal_day
                    AND info.firm_nm = mgr.mgr_nm
                    AND info.prod_nm = mgr.prod_Nm
                    and INFO.UNIV_NM=univ.univ_nm
                   -- AND info.hldg_cnt > 0
                   ) SRC
         ON (TGT.day_key=SRC.day_key AND TGT.mgr_list_key=SRC.mgr_list_key)
         WHEN NOT MATCHED THEN
                INSERT (mgr_info_key,day_key,mgr_list_key,univ_key,aum_tot,firm_incpn_yr,prod_tot,prfrd_bmk,prim_univ,prod_stat,curr_holdg_cnt,pct_holdg_top_ten,annl_ltm,curr_cash_posn,wgta_mkt_cap,mdn_mkt_cap,prim_eqty_cap,prim_eqty_styl,curr_row_flg,row_strt_dttm)
                VALUES (SRC.info_key,SRC.day_key,src.mgr_list_key,src.univ_key,src.firm_tot_aum,src.firm_yr,src.prod_tot,src.pref_bm,src.prim_univ,src.prod_stat,src.hldg_cnt,src.pct_hldg_top_ten,src.annl_ltm,src.cash_posn,src.wgtd_avg_mkt,src.med_mkt,src.prim_eq_cap,src.prim_eq_style,'Y',sysdate)
         WHEN MATCHED THEN
                UPDATE SET 
                    aum_tot              =   src.firm_tot_aum,
                    firm_incpn_yr       =   src.firm_yr,
                    prod_tot              =   src.prod_tot,
                    prfrd_bmk           = src.pref_bm,
                    prim_univ           = src.prim_univ,
                    prod_stat           =  src.prod_stat,
                    curr_holdg_cnt  = src.hldg_cnt,
                    pct_HOLDG_top_ten     =  src.pct_hldg_top_ten,
                    annl_ltm            = src.annl_ltm,
                    curr_cash_posn  = src.cash_posn,
                    wgta_mkt_cap    = src.wgtd_avg_mkt,
                    mdn_mkt_cap     = src.med_mkt,
                    prim_eqty_cap    = src.prim_eq_cap,
                    prim_eqty_styl    = src.prim_eq_style,
                    curr_row_flg    = 'Y',
                    row_strt_dttm   = sysdate;
                    
                    COMMIT;
                    
                    
end load_Manager_info;

Procedure Load_Final_Score IS
        t_row NUMBER; 
        t_date DATE;
        t_univ  NUMBER;
BEGIN
        
    -- get the key
    SELECT NVL(MAX(fnl_scor_key),0) INTO t_row
    FROM manager_final_score;
    
    SELECT LAST_DAY(MAX(SRCH_DT)) INTO t_date
    FROM STG.MANAGER_SCORE;
    
    -- get the universe.
    BEGIN
                     SELECT UNIV_KEY INTO t_univ
                     FROM UNIVERSE 
                     WHERE UNIV_NM IN (
                     SELECT UNIV_NM
                     FROM STG.MANAGER_SCORE
                     GROUP BY UNIV_NM);
    EXCEPTION
        WHEN OTHERS THEN
        RAISE;
   END;
    
    MERGE INTO HDM.MANAGER_FINAL_SCORE TGT
    USING   (
                    SELECT  t_row + rownum final_key,SOURCE.*
                    FROM (
                            SELECT a.univ_key,a.mgr_list_key,a.day_key,C.MSR_CAT_KEY,
--                  MOVED THE REVERSE RANKING CALCULATION TO THE DATA LOAD AT THE TIME OF CALCULATING RANKING.                            
 --                           sum((Case When yr3_wgt_flg = 'Y'  and yr3_flg=3 Then  (Case When B.RANK_ORD='O' Then a.ret_rank else 100 - ret_rank end) * B.CAT_WGT else 0  end )) yr3_score ,
  --                          sum((Case When yr5_wgt_flg = 'Y' and yr5_flg=5  Then  (Case When B.RANK_ORD='O' Then a.ret_rank else 100 - ret_rank end) * B.CAT_WGT else 0  end )) yr5_score ,
  --                          sum((Case When yr10_wgt_flg = 'Y'  and yr10_flg=10 Then  (Case When B.RANK_ORD='O' Then a.ret_rank else 100 - ret_rank end) * B.CAT_WGT else 0  end )) yr10_score 
                            sum((Case When yr3_wgt_flg = 'Y'  and yr3_flg=3 Then  ret_rank * B.CAT_WGT else 0  end )) yr3_score ,
                            sum((Case When yr5_wgt_flg = 'Y' and yr5_flg=5  Then ret_Rank * B.CAT_WGT else 0  end )) yr5_score ,
                            sum((Case When yr10_wgt_flg = 'Y'  and yr10_flg=10 Then ret_rank * B.CAT_WGT else 0  end )) yr10_score   
                             from manager_rank_data a,measure_list b,measure_category c,manager_list d,HDM.Calendar cal,
                             ( 
                                        select mgr_list_key,univ_key,
                                        max((Case when msr_nm='Excess Rtn - 3 Years ' and ret_val <> 0 Then 3 else 0 end)) yr3_flg,
                                        max((Case when msr_nm='Excess Rtn - 5 Years ' and ret_val <> 0 Then 5 else 0 end)) yr5_flg,
                                        max((Case when msr_nm='Excess Rtn - 10 Years ' and ret_val <> 0 Then 10 else 0 end)) yr10_flg
                                        from (
                                        select univ_key,mgr_list_key,msr_nm,yr3_wgt_flg,yr5_wgt_flg,yr10_wgt_flg,max(ret_val) ret_val
                                        from manager_rank_data a,measure_list b
                                        where  a.msr_list_key=b.msr_list_key 
                                        and msr_nm in (  'Excess Rtn - 3 Years ' , 'Excess Rtn - 5 Years ', 'Excess Rtn - 10 Years ')  
                                        group by univ_key,mgr_list_key,msr_nm,yr3_wgt_flg,yr5_wgt_flg,yr10_wgt_flg
                                        ) group by univ_key,mgr_list_key    
                             ) mgr
                             where a.msr_list_key=b.msr_list_key
                             and B.MSR_CAT_KEY=c.msr_cat_key
                             and a.mgr_list_key=d.mgr_list_key
                             and a.mgr_list_key=mgr.mgr_list_key
                             and a.univ_key=mgr.univ_key
                             and A.DAY_KEY=cal.day_key
                             and a.ret_rank <> 0 and ret_rank is not null
                             and cal_day = t_date
                             and a.univ_key=t_univ
                             group by a.univ_key,a.mgr_list_key,a.day_key,C.MSR_CAT_KEY
                             order by yr3_score
                    ) SOURCE
           ) SRC
    ON (tgt.msr_cat_key=src.msr_cat_key AND TGT.MGR_LIST_KEY=src.mgr_list_key AND tgt.day_key=src.day_key AND tgt.univ_key=src.univ_key)                     
     WHEN NOT MATCHED THEN
                 INSERT  ( fnl_scor_key,univ_key,mgr_list_key,msr_cat_key,day_key,yr3_scor,yr5_scor,yr10_scor,curr_row_flg,row_strt_dttm)
                 VALUES ( src.final_key,src.univ_key,src.mgr_list_key,src.msr_cat_key,src.day_key,src.yr3_score,src.yr5_score,src.yr10_score,'Y',sysdate)
     WHEN MATCHED THEN
                UPDATE SET 
                                yr3_scor=src.yr3_score,
                                yr5_scor=src.yr5_score,
                                yr10_scor=src.yr10_score;
                                
     COMMIT;
     
     -- calculate final score ranking (percentile)
     
    --yr3 rank
    MERGE INTO HDM.MANAGER_FINAL_SCORE TGT
    USING   (
                     SELECT sum(yr3_scor) yr3,mgr_list_key,a.day_key,
                     ntile(100) over (  order by nvl(sum( yr3_scor),-99999) ) as yr3_rank,
                     ntile(100) over ( order by nvl(sum( yr5_scor),-99999) ) as yr5_rank,
                     ntile(100) over ( order by nvl(sum( yr10_scor),-99999) ) as yr10_rank
                     FROM HDM.MANAGER_FINAL_SCORE a,hdm.calendar b
                     where a.day_key=b.day_key
                     and yr3_scor > 0
                     and cal_day = t_date
                     and univ_key=t_univ
                     GROUP BY mgr_list_key,a.day_key
                  ) SRC
    ON  (tgt.mgr_list_key=src.mgr_list_key and tgt.day_key=src.day_key)
    WHEN MATCHED THEN
        UPDATE SET
            yr3_rank   = src.yr3_rank;

 -- yr5 rank
     MERGE INTO HDM.MANAGER_FINAL_SCORE TGT
    USING   (
                     SELECT sum(yr3_scor) yr3,mgr_list_key,a.day_key,
                     ntile(100) over (  order by nvl(sum( yr3_scor),-99999) ) as yr3_rank,
                     ntile(100) over ( order by nvl(sum( yr5_scor),-99999) ) as yr5_rank,
                     ntile(100) over ( order by nvl(sum( yr10_scor),-99999) ) as yr10_rank
                     FROM HDM.MANAGER_FINAL_SCORE a,hdm.calendar b
                     where a.day_key=b.day_key
                     and cal_day = t_date
                     and univ_key=t_univ
                     and yr5_scor > 0
                     GROUP BY mgr_list_key,a.day_key
                  ) SRC
    ON  (tgt.mgr_list_key=src.mgr_list_key and tgt.day_key=src.day_key)
    WHEN MATCHED THEN
        UPDATE SET
            yr5_rank   = src.yr5_rank;

    MERGE INTO HDM.MANAGER_FINAL_SCORE TGT
    USING   (
                     SELECT sum(yr3_scor) yr3,mgr_list_key,a.day_key,
                     ntile(100) over (  order by nvl(sum( yr3_scor),-99999) ) as yr3_rank,
                     ntile(100) over ( order by nvl(sum( yr5_scor),-99999) ) as yr5_rank,
                     ntile(100) over ( order by nvl(sum( yr10_scor),-99999) ) as yr10_rank
                     FROM HDM.MANAGER_FINAL_SCORE a,hdm.calendar b
                     where a.day_key=b.day_key
                     and cal_day = t_date
                     and yr10_scor > 0
                     and univ_key=t_univ
                     GROUP BY mgr_list_key,a.day_key
                  ) SRC
    ON  (tgt.mgr_list_key=src.mgr_list_key and tgt.day_key=src.day_key)
    WHEN MATCHED THEN
        UPDATE SET
            yr10_rank  = src.yr10_rank;
 
   COMMIT;


END Load_Final_Score;
 
Procedure Load_Univ_name IS
    t_row NUMBER;
BEGIN
        
        -- get the last key
        SELECT nvl(MAX(univ_key),0) INTO t_row
        FROM Universe;
        
        MERGE INTO UNIVERSE tgt
        USING
            (SELECT rownum + t_row univ_key,UNIV_NM 
                FROM (
                        SELECT TRIM(univ_nm) UNIV_NM FROM STG.MANAGER_INFO
                        GROUP BY univ_nm)
            ) SRC
        ON (tgt.univ_nm=src.univ_nm)
        WHEN NOT MATCHED THEN
        INSERT  (univ_key,univ_nm,curr_row_flg,row_strt_dttm)
        VALUES (src.univ_key,src.univ_nm,'Y',sysdate);
        
        COMMIT;
 END;
 
 Procedure Cleanup IS
 
 BEGIN
    execute immediate 'TRUNCATE TABLE MANAGER_RANK_DATA';
    execute immediate 'TRUNCATE TABLE MANAGER_INFORMATION';
    execute immediate 'TRUNCATE TABLE MANAGER_FINAL_SCORE';
 END Cleanup;    
 

end pkg_manager_search;

CREATE OR REPLACE PACKAGE HDM.pkg_sdcm_update AS
PROCEDURE adw_sdcm_update;
end pkg_sdcm_update;

CREATE OR REPLACE PACKAGE BODY HDM.pkg_sdcm_update AS

PROCEDURE adw_sdcm_update IS
    CURSOR SRC IS
                select dlr.dlr_role_key,ACCT.CUST_ACCT_KEY,FUND.SHR_CLS_FUND_KEY,trde_dt,ord_nbr,shrs,gr_amt
                from stg.cash_movement sdcm,adw.share_class_fund fund,ADW.DEALER dlr,ADW.CUSTOMER_ACCOUNT acct
                where Sdcm.FUND_CD = FUND.FUND_NBR
                and SDCM.NOLOAD_ACCT_NBR=ACCT.ACCT_NBR
                and SDCM.DLR_ID=DLR.DLR_ID
                and fund.curr_row_flg='Y'
                and acct.curr_row_flg='Y'
                and dlr.curr_row_flg='Y'
                and sdcm.dlr_id <> 0
                and sdcm.noload_acct_nbr <> 0
                and ord_nbr <> 0;
                
   -- TYPE src_table IS TABLE OF src%ROWTYPE;
   
   TYPE src_table IS RECORD (
                DLR_ROLE_KEY  dbms_sql.number_table,
                cust_acct_key dbms_sql.number_table,
                shr_cls_fund_key dbms_sql.number_table,
                trde_dt DBMS_SQL.DATE_TABLE,
                ord_nbr dbms_sql.number_table,
                shrs dbms_sql.number_table,
                gr_amt dbms_sql.number_table
    );
    
    SrcData src_table;
    
    t_row NUMBER;
    
BEGIN
        OPEN src;
        LOOP
            FETCH src BULK COLLECT INTO 
                    SrcData.dlr_role_key,
                     SrcData.cust_acct_key,
                      SrcData.shr_cls_fund_key,
                       SrcData.trde_dt,
                        SrcData.ord_nbr,
                         SrcData.shrs,
                          SrcData.gr_amt;
                          
            FORALL t_row IN 1..SrcData.dlr_role_key.count
                UPDATE ADW.sdcm_cashflow
                SET cust_acct_key = SrcData.cust_acct_key(t_row),
                row_stop_dttm = row_strt_dttm,
                row_strt_dttm = sysdate,
                dq_lvl_cd = 'Y'
                WHERE dlr_role_key=SrcData.dlr_role_key(t_row)
                AND shr_cls_fund_key=SrcData.shr_cls_fund_key(t_row)
                AND trde_dt=SrcData.trde_dt(t_row)
                AND ord_nbr = SrcData.ord_nbr(t_row)
                AND shrs=SrcData.shrs(t_row)
                AND gr_amt=SrcData.gr_amt(t_row)
                AND CUST_acct_key IS NULL;
                EXIT WHEN src%NOTFOUND;
                
                
        END LOOP;
        COMMIT;
        CLOSE src;
END adw_sdcm_update;    
end pkg_sdcm_update;

CREATE OR REPLACE PACKAGE HDM.PKG_SMIP_PERFORMANCE as

    TYPE  SMIP_FUND_TABLE is TABLE OF SMIP_FUND_PERFORMANCE%ROWTYPE;

    
FUNCTION CALC_AWP_PERFORMANCE(p_end_date IN DATE) RETURN SMIP_FUND_TABLE PIPELINED;

FUNCTION CALC_NFQ_PERFORMANCE(p_end_date IN DATE) RETURN SMIP_FUND_TABLE PIPELINED;

PROCEDURE LOAD_SMIP_PERFORMANCE(p_end_date IN DATE) ;

PROCEDURE LOAD_AWP_PERFORMANCE(p_end_date IN DATE);

PROCEDURE LOAD_NFQ_PERFORMANCE(p_end_date IN DATE);

END PKG_SMIP_PERFORMANCE;

CREATE OR REPLACE PACKAGE BODY HDM.PKG_SMIP_PERFORMANCE as

FUNCTION CALC_AWP_PERFORMANCE(p_end_date IN DATE) RETURN SMIP_FUND_TABLE PIPELINED IS
    CURSOR cMetric IS 
     SELECT smip_mtric_key,mtric_cd,mtric_wgt,a.per_nm,a.per_wgt,a.per_key,expctd_perf,B.PER_MSR * 12 per_months
    from smip_metrics a,return_periods b
    where a.per_key=b.per_key
    and mtric_cd = 'AWP'
    and A.curr_row_flg = 'Y'
    and A.row_strt_dttm <= sysdate;
    
    CURSOR cfund(p_period_key number) IS
    select fnd.fund_Key,cmp.fund_compst_key,fund_nbr,PERF_incpn_dt fund_incpn_dt,fund_sort_ord,rel.bmk_idx_key,cal_day,per.per_key,per_nm,fund_ret,idx_ret,(fund_ret - idx_ret)  bp_diff,
    sum((fund_ret - idx_ret)) over () total_diff
    from hdm.fund fnd,
            hdm.fund_composite cmp,
            hdm.FUND_INDEX_RLTN rel,
            hdm.benchmark_index idx,
            HDM.PERF_RUN_TYPE app,
            hdm.fund_return fret,
            hdm.index_return iret,
            hdm.return_periods per,
            hdm.calendar cal
    where FND.FUND_COMPST_KEY=cmp.fund_compst_key
    and cmp.fund_compst_key=rel.fund_compst_key
    and rel.bmk_idx_key=idx.bmk_idx_key
    and rel.perf_run_type_key=app.perf_run_type_key
    and fret.fund_key=fnd.fund_key
    and fret.per_key=per.per_key
    and fret.day_key=cal.day_key
    and rel.bmk_idx_key=iret.bmk_idx_key
    and rel.fund_compst_key=iret.fund_compst_key
    and iret.per_key=per.per_key
    and iret.day_key=cal.day_key
    and shr_cls_cd = 1
   -- and fund_stat_cd in (1,5)
    and rel.prim_bmk_flg='Y'
   -- and CMP.COMPST_STAT_CD in ('O','S')
    and cmp.asset_cls_nm not In 'Target Retirement'
    and app.run_type_cd = 'REPORTING'
    and cal_day=p_end_date
    and PER.PER_key=p_period_key
    and FND.SHR_CLS_DESC='Institutional Class'
    order by fund_sort_ord;      
    
    t_day_key HDM.CALENDAR.day_key%TYPE;
    t_fund_ctr NUMBER;
    t_metric_ctr NUMBER;
    t_fund_ret HDM.FUND_RETURN.fund_ret%TYPE;
    t_idx_ret hdm.index_return.idx_ret%TYPE;
    t_outPerf Number;
    t_avg_asset_rt Number;
    t_wavg_perf Number;  -- Weighted Average Performance.
    t_min_outstanding Number;  -- Minimum Outstanding Performance
    t_min_otst_rt Number;  -- Minimum outstanding Rate
    
    TYPE fund_table IS TABLE OF cFund%ROWTYPE;
    TYPE metric_table IS TABLE OF cMetric%ROWTYPE;
    
    t_result HDM.SMIP_FUND_PERFORMANCE%ROWTYPE;
    
    tbl_fund fund_table;
    tbl_metric metric_table;
    t_temp_asset number;
    t_temp_fund number;
    
BEGIN
    -- Get the Date Key
    BEGIN
        SELECT day_key INTO t_day_key
        FROM hdm.calendar
        WHERE cal_day = p_end_date;
    END;
 
    -- Start the metric loop
    OPEN cMetric;
    FETCH cMetric BULK COLLECT INTO tbl_metric;
        
    FOR c_metric_ctr IN 1..tbl_metric.COUNT
    LOOP   
        -- Start the fund loop
        dbms_output.put_line('metric key:' || tbl_metric(c_metric_ctr).smip_mtric_key || ';' || tbl_metric.count);
        OPEN cFund(tbl_metric(c_metric_ctr).per_key);
        FETCH cFund BULK COLLECT INTO tbl_fund;
        FOR t_fund_ctr IN 1..tbl_fund.COUNT
        LOOP

            -- get the Fund Performance Number
            t_fund_ret := NULL;
            BEGIN
                SELECT round(fund_ret,4) INTO t_fund_ret
                FROM hdm.fund_return
                WHERE fund_key = tbl_fund(t_fund_ctr).fund_key
                AND day_key = t_day_key
                AND per_key= tbl_metric(c_metric_ctr).per_key;
            END;
                
            DBMS_OUTPUT.PUT_LINE('STEP 1: FUND RETURN; FUND = ' || tbl_fund(t_fund_ctr).fund_key || ' PERIOD = ' || tbl_metric(c_metric_ctr).per_key || 'DATE = ' ||  t_day_key);
            -- Get Index Return
            t_idx_ret := NULL;
            BEGIN
                SELECT round(idx_ret,4) INTO t_idx_ret
                FROM hdm.index_return
                WHERE bmk_idx_key = tbl_fund(t_fund_ctr).bmk_idx_key
                AND fund_compst_key = tbl_fund(t_fund_ctr).fund_compst_key
                AND day_key = t_day_key
                AND per_key= tbl_metric(c_metric_ctr).per_key;
            END;
            DBMS_OUTPUT.PUT_LINE('STEP 2: INDEX RETURN; INDEX = ' || tbl_fund(t_fund_ctr).BMK_IDX_key || ' PERIOD = ' || tbl_metric(c_metric_ctr).per_key || 'DATE = ' ||  t_day_key);
            -- Calc the Out/Under Performance of the Fund.
            
            t_OutPerf := t_fund_ret - t_idx_ret;
            dbms_output.put_line('Perf diff : ' || tbl_fund(t_fund_ctr).fund_key || ' = ' ||  t_OutPerf);
            
            -- get the average asset performance rate.
            BEGIN
                SELECT AVG_ASSET_RT,avg_tna,tot_avg INTO t_avg_asset_rt,t_temp_asset,t_temp_fund
                FROM (
                  SELECT
                  AVG(NAV_AMT) / sum(avg(nav_amt)) over ()  avg_asset_rt,fund_compst_key,avg(nav_amt) avg_tna,  sum(avg(nav_amt)) over ()  tot_avg
                FROM (
                SELECT
                  sum(val.NAV_AMT) NAV_AMT,cmp.fund_compst_key,cal.day_key
                FROM
                  HDM.FUND_COMPOSITE cmp,
                  HDM.FUND_VALUATION val,
                  HDM.VW_CALENDAR cal,
                  HDM.VW_FUND fnd
                WHERE
                  ( fnd.FUND_KEY=val.FUND_KEY  )
                  AND  ( cal.DAY_KEY=val.DAY_KEY  )
                  AND  ( fnd.FUND_COMPST_KEY=cmp.FUND_COMPST_KEY  )
                  AND  
                  (
                   ( cmp.ASSET_CLS_NM <> 'Target Retirement'  )
                   AND
                   cal.CAL_DAY  between add_months(p_end_date,tbl_metric(c_metric_ctr).per_months * -1) + 1 and  p_end_date
                   and (select min(perf_incpn_dt) from fund where fund_compst_key = fnd.fund_compst_key) <= (add_months(p_end_date,tbl_metric(c_metric_ctr).per_months * -1) + 1)   -- count the assets only if fund was old enough for the period.
                   --AND FND.FUND_STAT_CD  in (1,5)
                   )
                   group by cmp.fund_compst_key,cal.day_key
                  )  
                  group by fund_compst_key
                  ) WHERE FUND_COMPST_KEY=    tbl_fund(t_fund_ctr).fund_compst_key;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('ERROR ON 142. ' || tbl_metric(c_metric_ctr).per_months);
                RAISE;
            END;
            
 /*           if  tbl_fund(t_fund_ctr).fund_key = 2 then
                dbms_output.put_line(t_avg_asset_rt);
                dbms_output.put_line(t_temp_asset);
                dbms_output.put_line(t_temp_fund);
                --dbms_output.put_line( add_months(p_end_date,tbl_metric(c_metric_ctr).per_months * -1) + 1);
            end if; */
            -- Calculate the Weighted Performance.
            t_wavg_perf  := tbl_fund(t_fund_ctr).bp_diff  * t_avg_asset_rt;
            
            -- Calculate minimum weighted outstanding
            -- t_min_outstanding , t_min_otst_rt
            BEGIN
                 select min_perf_rt INTO t_min_otst_rt
                 from hdm.smip_fund_perf_level
                 where fund_key = tbl_fund(t_fund_ctr).fund_key
                 and curr_row_flg = 'Y'
                 and row_strt_dttm <= sysdate;
            exception
                when others then
                    t_min_otst_rt := null;
            END;
            
            t_min_outstanding := t_avg_asset_rt * t_min_otst_rt;
            
            -- SEND OUT THE ROW.
            dbms_output.put_line(t_min_outstanding || ' ;' || t_outperf || ';' || tbl_metric(c_metric_ctr).smip_mtric_key);
            
            IF t_OutPerf is NOT NULL then
                t_result.smip_mtric_key           := tbl_metric(c_metric_ctr).smip_mtric_key;
                t_result.day_key                := t_day_key;
                t_result.fund_key                := tbl_fund(t_fund_ctr).fund_key;
                t_result.out_perf_basis_pt        := t_OutPerf;
                t_result.wgt_net_out_perf        := t_wavg_perf;
                t_result.wgt_min_otst_perf        := t_min_outstanding;
                t_result.curr_row_flg             := 'Y';
                t_result.row_strt_dttm            := sysdate;
                
                PIPE ROW(t_result);
            END IF;
           
        END LOOP;
        
        CLOSE cFund;
    END LOOP;
    CLOSE cMetric;
    
    RETURN;
    
    exception when others then
        dbms_output.put_line(sqlerrm);
        DBMS_OUTPUT.put_line (dbms_utility.format_error_backtrace());
        RAISE;
END CALC_AWP_PERFORMANCE;


-- New fund return
FUNCTION CALC_NFQ_PERFORMANCE(p_end_date IN DATE) RETURN SMIP_FUND_TABLE PIPELINED IS
    CURSOR cMetric IS 
     SELECT smip_mtric_key,mtric_cd,mtric_wgt,a.per_nm,a.per_wgt,a.per_key,expctd_perf,B.PER_MSR * 12 per_months
    from smip_metrics a,return_periods b
    where a.per_key=b.per_key
    and  mtric_cd = 'NFQ' 
    and A.curr_row_flg = 'Y'
    and A.row_strt_dttm <= sysdate;
    
    CURSOR cfund(p_period_key number) IS
    select fnd.fund_Key,cmp.fund_compst_key,fund_nbr,perf_incpn_dt fund_incpn_dt,fund_sort_ord,rel.bmk_idx_key,cal_day,per.per_key,per_nm,fund_ret,idx_ret,(fund_ret - idx_ret)  bp_diff,
    sum((fund_ret - idx_ret)) over () total_diff
    from hdm.fund fnd,
            hdm.fund_composite cmp,
            hdm.FUND_INDEX_RLTN rel,
            hdm.benchmark_index idx,
            HDM.PERF_RUN_TYPE app,
            hdm.fund_return fret,
            hdm.index_return iret,
            hdm.return_periods per,
            hdm.calendar cal
    where FND.FUND_COMPST_KEY=cmp.fund_compst_key
    and cmp.fund_compst_key=rel.fund_compst_key
    and rel.bmk_idx_key=idx.bmk_idx_key
    and rel.perf_run_type_key=app.perf_run_type_key
    and fret.fund_key=fnd.fund_key
    and fret.per_key=per.per_key
    and fret.day_key=cal.day_key
    and rel.bmk_idx_key=iret.bmk_idx_key
    and rel.fund_compst_key=iret.fund_compst_key
    and iret.per_key=per.per_key
    and iret.day_key=cal.day_key
    and shr_cls_cd = 1
    --and fund_stat_cd in (1,5)
    and FND.perf_INCPN_DT >= add_months(p_end_date,-60)
    and rel.prim_bmk_flg='Y'
   -- and CMP.COMPST_STAT_CD in ('O','S')
    and cmp.asset_cls_nm not In 'Target Retirement'
    and app.run_type_cd = 'REPORTING'
    and cal_day=p_end_date
    and PER.PER_key=p_period_key
    and FND.SHR_CLS_DESC='Institutional Class'
    order by fund_sort_ord;      
    
    t_day_key HDM.CALENDAR.day_key%TYPE;
    t_fund_ctr NUMBER;
    t_metric_ctr NUMBER;
    t_fund_ret HDM.FUND_RETURN.fund_ret%TYPE;
    t_idx_ret hdm.index_return.idx_ret%TYPE;
    t_outPerf Number;
    t_avg_asset_rt Number;
    t_wavg_perf Number;  -- Weighted Average Performance.
    t_min_outstanding Number;  -- Minimum Outstanding Performance
    t_min_otst_rt Number;  -- Minimum outstanding Rate
    t_quintile Number;  -- Quintile rank
    t_nfq_mtric_key NUMBER;
    t_idx_quintile NUmber;
    
    TYPE fund_table IS TABLE OF cFund%ROWTYPE;
    TYPE metric_table IS TABLE OF cMetric%ROWTYPE;
    
    t_result HDM.SMIP_FUND_PERFORMANCE%ROWTYPE;
    
    tbl_fund fund_table;
    tbl_metric metric_table;
    
BEGIN
    -- Get the Date Key
    BEGIN
        SELECT day_key INTO t_day_key
        FROM hdm.calendar
        WHERE cal_day = p_end_date;
    END;
 
    -- Start the metric loop
    OPEN cMetric;
    FETCH cMetric BULK COLLECT INTO tbl_metric;
        
    FOR c_metric_ctr IN 1..tbl_metric.COUNT
    LOOP   
        -- Start the fund loop
        OPEN cFund(tbl_metric(c_metric_ctr).per_key);
        FETCH cFund BULK COLLECT INTO tbl_fund;
        FOR t_fund_ctr IN 1..tbl_fund.COUNT
        LOOP

            -- get the Fund Performance Number
            t_fund_ret := NULL;
            BEGIN
                SELECT round(fund_ret,4) INTO t_fund_ret
                FROM hdm.fund_return
                WHERE fund_key = tbl_fund(t_fund_ctr).fund_key
                AND day_key = t_day_key
                AND per_key= tbl_metric(c_metric_ctr).per_key;
            END;
                
            -- Get Index Return
            
            t_idx_ret := NULL;
            BEGIN
                SELECT round(idx_ret,4) INTO t_idx_ret
                FROM hdm.index_return
                WHERE bmk_idx_key = tbl_fund(t_fund_ctr).bmk_idx_key
                AND fund_compst_key = tbl_fund(t_fund_ctr).fund_compst_key
                AND day_key = t_day_key
                AND per_key= tbl_metric(c_metric_ctr).per_key;
            END;
            
            -- Calc the Out/Under Performance of the Fund.
            
            t_OutPerf := t_fund_ret - t_idx_ret;
            t_avg_asset_rt := NULL;
           -- DBMS_OUTPUT.PUT_LINE('OUT PERF: ' || t_OutPerf || ' fund ' || tbl_fund(t_fund_ctr).fund_key);
            -- get the average asset performance rate.
            BEGIN
                SELECT AVG_ASSET_RT INTO t_avg_asset_rt
                FROM (
                  SELECT
                  AVG(NAV_AMT) / sum(avg(nav_amt)) over ()  avg_asset_rt,fund_compst_key,avg(nav_amt) avg_tna,  sum(avg(nav_amt)) over ()  tot_avg
                FROM (
                SELECT
                  sum(val.NAV_AMT) NAV_AMT,cmp.fund_compst_key,cal.day_key
                FROM
                  HDM.FUND_COMPOSITE cmp,
                  HDM.FUND_VALUATION val,
                  HDM.VW_CALENDAR cal,
                  HDM.VW_FUND fnd
                WHERE
                  ( fnd.FUND_KEY=val.FUND_KEY  )
                  AND  ( cal.DAY_KEY=val.DAY_KEY  )
                  AND  ( fnd.FUND_COMPST_KEY=cmp.FUND_COMPST_KEY  )
                  AND  
                  (
                   ( cmp.ASSET_CLS_NM <> 'Target Retirement'  )
                   AND
                   cal.CAL_DAY  between add_months(p_end_date,tbl_metric(c_metric_ctr).per_months * -1) + 1 and  p_end_date
                   )
                   group by cmp.fund_compst_key,cal.day_key
                  )  
                  group by fund_compst_key
                  ) WHERE FUND_COMPST_KEY=    tbl_fund(t_fund_ctr).fund_compst_key;
            END;
            --DBMS_OUTPUT.PUT_LINE('completed avg asset rate');
            -- Calculate the Weighted Performance.
            t_wavg_perf  := tbl_fund(t_fund_ctr).bp_diff  * t_avg_asset_rt;
            
            -- Calculate minimum weighted outstanding
            -- t_min_outstanding , t_min_otst_rt
            t_min_otst_rt := NULL;
            
            BEGIN
                 select min_perf_rt INTO t_min_otst_rt
                 from hdm.smip_fund_perf_level
                 where fund_key = tbl_fund(t_fund_ctr).fund_key
                 and curr_row_flg = 'Y'
                 and row_strt_dttm <= sysdate;
            exception
            when others then
                dbms_output.put_line(' failed at ' || sqlerrm || ' at fund key' || tbl_fund(t_fund_ctr).fund_key);
                raise;
            END;
            
            t_min_outstanding := t_avg_asset_rt * t_min_otst_rt;
            
            -- Calculate the Qunitine.
            t_quintile := NULL;
            
            -- get the metrics key for 'NFQ'
            BEGIN
                SELECT smip_mtric_key INTO t_nfq_mtric_key
                FROM smip_metrics
                WHERE (mtric_cd = 'QNT' OR (mtric_cd = 'NFQ' and PER_NM = 'INCP'))
                AND per_key = tbl_metric(c_metric_ctr).per_key;
            END;
           -- DBMS_OUTPUT.PUT_LINE('got data from metrics ');
            BEGIN
                SELECT fund_peer_grp_qntl,idx_peer_grp_qntl INTO t_quintile,t_idx_quintile
                FROM smip_quintile_perf
                WHERE smip_mtric_key =  t_nfq_mtric_key
                AND fund_key                = tbl_fund(t_fund_ctr).fund_key
                and fund_peer_grp_qntl > 0
                AND day_key                 = t_day_key;
                
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                dbms_output.put_line('No Quintile data for this fund key ' || tbl_fund(t_fund_ctr).fund_key || ' for metric key ' ||  t_nfq_mtric_key);
                NULL;
            WHEN OTHERS THEN
                raise;  
            END;
            DBMS_OUTPUT.PUT_LINE('got data from quitile perf: ' || t_OutPerf || ' period: ' || tbl_metric(c_metric_ctr).smip_mtric_key);
            -- SEND OUT THE ROW.
            
            IF t_OutPerf is NOT NULL then
                t_result.smip_mtric_key             := tbl_metric(c_metric_ctr).smip_mtric_key;
                t_result.day_key                        := t_day_key;
                t_result.fund_key                       := tbl_fund(t_fund_ctr).fund_key;
                t_result.out_perf_basis_pt          := t_OutPerf;
                t_result.wgt_net_out_perf          := t_wavg_perf;
                t_result.wgt_min_otst_perf         := t_min_outstanding;
                t_result. FUND_Peer_GRP_QNTL_Rank       := t_quintile;
                t_result.idx_Peer_GRP_QNTL_Rank            := t_idx_quintile;
                t_result.curr_row_flg                  := 'Y';
                t_result.row_strt_dttm                := sysdate;
                dbms_output.put_line('this fund trans: ' || tbl_fund(t_fund_ctr).fund_key);
                PIPE ROW(t_result);
            END IF;
        END LOOP;
        CLOSE cFund;
    END LOOP;
    CLOSE cMetric;
    
    RETURN;
exception
when others then
    raise;
END CALC_NFQ_PERFORMANCE;

-- FINAL PERFORMANCE GENERATION.
PROCEDURE LOAD_SMIP_PERFORMANCE(p_end_date IN DATE)  IS
        t_perf_key NUMBER;   
 
    
        
BEGIN

        -- Get the last primary Key
        BEGIN
            SELECT nvl(max(smip_perf_key),0) INTO t_perf_key
            FROM hdm.smip_performance;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            t_perf_key := 0;
        END;
        
        -- LOAD AWD to SMIP table
        MERGE INTO smip_performance TGT
        USING (  SELECT ROWNUM + t_perf_key smip_perf_key,smip_mtric_key,day_key,fund_cnt,perfm_fund_cnt,perfm_fund_pctg,out_perf_basis_pt,min_otst_perf,wgt_net_out_perf,perf_var_pctg,wgt_perf_pctg
                      FROM (
                             select a.smip_mtric_key,a.day_key,per_wgt,per_sort_ord,
                             count(smip_fund_perf_key) fund_cnt,
                             sum((Case when out_perf_basis_pt > 0 then 1 else 0 end)) perfm_fund_cnt,
                             sum((Case when out_perf_basis_pt > 0 then 1 else 0 end)) / count(smip_fund_perf_key) perfm_fund_pctg,
                             sum(out_perf_basis_pt) out_perf_basis_pt,
                             sum(wgt_min_otst_perf) Min_otst_perf,
                             sum(wgt_net_out_perf) wgt_net_out_perf,
                             sum(wgt_net_out_perf) / (Case When sum(wgt_min_otst_perf) =0 then 1 else sum(wgt_min_otst_perf) end) perf_var_pctg,
                              sum(wgt_net_out_perf) /  (Case When sum(wgt_min_otst_perf) =0 then 1 else sum(wgt_min_otst_perf) end) * per_wgt wgt_perf_pctg
                              from hdm.smip_fund_performance a,hdm.smip_metrics b,hdm.return_periods c,smip_fund_perf_level fp
                              where a.smip_mtric_key=b.smip_mtric_key
                              and b.per_key=c.per_key
                              and A.FUND_KEY=fp.fund_key
                              and mtric_cd = 'AWP'
                              --and B.MTRIC_INLSN_FLG='Y'
                              and FP.FUND_INLSN_FLG = 'Y'
                              and a.day_key = (select day_key from hdm.calendar where cal_day = p_end_date)
                              group by a.smip_mtric_key,a.day_key,per_wgt,per_sort_ord
                          )  order by day_key,   PER_SORT_ORD    
                   ) SRC
         ON (TGT.day_key=SRC.day_key AND TGT.smip_mtric_key=SRC.smip_mtric_key)
         WHEN NOT MATCHED THEN
          INSERT ( smip_perf_key,
                        smip_mtric_key,
                        day_key,
                        fund_cnt,
                        perfm_fund_cnt,
                        perfm_fund_pctg,
                        perf_var_pctg,
                        min_otst_perf,
                        wgt_perf_pctg,
                        out_perf_basis_pt,
                        wgt_net_out_perf,
                        curr_row_flg,
                        row_strt_dttm )    
            VALUES
                       (src.smip_perf_key,
                        src.smip_mtric_key,
                        src.day_key,
                        src.fund_cnt,
                        src.perfm_fund_cnt,
                        src.perfm_fund_pctg,
                        src.perf_var_pctg,
                        src.min_otst_perf,
                        src.wgt_perf_pctg,
                        src.out_perf_basis_pt,
                        src.wgt_net_out_perf,
                        'Y',
                        sysdate)
           WHEN MATCHED THEN
           UPDATE SET
                        tgt.fund_cnt            =    src.fund_cnt,
                        tgt.perfm_fund_cnt  =    src.perfm_fund_cnt,
                        tgt.perfm_fund_pctg=    src.perfm_fund_pctg,
                        tgt.perf_var_pctg     =    src.perf_var_pctg,
                        tgt.min_otst_perf     =    src.min_otst_perf,
                        tgt.wgt_perf_pctg    =     src.wgt_perf_pctg,
                        tgt.out_perf_basis_pt=    src.out_perf_basis_pt,
                        tgt.wgt_net_out_perf=    src.wgt_net_out_perf,
                        tgt.row_stop_dttm    =   sysdate;               
                         
        COMMIT;
        
        t_perf_key := 0;
        -- Get the last primary Key
        BEGIN
            SELECT nvl(max(smip_perf_key),0) INTO t_perf_key
            FROM hdm.smip_performance;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            t_perf_key := 0;
        END;
        
        -- LOAD QNT to SMIP table
        MERGE INTO smip_performance TGT
        USING (  SELECT ROWNUM + t_perf_key smip_perf_key,smip_mtric_key,day_key,fund_cnt,perfm_fund_cnt,perfm_fund_pctg,fund_thrs_bmk_cnt,fund_thrs_bmk_pct,perf_var_pctg,wgt_perf_pctg,fund_meet_bmk_cnt,fund_meet_bmk_pct,qntl_avg_pct,avg_actl_perf_pct,qntl_wgt_perf
                              FROM(    
                                     SELECT  a.smip_mtric_key,a.day_key,C.EXPCTD_PERF,C.per_WGT,
                                     sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) fund_cnt ,
                                     sum((Case when fund_peer_grp_qntl in (1,2) then 1 else 0 end)) perfm_fund_cnt,
                                     sum((Case when fund_peer_grp_qntl in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) PERFm_FUND_PCTG,
                                     sum((case when fund_peer_grp_qntl in (1,2) and fund_peer_grp_qntl <= idx_peer_grp_qntl then 1 else 0 end)) FUND_THRS_BMK_CNT,
                                      sum((case when fund_peer_grp_qntl in (1,2) and fund_peer_grp_qntl <= idx_peer_grp_qntl then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) FUND_THRS_BMK_PCT,
                                     ( sum((case when fund_peer_grp_qntl in (1,2) and fund_peer_grp_qntl <= idx_peer_grp_qntl then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end))) /  expctd_perf  PERF_VAR_PCTG,
                                     (( sum((case when fund_peer_grp_qntl in (1,2) and fund_peer_grp_qntl <= idx_peer_grp_qntl then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end))) /  expctd_perf ) * per_wgt WGT_PERF_PCTG,
                                     --sum((case when fund_peer_grp_qntl <= idx_peer_grp_qntl then 1 else 0 end)) fund_meet_bmk_cnt,
                                     sum((case when fund_ret > idx_ret then 1 else 0 end)) fund_meet_bmk_cnt,  -- Changed as per Jake 1/19/17
                                     --sum((case when fund_peer_grp_qntl <= idx_peer_grp_qntl then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) fund_meet_bmk_pct,
                                     sum((case when fund_ret > idx_ret then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) fund_meet_bmk_pct,   -- Changed as per Jake 1/19/17
                                     --(sum((case when fund_peer_grp_qntl <= idx_peer_grp_qntl then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) + sum((Case when fund_peer_grp_qntl in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) ) /2 qntl_avg_pct,
                                     (sum((case when fund_ret > idx_ret  then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) + sum((Case when fund_peer_grp_qntl in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) ) /2 qntl_avg_pct,   -- Changed as per Jake 1/19/17
                                     --((sum((case when fund_peer_grp_qntl <= idx_peer_grp_qntl then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) + sum((Case when fund_peer_grp_qntl in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) ) /2) / expctd_perf  avg_actl_perf_pct,
                                     ((sum((case when fund_ret > idx_ret then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) + sum((Case when fund_peer_grp_qntl in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) ) /2) / expctd_perf  avg_actl_perf_pct, -- Changed as per Jake 1/19/17
                                    -- (((sum((case when fund_peer_grp_qntl <= idx_peer_grp_qntl then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) + sum((Case when fund_peer_grp_qntl in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) ) /2) / expctd_perf ) * per_wgt qntl_wgt_perf
                                     (((sum((case when fund_ret > idx_ret then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) + sum((Case when fund_peer_grp_qntl in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl > 0 then 1 else 0 end)) ) /2) / expctd_perf ) * per_wgt qntl_wgt_perf -- Changed as per Jake 1/19/17
                                     from smip_quintile_perf a,hdm.fund b,smip_metrics c,hdm.calendar d,smip_fund_perf_level fp
                                     where a.fund_key=b.fund_key
                                     and a.smip_mtric_key=c.smip_mtric_key
                                     and a.day_key=d.day_key
                                     and a.fund_key=FP.fund_key
                                     and B.SHR_CLS_CD = 1
                                     and fund_peer_grp_qntl > 0
                                     and fund_nbr < 2600
                                     and c.per_nm <> 'INCP'
                                     and FP.QNTL_INLSN_FLG = 'Y'
                                     and cal_day = p_end_date
                                     group by a.smip_mtric_key,a.day_key,C.EXPCTD_PERF,C.per_WGT)
                   ) SRC
         ON (TGT.day_key=SRC.day_key AND TGT.smip_mtric_key=SRC.smip_mtric_key)
         WHEN NOT MATCHED THEN
          INSERT ( smip_perf_key,
                        smip_mtric_key,
                        day_key,
                        fund_cnt,
                        perfm_fund_cnt,
                        perfm_fund_pctg,
                        fund_thrs_bmk_cnt,
                        fund_thrs_bmk_pct,
                        perf_var_pctg,
                        wgt_perf_pctg,
                        fund_meet_bmk_cnt,
                        fund_meet_bmk_pct,
                        qntl_avg_pct,
                        avg_actl_perf_pct,
                        qntl_wgt_perf,
                        curr_row_flg,
                        row_strt_dttm )    
            VALUES
                       (src.smip_perf_key,
                        src.smip_mtric_key,
                        src.day_key,
                        src.fund_cnt,
                        src.perfm_fund_cnt,
                        src.perfm_fund_pctg,
                        src.fund_thrs_bmk_cnt,
                        src.fund_thrs_bmk_pct,                        
                        src.perf_var_pctg,
                        src.wgt_perf_pctg,
                        src.fund_meet_bmk_cnt,
                        src.fund_meet_bmk_pct,
                        src.qntl_avg_pct,
                        src.avg_actl_perf_pct,
                        src.qntl_wgt_perf,                        
                        'Y',
                        sysdate)
           WHEN MATCHED THEN
           UPDATE SET
                        tgt.fund_cnt          = src.fund_cnt,
                        tgt.perfm_fund_cnt    = src.perfm_fund_cnt,
                        tgt.perfm_fund_pctg   = src.perfm_fund_pctg,
                        tgt.perf_var_pctg     = src.perf_var_pctg,
                        tgt.wgt_perf_pctg     = src.wgt_perf_pctg,
                        tgt.fund_thrs_bmk_cnt = src.fund_thrs_bmk_cnt,
                        tgt.fund_thrs_bmk_pct = src.fund_thrs_bmk_pct,  
                        tgt.fund_meet_bmk_cnt = src.fund_meet_bmk_cnt,
                        tgt.fund_meet_bmk_pct = src.fund_meet_bmk_pct,
                        tgt.qntl_avg_pct      = src.qntl_avg_pct,
                        tgt.avg_actl_perf_pct = src.avg_actl_perf_pct,
                        tgt.qntl_wgt_perf     = src.qntl_wgt_perf,                        
                        tgt.row_stop_dttm     = sysdate;               
                         
        COMMIT;        
 
-- LOAD THE NEW FUNDS (NFQ)

        t_perf_key := 0;
        -- Get the last primary Key
        BEGIN
            SELECT nvl(max(smip_perf_key),0) INTO t_perf_key
            FROM hdm.smip_performance;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            t_perf_key := 0;
        END;
        
        -- LOAD NFQ to SMIP table
        MERGE INTO smip_performance TGT
        USING (  SELECT ROWNUM + t_perf_key smip_perf_key,smip_mtric_key,day_key,fund_cnt,perfm_fund_cnt,perfm_fund_pctg,fund_thrs_bmk_cnt,fund_thrs_bmk_pct,perf_var_pctg,wgt_perf_pctg,fund_meet_bmk_cnt,fund_meet_bmk_pct,qntl_avg_pct,avg_actl_perf_pct,qntl_wgt_perf
                              FROM(    
                                SELECT B.EXPCTD_PERF,B.PER_WGT,
                                a.smip_mtric_key,a.day_key,
                                count(smip_fund_perf_key) fund_cnt,
                                sum((case when fund_peer_grp_qntl_rank in (1,2) then 1 else 0 end)) perfm_fund_cnt,
                                sum((case when fund_peer_grp_qntl_rank in (1,2) then 1 else 0 end)) / count(smip_fund_perf_key) perfm_fund_pctg,
                                sum((case when  fund_peer_grp_qntl_rank in (1,2) AND  fund_peer_grp_qntl_rank <=   idx_peer_grp_qntl_rank then 1 else 0  end)) FUND_THRS_BMK_CNT,
                                sum((case when  fund_peer_grp_qntl_rank in (1,2) AND  fund_peer_grp_qntl_rank <=  idx_peer_grp_qntl_rank then 1 else 0  end)) / count(smip_fund_perf_key) FUND_thrs_BMK_PCT,
                                (sum((case when  fund_peer_grp_qntl_rank in (1,2) AND  fund_peer_grp_qntl_rank <=   idx_peer_grp_qntl_rank then 1 else 0  end)) / count(smip_fund_perf_key)) / b.expctd_perf  PERF_VAR_PCTG,
                                ((sum((case when fund_peer_grp_qntl_rank in (1,2) AND   fund_peer_grp_qntl_rank <=  idx_peer_grp_qntl_rank then 1 else 0  end)) / count(smip_fund_perf_key)) / b.expctd_perf) * per_wgt WGT_PERF_PCTG,
                                sum((case when fund_peer_grp_qntl_rank <= idx_peer_grp_qntl_rank then 1 else 0 end)) fund_meet_bmk_cnt,
                                sum((case when fund_peer_grp_qntl_rank <= idx_peer_grp_qntl_rank then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl_rank > 0 then 1 else 0 end)) fund_meet_bmk_pct,
                                (sum((case when fund_peer_grp_qntl_rank <= idx_peer_grp_qntl_rank then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl_rank > 0 then 1 else 0 end)) + sum((Case when fund_peer_grp_qntl_rank in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl_rank > 0 then 1 else 0 end)) ) /2 qntl_avg_pct,
                                ((sum((case when fund_peer_grp_qntl_rank <= idx_peer_grp_qntl_rank then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl_rank > 0 then 1 else 0 end)) + sum((Case when fund_peer_grp_qntl_rank in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl_rank > 0 then 1 else 0 end)) ) /2) / expctd_perf  avg_actl_perf_pct,
                                (((sum((case when fund_peer_grp_qntl_rank <= idx_peer_grp_qntl_rank then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl_rank > 0 then 1 else 0 end)) + sum((Case when fund_peer_grp_qntl_rank in (1,2) then 1 else 0 end)) / sum((Case When fund_peer_grp_qntl_rank > 0 then 1 else 0 end)) ) /2) / expctd_perf ) * per_wgt qntl_wgt_perf
                                from smip_fund_performance a,smip_metrics b,calendar c,smip_fund_perf_level fp
                                where A.SMIP_MTRIC_KEY=b.smip_mtric_key
                                and a.day_key=C.DAY_KEY
                                and a.fund_key=FP.fund_key
                                and c.cal_day = p_end_date
                                and B.MTRIC_CD = 'NFQ'
                                and FP.NEW_FUND_INLSN_FLG='Y'
                                group by 
                                B.EXPCTD_PERF,B.PER_WGT,
                                a.smip_mtric_key,a.day_key)
                   ) SRC
         ON (TGT.day_key=SRC.day_key AND TGT.smip_mtric_key=SRC.smip_mtric_key)
         WHEN NOT MATCHED THEN
          INSERT ( smip_perf_key,
                        smip_mtric_key,
                        day_key,
                        fund_cnt,
                        perfm_fund_cnt,
                        perfm_fund_pctg,
                        fund_thrs_bmk_cnt,
                        fund_thrs_bmk_pct,
                        perf_var_pctg,
                        wgt_perf_pctg,
                        fund_meet_bmk_cnt,
                        fund_meet_bmk_pct,
                        qntl_avg_pct,
                        avg_actl_perf_pct,
                        qntl_wgt_perf,                        
                        curr_row_flg,
                        row_strt_dttm )    
            VALUES
                       (src.smip_perf_key,
                        src.smip_mtric_key,
                        src.day_key,
                        src.fund_cnt,
                        src.perfm_fund_cnt,
                        src.perfm_fund_pctg,
                        src.fund_thrs_bmk_cnt,
                        src.fund_thrs_bmk_pct,                        
                        src.perf_var_pctg,
                        src.wgt_perf_pctg,
                        src.fund_meet_bmk_cnt,
                        src.fund_meet_bmk_pct,
                        src.qntl_avg_pct,
                        src.avg_actl_perf_pct,
                        src.qntl_wgt_perf,                        
                        'Y',
                        sysdate)
           WHEN MATCHED THEN
           UPDATE SET
                        tgt.fund_cnt            =    src.fund_cnt,
                        tgt.perfm_fund_cnt  =    src.perfm_fund_cnt,
                        tgt.perfm_fund_pctg=    src.perfm_fund_pctg,
                        tgt.perf_var_pctg     =    src.perf_var_pctg,
                        tgt.wgt_perf_pctg    =     src.wgt_perf_pctg,
                        tgt.fund_thrs_bmk_cnt  =    src.fund_thrs_bmk_cnt,
                        tgt.fund_thrs_bmk_pct  =    src.fund_thrs_bmk_pct, 
                        tgt.fund_meet_bmk_cnt = src.fund_meet_bmk_cnt,
                        tgt.fund_meet_bmk_pct = src.fund_meet_bmk_pct,
                        tgt.qntl_avg_pct      = src.qntl_avg_pct,
                        tgt.avg_actl_perf_pct = src.avg_actl_perf_pct,
                        tgt.qntl_wgt_perf     = src.qntl_wgt_perf,                         
                        tgt.row_stop_dttm    =   sysdate;               
                         
        COMMIT;              

-- LOAD THE GROUP TRUST MEASURES

        t_perf_key := 0;
        -- Get the last primary Key
        BEGIN
            SELECT nvl(max(smip_perf_key),0) INTO t_perf_key
            FROM hdm.smip_performance;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            t_perf_key := 0;
        END;
        
        -- LOAD NFQ to SMIP table
        MERGE INTO smip_performance TGT
        USING (    SELECT rownum + t_perf_key smip_perf_key,smip_mtric_key,day_key,perfm_fund_pctg,wgt_net_out_perf,min_otst_perf,perf_var_pctg,wgt_perf_pctg
                        FROM (
                            select  per_key,c.asset_alloc_prxy ,per_wgt,abnml_thrs,c.acceptable_perf,B.SMIP_MTRIC_KEY,a.day_key,
                            sum(round(perf_diff,4)) PERFm_FUND_PCTG,
                            sum(round(perf_diff,4))  / c.asset_alloc_prxy WGT_NET_OUT_PERF,
                            (Case When (sum(round(perf_diff,4))  / c.asset_alloc_prxy) >  abnml_thrs then abnml_thrs else  (sum(round(perf_diff,4))  / c.asset_alloc_prxy) end )  * b.per_wgt MIN_OTST_PERF,
                            -- sum(perf_diff)  / acceptable_perf PERF_VAR_PCTG, -- # Changed on 10/22/15 to include the new formula.
                            sum(grp_trst_perf / (bmk_perf + acceptable_perf)) PERF_VAR_PCTG, -- // changed on 10/22/15
                            (sum(grp_trst_perf / (bmk_perf + acceptable_perf)))  *  b.per_wgt  WGT_PERF_PCTG -- // changed on 10/30/15
                            --(Case When (sum(grp_trst_perf / (bmk_perf + acceptable_perf))) >  abnml_thrs then abnml_thrs else  (sum(grp_trst_perf / (bmk_perf + acceptable_perf))) end )  *  b.per_wgt  WGT_PERF_PCTG -- // changed on 10/22/15
                            --(Case When (sum(round(perf_diff,4))  / c.acceptable_perf) >  abnml_thrs then abnml_thrs else  (sum(round(perf_diff,4))  / c.acceptable_perf) end )  *  b.per_wgt  WGT_PERF_PCTG  -- // changed on 10/22/15
                            from HDM.SMIP_GROUP_TRUST_PERF a,hdm.smip_metrics b,
                            (select sum(asset_allocn * asset_allocn_prxy) asset_alloc_prxy,
                             sum(asset_allocn * accptbl_allocn_prxy) acceptable_perf
                            from group_trust_levels ) c  -- only one row. cartesion.
                            ,hdm.calendar d
                            where a.smip_mtric_key=b.smip_mtric_key
                            and a.day_key=d.day_key
                            and B.MTRIC_CD='GRT'
                            and  cal_day = p_end_date
                            group by  per_key,c.asset_alloc_prxy ,per_wgt,abnml_thrs,acceptable_perf,B.SMIP_MTRIC_KEY,a.day_key
                            order by 1)
                   ) SRC
         ON (TGT.day_key=SRC.day_key AND TGT.smip_mtric_key=SRC.smip_mtric_key)
         WHEN NOT MATCHED THEN
          INSERT ( smip_perf_key,
                        smip_mtric_key,
                        day_key,
                        perfm_fund_pctg,
                        wgt_net_out_perf,
                        min_otst_perf,
                        perf_var_pctg,
                        wgt_perf_pctg,
                        curr_row_flg,
                        row_strt_dttm )    
            VALUES
                       (src.smip_perf_key,
                        src.smip_mtric_key,
                        src.day_key,
                        src.perfm_fund_pctg,
                        src.wgt_net_out_perf,
                        src.min_otst_perf,
                        src.perf_var_pctg,
                        src.wgt_perf_pctg,
                        'Y',
                        sysdate)
           WHEN MATCHED THEN
           UPDATE SET
                        tgt.perfm_fund_pctg    =    src.perfm_fund_pctg,
                        tgt.min_otst_perf         =    src.min_otst_perf,
                        tgt.wgt_net_out_perf    =    src.wgt_net_out_perf,
                        tgt.perf_var_pctg          =    src.perf_var_pctg,
                        tgt.wgt_perf_pctg          =    src.wgt_perf_pctg,
                        tgt.row_stop_dttm         =   sysdate;               
                         
        COMMIT;  

END LOAD_SMIP_PERFORMANCE;

PROCEDURE LOAD_AWP_PERFORMANCE(p_end_date IN DATE) IS
    
        t_perf_key NUMBER;   
 
    
        
BEGIN

        -- Get the last primary Key
        BEGIN
            SELECT nvl(max(smip_fund_perf_key),0) INTO t_perf_key
            FROM hdm.smip_fund_performance;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            t_perf_key := 0;
        END;
        
        -- load  awp
        
        MERGE INTO smip_fund_performance TGT
        USING (
                    SELECT t_perf_key + rownum smip_fund_perf_key,
                                smip_mtric_key,
                                day_key,
                                fund_key,
                                out_perf_basis_pt,
                                wgt_net_out_perf,
                                wgt_min_otst_perf,
                                FUND_Peer_GRP_QNTL_Rank,
                                idx_Peer_GRP_QNTL_Rank,
                                curr_row_flg,
                                row_strt_dttm
                     FROM TABLE(PKG_SMIP_PERFORMANCE.CALC_AWP_PERFORMANCE(p_end_date))
          ) SRC
          ON (tgt.smip_mtric_key=src.smip_mtric_key AND tgt.fund_key=src.fund_key AND tgt.day_key=src.day_key)
          WHEN NOT MATCHED THEN
          INSERT (
                                smip_fund_perf_key,
                                smip_mtric_key,
                                day_key,
                                fund_key,
                                out_perf_basis_pt,
                                wgt_net_out_perf,
                                wgt_min_otst_perf,
                                FUND_Peer_GRP_QNTL_Rank,
                                idx_Peer_GRP_QNTL_Rank,
                                curr_row_flg,
                                row_strt_dttm )
           VALUES        ( src.smip_fund_perf_key,
                                src.smip_mtric_key,
                                src.day_key,
                                src.fund_key,
                                src.out_perf_basis_pt,
                                src.wgt_net_out_perf,
                                src.wgt_min_otst_perf,
                                src. FUND_Peer_GRP_QNTL_Rank,
                                src. idx_Peer_GRP_QNTL_Rank,
                                src.curr_row_flg,
                                src.row_strt_dttm)
            WHEN MATCHED THEN
            UPDATE SET
                                out_perf_basis_pt   = src.out_perf_basis_pt,
                                wgt_net_out_perf   = src.wgt_net_out_perf,
                                wgt_min_otst_perf  = src.wgt_min_otst_perf,
                                FUND_Peer_GRP_QNTL_Rank= src. FUND_Peer_GRP_QNTL_Rank,
                                idx_Peer_GRP_QNTL_Rank   =  src.idx_Peer_GRP_QNTL_Rank,
                                curr_row_flg         = src.curr_row_flg,
                                row_strt_dttm       = src.row_strt_dttm;
                                        
            COMMIT;
            
END LOAD_AWP_PERFORMANCE;
        
PROCEDURE LOAD_NFQ_PERFORMANCE(p_end_date IN DATE) IS
    
        t_perf_key NUMBER;   
 
    
        
BEGIN

        -- Get the last primary Key
        BEGIN
            SELECT nvl(max(smip_fund_perf_key),0) INTO t_perf_key
            FROM hdm.smip_fund_performance;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
            t_perf_key := 0;
        END;
        
        -- load  awp
        
        MERGE INTO smip_fund_performance TGT
        USING (
                    SELECT t_perf_key + rownum smip_fund_perf_key,
                                smip_mtric_key,
                                day_key,
                                fund_key,
                                out_perf_basis_pt,
                                wgt_net_out_perf,
                                wgt_min_otst_perf,
                                 FUND_Peer_GRP_QNTL_Rank,
                                 idx_Peer_GRP_QNTL_Rank,
                                curr_row_flg,
                                row_strt_dttm
                     FROM TABLE(PKG_SMIP_PERFORMANCE.CALC_NFQ_PERFORMANCE(p_end_date))
          ) SRC
          ON (tgt.smip_mtric_key=src.smip_mtric_key AND tgt.fund_key=src.fund_key AND tgt.day_key=src.day_key)
          WHEN NOT MATCHED THEN
          INSERT (
                                smip_fund_perf_key,
                                smip_mtric_key,
                                day_key,
                                fund_key,
                                out_perf_basis_pt,
                                wgt_net_out_perf,
                                wgt_min_otst_perf,
                                 FUND_Peer_GRP_QNTL_Rank,
                                 idx_Peer_GRP_QNTL_Rank,
                                curr_row_flg,
                                row_strt_dttm )
           VALUES        ( src.smip_fund_perf_key,
                                src.smip_mtric_key,
                                src.day_key,
                                src.fund_key,
                                src.out_perf_basis_pt,
                                src.wgt_net_out_perf,
                                src.wgt_min_otst_perf,
                                src.FUND_Peer_GRP_QNTL_Rank,
                                src.idx_Peer_GRP_QNTL_Rank,
                                src.curr_row_flg,
                                src.row_strt_dttm)
            WHEN MATCHED THEN
            UPDATE SET
                                out_perf_basis_pt  = src.out_perf_basis_pt,
                                wgt_net_out_perf   = src.wgt_net_out_perf,
                                wgt_min_otst_perf = src.wgt_min_otst_perf,
                                FUND_Peer_GRP_QNTL_Rank = src.FUND_Peer_GRP_QNTL_Rank,
                                idx_Peer_GRP_QNTL_Rank     = src.idx_Peer_GRP_QNTL_Rank,
                                curr_row_flg         = src.curr_row_flg,
                                row_stop_dttm      = src.row_strt_dttm;
                                        
            COMMIT;
            
END LOAD_NFQ_PERFORMANCE; 
       
END PKG_SMIP_PERFORMANCE;

CREATE OR REPLACE PACKAGE HDM.PKG_TARGET_FUNDS AS


    TYPE TARGET_RETURN_TABLE IS TABLE OF Target_Return%ROWTYPE;
    PROCEDURE Load_Wealth_index(p_wi_date IN DATE);
    PROCEDURE load_target_Return(p_end_date IN DATE);

    Function calc_Target_Return(p_end_date IN Date,p_strt_date IN Date,p_fund_key In Number,p_type_key IN Number,p_annl_fctr IN Number,p_Period_ut IN Varchar2) Return Number;
    Function calc_Actual_Return(p_end_date IN Date,p_strt_date IN Date,p_fund_key In Number,p_type IN VARCHAR2,p_annl_fctr IN Number) Return Number;
    FUNCTION get_target_Returns(p_date IN DATE,p_monthend_flg IN VARCHAR2 DEFAULT NULL) RETURN Target_Return_TABLE PIPELINED; 
    
    PROCEDURE Load_GP_Wealth_index(p_wi_date IN DATE);   
    
END PKG_TARGET_FUNDS;

CREATE OR REPLACE PACKAGE BODY HDM.PKG_TARGET_FUNDS AS

    PROCEDURE Load_Wealth_index(p_wi_date IN DATE) IS
    
        CURSOR DailyReturn(p_date IN DATE) IS
        SELECT trgt_fund_key,day_key, sum(tgt_fund_dly_ret) tgt_fund_dly_ret,
        sum(tgt_idx_dly_ret) tgt_idx_dly_ret,wgt_type
        FROM (
            SELECT c.day_key,TRGT_FUND_KEY, fund_ret * trgt_fund_wgt tgt_fund_dly_ret, 
            idx_ret * trgt_fund_wgt tgt_idx_dly_ret,wgt_type
            FROM target_weights wt,fund tf,fund_index_rltn rl,fund f,benchmark_index b
            ,fund_return v,calendar c,hdm.return_periods p,HDM.PERF_RUN_TYPE t,index_return idr
            WHERE wt.trgt_fund_key=tf.fund_key
            and wt.fund_key=f.fund_key
            and f.fund_compst_key = rl.fund_compst_key
            and rl.bmk_idx_key=b.bmk_idx_key
            and v.fund_key(+)=f.fund_key
            and v.day_key(+)=c.day_key
            and v.per_key(+)=p.per_key
            and rl.perf_run_type_key=t.perf_run_type_key
            and rl.bmk_idx_key=idr.bmk_idx_key
            and rl.fund_compst_key=idr.fund_compst_key
            and idr.day_key=c.day_key
            and idr.per_key=p.per_key
            AND efftv_strt_dt <= p_date AND efftv_end_dt >= p_date
            AND wgt_type IN ('FIRM','MODEL')
            AND (tf.fund_stat_cd in (1,5) or (tf.fund_incpn_dt <= p_date and tf.fund_clos_dt >= p_date))
            and rl.prim_bmk_flg = 'Y'
            and rl.link_prio = 1  
            and c.cal_day = p_date  
            and per_nm = '1DY' 
            and t.run_type_cd ='REPORTING'  -- this will help pickup the right splice.
        ) group by trgt_fund_key,day_key,wgt_type ; 
        
        t_last_wi_rfwt NUMBER;
        t_dly_ret      NUMBER;
        t_new_wi_rfwt NUMBER;
        t_new_wi_rbwt NUMBER;
        
        TYPE wt_record IS RECORD (
                tgt_fund_key  dbms_sql.number_table,
                day_key      dbms_sql.number_table,
                tgt_fund_dly_ret dbms_sql.number_table,
                tgt_idx_dly_ret  dbms_sql.number_table,
                tgt_wgt_type   DBMS_SQL.VARCHAR2_TABLE
        );
        
        tgt_wt_table    wt_record;
        t_date          DATE;
        t_last_bus_day  DATE;
        t_prev_bus_day  DATE;
        t_curr_wi_key   NUMBER;
        t_surr_key      NUMBER;
        t_rectype       VARCHAR2(4);
        t_bus_day_flg   VARCHAR2(1);
    BEGIN
        -- initialize
        t_new_wi_rfwt := NULL;
        t_new_wi_rbwt := NULL;
        t_date := p_wi_date;
        
        -- get last business day
        -- sn(6/15/18)--  Changes to accomodate non-business day index changes.
        -- do not look for last business day.  Use the passed down date.  
        
        SELECT max(cal_day) INTO t_last_bus_day
        FROM hdm.calendar
        WHERE cal_day <= t_date;
        --AND bus_day_flg = 'Y';
        
        --t_last_bus_day := t_date;
        
        -- get Business Dau prior to last business day
        -- sn(6/15/18) -- Look for last date prior to passed date.  If no WI, look for the prior date.
        SELECT max(cal_day) INTO t_prev_bus_day
        FROM hdm.calendar CAL, HDM.TARGET_WEALTH_INDEX  tgt
        WHERE tgt.DAY_KEY=CAL.DAY_KEY
        AND cal_day < t_last_bus_day ;
       -- AND bus_day_flg = 'Y';        
        
        -- GET THE max Key value for wealth index table
        SELECT nvl(MAX(trgt_wlth_idx_key),0) INTO t_curr_wi_key
        FROM target_wealth_index;
        
        --OPEN DailyReturn(t_last_bus_day);  -- // changed on 11/13/15.
        OPEN DailyReturn(t_date);
        FETCH DailyReturn BULK COLLECT INTO
            Tgt_wt_table.tgt_fund_key,
            Tgt_wt_table.day_key,
            Tgt_wt_table.tgt_fund_dly_ret,
            Tgt_wt_table.tgt_idx_dly_ret,
            Tgt_wt_table.tgt_wgt_type;
        
        t_surr_key := 0;
        FOR t_Row IN 1..Tgt_wt_table.Tgt_fund_key.COUNT LOOP
            -- get the last wealth index values
            -- RFWT
--            DBMS_OUTPUT.PUT_LINE('DAY KEY = ' || tgt_wt_table.day_key(t_row));
--            DBMS_OUTPUT.PUT_LINE('FUND KEY = ' || tgt_wt_table.TGT_FUND_key(t_row));
--            DBMS_OUTPUT.PUT_LINE('Last Day = ' || t_prev_bus_day);
            For t_Loop IN 1..2 LOOP
                t_dly_ret := NULL;
                Case
                When t_Loop = 1 and Tgt_wt_table.tgt_wgt_type(t_Row) = 'FIRM' Then 
                    t_recType := 'RFWT';
                    t_dly_ret := Tgt_wt_table.tgt_fund_dly_ret(t_row);
                When t_Loop = 2 and Tgt_wt_table.tgt_wgt_type(t_Row) = 'FIRM' Then 
                    t_recType := 'RBWT';
                    t_dly_ret := Tgt_wt_table.tgt_idx_dly_ret(t_row);
                When t_Loop = 1 and Tgt_wt_table.tgt_wgt_type(t_Row) = 'MODEL' Then 
                    t_recType := 'RFWM';
                    t_dly_ret := Tgt_wt_table.tgt_fund_dly_ret(t_row);
                When t_Loop = 2 and Tgt_wt_table.tgt_wgt_type(t_Row) = 'MODEL' Then 
                    t_recType := 'RBWM';
                    t_dly_ret := Tgt_wt_table.tgt_idx_dly_ret(t_row);
                End Case;
                
                -- SN(6/15/18)  Changes to look for last business day if prior day has no WI.  Possible if month end is a weekend and price changed. (future)
                /*
                    SELECT wlth_idx INTO t_last_wi_rfwt
                    FROM target_wealth_index
                    WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                    AND day_key = (select day_key from calendar where cal_day = t_prev_bus_day)
                    AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
                */
                begin
                
                    -- if prev WI date  is a business day then you should have a wealth index in the table.  If not it should error out.  
                    -- If not, go find the last wi (it could be a non-bus day due to price change)
                    -- FIND THE previous date based on type of target calc.
                    SELECT max(cal_day) INTO t_prev_bus_day
                    FROM hdm.calendar CAL, HDM.TARGET_WEALTH_INDEX  tgt
                    WHERE tgt.DAY_KEY=CAL.DAY_KEY
                    AND cal_day < t_last_bus_day 
                    and tgt.TRGT_PERF_TYPE_KEY = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType);   
                                     
                    SELECT bus_day_flg INTO t_bus_day_flg
                    FROM HDM.CALENDAR
                    WHERE cal_day = t_prev_bus_day;
                 --   IF t_bus_day_flg = 'Y' Then
                        SELECT wlth_idx INTO t_last_wi_rfwt
                        FROM TARGET_wealth_index
                        WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                        AND day_key = (select day_key from calendar where cal_day = t_prev_bus_day)
                        AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
               /*     ELSE
                        SELECT wlth_idx INTO t_last_wi_rfwt
                        FROM TARGET_wealth_index
                        WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                        AND day_key = (select day_key from calendar where cal_day = (SELECT MAX(CAL_DAY) FROM CALENDAR CC,TARGET_WEALTH_INDEX WI WHERE CC.DAY_KEY=WI.DAY_KEY AND CAL_DAY <= t_prev_bus_day and WI.FUND_KEY=tgt_wt_table.tgt_fund_key(t_row)))
                        AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
                    END IF; */
                exception
                when NO_DATA_FOUND then
                    dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
                    dbms_output.put_line ('*** * NO WEALTH INDEX DATA FOR THIS DATE: ' || to_char(t_prev_bus_day,'mm/dd/yyyy') ||', fund: ' || tgt_wt_table.tgt_fund_key(t_row) || ', ' || t_rectype);
                    rollback;        
                    RAISE;
                WHEN others then
                    dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
                    dbms_output.put_line ('fund: ' || tgt_wt_table.tgt_fund_key(t_row) ||' , ' || to_char(t_prev_bus_day,'mm/dd/yyyy'));
                    rollback;        
                    RAISE;
                END;
                

            
                -- CALCULATE the RFWT AND RBWT wealth index
                t_new_wi_rfwt := 1 * t_last_wi_rfwt * (1 + t_dly_ret);
            
                --DBMS_OUTPUT.PUT_LINE('New WI RFWT = ' ||  t_new_wi_rfwt);
            
                -- insert the values -RFWT;
                If t_new_wi_rfwt is not null then
                    t_surr_key := t_surr_key + 1;
                    MERGE INTO TARGET_WEALTH_INDEX TGT
                    USING (SELECT t_curr_wi_key + t_surr_key trgt_wlth_idx_key,(select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType) trgt_perf_type_key,
                                Tgt_wt_table.day_key(t_row) day_key,Tgt_wt_table.tgt_fund_key(t_row) fund_key,t_new_wi_rfwt wlth_idx,Tgt_wt_table.tgt_fund_dly_ret(t_row) daily_ret,
                                'Y' curr_row_flg,sysdate row_strt_dttm 
                            FROM DUAL) SRC
                    ON (TGT.fund_key=SRC.fund_key AND TGT.day_key=SRC.day_key AND TGT.trgt_perf_type_key = SRC.trgt_perf_type_key)
                    WHEN NOT MATCHED THEN
                    INSERT (TRGT_WLTH_IDX_KEY,
                            TRGT_PERF_TYPE_KEY,
                            DAY_KEY,
                            FUND_KEY,
                            WLTH_IDX,
                            DAILY_RET,
                            CURR_ROW_FLG,
                            ROW_STRT_DTTM)
                    VALUES (SRC.TRGT_WLTH_IDX_KEY,
                            SRC.TRGT_PERF_TYPE_KEY,
                            SRC.DAY_KEY,
                            SRC.FUND_KEY,
                            SRC.WLTH_IDX,
                            SRC.DAILY_RET,
                            SRC.CURR_ROW_FLG,
                            SRC.ROW_STRT_DTTM)
                    WHEN MATCHED THEN
                    UPDATE SET 
                            TGT.WLTH_IDX  = SRC.WLTH_IDX,
                            TGT.DAILY_RET = SRC.DAILY_RET,
                            TGT.ROW_STRT_DTTM = SRC.ROW_STRT_DTTM;
                End If;
  
                        
                       -- dbms_output.put_line ('Record for ' ||   Tgt_wt_table.day_key(t_row)  || ' inserted');                  
            End Loop;
        End Loop;
        
        COMMIT;
        
    EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;
        
    END Load_Wealth_index;
    
   FUNCTION get_target_Returns(p_date IN DATE,p_monthend_flg IN VARCHAR2 DEFAULT NULL) RETURN Target_Return_TABLE PIPELINED 
    IS
        CURSOR cFUND(p_in_date IN DATE) IS 
        -- 2/3/17 -- CHANGED for performance inception date.
        SELECT FUND_KEY,PERF_INCPN_DT FUND_INCPN_DT,COMP.FUND_COMPST_KEY
        FROM FUND, FUND_COMPOSITE COMP
        WHERE FUND.FUND_COMPST_KEY = COMP.FUND_COMPST_KEY
        AND FUND.SHR_CLS_CD = 1
        AND (FUND.FUND_STAT_CD IN ('O','S') OR NVL(FUND_CLOS_DT,TRUNC(SYSDATE))>= p_in_date)
        AND asset_cls_nm = 'Target Retirement'
        ORDER BY fund_sort_ord;
        
        CURSOR cPeriod(p_year IN NUMBER) IS
        SELECT per_key,per_Nm,per_ut,per_msr,annl_fctr,per_sort_ord
        FROM RETURN_PERIODS
        WHERE NOT ( per_ut = 'YR' and per_msr > p_year)
        AND (DAILY_MSR_FLG = 'Y' OR MO_END_MSR_FLG = 'Y')
        ORDER BY per_sort_ord;
        
        -- CARTESION join with Fund Index Relation Table.
        CURSOR cTyp(p_compst_key IN NUMBER) IS
        SELECT TRGT_PERF_TYPE_KEY,TRGT_PERF_TYPE,PERF_TYPE_FLG,OBJ_TYPE, NULL AS BMK_IDX_KEY,perf_sort_ord
        FROM target_performance_type 
        WHERE perf_type_flg <> 'B'
        UNION 
        SELECT TRGT_PERF_TYPE_KEY,TRGT_PERF_TYPE,PERF_TYPE_FLG,OBJ_TYPE,BMK_IDX_KEY,perf_sort_ord
        FROM target_performance_type tp,fund_index_rltn rl
        WHERE perf_type_flg = 'B'
        AND rl.fund_compst_key = p_compst_key
        AND rl.perf_run_type_key = 2
        ORDER BY perf_sort_ord;
        
        -- temp for only running glidepath returns.  
        /*
        CURSOR cTyp(p_compst_key IN NUMBER) IS
        SELECT TRGT_PERF_TYPE_KEY,TRGT_PERF_TYPE,PERF_TYPE_FLG,OBJ_TYPE, NULL AS BMK_IDX_KEY,perf_sort_ord
        FROM target_performance_type 
        WHERE TRGT_PERF_TYPE IN ('RFOG','RBOG');        
        */
        
        t_date DATE;
        t_begin_date DATE;
        t_incp_dt DATE;
        t_incp_yr NUMBER;
        t_yr_days NUMBER;
        t_annlzn_fctr NUMBER;
        t_return NUMBER;
        t_ret_key NUMBER;
        t_day_key NUMBER; 
        t_sort_order NUMBER;
        t_curr_yr_days NUMBER;

        
        TYPE FUND_ROW IS TABLE OF cFUND%ROWTYPE;
        TYPE PERIOD_ROW IS TABLE OF cPERIOD%ROWTYPE;
        TYPE TYP_ROW IS TABLE OF cTYP%ROWTYPE;
        
        FUND_TABLE FUND_ROW;
        PERIOD_TABLE PERIOD_ROW;
        TYP_TABLE TYP_ROW;
        t_return_row Target_Return%RowType;
        
        
    BEGIN    
        t_date := p_date;
        t_sort_order := 0;
         
        -- Get the day Key for the Performance Date
        Begin
            SELECT Day_key INTO t_day_key
            FROM calendar
            WHERE cal_day = t_date;
        End;
        -- Get number of days in current Year
        t_curr_yr_days := t_date - add_months(t_date,-12);
        -- START THE FUND LOOP
        OPEN cFUND(t_date);
        FETCH cFUND BULK COLLECT INTO fund_table;
        
        FOR t_fund_ctr IN 1..fund_table.COUNT LOOP
            -- DERIVE INCEPTION DATE
            -- FOR FUNDS STARTED ON 1/2/2009, consider them as started on 1/1/2009.
            t_incp_dt := trunc(fund_table(t_fund_ctr).FUND_INCPN_DT,'mm');
            t_incp_yr := floor(months_between(t_date + 1,t_incp_dt) / 12);
            
            -- LOOP FOR PERIODS
            OPEN cPERIOD(t_incp_yr);
            FETCH cPERIOD BULK COLLECT INTO PERIOD_TABLE;
            
            FOR t_period_ctr IN 1..PERIOD_TABLE.COUNT LOOP
                t_begin_date := hdm.pkg_fund_perf.get_begin_dt(period_table(t_period_ctr).per_ut,period_table(t_period_ctr).per_msr,t_incp_dt,t_date);
                
                -- Any performance gain on first day should be excluded from return calculation.
            
                t_begin_date := (Case When t_begin_date < fund_table(t_fund_ctr).fund_incpn_dt Then fund_table(t_fund_ctr).fund_incpn_dt Else t_begin_date end);
                
                if period_table(t_period_ctr).per_ut = 'IN' Then
                    -- recalc years since inception.
                    -- t_incp_yr calc is different from above year calculation due to nbr of days = 1 year.
                    t_incp_yr := floor(months_between(t_date,t_incp_dt) / 12);
                    -- get number of days in current year.
                    t_yr_days := trunc(t_date) - (add_months(t_incp_dt,(floor(months_between(t_date,t_incp_dt)/12)* 12 )) - 1);
                     
                    t_annlzn_fctr := t_incp_yr + (t_yr_days / t_curr_yr_days);
                else
                    t_annlzn_fctr := period_table(t_period_ctr).annl_fctr;

                end if;
                
                -- annualization factor should be at least 1;
                t_annlzn_fctr := (Case When t_annlzn_fctr < 1 Then 1 else t_annlzn_fctr end);
                
                OPEN cTYP(fund_table(t_fund_ctr).fund_compst_key);
                FETCH cTYP BULK COLLECT INTO TYP_TABLE;
                FOR t_type_Ctr IN 1..typ_table.COUNT LOOP
                    --  Fund calculation
                    -- Initialize the record.
                    t_return := NULL;
                    CASE typ_table(t_type_ctr).perf_type_flg
                        When  'F' Then  -- Fund Return Calc
                            t_return  := PKG_FUND_PERF.calc_Return(t_date,t_begin_date,fund_table(t_fund_ctr).fund_key,t_annlzn_fctr,typ_table(t_type_ctr).Obj_Type);
                        When 'D' Then  --  Target Weight & Model Returns
                          
                            t_return := PKG_TARGET_FUNDS.Calc_Target_Return(t_date,t_begin_date,fund_table(t_fund_ctr).fund_key,typ_table(t_type_ctr).trgt_perf_type_key,t_annlzn_fctr,period_table(t_period_ctr).per_ut);
                        When 'A' Then  -- Actual weights return;
                          --  dbms_output.put_line('end date: ' || t_date);
                         --   dbms_output.put_line('start date: ' || t_begin_date);
                          --  dbms_output.put_line('fund_key: ' || fund_table(t_fund_ctr).fund_key);
                          --  dbms_output.put_line('type: ' || typ_table(t_type_ctr).Obj_Type);
                          --  dbms_output.put_line('annl fctr: ' || t_annlzn_fctr);
                            t_return := PKG_TARGET_FUNDS.Calc_Actual_Return(t_date,t_begin_date,fund_table(t_fund_ctr).fund_key,typ_table(t_type_ctr).Obj_Type,t_annlzn_fctr);
                        When 'B' Then -- Benchmarks    
                            t_return := PKG_FUND_PERF.calc_Return(t_date,t_begin_date,typ_table(t_type_ctr).bmk_idx_key,t_annlzn_fctr,typ_table(t_type_ctr).Obj_Type);
                            
                        Else
                            t_return := NULL;
                    END CASE;
                    
                    if t_return IS NOT NULL THEN
                        t_ret_key := t_ret_key + 1;
                        t_sort_order := t_sort_order +1;
                        -- Populate the record
                        t_return_row.trgt_ret_key := t_ret_key;
                        t_return_row.fund_key := fund_table(t_fund_ctr).fund_key;
                        t_return_row.trgt_perf_type_key := typ_table(t_type_ctr).trgt_perf_type_key;
                        t_return_row.day_key := t_day_key;
                        t_return_row.per_key := period_table(t_period_ctr).per_key;
                        t_return_row.bmk_idx_key :=  typ_table(t_type_ctr).bmk_idx_key;
                        t_return_row.trgt_ret := t_return;
                        t_return_row.beg_dt := t_begin_date;
                        t_return_row.annlzn_fctr := t_annlzn_fctr;
                        t_return_row.displ_sort_ord := t_sort_order;
                        t_return_row.curr_row_flg := 'Y';
                        t_return_row.row_strt_dttm := SYSDATE;
                        
                        -- Send the record 
                        PIPE ROW(t_return_row);
                    End If;
                End Loop; -- Loop for Performance Types
                CLOSE cTyp;
            End Loop;  -- Loop for Return Periods
            CLOSE cPeriod;
        End Loop;  -- Loop For Fund
        CLOSE cFUND;
        RETURN;
    EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;        
    END get_target_Returns;    
                    
    Function calc_Target_Return(p_end_date IN Date,p_strt_date IN Date,p_fund_key In Number,p_type_key IN Number,p_annl_fctr IN Number,p_Period_ut IN Varchar2) Return Number 
    IS
        t_return NUMBER;
        t_beg_date Date;
        t_strt_WI NUMBER;
        t_end_WI NUMBER;  
        t_bus_day DATE;  
        t_Cntr NUMBER;
    Begin
            BEGIN
            -- Calculate the start date for getting the starting wealth index .
            -- if begin date falls on a holiday or weekened, then it should pickup the latest business day prior to that date.  
            --  Exception is Month end, which will have a Wealth Index.
              
                If p_period_ut <> 'IN' Then
                    select Count(cal_day) INTO t_Cntr
                    FROM target_wealth_index wi,calendar cal
                    WHERE  wi.day_key = cal.day_key
                    and wi.fund_key = p_fund_key
                    and cal_day  =    p_strt_date;
                    
                    If t_Cntr = 0 Then
                        select Max(cal_day) INTO t_beg_date
                        FROM target_wealth_index wi,calendar cal
                        WHERE  wi.day_key = cal.day_key
                        and cal_day  <=    p_strt_date
                        and bus_day_flg = 'Y';
                    Else
                       t_beg_date := p_strt_date;
                    End If; 
                       
                Else
                    t_beg_date := p_strt_date;
                End if;
                
                
                SELECT wlth_idx INTO t_strt_WI
                FROM target_wealth_index wi,calendar cal
                WHERE wi.day_key = cal.day_key
                AND cal_day = t_beg_date
                AND fund_key = p_fund_key
                AND trgt_perf_type_key = p_type_key;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                dbms_output.put_line('NO Starting Wealth Index Record Found');
                dbms_output.put_line('TW : ' || p_end_date || ' , ' || t_beg_date || ', fund = ' || p_fund_key || ', annl fctr = ' || p_annl_fctr || ', type = ' || p_type_key);
                Return NULL;
            WHEN OTHERS THEN
                RAISE;
            End;
            
            -- Get ending Wealth Index
            -- Check if the end date is a holiday -- if yes then get the most recent Wealth Index. // Not used currently.
            select nvl(max(cal_day),t_bus_day) INTO t_bus_day
            From calendar
            Where cal_day <= t_bus_day
            and bus_day_flg = 'Y' and cal_mo_end_flg = 'N';
            
            
            Begin
                SELECT wlth_idx INTO t_end_WI
                FROM target_wealth_index wi,calendar cal
                WHERE wi.day_key = cal.day_key
                AND cal_day = p_end_date
                AND fund_key = p_fund_key
                AND trgt_perf_type_key = p_type_key; 
            EXCEPTION                       
            WHEN NO_DATA_FOUND THEN
                dbms_output.put_line('NO Ending Wealth Index Record Found');
                dbms_output.put_line('Initial : ' || t_bus_day || ' , ' || t_beg_date || ', fund = ' || p_fund_key || ', annl fctr = ' || p_annl_fctr || ', type = ' || p_type_key);
                Return NULL;
            WHEN OTHERS THEN
                RAISE;
            End;        
            
            -- Calculate Return
            t_return := Power((t_end_WI / t_Strt_WI), (1/p_annl_fctr)) - 1;
            
            Return t_return;
    End Calc_Target_Return;

    Function calc_Actual_Return(p_end_date IN Date,p_strt_date IN Date,p_fund_key In Number,p_type IN VARCHAR2,p_annl_fctr IN Number) Return Number 
    IS
        
        CURSOR cWEIGHT(p_date IN DATE,p_target_fund IN Number) IS
        SELECT trgt_fund_key,wt.day_key,wt.fund_key,rl.bmk_idx_key,daily_holdg_wgt
        from target_daily_weight wt, calendar cal,fund_index_rltn rl,fund fn,benchmark_Index bm
        where wt.day_key= cal.day_key
        and wt.fund_key=fn.fund_key
        and fn.fund_compst_key=rl.fund_compst_key
        and rl.bmk_idx_key = bm.bmk_idx_key
        and cal_day = p_date
        and trgt_fund_key = p_fund_key
        and perf_run_type_key = 3
        and prim_bmk_flg = 'Y';
        
        t_return NUMBER;
        t_retval NUMBER;
        t_Key NUMBER;
        t_date DATE;
        
        TYPE Weight_Row IS TABLE OF cWEIGHT%ROWTYPE;
        Weight_Table Weight_Row;
    Begin

        -- Get the correct end date by checking the business day flag.
        SELECT max(cal_day) Into t_date
        FROM CALENDAR
        WHERE CAL_DAY <= p_end_date
        And bus_day_flg = 'Y';
        
        OPEN cWEIGHT(t_date,p_fund_key);
        FETCH cWEIGHT BULK COLLECT INTO Weight_Table;
        --dbms_output.put_line('Initial : ' || p_end_date || ' , ' || p_strt_date || ', fund = ' || p_fund_key || ', annl fctr = ' || p_annl_fctr || ', type = ' || p_type);

        -- Start the Loop
        FOR t_WRow     IN 1..Weight_Table.COUNT LOOP
            t_Key := (Case When p_type = 'FUND' Then Weight_Table(t_WRow).fund_key Else Weight_Table(t_WRow).bmk_idx_key end);
            t_retval  := PKG_FUND_PERF.calc_Return(p_end_date,p_strt_date,t_key,p_annl_fctr,p_type);
            t_return  := nvl(t_return,0) + ( t_retval * Weight_table(t_WRow).daily_holdg_wgt );
            --dbms_output.put_line(p_end_date || ' , ' || p_strt_date || ', t_key = ' || t_key || ', annl fctr = ' || p_annl_fctr || ', type = ' || p_type);
            --dbms_output.put_line('Return = ' || t_return);
        END LOOP;
        
        Return t_return;
    End calc_Actual_Return;   
    
	PROCEDURE load_target_Return(p_end_date IN DATE)
	IS
		t_Row NUMBER;
	BEGIN
		-- Get the current maximum key
		BEGIN
			SELECT NVL(MAX(trgt_ret_key),0) INTO t_Row
			FROM TARGET_RETURN;
		END;
		
		MERGE INTO TARGET_RETURN TGT
		USING (
				SELECT t_row + rownum AS TRGT_RET_KEY,FUND_KEY,TRGT_PERF_TYPE_KEy,DAY_KEY,PER_KEY,BMK_IDX_KEY,nvl(BMK_IDX_KEY,-1) bmk_lkup_key,TRGT_RET,BEG_DT,ANNLZN_FCTR,DISPL_SORT_ORD,CURR_ROW_FLG,ROW_STRT_DTTM
				FROM TABLE(PKG_TARGET_FUNDS.GET_TARGET_RETURNS(p_end_date))
			  ) SRC
		ON 	(tgt.fund_key=src.fund_key AND tgt.trgt_perf_type_key=src.trgt_perf_type_Key AND NVL(tgt.bmk_idx_key,-1)=src.bmk_LKUP_key AND tgt.day_key=src.day_key AND tgt.per_key=src.per_key)
		WHEN NOT MATCHED THEN
				INSERT (TRGT_RET_KEY,FUND_KEY,TRGT_PERF_TYPE_KEY,DAY_KEY,PER_KEY,BMK_IDX_KEY,TRGT_RET,BEG_DT,ANNLZN_FCTR,DISPL_SORT_ORD,CURR_ROW_FLG,ROW_STRT_DTTM)
				VALUES (SRC.TRGT_RET_KEY,SRC.FUND_KEY,SRC.TRGT_PERF_TYPE_KEY,SRC.DAY_KEY,SRC.PER_KEY,SRC.BMK_IDX_KEY,SRC.TRGT_RET,SRC.BEG_DT,SRC.ANNLZN_FCTR,SRC.DISPL_SORT_ORD,SRC.CURR_ROW_FLG,SRC.ROW_STRT_DTTM)
		WHEN MATCHED THEN
				UPDATE SET 
							TGT.TRGT_RET = SRC.TRGT_RET,
							TGT.BEG_DT = SRC.BEG_DT,
							TGT.ANNLZN_FCTR = SRC.ANNLZN_FCTR,
							TGT.DISPL_SORT_ORD = SRC.DISPL_SORT_ORD,
							TGT.CURR_ROW_FLG = SRC.CURR_ROW_FLG,
							TGT.ROW_STRT_DTTM = SRC.ROW_STRT_DTTM;
							
		COMMIT;
    EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;
	END load_target_Return;     
	
    PROCEDURE Load_gp_Wealth_index(p_wi_date IN DATE) IS
    
        CURSOR DailyReturn(p_date IN DATE) IS
        SELECT trgt_fund_key,day_key, sum(tgt_fund_dly_ret) tgt_fund_dly_ret,sum(tgt_idx_dly_ret) tgt_idx_dly_ret
        FROM (
            SELECT c.day_key,TRGT_FUND_KEY, fund_ret * gp_wgt tgt_fund_dly_ret, idx_ret * gp_wgt tgt_idx_dly_ret
            FROM target_orig_glidepath wt,fund tf,fund_index_rltn rl,fund f,benchmark_index b
            ,fund_return v,calendar c,hdm.return_periods p,HDM.PERF_RUN_TYPE t,index_return idr
            WHERE wt.trgt_fund_key=tf.fund_key
            and wt.fund_key=f.fund_key
            and f.fund_compst_key = rl.fund_compst_key
            and rl.bmk_idx_key=b.bmk_idx_key
            and v.fund_key=f.fund_key
            and v.day_key=c.day_key
            and v.per_key=p.per_key
            and rl.perf_run_type_key=t.perf_run_type_key
            and rl.bmk_idx_key=idr.bmk_idx_key
            and rl.fund_compst_key=idr.fund_compst_key
            and idr.day_key=c.day_key
            and idr.per_key=p.per_key
            AND efftv_strt_dt <= p_date AND efftv_end_dt >= p_date
            --AND wgt_type IN ('FIRM','MODEL')
            AND (tf.fund_stat_cd in (1,5) or (tf.fund_incpn_dt <= p_date and tf.fund_clos_dt >= p_date))
            and rl.prim_bmk_flg = 'Y'
            and rl.link_prio = 1  
            and c.cal_day = p_date  
            and per_nm = '1DY' 
            and t.run_type_cd ='REPORTING'  -- this will help pickup the right splice.
        ) group by trgt_fund_key,day_key ; 
        
        t_last_wi_rfwt NUMBER;
        t_dly_ret      NUMBER;
        t_new_wi_rfwt NUMBER;
        t_new_wi_rbwt NUMBER;
        
        TYPE wt_record IS RECORD (
                tgt_fund_key  dbms_sql.number_table,
                day_key      dbms_sql.number_table,
                tgt_fund_dly_ret dbms_sql.number_table,
                tgt_idx_dly_ret  dbms_sql.number_table,
                tgt_wgt_type   DBMS_SQL.VARCHAR2_TABLE
        );
        
        tgt_wt_table    wt_record;
        t_date          DATE;
        t_last_bus_day  DATE;
        t_prev_bus_day  DATE;
        t_curr_wi_key   NUMBER;
        t_surr_key      NUMBER;
        t_rectype       VARCHAR2(4);
        t_bus_day_flg   VARCHAR2(1);
    BEGIN
        -- initialize
        t_new_wi_rfwt := NULL;
        t_new_wi_rbwt := NULL;
        t_date := p_wi_date;
        
 
       
        t_last_bus_day := t_date;
        
        -- get Business Dau prior to last business day
        SELECT max(cal_day) INTO t_prev_bus_day
        FROM hdm.calendar
        WHERE cal_day < t_last_bus_day;
       -- AND bus_day_flg = 'Y';        
        
        -- GET THE max Key value for wealth index table
        SELECT nvl(MAX(trgt_wlth_idx_key),0) INTO t_curr_wi_key
        FROM target_wealth_index;
        
        --OPEN DailyReturn(t_last_bus_day);  -- // changed on 11/13/15.
        OPEN DailyReturn(t_date);
        FETCH DailyReturn BULK COLLECT INTO
            Tgt_wt_table.tgt_fund_key,
            Tgt_wt_table.day_key,
            Tgt_wt_table.tgt_fund_dly_ret,
            Tgt_wt_table.tgt_idx_dly_ret;
        
        t_surr_key := 0;
        FOR t_Row IN 1..Tgt_wt_table.Tgt_fund_key.COUNT LOOP
            -- get the last wealth index values
            -- RFWT
            DBMS_OUTPUT.PUT_LINE('DAY KEY = ' || tgt_wt_table.day_key(t_row));
            DBMS_OUTPUT.PUT_LINE('FUND KEY = ' || tgt_wt_table.TGT_FUND_key(t_row));
            DBMS_OUTPUT.PUT_LINE('Last Day = ' || t_prev_bus_day);
            For t_Loop IN 1..2 LOOP
                t_dly_ret := NULL;
                Case
                When t_Loop = 1  Then 
                    t_recType := 'RFOG';
                    t_dly_ret := Tgt_wt_table.tgt_fund_dly_ret(t_row);
                When t_Loop = 2  Then 
                    t_recType := 'RBOG';
                    t_dly_ret := Tgt_wt_table.tgt_idx_dly_ret(t_row);
                End Case;

                begin
                
                    -- if prev WI date  is a business day then you should have a wealth index in the table.  If not it should error out.  
                    -- If not, go find the last wi (it could be a non-bus day due to price change)
                    SELECT bus_day_flg INTO t_bus_day_flg
                    FROM HDM.CALENDAR
                    WHERE cal_day = t_prev_bus_day;
                    IF t_bus_day_flg = 'Y' Then
                        SELECT wlth_idx INTO t_last_wi_rfwt
                        FROM TARGET_wealth_index
                        WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                        AND day_key = (select day_key from calendar where cal_day = t_prev_bus_day)
                        AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
                    ELSE
                        SELECT wlth_idx INTO t_last_wi_rfwt
                        FROM TARGET_wealth_index a
                        WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                        AND day_key = (select day_key from calendar where cal_day = (SELECT MAX(CAL_DAY) FROM CALENDAR CC,TARGET_WEALTH_INDEX WI WHERE CC.DAY_KEY=WI.DAY_KEY AND WI.trgt_perf_type_key=a.trgt_perf_type_key and CAL_DAY <= t_prev_bus_day))
                        AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
                    END IF;
                exception
                when NO_DATA_FOUND then
                    dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
                    dbms_output.put_line ('*** * NO WEALTH INDEX DATA FOR THIS DATE: ' || to_char(t_prev_bus_day,'dd-Mmm-yyyy'));
                    rollback;        
                    RAISE;
                WHEN others then
                    dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
                    rollback;        
                    RAISE;
                END;
                
        

            
                -- CALCULATE the RFWT AND RBWT wealth index
                t_new_wi_rfwt := 1 * t_last_wi_rfwt * (1 + t_dly_ret);
            
                --DBMS_OUTPUT.PUT_LINE('New WI RFWT = ' ||  t_new_wi_rfwt);
            
                -- insert the values -RFWT;
                If t_new_wi_rfwt is not null then
                    t_surr_key := t_surr_key + 1;
                    MERGE INTO TARGET_WEALTH_INDEX TGT
                    USING (SELECT t_curr_wi_key + t_surr_key trgt_wlth_idx_key,(select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType) trgt_perf_type_key,
                                Tgt_wt_table.day_key(t_row) day_key,Tgt_wt_table.tgt_fund_key(t_row) fund_key,t_new_wi_rfwt wlth_idx,Tgt_wt_table.tgt_fund_dly_ret(t_row) daily_ret,
                                'Y' curr_row_flg,sysdate row_strt_dttm 
                            FROM DUAL) SRC
                    ON (TGT.fund_key=SRC.fund_key AND TGT.day_key=SRC.day_key AND TGT.trgt_perf_type_key = SRC.trgt_perf_type_key)
                    WHEN NOT MATCHED THEN
                    INSERT (TRGT_WLTH_IDX_KEY,
                            TRGT_PERF_TYPE_KEY,
                            DAY_KEY,
                            FUND_KEY,
                            WLTH_IDX,
                            DAILY_RET,
                            CURR_ROW_FLG,
                            ROW_STRT_DTTM)
                    VALUES (SRC.TRGT_WLTH_IDX_KEY,
                            SRC.TRGT_PERF_TYPE_KEY,
                            SRC.DAY_KEY,
                            SRC.FUND_KEY,
                            SRC.WLTH_IDX,
                            SRC.DAILY_RET,
                            SRC.CURR_ROW_FLG,
                            SRC.ROW_STRT_DTTM)
                    WHEN MATCHED THEN
                    UPDATE SET 
                            TGT.WLTH_IDX  = SRC.WLTH_IDX,
                            TGT.DAILY_RET = SRC.DAILY_RET,
                            TGT.ROW_STRT_DTTM = SRC.ROW_STRT_DTTM;
                End If;
  
                        
                       -- dbms_output.put_line ('Record for ' ||   Tgt_wt_table.day_key(t_row)  || ' inserted');                  
            End Loop;
        End Loop;
        
        COMMIT;
        
    EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;
        
    END Load_gp_Wealth_index;
    

    
END PKG_TARGET_FUNDS;

CREATE OR REPLACE PACKAGE HDM.PKG_TGTX_FUNDS AS

/*  

	Custom Package created for calculating custom returns and wealth index for target funds
	
*/	

    TYPE TARGET_RETURN_TABLE IS TABLE OF Target_Return%ROWTYPE;
    PROCEDURE Load_Wealth_index(p_wi_date IN DATE);
    PROCEDURE load_target_Return(p_end_date IN DATE);

    Function calc_Target_Return(p_end_date IN Date,p_strt_date IN Date,p_fund_key In Number,p_type_key IN Number,p_annl_fctr IN Number,p_Period_ut IN Varchar2) Return Number;
    Function calc_Actual_Return(p_end_date IN Date,p_strt_date IN Date,p_fund_key In Number,p_type IN VARCHAR2,p_annl_fctr IN Number) Return Number;
    FUNCTION get_target_Returns(p_date IN DATE,p_monthend_flg IN VARCHAR2 DEFAULT NULL) RETURN Target_Return_TABLE PIPELINED; 
    
    PROCEDURE Load_GP_Wealth_index(p_wi_date IN DATE);  
    
END PKG_TGTX_FUNDS;

CREATE OR REPLACE PACKAGE BODY HDM.PKG_TGTX_FUNDS AS

    PROCEDURE Load_Wealth_index(p_wi_date IN DATE) IS
    
        CURSOR DailyReturn(p_date IN DATE) IS
        SELECT trgt_fund_key,day_key, sum(tgt_fund_dly_ret) tgt_fund_dly_ret,
        sum(tgt_idx_dly_ret) tgt_idx_dly_ret,wgt_type
        FROM (
            SELECT c.day_key,TRGT_FUND_KEY, fund_ret * trgt_fund_wgt tgt_fund_dly_ret, 
            idx_ret * trgt_fund_wgt tgt_idx_dly_ret,wgt_type
            FROM target_weights wt,fund tf,fund_index_rltn rl,fund f,benchmark_index b
            ,fund_return v,calendar c,hdm.return_periods p,HDM.PERF_RUN_TYPE t,index_return idr
            WHERE wt.trgt_fund_key=tf.fund_key
            and wt.fund_key=f.fund_key
            and f.fund_compst_key = rl.fund_compst_key
            and rl.bmk_idx_key=b.bmk_idx_key
            and v.fund_key=f.fund_key
            and v.day_key=c.day_key
            and v.per_key=p.per_key
            and rl.perf_run_type_key=t.perf_run_type_key
            and rl.bmk_idx_key=idr.bmk_idx_key
            and rl.fund_compst_key=idr.fund_compst_key
            and idr.day_key=c.day_key
            and idr.per_key=p.per_key
            AND efftv_strt_dt <= p_date AND efftv_end_dt >= p_date
            AND wgt_type IN ('FIRM','MODEL')
            AND (tf.fund_stat_cd in (1,5) or (tf.fund_incpn_dt <= p_date and tf.fund_clos_dt >= p_date))
            and rl.prim_bmk_flg = 'Y'
            and rl.link_prio = 1  
            and c.cal_day = p_date  
            and per_nm = '1MO' -- changed from 1 day to 1 month.
            and t.run_type_cd ='REPORTING'  -- this will help pickup the right splice.
        ) group by trgt_fund_key,day_key,wgt_type ; 
        
        t_last_wi_rfwt NUMBER;
        t_dly_ret      NUMBER;
        t_new_wi_rfwt NUMBER;
        t_new_wi_rbwt NUMBER;
        
        TYPE wt_record IS RECORD (
                tgt_fund_key  dbms_sql.number_table,
                day_key      dbms_sql.number_table,
                tgt_fund_dly_ret dbms_sql.number_table,
                tgt_idx_dly_ret  dbms_sql.number_table,
                tgt_wgt_type   DBMS_SQL.VARCHAR2_TABLE
        );
        
        tgt_wt_table    wt_record;
        t_date          DATE;
        t_last_bus_day  DATE;
        t_prev_bus_day  DATE;
        t_curr_wi_key   NUMBER;
        t_surr_key      NUMBER;
        t_rectype       VARCHAR2(4);
        t_bus_day_flg   VARCHAR2(1);
    BEGIN
        -- initialize
        t_new_wi_rfwt := NULL;
        t_new_wi_rbwt := NULL;
        t_date := p_wi_date;
        
        -- get last business day
        -- sn(6/15/18)--  Changes to accomodate non-business day index changes.
        -- do not look for last business day.  Use the passed down date.  
        
        SELECT max(cal_day) INTO t_last_bus_day
        FROM hdm.calendar
        WHERE cal_day <= t_date;
        --AND bus_day_flg = 'Y';
        
        --t_last_bus_day := t_date;
        
        -- get Business Dau prior to last business day
        -- sn(6/15/18) -- Look for last date prior to passed date.  If no WI, look for the prior date.
        SELECT max(cal_day) INTO t_prev_bus_day
        FROM hdm.calendar
        WHERE cal_day < t_last_bus_day
        AND CAL_MO_END_FLG='Y';
       -- AND bus_day_flg = 'Y';        
        
        -- GET THE max Key value for wealth index table
        SELECT nvl(MAX(trgt_wlth_idx_key),0) INTO t_curr_wi_key
        FROM tgtx_wealth_index;
        
        --OPEN DailyReturn(t_last_bus_day);  -- // changed on 11/13/15.
        OPEN DailyReturn(t_date);
        FETCH DailyReturn BULK COLLECT INTO
            Tgt_wt_table.tgt_fund_key,
            Tgt_wt_table.day_key,
            Tgt_wt_table.tgt_fund_dly_ret,
            Tgt_wt_table.tgt_idx_dly_ret,
            Tgt_wt_table.tgt_wgt_type;
        
        t_surr_key := 0;
        FOR t_Row IN 1..Tgt_wt_table.Tgt_fund_key.COUNT LOOP
            -- get the last wealth index values
            -- RFWT
--            DBMS_OUTPUT.PUT_LINE('DAY KEY = ' || tgt_wt_table.day_key(t_row));
--            DBMS_OUTPUT.PUT_LINE('FUND KEY = ' || tgt_wt_table.TGT_FUND_key(t_row));
--            DBMS_OUTPUT.PUT_LINE('Last Day = ' || t_prev_bus_day);
            For t_Loop IN 1..2 LOOP
                t_dly_ret := NULL;
                Case
                When t_Loop = 1 and Tgt_wt_table.tgt_wgt_type(t_Row) = 'FIRM' Then 
                    t_recType := 'RFWT';
                    t_dly_ret := Tgt_wt_table.tgt_fund_dly_ret(t_row);
                When t_Loop = 2 and Tgt_wt_table.tgt_wgt_type(t_Row) = 'FIRM' Then 
                    t_recType := 'RBWT';
                    t_dly_ret := Tgt_wt_table.tgt_idx_dly_ret(t_row);
                When t_Loop = 1 and Tgt_wt_table.tgt_wgt_type(t_Row) = 'MODEL' Then 
                    t_recType := 'RFWM';
                    t_dly_ret := Tgt_wt_table.tgt_fund_dly_ret(t_row);
                When t_Loop = 2 and Tgt_wt_table.tgt_wgt_type(t_Row) = 'MODEL' Then 
                    t_recType := 'RBWM';
                    t_dly_ret := Tgt_wt_table.tgt_idx_dly_ret(t_row);
                End Case;
                
                -- SN(6/15/18)  Changes to look for last business day if prior day has no WI.  Possible if month end is a weekend and price changed. (future)
                /*
                    SELECT wlth_idx INTO t_last_wi_rfwt
                    FROM target_wealth_index
                    WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                    AND day_key = (select day_key from calendar where cal_day = t_prev_bus_day)
                    AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
                */
                begin
                
                    -- if prev WI date  is a business day then you should have a wealth index in the table.  If not it should error out.  
                    -- If not, go find the last wi (it could be a non-bus day due to price change)
                    SELECT bus_day_flg INTO t_bus_day_flg
                    FROM HDM.CALENDAR
                    WHERE cal_day = t_prev_bus_day;
                    IF t_bus_day_flg = 'Y' Then
                        SELECT wlth_idx INTO t_last_wi_rfwt
                        FROM Tgtx_wealth_index
                        WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                        AND day_key = (select day_key from calendar where cal_day = t_prev_bus_day)
                        AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
                    ELSE
                        SELECT wlth_idx INTO t_last_wi_rfwt
                        FROM Tgtx_wealth_index
                        WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                        AND day_key = (select day_key from calendar where cal_day = (SELECT MAX(CAL_DAY) FROM CALENDAR CC,Tgtx_WEALTH_INDEX WI WHERE CC.DAY_KEY=WI.DAY_KEY AND CAL_DAY <= t_prev_bus_day and WI.FUND_KEY=tgt_wt_table.tgt_fund_key(t_row)))
                        AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
                    END IF;
                exception
                when NO_DATA_FOUND then
                    dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
                    dbms_output.put_line ('*** * NO WEALTH INDEX DATA FOR THIS DATE: ' || to_char(t_prev_bus_day,'dd-Mmm-yyyy'));
                    rollback;        
                    RAISE;
                WHEN others then
                    dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
                    rollback;        
                    RAISE;
                END;
                

            
                -- CALCULATE the RFWT AND RBWT wealth index
                t_new_wi_rfwt := 1 * t_last_wi_rfwt * (1 + t_dly_ret);
            
                --DBMS_OUTPUT.PUT_LINE('New WI RFWT = ' ||  t_new_wi_rfwt);
            
                -- insert the values -RFWT;
                If t_new_wi_rfwt is not null then
                    t_surr_key := t_surr_key + 1;
                    MERGE INTO Tgtx_WEALTH_INDEX TGT
                    USING (SELECT t_curr_wi_key + t_surr_key trgt_wlth_idx_key,(select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType) trgt_perf_type_key,
                                Tgt_wt_table.day_key(t_row) day_key,Tgt_wt_table.tgt_fund_key(t_row) fund_key,t_new_wi_rfwt wlth_idx,Tgt_wt_table.tgt_fund_dly_ret(t_row) daily_ret,
                                'Y' curr_row_flg,sysdate row_strt_dttm 
                            FROM DUAL) SRC
                    ON (TGT.fund_key=SRC.fund_key AND TGT.day_key=SRC.day_key AND TGT.trgt_perf_type_key = SRC.trgt_perf_type_key)
                    WHEN NOT MATCHED THEN
                    INSERT (TRGT_WLTH_IDX_KEY,
                            TRGT_PERF_TYPE_KEY,
                            DAY_KEY,
                            FUND_KEY,
                            WLTH_IDX,
                            DAILY_RET,
                            CURR_ROW_FLG,
                            ROW_STRT_DTTM)
                    VALUES (SRC.TRGT_WLTH_IDX_KEY,
                            SRC.TRGT_PERF_TYPE_KEY,
                            SRC.DAY_KEY,
                            SRC.FUND_KEY,
                            SRC.WLTH_IDX,
                            SRC.DAILY_RET,
                            SRC.CURR_ROW_FLG,
                            SRC.ROW_STRT_DTTM)
                    WHEN MATCHED THEN
                    UPDATE SET 
                            TGT.WLTH_IDX  = SRC.WLTH_IDX,
                            TGT.DAILY_RET = SRC.DAILY_RET,
                            TGT.ROW_STRT_DTTM = SRC.ROW_STRT_DTTM;
                End If;
  
                        
                       -- dbms_output.put_line ('Record for ' ||   Tgt_wt_table.day_key(t_row)  || ' inserted');                  
            End Loop;
        End Loop;
        
        COMMIT;
        
    EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;
        
    END Load_Wealth_index;
    
   FUNCTION get_target_Returns(p_date IN DATE,p_monthend_flg IN VARCHAR2 DEFAULT NULL) RETURN Target_Return_TABLE PIPELINED 
    IS
        CURSOR cFUND(p_in_date IN DATE) IS 
        -- 2/3/17 -- CHANGED for performance inception date.
        SELECT FUND_KEY,PERF_INCPN_DT FUND_INCPN_DT,COMP.FUND_COMPST_KEY
        FROM FUND, FUND_COMPOSITE COMP
        WHERE FUND.FUND_COMPST_KEY = COMP.FUND_COMPST_KEY
        AND FUND.SHR_CLS_CD = 1
        AND (FUND.FUND_STAT_CD IN ('O','S') OR NVL(FUND_CLOS_DT,TRUNC(SYSDATE))>= p_in_date)
        AND asset_cls_nm = 'Target Retirement'
        ORDER BY fund_sort_ord;
        
        CURSOR cPeriod(p_year IN NUMBER) IS
        SELECT per_key,per_Nm,per_ut,per_msr,annl_fctr,per_sort_ord
        FROM RETURN_PERIODS
        WHERE NOT ( per_ut = 'YR' and per_msr > p_year)
        AND (DAILY_MSR_FLG = 'Y' OR MO_END_MSR_FLG = 'Y')
        AND PER_NM NOT IN ('1DY','1WK','MTD','30DY','MTH','90DY')
        ORDER BY per_sort_ord;
        
        -- CARTESION join with Fund Index Relation Table.
        CURSOR cTyp(p_compst_key IN NUMBER) IS
        SELECT TRGT_PERF_TYPE_KEY,TRGT_PERF_TYPE,PERF_TYPE_FLG,OBJ_TYPE, NULL AS BMK_IDX_KEY,perf_sort_ord
        FROM target_performance_type 
        WHERE perf_type_flg <> 'B'
        UNION 
        SELECT TRGT_PERF_TYPE_KEY,TRGT_PERF_TYPE,PERF_TYPE_FLG,OBJ_TYPE,BMK_IDX_KEY,perf_sort_ord
        FROM target_performance_type tp,fund_index_rltn rl
        WHERE perf_type_flg = 'B'
        AND rl.fund_compst_key = p_compst_key
        AND rl.perf_run_type_key = 2
        ORDER BY perf_sort_ord;
        
        -- temp for only running glidepath returns.  
        /*
        CURSOR cTyp(p_compst_key IN NUMBER) IS
        SELECT TRGT_PERF_TYPE_KEY,TRGT_PERF_TYPE,PERF_TYPE_FLG,OBJ_TYPE, NULL AS BMK_IDX_KEY,perf_sort_ord
        FROM target_performance_type 
        WHERE TRGT_PERF_TYPE IN ('RFOG','RBOG');        
        */
        
        t_date DATE;
        t_begin_date DATE;
        t_incp_dt DATE;
        t_incp_yr NUMBER;
        t_yr_days NUMBER;
        t_annlzn_fctr NUMBER;
        t_return NUMBER;
        t_ret_key NUMBER;
        t_day_key NUMBER; 
        t_sort_order NUMBER;
        t_curr_yr_days NUMBER;

        
        TYPE FUND_ROW IS TABLE OF cFUND%ROWTYPE;
        TYPE PERIOD_ROW IS TABLE OF cPERIOD%ROWTYPE;
        TYPE TYP_ROW IS TABLE OF cTYP%ROWTYPE;
        
        FUND_TABLE FUND_ROW;
        PERIOD_TABLE PERIOD_ROW;
        TYP_TABLE TYP_ROW;
        t_return_row Target_Return%RowType;
        
        
    BEGIN    
        t_date := p_date;
        t_sort_order := 0;
         
        -- Get the day Key for the Performance Date
        Begin
            SELECT Day_key INTO t_day_key
            FROM calendar
            WHERE cal_day = t_date;
        End;
        -- Get number of days in current Year
        t_curr_yr_days := t_date - add_months(t_date,-12);
        -- START THE FUND LOOP
        OPEN cFUND(t_date);
        FETCH cFUND BULK COLLECT INTO fund_table;
        
        FOR t_fund_ctr IN 1..fund_table.COUNT LOOP
            -- DERIVE INCEPTION DATE
            -- FOR FUNDS STARTED ON 1/2/2009, consider them as started on 1/1/2009.
            t_incp_dt := trunc(fund_table(t_fund_ctr).FUND_INCPN_DT,'mm');
            t_incp_yr := floor(months_between(t_date + 1,t_incp_dt) / 12);
            
            -- LOOP FOR PERIODS
            OPEN cPERIOD(t_incp_yr);
            FETCH cPERIOD BULK COLLECT INTO PERIOD_TABLE;
            
            FOR t_period_ctr IN 1..PERIOD_TABLE.COUNT LOOP
                t_begin_date := hdm.pkg_fund_perf.get_begin_dt(period_table(t_period_ctr).per_ut,period_table(t_period_ctr).per_msr,t_incp_dt,t_date);
                
                -- Any performance gain on first day should be excluded from return calculation.
            
                t_begin_date := (Case When t_begin_date < fund_table(t_fund_ctr).fund_incpn_dt Then fund_table(t_fund_ctr).fund_incpn_dt Else t_begin_date end);
                
                if period_table(t_period_ctr).per_ut = 'IN' Then
                    -- recalc years since inception.
                    -- t_incp_yr calc is different from above year calculation due to nbr of days = 1 year.
                    t_incp_yr := floor(months_between(t_date,t_incp_dt) / 12);
                    -- get number of days in current year.
                    t_yr_days := trunc(t_date) - (add_months(t_incp_dt,(floor(months_between(t_date,t_incp_dt)/12)* 12 )) - 1);
                     
                    t_annlzn_fctr := t_incp_yr + (t_yr_days / t_curr_yr_days);
                else
                    t_annlzn_fctr := period_table(t_period_ctr).annl_fctr;

                end if;
                
                -- annualization factor should be at least 1;
                t_annlzn_fctr := (Case When t_annlzn_fctr < 1 Then 1 else t_annlzn_fctr end);
                
                OPEN cTYP(fund_table(t_fund_ctr).fund_compst_key);
                FETCH cTYP BULK COLLECT INTO TYP_TABLE;
                FOR t_type_Ctr IN 1..typ_table.COUNT LOOP
                    --  Fund calculation
                    -- Initialize the record.
                    t_return := NULL;
                    CASE typ_table(t_type_ctr).perf_type_flg
                        When  'F' Then  -- Fund Return Calc
                            t_return  := PKG_FUND_PERF.calc_Return(t_date,t_begin_date,fund_table(t_fund_ctr).fund_key,t_annlzn_fctr,typ_table(t_type_ctr).Obj_Type);
                        When 'D' Then  --  Target Weight & Model Returns
                          
                            t_return := PKG_Tgtx_FUNDS.Calc_Target_Return(t_date,t_begin_date,fund_table(t_fund_ctr).fund_key,typ_table(t_type_ctr).trgt_perf_type_key,t_annlzn_fctr,period_table(t_period_ctr).per_ut);
                        When 'A' Then  -- Actual weights return;
                          --  dbms_output.put_line('end date: ' || t_date);
                         --   dbms_output.put_line('start date: ' || t_begin_date);
                          --  dbms_output.put_line('fund_key: ' || fund_table(t_fund_ctr).fund_key);
                          --  dbms_output.put_line('type: ' || typ_table(t_type_ctr).Obj_Type);
                          --  dbms_output.put_line('annl fctr: ' || t_annlzn_fctr);
                            t_return := PKG_tgtx_FUNDS.Calc_Actual_Return(t_date,t_begin_date,fund_table(t_fund_ctr).fund_key,typ_table(t_type_ctr).Obj_Type,t_annlzn_fctr);
                        When 'B' Then -- Benchmarks    
                            t_return := PKG_FUND_PERF.calc_Return(t_date,t_begin_date,typ_table(t_type_ctr).bmk_idx_key,t_annlzn_fctr,typ_table(t_type_ctr).Obj_Type);
                            
                        Else
                            t_return := NULL;
                    END CASE;
                    
                    if t_return IS NOT NULL THEN
                        t_ret_key := t_ret_key + 1;
                        t_sort_order := t_sort_order +1;
                        -- Populate the record
                        t_return_row.trgt_ret_key := t_ret_key;
                        t_return_row.fund_key := fund_table(t_fund_ctr).fund_key;
                        t_return_row.trgt_perf_type_key := typ_table(t_type_ctr).trgt_perf_type_key;
                        t_return_row.day_key := t_day_key;
                        t_return_row.per_key := period_table(t_period_ctr).per_key;
                        t_return_row.bmk_idx_key :=  typ_table(t_type_ctr).bmk_idx_key;
                        t_return_row.trgt_ret := t_return;
                        t_return_row.beg_dt := t_begin_date;
                        t_return_row.annlzn_fctr := t_annlzn_fctr;
                        t_return_row.displ_sort_ord := t_sort_order;
                        t_return_row.curr_row_flg := 'Y';
                        t_return_row.row_strt_dttm := SYSDATE;
                        
                        -- Send the record 
                        PIPE ROW(t_return_row);
                    End If;
                End Loop; -- Loop for Performance Types
                CLOSE cTyp;
            End Loop;  -- Loop for Return Periods
            CLOSE cPeriod;
        End Loop;  -- Loop For Fund
        CLOSE cFUND;
        RETURN;
    EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;        
    END get_target_Returns;    
                    
    Function calc_Target_Return(p_end_date IN Date,p_strt_date IN Date,p_fund_key In Number,p_type_key IN Number,p_annl_fctr IN Number,p_Period_ut IN Varchar2) Return Number 
    IS
        t_return NUMBER;
        t_beg_date Date;
        t_strt_WI NUMBER;
        t_end_WI NUMBER;  
        t_bus_day DATE;  
        t_Cntr NUMBER;
    Begin
            BEGIN
            -- Calculate the start date for getting the starting wealth index .
            -- if begin date falls on a holiday or weekened, then it should pickup the latest business day prior to that date.  
            --  Exception is Month end, which will have a Wealth Index.
              
                If p_period_ut <> 'IN' Then
                    select Count(cal_day) INTO t_Cntr
                    FROM tgtx_wealth_index wi,calendar cal
                    WHERE  wi.day_key = cal.day_key
                    and wi.fund_key = p_fund_key
                    and cal_day  =    p_strt_date;
                    
                    If t_Cntr = 0 Then
                        select Max(cal_day) INTO t_beg_date
                        FROM tgtx_wealth_index wi,calendar cal
                        WHERE  wi.day_key = cal.day_key
                        and cal_day  <=    p_strt_date
                        and bus_day_flg = 'Y';
                    Else
                       t_beg_date := p_strt_date;
                    End If; 
                       
                Else
                    t_beg_date := p_strt_date;
                End if;
                
                
                SELECT wlth_idx INTO t_strt_WI
                FROM tgtx_wealth_index wi,calendar cal
                WHERE wi.day_key = cal.day_key
                AND cal_day = t_beg_date
                AND fund_key = p_fund_key
                AND trgt_perf_type_key = p_type_key;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                dbms_output.put_line('NO Starting Wealth Index Record Found');
                dbms_output.put_line('TW : ' || p_end_date || ' , ' || t_beg_date || ', fund = ' || p_fund_key || ', annl fctr = ' || p_annl_fctr || ', type = ' || p_type_key);
                Return NULL;
            WHEN OTHERS THEN
                RAISE;
            End;
            
            -- Get ending Wealth Index
            -- Check if the end date is a holiday -- if yes then get the most recent Wealth Index. // Not used currently.
            select nvl(max(cal_day),t_bus_day) INTO t_bus_day
            From calendar
            Where cal_day <= t_bus_day
            and bus_day_flg = 'Y' and cal_mo_end_flg = 'N';
            
            
            Begin
                SELECT wlth_idx INTO t_end_WI
                FROM tgtx_wealth_index wi,calendar cal
                WHERE wi.day_key = cal.day_key
                AND cal_day = p_end_date
                AND fund_key = p_fund_key
                AND trgt_perf_type_key = p_type_key; 
            EXCEPTION                       
            WHEN NO_DATA_FOUND THEN
                dbms_output.put_line('NO Ending Wealth Index Record Found');
                dbms_output.put_line('Initial : ' || t_bus_day || ' , ' || t_beg_date || ', fund = ' || p_fund_key || ', annl fctr = ' || p_annl_fctr || ', type = ' || p_type_key);
                Return NULL;
            WHEN OTHERS THEN
                RAISE;
            End;        
            
            -- Calculate Return
            t_return := Power((t_end_WI / t_Strt_WI), (1/p_annl_fctr)) - 1;
            
            Return t_return;
    End Calc_Target_Return;

    Function calc_Actual_Return(p_end_date IN Date,p_strt_date IN Date,p_fund_key In Number,p_type IN VARCHAR2,p_annl_fctr IN Number) Return Number 
    IS
        
        CURSOR cWEIGHT(p_date IN DATE,p_target_fund IN Number) IS
        SELECT trgt_fund_key,wt.day_key,wt.fund_key,rl.bmk_idx_key,daily_holdg_wgt
        from target_daily_weight wt, calendar cal,fund_index_rltn rl,fund fn,benchmark_Index bm
        where wt.day_key= cal.day_key
        and wt.fund_key=fn.fund_key
        and fn.fund_compst_key=rl.fund_compst_key
        and rl.bmk_idx_key = bm.bmk_idx_key
        and cal_day = p_date
        and trgt_fund_key = p_fund_key
        and perf_run_type_key = 3
        and prim_bmk_flg = 'Y';
        
        t_return NUMBER;
        t_retval NUMBER;
        t_Key NUMBER;
        t_date DATE;
        
        TYPE Weight_Row IS TABLE OF cWEIGHT%ROWTYPE;
        Weight_Table Weight_Row;
    Begin

        -- Get the correct end date by checking the business day flag.
        SELECT max(cal_day) Into t_date
        FROM CALENDAR
        WHERE CAL_DAY <= p_end_date
        And bus_day_flg = 'Y';
        
        OPEN cWEIGHT(t_date,p_fund_key);
        FETCH cWEIGHT BULK COLLECT INTO Weight_Table;
        --dbms_output.put_line('Initial : ' || p_end_date || ' , ' || p_strt_date || ', fund = ' || p_fund_key || ', annl fctr = ' || p_annl_fctr || ', type = ' || p_type);

        -- Start the Loop
        FOR t_WRow     IN 1..Weight_Table.COUNT LOOP
            t_Key := (Case When p_type = 'FUND' Then Weight_Table(t_WRow).fund_key Else Weight_Table(t_WRow).bmk_idx_key end);
            t_retval  := PKG_FUND_PERF.calc_Return(p_end_date,p_strt_date,t_key,p_annl_fctr,p_type);
            t_return  := nvl(t_return,0) + ( t_retval * Weight_table(t_WRow).daily_holdg_wgt );
            --dbms_output.put_line(p_end_date || ' , ' || p_strt_date || ', t_key = ' || t_key || ', annl fctr = ' || p_annl_fctr || ', type = ' || p_type);
            --dbms_output.put_line('Return = ' || t_return);
        END LOOP;
        
        Return t_return;
    End calc_Actual_Return;   
    
	PROCEDURE load_target_Return(p_end_date IN DATE)
	IS
		t_Row NUMBER;
	BEGIN
		-- Get the current maximum key
		BEGIN
			SELECT NVL(MAX(trgt_ret_key),0) INTO t_Row
			FROM Tgtx_RETURN;
		END;
		
		MERGE INTO Tgtx_RETURN TGT
		USING (
				SELECT t_row + rownum AS TRGT_RET_KEY,FUND_KEY,TRGT_PERF_TYPE_KEy,DAY_KEY,PER_KEY,BMK_IDX_KEY,nvl(BMK_IDX_KEY,-1) bmk_lkup_key,TRGT_RET,BEG_DT,ANNLZN_FCTR,DISPL_SORT_ORD,CURR_ROW_FLG,ROW_STRT_DTTM
				FROM TABLE(PKG_Tgtx_FUNDS.GET_TARGET_RETURNS(p_end_date))
			  ) SRC
		ON 	(tgt.fund_key=src.fund_key AND tgt.trgt_perf_type_key=src.trgt_perf_type_Key AND NVL(tgt.bmk_idx_key,-1)=src.bmk_LKUP_key AND tgt.day_key=src.day_key AND tgt.per_key=src.per_key)
		WHEN NOT MATCHED THEN
				INSERT (TRGT_RET_KEY,FUND_KEY,TRGT_PERF_TYPE_KEY,DAY_KEY,PER_KEY,BMK_IDX_KEY,TRGT_RET,BEG_DT,ANNLZN_FCTR,DISPL_SORT_ORD,CURR_ROW_FLG,ROW_STRT_DTTM)
				VALUES (SRC.TRGT_RET_KEY,SRC.FUND_KEY,SRC.TRGT_PERF_TYPE_KEY,SRC.DAY_KEY,SRC.PER_KEY,SRC.BMK_IDX_KEY,SRC.TRGT_RET,SRC.BEG_DT,SRC.ANNLZN_FCTR,SRC.DISPL_SORT_ORD,SRC.CURR_ROW_FLG,SRC.ROW_STRT_DTTM)
		WHEN MATCHED THEN
				UPDATE SET 
							TGT.TRGT_RET = SRC.TRGT_RET,
							TGT.BEG_DT = SRC.BEG_DT,
							TGT.ANNLZN_FCTR = SRC.ANNLZN_FCTR,
							TGT.DISPL_SORT_ORD = SRC.DISPL_SORT_ORD,
							TGT.CURR_ROW_FLG = SRC.CURR_ROW_FLG,
							TGT.ROW_STRT_DTTM = SRC.ROW_STRT_DTTM;
							
		COMMIT;
    EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;
	END load_target_Return;     
	
    PROCEDURE Load_gp_Wealth_index(p_wi_date IN DATE) IS
    
        CURSOR DailyReturn(p_date IN DATE) IS
        SELECT trgt_fund_key,day_key, sum(tgt_fund_dly_ret) tgt_fund_dly_ret,sum(tgt_idx_dly_ret) tgt_idx_dly_ret
        FROM (
            SELECT c.day_key,TRGT_FUND_KEY, fund_ret * gp_wgt tgt_fund_dly_ret, idx_ret * gp_wgt tgt_idx_dly_ret
            FROM target_orig_glidepath wt,fund tf,fund_index_rltn rl,fund f,benchmark_index b
            ,fund_return v,calendar c,hdm.return_periods p,HDM.PERF_RUN_TYPE t,index_return idr
            WHERE wt.trgt_fund_key=tf.fund_key
            and wt.fund_key=f.fund_key
            and f.fund_compst_key = rl.fund_compst_key
            and rl.bmk_idx_key=b.bmk_idx_key
            and v.fund_key=f.fund_key
            and v.day_key=c.day_key
            and v.per_key=p.per_key
            and rl.perf_run_type_key=t.perf_run_type_key
            and rl.bmk_idx_key=idr.bmk_idx_key
            and rl.fund_compst_key=idr.fund_compst_key
            and idr.day_key=c.day_key
            and idr.per_key=p.per_key
            AND efftv_strt_dt <= p_date AND efftv_end_dt >= p_date
            --AND wgt_type IN ('FIRM','MODEL')
            AND (tf.fund_stat_cd in (1,5) or (tf.fund_incpn_dt <= p_date and tf.fund_clos_dt >= p_date))
            and rl.prim_bmk_flg = 'Y'
            and rl.link_prio = 1  
            and c.cal_day = p_date  
            and per_nm = '1MO' 
            and t.run_type_cd ='REPORTING'  -- this will help pickup the right splice.
        ) group by trgt_fund_key,day_key ; 
        
        t_last_wi_rfwt NUMBER;
        t_dly_ret      NUMBER;
        t_new_wi_rfwt NUMBER;
        t_new_wi_rbwt NUMBER;
        
        TYPE wt_record IS RECORD (
                tgt_fund_key  dbms_sql.number_table,
                day_key      dbms_sql.number_table,
                tgt_fund_dly_ret dbms_sql.number_table,
                tgt_idx_dly_ret  dbms_sql.number_table,
                tgt_wgt_type   DBMS_SQL.VARCHAR2_TABLE
        );
        
        tgt_wt_table    wt_record;
        t_date          DATE;
        t_last_bus_day  DATE;
        t_prev_bus_day  DATE;
        t_curr_wi_key   NUMBER;
        t_surr_key      NUMBER;
        t_rectype       VARCHAR2(4);
        t_bus_day_flg   VARCHAR2(1);
    BEGIN
        -- initialize
        t_new_wi_rfwt := NULL;
        t_new_wi_rbwt := NULL;
        t_date := p_wi_date;
        
 
       
        t_last_bus_day := t_date;
        
        -- get Business Dau prior to last business day
        SELECT max(cal_day) INTO t_prev_bus_day
        FROM hdm.calendar
        WHERE cal_day < t_last_bus_day
        AND CAL_MO_END_FLG='Y';
       -- AND bus_day_flg = 'Y';        
        
        -- GET THE max Key value for wealth index table
        SELECT nvl(MAX(trgt_wlth_idx_key),0) INTO t_curr_wi_key
        FROM tgtx_wealth_index;
        
        --OPEN DailyReturn(t_last_bus_day);  -- // changed on 11/13/15.
        OPEN DailyReturn(t_date);
        FETCH DailyReturn BULK COLLECT INTO
            Tgt_wt_table.tgt_fund_key,
            Tgt_wt_table.day_key,
            Tgt_wt_table.tgt_fund_dly_ret,
            Tgt_wt_table.tgt_idx_dly_ret;
        
        t_surr_key := 0;
        FOR t_Row IN 1..Tgt_wt_table.Tgt_fund_key.COUNT LOOP
            -- get the last wealth index values
            -- RFWT
            DBMS_OUTPUT.PUT_LINE('DAY KEY = ' || tgt_wt_table.day_key(t_row));
            DBMS_OUTPUT.PUT_LINE('FUND KEY = ' || tgt_wt_table.TGT_FUND_key(t_row));
            DBMS_OUTPUT.PUT_LINE('Last Day = ' || t_prev_bus_day);
            For t_Loop IN 1..2 LOOP
                t_dly_ret := NULL;
                Case
                When t_Loop = 1  Then 
                    t_recType := 'RFOG';
                    t_dly_ret := Tgt_wt_table.tgt_fund_dly_ret(t_row);
                When t_Loop = 2  Then 
                    t_recType := 'RBOG';
                    t_dly_ret := Tgt_wt_table.tgt_idx_dly_ret(t_row);
                End Case;

                begin
                
                    -- if prev WI date  is a business day then you should have a wealth index in the table.  If not it should error out.  
                    -- If not, go find the last wi (it could be a non-bus day due to price change)
                    SELECT bus_day_flg INTO t_bus_day_flg
                    FROM HDM.CALENDAR
                    WHERE cal_day = t_prev_bus_day;
                    IF t_bus_day_flg = 'Y' Then
                        SELECT wlth_idx INTO t_last_wi_rfwt
                        FROM Tgtx_wealth_index
                        WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                        AND day_key = (select day_key from calendar where cal_day = t_prev_bus_day)
                        AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
                    ELSE
                        SELECT wlth_idx INTO t_last_wi_rfwt
                        FROM Tgtx_wealth_index
                        WHERE trgt_perf_type_key = (select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType)
                        AND day_key = (select day_key from calendar where cal_day = (SELECT MAX(CAL_DAY) FROM CALENDAR CC,Tgtx_WEALTH_INDEX WI WHERE CC.DAY_KEY=WI.DAY_KEY AND CAL_DAY <= t_prev_bus_day))
                        AND fund_key = tgt_wt_table.tgt_fund_key(t_row);
                    END IF;
                exception
                when NO_DATA_FOUND then
                    dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
                    dbms_output.put_line ('*** * NO WEALTH INDEX DATA FOR THIS DATE: ' || to_char(t_prev_bus_day,'dd-Mmm-yyyy'));
                    rollback;        
                    RAISE;
                WHEN others then
                    dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
                    dbms_output.put_line('----------------------------------------------');
                    dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
                    rollback;        
                    RAISE;
                END;
                
        

            
                -- CALCULATE the RFWT AND RBWT wealth index
                t_new_wi_rfwt := 1 * t_last_wi_rfwt * (1 + t_dly_ret);
            
                --DBMS_OUTPUT.PUT_LINE('New WI RFWT = ' ||  t_new_wi_rfwt);
            
                -- insert the values -RFWT;
                If t_new_wi_rfwt is not null then
                    t_surr_key := t_surr_key + 1;
                    MERGE INTO Tgtx_WEALTH_INDEX TGT
                    USING (SELECT t_curr_wi_key + t_surr_key trgt_wlth_idx_key,(select trgt_perf_type_key from target_performance_type where trgt_perf_type = t_recType) trgt_perf_type_key,
                                Tgt_wt_table.day_key(t_row) day_key,Tgt_wt_table.tgt_fund_key(t_row) fund_key,t_new_wi_rfwt wlth_idx,Tgt_wt_table.tgt_fund_dly_ret(t_row) daily_ret,
                                'Y' curr_row_flg,sysdate row_strt_dttm 
                            FROM DUAL) SRC
                    ON (TGT.fund_key=SRC.fund_key AND TGT.day_key=SRC.day_key AND TGT.trgt_perf_type_key = SRC.trgt_perf_type_key)
                    WHEN NOT MATCHED THEN
                    INSERT (TRGT_WLTH_IDX_KEY,
                            TRGT_PERF_TYPE_KEY,
                            DAY_KEY,
                            FUND_KEY,
                            WLTH_IDX,
                            DAILY_RET,
                            CURR_ROW_FLG,
                            ROW_STRT_DTTM)
                    VALUES (SRC.TRGT_WLTH_IDX_KEY,
                            SRC.TRGT_PERF_TYPE_KEY,
                            SRC.DAY_KEY,
                            SRC.FUND_KEY,
                            SRC.WLTH_IDX,
                            SRC.DAILY_RET,
                            SRC.CURR_ROW_FLG,
                            SRC.ROW_STRT_DTTM)
                    WHEN MATCHED THEN
                    UPDATE SET 
                            TGT.WLTH_IDX  = SRC.WLTH_IDX,
                            TGT.DAILY_RET = SRC.DAILY_RET,
                            TGT.ROW_STRT_DTTM = SRC.ROW_STRT_DTTM;
                End If;
  
                        
                       -- dbms_output.put_line ('Record for ' ||   Tgt_wt_table.day_key(t_row)  || ' inserted');                  
            End Loop;
        End Loop;
        
        COMMIT;
        
    EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line ('##ERROR:  ' || SQLCODE || ' Error Message: ' || SQLERRM);
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error BackTrace: ' || DBMS_UTILITY.format_error_backtrace());
        dbms_output.put_line('----------------------------------------------');
        dbms_output.put_line('Error Stack: ' || DBMS_UTILITY.format_error_stack()); 
        rollback;        
        RAISE;
        
    END Load_gp_Wealth_index;
    

    
END PKG_TGTX_FUNDS;

CREATE OR REPLACE PACKAGE HDM.secure_data
AS
 FUNCTION decrypt_raw2number (p_in_raw RAW) return NUMBER;
 FUNCTION decrypt_raw2string (p_in_raw RAW) return VARCHAR2;
 FUNCTION encrypt_number2raw (p_in_number NUMBER) return RAW;
 FUNCTION encrypt_string2raw (p_in_String VARCHAR2) return RAW;
 FUNCTION hash_stringtoraw(p_in_String VARCHAR2) return RAW;
END secure_data;

CREATE OR REPLACE PACKAGE BODY HDM.secure_data wrapped
a000000
1
abcd
abcd
abcd
abcd
abcd
abcd
abcd
abcd
abcd
abcd
abcd
abcd
abcd
abcd
abcd
b
a4e 2a9
NclxsMx/baJd1DocrholsHltbEQwg82JVdyGfC8C/oOuf7+yCQIJsN7XK8oxSgVgeZCFUm+B
l+bdmd25+GS15rcJK21GZ+FmXDIAxoZYgg+jXrxr1fvYxTOlMKr4we4ZaFXkvDWgNdkrwEwr
056vPbw8YLP2VgWktp7xg4klealzbqhs9QoURsrF/Tam3xkRQypVC7G93OvSfu42XAoeK9rM
Puo1pW3FvOiehK5vIB+VqI26okTSLEkfJdl5VWC79u5hHnkLCj3gAZm1Qt6CwkwLNOV7lkZM
ZUj8siCbAnKpdVSN07K7DcFAxuThAhK20sPmjFdxhzUXcs4vLT1ThcwQAK9gipBw25sxiZFe
QczsY7c+Q3XnNVqni04B/4jZSc/UnmW2x1k66ZeI1HXM5uhxuGAbTsxZKYr9I34TEYE8ibZm
cnlSYOrUMbL6DrF/X33fajuH8cx8BDV2SwAvyW500xKU08JUDut32/iuXuuycxMTrtnXylU+
VOC/99GKW0Pt47V47h69iP2q3NnI8XUIndTQGaapfOTdCWRNvDJrEMEZUqoLV+5VUiiAFpWP
bRakGM5Udc9JUkjWQPmVNUvlxjUULRKzJKi9OLv2ty/sAI9L6Nd7q4wKvwfugRbOI7+VB2mP
uT+ZNocA0X0/+c6ZH4LAew==;
