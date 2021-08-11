CREATE OR REPLACE procedure HDM.DELMAY16_proc_update_acct_posn as
--inital procedure HDM.proc_update_acct_posnwas created 5/3/2012 3:46pm, last DDL change 5/11/2012 12:49pm
-- rename it as DELMAY16_, if no one use it by May 2016 this procedure can be dropped
    cursor c1 is
        select acct_posn_key,hbr_dlr_key
        from hbr_dlr_update;
        
    type pos_table is record (
        acct_posn_key dbms_sql.number_table,
        hbr_dlr_key dbms_sql.number_table
    );
    
    SrcData pos_table;
    t_row number;
    
begin
    dbms_output.put_line('Start: ' || to_char(sysdate,'hh:mi:ss'));
    open c1;
    loop
        fetch c1 bulk collect into 
            srcdata.acct_posn_key,
            srcdata.hbr_dlr_key
            limit 100000;
            
        forall t_row in 1..srcdata.acct_posn_key.count
            update hdm.account_Position
                set hbr_dlr_key = srcdata.hbr_dlr_key(t_row)
                where acct_posn_key = srcdata.acct_posn_key(t_row);
                exit when c1%notfound;
            commit;
    end loop;
    commit;
    close c1;
    dbms_output.put_line('End: ' || to_char(sysdate,'hh:mi:ss'));
end DELMAY16_proc_update_acct_posn;

CREATE OR REPLACE PROCEDURE HDM.load_dealer_position(p_YYYYMM varchar2)
AS
-------------------------------------------------------------------------------
-- NAME:    load_dealer_position
--
-- PURPOSE: Sum account positions by dealer, and load in aggregate table.
--          These daily amounts will be used to create average dealer balances
--          for the month which will serve as support for dealer fee payments.
--          This load was previously done VIA one simple insert statement, but
--          as the size of the Account_Position table grew, the insert statement
--          performance degraded rapidly.  This procedure is much faster 
--          because the query works on one position date at a time, which allows
--          for a much faster query plan than that used when trying to get all
--          the position days in one fell swoop.
--
-- Revisions:
-- Date        Author           Description
  ----------  ---------  -------------------------------------------------------
--05/05/2010   jcorkery  Initial procedure creation
--06/21/2010   jcorkery  Changed to summarize using the dealer relationships
--                         in effect on the 3rd business day of the next month

--------------------------------------------------------------------------------


   --Local variable for input param.  This is the mo_cd from the calendar
   --table from which to create a cursor of all the day_key's for that
   --month.  The account positions for all those days will be grouped
   --by dealer and loaded into the dealer_position table. 
   v_YYYYMM varchar2(6);

   v_date_holder date; 
   
   v_3rd_Busn_Day_Key number;  --day key variable

   --all date keys to be loaded
   CURSOR cur_position_dates
   IS
      SELECT   CAL.DAY_KEY
        FROM   vw_calendar CAL
       WHERE   CAL.MO_CD = v_YYYYMM;
BEGIN
       --pass the input param to local variable
   v_YYYYMM := p_YYYYMM; 
 
        --get the 3rd business day of the next month which is when accounting
        --wants the dealer relationships in effect
   --Busn Day 1
SELECT MIN(cal_day) INTO v_date_holder       FROM  calendar C WHERE C.MO_CD > v_YYYYMM AND C.BUS_DAY_FLG = 'Y';
    --Busn Day 2
SELECT MIN(cal_day) INTO v_date_holder       FROM  calendar C WHERE C.CAL_DAY > v_date_holder AND C.BUS_DAY_FLG = 'Y';
    --Busn Day 3
SELECT MIN(cal_day) INTO v_date_holder       FROM  calendar C WHERE C.CAL_DAY > v_date_holder AND C.BUS_DAY_FLG = 'Y';
    --Day key of 3rd busn day
SELECT C.DAY_KEY    INTO  v_3rd_Busn_Day_Key FROM calendar C  WHERE C.CAL_DAY = v_date_holder;


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
             LEFT JOIN (SELECT PDLR.ACCT_NBR, PDLR.FUND_KEY, PDLR.DLR_KEY FROM HDM.ACCOUNT_POSITION PDLR WHERE PDLR.DAY_KEY = v_3rd_Busn_Day_Key ) DLR
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

END load_dealer_position;

CREATE OR REPLACE PROCEDURE HDM.Prep_MDD_Objects
AS
BEGIN
   --set the current business day for most date_ranges
   UPDATE   RFS_REPORT_DATE
      SET   REPORT_EFFECTIVE_DATE =
               (SELECT   CAL_DAY
                  FROM   VW_CALENDAR
                 WHERE   CAL_DAY =
                            (SELECT   MAX (CAL_DAY)
                               FROM   VW_CALENDAR
                              WHERE   CAL_DAY BETWEEN (TRUNC (SYSDATE) - 7)
                                                  AND  TRUNC (SYSDATE - 1)
                                      AND BUS_DAY_FLG = 'Y'))
    WHERE   date_range IN ('DLY', 'MTD', 'YTD', 'BAL');

   --set the prior year-end date for the prior year record
   UPDATE   RFS_REPORT_DATE
      SET   REPORT_EFFECTIVE_DATE =
               (SELECT   cal_day
                  FROM   VW_CALENDAR
                 WHERE   cal_day =
                            (SELECT   MAX (CAL_DAY)
                               FROM   VW_CALENDAR
                              WHERE   YE_FLG = 'Y'
                                      AND cal_day BETWEEN ADD_MONTHS (
                                                             TRUNC (SYSDATE),
                                                             -12
                                                          )
                                                      AND  TRUNC (SYSDATE)))
    WHERE   date_range NOT IN ('DLY', 'MTD', 'YTD', 'BAL');

   COMMIT;

    --compile all MV's
   
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_LIST compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_BALANCE compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_BALANCE_W_TOTAL compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_SUBSCRIPTION compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_REDEMPTION compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_NET compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_NET_W_TOTAL compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_FND_MRG_CASHFLOW_SUMMARY compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_CURRENT_SHD_ACCT_BAL compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_REPORT_DATE compile';   



   --update the materialized views
   DBMS_MVIEW.REFRESH ('HDM.MV_INST_LIST', 'C');
   DBMS_MVIEW.REFRESH ('HDM.MV_INST_BALANCE', 'C');
   DBMS_MVIEW.REFRESH ('HDM.MV_INST_BALANCE_W_TOTAL', 'C');
   DBMS_MVIEW.REFRESH ('HDM.MV_INST_SUBSCRIPTION', 'C');
   DBMS_MVIEW.REFRESH ('HDM.MV_INST_REDEMPTION', 'C');
   DBMS_MVIEW.REFRESH ('HDM.MV_INST_NET', 'C');
   DBMS_MVIEW.REFRESH ('HDM.MV_INST_NET_W_TOTAL', 'C');
   DBMS_MVIEW.REFRESH ('HDM.MV_FND_MRG_CASHFLOW_SUMMARY', 'C');
   DBMS_MVIEW.REFRESH ('HDM.MV_CURRENT_SHD_ACCT_BAL', 'C');
   DBMS_MVIEW.REFRESH ('HDM.MV_REPORT_DATE', 'C');

 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_LIST compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_BALANCE compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_BALANCE_W_TOTAL compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_SUBSCRIPTION compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_REDEMPTION compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_NET compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_INST_NET_W_TOTAL compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_FND_MRG_CASHFLOW_SUMMARY compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_CURRENT_SHD_ACCT_BAL compile';
 EXECUTE IMMEDIATE 'Alter materialized view HDM.MV_REPORT_DATE compile';  
   
END Prep_MDD_Objects;
