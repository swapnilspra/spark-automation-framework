CREATE OR REPLACE FUNCTION hdwsys."tgr_ebp_id_ins$etl_business_process"()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    SELECT
        nextval('hdwsys.seq_ebp_id')
        INTO STRICT new.id;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION hdwsys."tgr_epc_id_ins$etl_process"()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    tmpVar DOUBLE PRECISION;
/*
*****************************************************************************
   NAME:
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        03/06/2006   HELEN SUN       Created this trigger.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:
      Sysdate:         03/06/2006
      Date and Time:   03/06/2006, 01:56:06 PM
      Username:         (set in TOAD Options, Proc Templates)
      Table Name:       ETL_PROCESS (set in the "New PL/SQL Object" dialog)
      Trigger Options:  (set in the "New PL/SQL Object" dialog)
*****************************************************************************
*/
BEGIN
    tmpVar := 0;
    SELECT
        nextval('hdwsys.seq_epc_id')
        INTO STRICT tmpVar;
    new.id := tmpVar;
    new.insert_timestamp := aws_oracle_ext.SYSDATE();
    new.insert_user := SESSION_USER;
    RETURN NEW;
    EXCEPTION
        WHEN others THEN
            RAISE;
            RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION hdwsys."tgr_esc_id_ins$etl_status_code"()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    tmpVar DOUBLE PRECISION;
/*
*****************************************************************************
   NAME:
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        03/06/2006   HELEN SUN       Created this trigger.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:
      Sysdate:         03/06/2006
      Date and Time:   03/06/2006, 01:56:06 PM
      Username:         (set in TOAD Options, Proc Templates)
      Table Name:       ETL_STATUS_CODE (set in the "New PL/SQL Object" dialog)
      Trigger Options:  (set in the "New PL/SQL Object" dialog)
*****************************************************************************
*/
BEGIN
    tmpVar := 0;
    SELECT
        nextval('hdwsys.seq_esc_id')
        INTO STRICT tmpVar;
    new.id := tmpVar;
    new.insert_timestamp := aws_oracle_ext.SYSDATE();
    new.insert_user := SESSION_USER;
    RETURN NEW;
    EXCEPTION
        WHEN others THEN
            RAISE;
            RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION hdwsys."tgr_esh_id_ins$etl_status_history"()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    tmpVar DOUBLE PRECISION;
/*
*****************************************************************************
   NAME:
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        03/06/2006   HELEN SUN       Created this trigger.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:
      Sysdate:         03/06/2006
      Date and Time:   03/06/2006, 01:56:06 PM
      Username:         (set in TOAD Options, Proc Templates)
      Table Name:       ETL_STATUS_HISTORY (set in the "New PL/SQL Object" dialog)
      Trigger Options:  (set in the "New PL/SQL Object" dialog)
*****************************************************************************
*/
BEGIN
    tmpVar := 0;
    SELECT
        nextval('hdwsys.seq_esh_id')
        INTO STRICT tmpVar;
    new.id := tmpVar;
    new.insert_timestamp := aws_oracle_ext.SYSDATE();
    new.insert_user := SESSION_USER;
    RETURN NEW;
    EXCEPTION
        WHEN others THEN
            RAISE;
            RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION hdwsys."tgr_esr_id_ins$etl_source"()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    tmpVar DOUBLE PRECISION;
/*
*****************************************************************************
   NAME:
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        03/06/2006   HELEN SUN       Created this trigger.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:
      Sysdate:         03/06/2006
      Date and Time:   03/06/2006, 01:56:06 PM
      Username:         (set in TOAD Options, Proc Templates)
      Table Name:       ETL_SOURCE (set in the "New PL/SQL Object" dialog)
      Trigger Options:  (set in the "New PL/SQL Object" dialog)
*****************************************************************************
*/
BEGIN
    tmpVar := 0;
    SELECT
        nextval('hdwsys.seq_esr_id')
        INTO STRICT tmpVar;
    new.id := tmpVar;
    new.insert_timestamp := aws_oracle_ext.SYSDATE();
    new.insert_user := SESSION_USER;
    RETURN NEW;
    EXCEPTION
        WHEN others THEN
            RAISE;
            RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION hdwsys."tgr_ets_id_ins$etl_schedule"()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    SELECT
        nextval('hdwsys.seq_ets_id')
        INTO STRICT new.id;
    RETURN NEW;
END;
$function$
;
