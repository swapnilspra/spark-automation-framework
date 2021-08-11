CREATE OR REPLACE procedure ODS.Truncate_Table(p_Table IN Varchar2) 
as
Begin
    execute immediate 'truncate table ' || p_table;
end;
