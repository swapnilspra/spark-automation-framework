-- script to create the roles in datamart db.
CREATE ROLE writer NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN;
create role reader nosuperuser nocreatedb nocreaterole noinherit nologin;
create role reporter nosuperuser nocreatedb nocreaterole noinherit nologin;