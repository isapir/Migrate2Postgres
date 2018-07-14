# Migrate2Postgres
Easily migrate from other DBMSs to Postgres

Usage: 

    java <options> net.twentyonesolutions.m2pg.PgMigrator <command> [<config-file> [<output-file>]]
    
where `command` can be:

 - `ddl` - to create the schema objects
 - `dml` - to copy the data
            
# Requirements:

 - Java 1.8
 - JDBC Drivers for the DBMSs used
 
Watch tutorial video:

[![Migrate a SQL Server Database to Postgres](http://img.youtube.com/vi/5eF9_UB73TI/0.jpg)](http://www.youtube.com/watch?v=5eF9_UB73TI "How to Easily Migrate a SQL Server Database to Postgres")
