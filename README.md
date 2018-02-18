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
 
