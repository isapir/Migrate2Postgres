# Migrate2Postgres

This project allows you to easily migrate databases from other JDBC-compliant DBMSs to Postgres.  The project is written in Java, so it is cross-platform and can run on any operating system that has a Java SE Runtime version 1.8 or later.

Currently the project ships with a [template for SQL Server](src/main/resources/templates/ms-sql-server.conf) as a source, but other source database systems can be added easily by following the same patterns that are documneted in the SQL Server template and the [example config files](examples/conf).


# Usage: 

    java <options> net.twentyonesolutions.m2pg.PgMigrator <command> [<config-file> [<output-file>]]
    
where `command` can be:

 - `ddl` - to create the schema objects
 - `dml` - to copy the data
            
# Requirements:

 - Java 1.8
 - JDBC Drivers for the DBMSs used
 
# Watch tutorial video:

[![Migrate a SQL Server Database to Postgres](http://img.youtube.com/vi/5eF9_UB73TI/0.jpg)](http://www.youtube.com/watch?v=5eF9_UB73TI "How to Easily Migrate a SQL Server Database to Postgres")
