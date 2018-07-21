# Migrate2Postgres

This project allows you to easily migrate databases from other JDBC-compliant DBMSs to Postgres.  The project is written in Java, so it is cross-platform and can run on any operating system that has a Java SE Runtime version 1.8 or later.

Currently the project ships with a [template for SQL Server](src/main/resources/templates/ms-sql-server.conf) as a source, but other source database systems can be added easily by following the same patterns that are documneted in the SQL Server template and the [example config files](examples/conf).


# Usage: 

    java <options> net.twentyonesolutions.m2pg.PgMigrator <command> [<config-file> [<output-file>]]

  `<options>`:
--
The JVM (Java) options, like `classpath` and memory settings if needed

  `<command>`:
--
 - `DDL` - Generate a script that will create the schema objects with the mapped data types, name transformations, identity columns, etc.  You should review the script prior to executing it with your preferred SQL client.
 
 - `DML` - Copy the data from the source database to the target Postgres database in the schema created in the `DDL` step.


# Requirements:

 - Java Runtime Environment (JRE) 1.8 or later
 - JDBC Drivers for the DBMSs used
 
# Watch tutorial video:

[![Migrate a SQL Server Database to Postgres](http://img.youtube.com/vi/5eF9_UB73TI/0.jpg)](http://www.youtube.com/watch?v=5eF9_UB73TI "How to Easily Migrate a SQL Server Database to Postgres")
