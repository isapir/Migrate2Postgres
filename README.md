# Migrate2Postgres

This project allows you to easily migrate databases from other JDBC-compliant DBMSs to Postgres.  The project is written in Java, so it is cross-platform and can run on any operating system that has a Java SE Runtime version 1.8 or later.

Currently the project ships with a [template for SQL Server](src/main/resources/templates/ms-sql-server.conf) as a source, but other source database systems can be added easily by following the same patterns that are documented in the SQL Server template and the [example config files](examples/conf).

# Requirements

 - Java Runtime Environment (JRE) 1.8 or later
 - JDBC Drivers for the DBMSs used

# Getting Started

Create a config file
--
The config file is a JSON file that includes all of the information needed for the migration.  

That information includes the connection details for the source and target databases, mappings of SQL types for the DDL phase (e.g. SQL Server's `NVARCHAR` to Postgres' `TEXT`), mappings of JDBC types for the DML phase, name transformations (e.g. `SomeTableName` to `some_table_name`), queries to run before (e.g. disable triggers) and after (e.g. re-enable triggers or `REFRESH MATERIALIZED VIEWS`) the DML process, number of concurrent threads, and more. 

See the comments in the [template for SQL Server](src/main/resources/templates/ms-sql-server.conf) and the included [example config files](examples/conf) for more information.

Run the DDL command
--
This will generate a SQL script with commands for `CREATE SCHEMA`, `CREATE TABLE`, etc.

Execute the generated DDL script
--
Review the script generated in the previous step and make changes if needed, then execute it in your favorite SQL client, e.g. psql, PgAdmin, or DBeaver.

Run the DML command
--
This will copy the data from the source database to your target Postgres database according to the settings in the config file.

Take a vacation
--
You probably just crammed weeks of work into a few hours.  I think that you deserve a vacation!

# Watch tutorial video

[![Migrate a SQL Server Database to Postgres](http://img.youtube.com/vi/5eF9_UB73TI/0.jpg)](http://www.youtube.com/watch?v=5eF9_UB73TI "How to Easily Migrate a SQL Server Database to Postgres")

# Usage: 

    java <options> net.twentyonesolutions.m2pg.PgMigrator <command> [<config-file> [<output-file>]]

  `<options>`
--
The JVM (Java) options, like `classpath` and memory settings if needed.

You can also pass some configuraion values in the options, which you might not want to keep in the config file, e.g. passwords etc.

  `<command>`
--
 - `DDL` - Generate a script that will create the schema objects with the mapped data types, name transformations, identity columns, etc.  You should review the script prior to executing it with your preferred SQL client.
 
 - `DML` - Copy the data from the source database to the target Postgres database in the schema created in the `DDL` step.

  `<config-file>`
--
Optional path to the config file. Defaults to `./Migrate2Postgres.conf`.

  `<output-file>`
--
Optional path of the output/log file. Defaults to current directory with the project name and timestamp. The arguments are passed by position, so `<output-file>` can only be passed if `<config-file>` was passed explicitly.

See also the [shell/batch example scripts](examples/bin)
