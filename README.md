# Migrate2Postgres

This tool allows you to easily migrate databases from other JDBC-compliant DBMSs to Postgres.  The project is written in Java, so it is cross-platform and can run on any operating system that has a Java SE Runtime version 1.8 or later.

Currently the project ships with a [template for SQL Server](src/main/resources/templates/ms-sql-server.conf) as a source, but other source database systems can be added easily by following the same patterns that are documented in the SQL Server template and the [example config files](examples/conf).

# Requirements

 - Java Runtime Environment (JRE) 1.8 or later
 - JDBC Drivers for the DBMSs used

# Getting Started

Create a config file
--
The config file is a JSON file that contains the information needed for the migration.  It can be a standalone file, or inherit from a template by specifying the `template` key where the value is a valid template, e.g. [ms-sql-server](src/main/resources/templates/ms-sql-server.conf).

That information includes the connection details for the source and target databases, mappings of SQL types for the DDL phase (e.g. SQL Server's `NVARCHAR` to Postgres' `TEXT`), mappings of JDBC types for the DML phase, name transformations (e.g. `SomeTableName` to `some_table_name`), queries to run before (e.g. disable triggers) and after (e.g. re-enable triggers or `REFRESH MATERIALIZED VIEWS`) the DML process, number of concurrent threads, and more.

The "effective" configuration values are applied in the following manner:

1) The `defaults` are read from [defaults.conf](src/main/resources/templates/defaults.conf)
2) If the config file has a key named `template`, then the template specified in the value is read
3) The values from the config file are set
4) Values that are wrapped with the `%` symbol are evaluated from other config settings or Java System Properties

Configuration file keys that match the keys in the template files override the template settings, so for example if the config file specifies the key `dml.threads` with a value of `4`, it will overwrite the setting specified in the `defaults` template, which is set to "cores" (cores means the number of CPU cores available to the JVM that runs the tool).

Values that are wrapped with the `%` symbol are treated as variables, and are evaluated at runtime.  The variable values can be set either in the config file by specifying the key path, or as Java System Properties.  So for example, you can specify the value of "AdventureWorks" to the key "source.db_name" in one of two ways:

1) By setting it in the config file as follows:

    `source : {
        db_name : "AdventureWorks"
    }`

2) By setting a Java System Property in the `<options>` via the JVM args, i.e.

    `-Dsource.db_name=AdventureWorks`
    
Then specifying the config value `%source.db_name%` will evaluate to "AdventureWorks" at runtime.  If the the same key is specified both in the config file and in the Java System Properties, the Java System Properties are used.

See the comments in the [defaults.conf](src/main/resources/templates/defaults.conf), [template for SQL Server](src/main/resources/templates/ms-sql-server.conf), and the included [example config files](examples/conf) for more information.

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

You can also pass some configuraion values in the options, which you might not want to keep in the config file, e.g. passwords etc., so for example if you set the following Java System Properties:

    -Dsqlserver.username=pgmigrator -Dsqlserver.password=secret
    
Then you can refer to it in the config file as follows:

    connections : {
        mssql : {
             user     : "%sqlserver.username%"
            ,password : "%sqlserver.password%"
            // rest ommitted for clarity
    }

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

# Config File Reference (WIP)

The Config file is in JSON format and it contains the details of the Migration Project.

At runtime, first the defaults.conf file is read, then if a template is specified in the project's config file its values are applied, and then the settings from the project's config file are applied.  Settings with the same path of keys overwrite previous values of the same path.

As a JSON file, backslashes must be escaped, so if you want to put the string `"a\b"` you must escape the backslash and write it as `"a\\b"`.

Values that are wrapped in `%` symbols are treated as varaibles and evaluated at runtime, so for example if you specify a value of `%sqlserver.password%`, the tool will look for a value with that key either in the JVM System Properties, or the config files, and replace the variable with that value.

```
*
|
+-- name                              string - name of migration project, used as prefix in logs etc.
|
+-- template                          string - a template to be used, e.g. "ms-sql-server"
|
+-- source                            string - the key from connections that will be used as the source connection
|
+-- target                            string - the key from connections that will be used as the target connection
|
+-- connections                       struct - key is the connection name, value is a struct with at least connectionString, user, password
|
+-- information_schema
    |
    +-- query                         string - SQL query that will return all of the tables and columns to be migrated
    |
    +-- database_name                 string - used in the information_schema.query to specify the source database
|
+-- schema_mapping                    struct - maps schema names if needed, e.g. "dbo" -> "public"
|
+-- table_mapping                     struct - maps table names if needed, e.g. "SomeVeryLongTableName" -> "a_table_name"
|
+-- column_mapping                    struct - maps column names if needed, e.g. "group" -> "group_name"
|
+-- table_transform                   string - ([""], "lower_case", "upper_case", "camel_to_snake_case", "dont_change")
|
+-- column_transform                  string - ([""], "lower_case", "upper_case", "camel_to_snake_case", "dont_change")
|
+-- ddl
    |
    +-- drop_schema                   ([false]|true) - whether to add DROP SCHEMA IF EXISTS before each schema
    |
    +-- sql_type_mapping              struct - maps SQL data types, e.g. DATETIME -> TIMESTAMPTZ, IMAGE -> BYTEA, etc.
    |
    +-- column_default_replace        struct - maps DEFAULT column values by using REGular EXpressions
|
+-- dml
    |
    +-- execute
        |
        +-- before_all                array of SQL commands to run before data copy
        |
        +-- after_all                 array of SQL commands to run after data copy
        |
        +-- recomended                ([""], "all") - specifying "all" will execute recommendations
		|
		+-- implicitConversionTypes   array of source column types that should be cast implicitly with postgres (for example "UNIQUEIDENTIFIER")
    |
    +-- threads                       (["cores", integer]) - number of concurrent connections
    |
    +-- on_error                      string - (["rollback"])
    |
    +-- jdbc_type_mapping             struct - maps nonstandard JDBC types during data copy
    |
    +-- source_column_quote_prefix    string - a prefix for quoting columns, e.g. `[` in SQL Server
    |
    +-- source_column_quote_suffix    string - a suffix for quoting columns, e.g. `]` in SQL Server
```

`name`
--
Indicates the name of the migration project.  Output files are prefixed with that name.

