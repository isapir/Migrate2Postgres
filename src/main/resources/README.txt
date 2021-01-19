To use:

WARNING:  This software makes changes to the Target database.  It is highly recommended to use a User Account on the Source database that has only READ ONLY permissions, to avoid any configuration errors or typos from causing unwanted changes.  ALWAYS BACK UP YOUR DATABSES BEFORE RUNNING THE MIGRATION TOOL!

1) Create a Migrate2Postgres config file in this directory.  See example in examples/conf.

	a) Optionally create a script file in this directory.  See example in examples/bin.  This is optional as you can enter the whole command in the Shell window, but is recommended for easy reuse and to avoid problems.

2) Run Migrate2Postgres with the DDL command.  That will generate a CREATE objects SQL script that you can then execute in psql or the Postgres client of your choice.  With psql, for example, you would run the command like so (the target database must already exist):

	psql --username=postgres --dbname=AdventureWorks --file=AdventureWorks-ddl-20180218113935.sql

3) After running the CREATE script from the previous step, run Migrate2Postgres with the DML command to copy the data from the source DBMS to your Postgres databse.  The tool will generate a log and some recommended actions which you should review and execute as needed.

Congratulations!  You have just done the work of weeks or months in a matter of minutes.  Try to take the rest of the day off - you deserve it :)
