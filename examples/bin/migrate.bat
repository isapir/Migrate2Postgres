
set M2PG_CONF=MsSqlAWT2Postgres.conf

set M2PG_PROP=
set M2PG_PROP=%M2PG_PROP% -Dconnections.mssql.user=readonly
set M2PG_PROP=%M2PG_PROP% -Dconnections.mssql.password=secret

set M2PG_CLASSPATH="lib/*"

:: get the command from the first arg passed, must be either DDL or DML
set M2PG_COMMAND=%1


java %M2PG_PROP% -cp %M2PG_CLASSPATH% net.twentyonesolutions.m2pg.PgMigrator %M2PG_COMMAND% %M2PG_CONF%
