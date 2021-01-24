
M2PG_CONF=MsSqlAWT2Postgres.conf

M2PG_PROP=
M2PG_PROP=$M2PG_PROP -Dconnections.mssql.user=readonly
M2PG_PROP=$M2PG_PROP -Dconnections.mssql.password=secret

M2PG_CLASSPATH="lib/*"

## get the command from the first arg passed, must be either DDL or DML
M2PG_COMMAND=$1


java $M2PG_PROP -cp $M2PG_CLASSPATH net.twentyonesolutions.m2pg.PgMigrator $M2PG_COMMAND $M2PG_CONF
