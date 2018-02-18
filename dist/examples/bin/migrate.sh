
M2PG_CONF=MsSqlAWT2Postgres.conf

M2PG_COMMAND=DDL

M2PG_PROP=
M2PG_PROP=%M2PG_PROP% -Dconnections.mssql.user=sa
M2PG_PROP=%M2PG_PROP% -Dconnections.mssql.password=secret

M2PG_CLASSPATH="lib/*"


java $M2PG_PROP -cp $M2PG_CLASSPATH net.twentyonesolutions.m2pg.PgMigrator $M2PG_COMMAND $M2PG_CONF
