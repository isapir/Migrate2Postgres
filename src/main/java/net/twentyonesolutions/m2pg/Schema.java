package net.twentyonesolutions.m2pg;

import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static net.twentyonesolutions.m2pg.Config.CHARACTER_MAXIMUM_LENGTH;
import static net.twentyonesolutions.m2pg.Config.COLUMN_DEFAULT;
import static net.twentyonesolutions.m2pg.Config.COLUMN_DEFINITION;
import static net.twentyonesolutions.m2pg.Config.COLUMN_NAME;
import static net.twentyonesolutions.m2pg.Config.DATA_TYPE;
import static net.twentyonesolutions.m2pg.Config.IS_COMPUTED;
import static net.twentyonesolutions.m2pg.Config.IS_IDENTITY;
import static net.twentyonesolutions.m2pg.Config.IS_NULLABLE;
import static net.twentyonesolutions.m2pg.Config.NUMERIC_PRECISION;
import static net.twentyonesolutions.m2pg.Config.ORDINAL_POSITION;
import static net.twentyonesolutions.m2pg.Config.TABLE_NAME;
import static net.twentyonesolutions.m2pg.Config.TABLE_SCHEMA;

public class Schema {

    Config config;
    Map<String, Table> schema;


    public Schema(Config config) throws Exception {

        this.config = config;

        String informationSchemaSql = (String) config.config.get("information_schema.query");
        if (informationSchemaSql.isEmpty()){
            throw new IllegalArgumentException("information_schema.query is missing");
        }

        Connection conSrc = config.connect(config.source);
        Statement statement = conSrc.createStatement();
        ResultSet resultSet = statement.executeQuery(informationSchemaSql);

        if (!resultSet.isBeforeFirst()){
            System.out.println("information_schema.query returned no results: \n" + informationSchemaSql);
            throw new IllegalArgumentException("information_schema.query returned no results");
        }

        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        int[] columnTypes = new int[columnCount];
        for (int i=1; i <= columnCount; i++){
            columnTypes[i - 1] = metaData.getColumnType(i);
        }

        schema = new TreeMap(String.CASE_INSENSITIVE_ORDER);

        while (resultSet.next()){

            String schemaName = resultSet.getString(TABLE_SCHEMA);
            String tableName  = resultSet.getString(TABLE_NAME);
            String fullTableName = schemaName + "." + tableName;

            Table table = schema.computeIfAbsent(fullTableName, Table::new);

            String columnName = resultSet.getString(COLUMN_NAME);
            boolean isIdentity = Util.isSqlTrue(resultSet.getString(IS_IDENTITY));
            boolean isComputed = Util.isSqlTrue(resultSet.getString(IS_COMPUTED));
            String definition = isComputed ? resultSet.getString(COLUMN_DEFINITION) : resultSet.getString(COLUMN_DEFAULT);

            Column col = new Column(
                 columnName
                ,resultSet.getString(DATA_TYPE)
                ,resultSet.getInt(ORDINAL_POSITION)
                ,Util.isSqlTrue(resultSet.getObject(IS_NULLABLE))
                ,resultSet.getInt(CHARACTER_MAXIMUM_LENGTH)
                ,resultSet.getInt(NUMERIC_PRECISION)
                ,isIdentity
                ,isComputed
                ,definition    // in MSSQL user must have GRANT VIEW DEFINITION ON database to see default values
            );

            table.columns.add(col);

            if (isIdentity)
                table.setIdentity(col);
        }
    }


    public String copyTable(String tableName, IProgress progress) throws IOException {

        StringBuilder log = new StringBuilder(1024);

        String qSelect, qInsert;
        int copied = 0;
        long rowCount = 0;

        Config config = this.config;
        Table table = this.getTable(tableName);

        String tgtSchema = config.translateSchema(table.schemaName);
//        String tgtTable = tgtSchema + "." + table.name;
        String tgtTable = config.getTargetTableName(table);

        log.append(String.format("/** copy table %s to %s */\n", tableName, tgtTable));

        long tc = System.currentTimeMillis();

        try {

            Connection conSrc = config.connect(config.source);
            Connection conTgt = config.connect(config.target);

            Statement statTgt = conTgt.createStatement();
            statTgt.execute("BEGIN TRANSACTION;");
            statTgt.execute("TRUNCATE TABLE " + tgtTable + ";");

            qSelect = "SELECT COUNT(*) AS row_count" + "\nFROM " + table.toString();
            Statement statSrc = conSrc.createStatement();
            ResultSet rs;
            rs = statSrc.executeQuery(qSelect);

            long longValue = 0;
            if (rs.next()) {
                rowCount = rs.getLong("row_count");
                log.append(String.format(" /* %,d rows */\n", rowCount));
            } else {
                throw new RuntimeException("No results found for " + qSelect);
            }

            qSelect = "SELECT " + table.getColumnListSrc(config) + "\nFROM " + table.toString();

            qInsert = "INSERT INTO " + tgtTable + " (" + table.getColumnListTgt(config) + ")";

            // add VALUES( ?, ... ) to INSERT query
            int insertColumnCount = qInsert.substring(qInsert.indexOf('(')).split(",").length;
            qInsert += "\nVALUES(" + String.join(", ", Collections.nCopies(insertColumnCount, "?")) + ")";

            statSrc = conSrc.createStatement();

            PreparedStatement statInsert = conTgt.prepareStatement(qInsert);

            statSrc.setFetchSize(1000);

            rs = statSrc.executeQuery(qSelect);

            ResultSetMetaData rsMetaData = rs.getMetaData();

            Map<String, String> jdbcTypeMapping = (Map<String, String>) config.dml.get("jdbc_type_mapping");

            int columnCount = rsMetaData.getColumnCount();

            int[] columnTypes = new int[columnCount];
            //        SQLType[] sqlTypes = new SQLType[columnCount];  // TODO: use this instead of columnTypes when pgjdbc will support setObject with SQLType

            for (int i = 1; i <= columnCount; i++) {

                int srcType = rsMetaData.getColumnType(i);
                int tgtType = srcType;

                // translate unsupported types, e.g. nvarchar to varchar, dml jdbcTypeMapping is based on JDBC types, while ddl jdbcTypeMapping is based on SQL types
                //            if (jdbcTypeMapping.containsKey(String.valueOf(srcType)))
                //                tgtType = Integer.parseInt(jdbcTypeMapping.getOrDefault(String.valueOf(srcType), String.valueOf(tgtType)));

                String srcTypeName = JDBCType.valueOf(srcType).getName();   // WARN: rsMetaData.getColumnTypeName(i) returns the vendor's name instead of JDBC name, e.g. ntext instead of longnvarchar for MSSQL
                if (jdbcTypeMapping.containsKey(srcTypeName)) {
                    String tgtTypeName = jdbcTypeMapping.get(srcTypeName);
                    tgtType = JDBCType.valueOf(tgtTypeName).getVendorTypeNumber();
                }

                columnTypes[i - 1] = tgtType;
            }

            boolean hasErrors = false;
            int row = 0;
            while (rs.next()) {

                row++;

                Object[] values = new Object[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    values[i - 1] = value;
                    statInsert.setObject(i, value, columnTypes[i - 1]);
                    //                statInsert.setObject(i, value, sqlTypes[i - 1]);    // throws java.sql.SQLFeatureNotSupportedException: Method org.postgresql.jdbc.PgPreparedStatement.setObject is not yet implemented.
                }

                try {

                    int executeResult = statInsert.executeUpdate();
                    copied += executeResult;
                }
                catch (SQLException ex) {

                    String failedSql = statInsert.toString().replace("\n", "\n\t") + ";";

                    System.err.println("\n\nInsert Failed. " + ex.toString());
//                    System.err.println("\n\tValues: " + Arrays.toString(values));
                    System.err.println("\n\t" + failedSql);

                    log.append("\n/** Error: Insert Failed. ")
//                            .append(tgtTable)
                            .append(ex.toString())
                            .append("\n\t")
                            .append(failedSql)
                            .append("\n");

                    hasErrors = true;
                    break;  // exit the loop and stop processing. TODO: modify for other exception handling strategies
//                    throw ex;
                }

                if (progress != null)
                    progress.progress(new IProgress.Status(tableName, row, rowCount));

                //            System.out.printf("\r%tT %,d/%,d %.2f%% %8s %s", System.currentTimeMillis(), row, rowCount, 100.0 * row / rowCount, executeResult, table.toString());
            }

            if (rowCount == 0){
                if (progress != null)   // report progress in case the table was empty
                    progress.progress(new IProgress.Status(tableName, 0, 0));
            }
            else {
                if (table.hasIdentity()) {

                    Column identity = table.getIdentity();
                    qSelect = "SELECT MAX(" + identity.name + ") AS max_value" + "\nFROM " + table.toString();
                    rs = statSrc.executeQuery(qSelect);
                    if (rs.next()) {

                        longValue = rs.getLong("max_value");

                        double recommendFactor = 1_000.0;

                        if (longValue > 1_000_000)
                            recommendFactor = 10_000.0;

                        long recommendValue = (long) (Math.ceil((longValue + recommendFactor) / recommendFactor) * recommendFactor);

                        String sqlRecommended = "ALTER TABLE " +
                                tgtTable +
                                " ALTER COLUMN " +
                                config.getTargetColumnName(identity.name) +
                                " RESTART WITH " +
                                recommendValue +
                                ";";

                        log.append(" -- Identity column ")
                                .append(identity.name)
                                .append(" has max value of ")
                                .append(longValue)
                                .append(". ");

                        if (config.dml.get("execute.recommended").toString().toLowerCase().equals("all")){
                            Util.executeQueries(Arrays.asList(sqlRecommended), log, conTgt);
                        }
                        else {
                            log.append("Recommended:\n\t")
                                    .append(sqlRecommended)
                                    .append("\n");
                        }
                    } else {
                        throw new RuntimeException("No results found for " + qSelect);
                    }
                }
            }

            if (hasErrors) {

                if (config.dml.getOrDefault("on_error", "rollback").equals("rollback")) {
                    log.append("  rolling back transaction **/\n");
                    statTgt.execute("ROLLBACK;");

                    if (progress != null)
                        progress.progress(new IProgress.Status(tableName, 0, 0));
                }
            } else {

                statTgt.execute("COMMIT;");
            }

            statSrc.cancel();  // if the statement did not complete then we should cancel it or else we have to wait for a timeout
            statSrc.close();
            rs.close();

            conSrc.close();
            conTgt.close();
        }
        catch (SQLException ex){
            ex.printStackTrace();
        }

        tc = System.currentTimeMillis() - tc;

        if (rowCount > 0)
            log.append(String.format(" /* copied %,d / %,d records in %.3f seconds **/\n", copied, rowCount, tc / 1000.0));

        return log.toString();
    }


    public Collection<Table> getTables(){

        return schema.values();
    }


    public Table getTable(String fullTableName){

        return schema.get(fullTableName);
    }


    public String generateDdl() {

        boolean doTransaction = true;
        boolean doDropSchema = (boolean) config.ddl.get("drop_schema");

        StringBuilder sb = new StringBuilder(4096);

        sb.append(PgMigrator.getBanner());

        if (doTransaction){
            sb.append("BEGIN TRANSACTION;\n\n");
        }

        Map<String, String> schemaMapping = (Map<String, String>)config.config.getOrDefault("schema_mapping", Collections.EMPTY_MAP);

        List<String> distinctSchemas = schema
                .values()
                .stream()
                .map(t -> schemaMapping.getOrDefault(t.schemaName, t.schemaName).toLowerCase())
                .distinct()
                .collect(Collectors.toList());

//        distinctSchemas = ((Map<String, String>)config.config.get("schema_mapping")).values()
//                .stream()
//                .distinct()
//                .collect(Collectors.toList());

        for (String tgtSchema : distinctSchemas){

            if (doDropSchema){
                sb.append("DROP SCHEMA IF EXISTS " + tgtSchema + " CASCADE;\n");
            }

            sb.append("CREATE SCHEMA IF NOT EXISTS " + tgtSchema + ";\n\n");
        }
        sb.append("\n");

        for (Table table : schema.values()){

            if (!doDropSchema){
                // script DROP TABLE if we do not drop the schema
                sb.append("DROP TABLE IF EXISTS " + config.getTargetTableName(table) + ";\n");
            }

            sb.append(
                table.getDdl(config)
            )
                .append("\n\n");
        }

//        sb.append("\n\n")
//            .append(config.ddl.get("after_all"));

        if (doTransaction){
            sb.append("\n-- ROLLBACK;\n");
            sb.append("\nCOMMIT;\n");
        }

        return sb.toString();
    }

}
