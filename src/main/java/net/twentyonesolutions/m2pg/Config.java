package net.twentyonesolutions.m2pg;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.UnaryOperator;


public class Config {

    public static final String TABLE_SCHEMA = "TABLE_SCHEMA";
    public static final String TABLE_NAME = "TABLE_NAME";
    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String COLUMN_DEFINITION = "COLUMN_DEFINITION";
    public static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    public static final String IS_COMPUTED = "IS_COMPUTED";
    public static final String IS_IDENTITY = "IS_IDENTITY";
    public static final String IS_NULLABLE = "IS_NULLABLE";
    public static final String DATA_TYPE = "DATA_TYPE";
    public static final String CHARACTER_MAXIMUM_LENGTH = "CHARACTER_MAXIMUM_LENGTH";
    public static final String NUMERIC_PRECISION = "NUMERIC_PRECISION";

    public static final String DEFAULT_CONFIG_FILENAME = "Migrate2Postgres.conf";

    static final UnaryOperator uppercaseValue = o -> o.toString().toUpperCase();

    Map<String, Object> config;
    Map<String, Map> connections;
    Map<String, Object> dml, ddl;
    Map<String, Object> schemaMapping, tableMapping, columnMapping;
    Map<String, String> columnDefaultReplace;
	Map<String, String> postgresTypeCasting;

    String name, source, target;


    public Config(Map<String, Object> config){

        this.config = config;

        this.name   = (String)this.config.getOrDefault("name", "Migrate2Postgres");
        this.source = (String)this.config.get("source");
        this.target = (String)this.config.get("target");

        Map<String, Object> mapSrc;
        mapSrc = (Map)config.getOrDefault("schema_mapping", Collections.EMPTY_MAP);
        this.schemaMapping = getCaseInsensitiveMap(mapSrc);

        mapSrc = (Map)config.getOrDefault("table_mapping", Collections.EMPTY_MAP);      // TODO: implement
        this.tableMapping = getCaseInsensitiveMap(mapSrc);

        mapSrc = (Map)config.getOrDefault("column_mapping", Collections.EMPTY_MAP);
        this.columnMapping = getCaseInsensitiveMap(mapSrc);

        this.parseConnections();
        this.parseDdl();
        this.parseDml();
    }


    private void parseDml(){

        String prefix = "dml.";
        Map<String, Object> mapSrc, result = new TreeMap(String.CASE_INSENSITIVE_ORDER);

        if (!config.containsKey("dml"))
            throw new IllegalArgumentException("DML section is not found in config (did you forget to use a template?)");

        String[] keys = new String[]{
             "execute.after_all"
            ,"execute.before_all"
            ,"execute.recommended"
            ,"on_error"
            ,"select"
            ,"source_column_quote_prefix"
            ,"source_column_quote_suffix"
            ,"threads"
			,"implicit_conversion_types"
        };

        // populate result with config value or default of empty string
        for (String k : keys){
            result.put(k, config.getOrDefault(prefix + k, ""));
        }

        // wrap single item String in List
        for (String k : new String[]{ "execute.before_all", "execute.after_all", "implicit_conversion_types"}){
            Object v = result.get(k);
            if (v instanceof String){
                result.put(k, new ArrayList<String>(){{ add((String)v); }});
            }
        }

        mapSrc = (Map)config.get(prefix + "jdbc_type_mapping");
        result.put("jdbc_type_mapping", getCaseInsensitiveMap(mapSrc, uppercaseValue));
		
        this.dml = result;
    }


    private void parseDdl(){

        String prefix = "ddl.";
        Map<String, Object> mapSrc, result = new TreeMap(String.CASE_INSENSITIVE_ORDER);

        if (!config.containsKey("ddl"))
            throw new IllegalArgumentException("DDL section is not found in config (did you forget to use a template?)");

        String[] keys = new String[]{
             "drop_schema"
        };

        for (String k : keys){
            result.put(k, config.getOrDefault(prefix + k, ""));
        }

        result.put("drop_schema", Util.isSqlTrue(result.get("drop_schema")));       // convert to boolean

        mapSrc = (Map)config.get(prefix + "sql_type_mapping");
        result.put("sql_type_mapping", getCaseInsensitiveMap(mapSrc, uppercaseValue));

        columnDefaultReplace = new LinkedHashMap();
        Map<String, String> inputs = (Map)config.getOrDefault(prefix + "column_default_replace", Collections.EMPTY_MAP);
        for (Map.Entry<String, String> e : inputs.entrySet()){
            columnDefaultReplace.put(e.getKey().trim(), e.getValue().trim());
        }
//        result.put("column_default_replace", columnDefaultReplace);

        this.ddl = result;
    }


    public String translateSchema(String srcSchema){
        return (String) schemaMapping.getOrDefault(srcSchema, srcSchema);
    }


    public String translateTable(String srcTable){
        return (String) tableMapping.getOrDefault(srcTable, srcTable);
    }


    public String translateColumn(String srcColumn){
        return (String) columnMapping.getOrDefault(srcColumn, srcColumn);
    }


    Map getCaseInsensitiveMap(Map<String, Object> mapSrc, UnaryOperator transformValue) {

        Map map = new TreeMap(String.CASE_INSENSITIVE_ORDER);

        if (mapSrc != null){
            for (Map.Entry e : mapSrc.entrySet()) {
                if (transformValue != null)
                    map.put(e.getKey(), transformValue.apply(e.getValue()));
                else
                    map.put(e.getKey(), e.getValue());
            }
        }

        return map;
    }


    Map getCaseInsensitiveMap(Map<String, Object> mapSrc) {
        return getCaseInsensitiveMap(mapSrc, null);
    }


    private void parseConnections() {

        Map<String, Map> result = new TreeMap(String.CASE_INSENSITIVE_ORDER);
        Map<String, Map> map = (Map) config.get("connections");

        for (Map.Entry<String, Map> e : map.entrySet()){

            String name = e.getKey();
            Map<String, String> connProps = (Map)e.getValue();

            Map connection = new TreeMap(String.CASE_INSENSITIVE_ORDER);
            connection.put("connectionString", connProps.get("connectionString"));

            Properties props = new Properties();
            for (Map.Entry<String, String> prop : connProps.entrySet()){

                if (prop.getKey().equals("connectionString"))
                    continue;

                props.put(prop.getKey(), prop.getValue());
            }

            connection.put("properties", props);
            result.put(name, connection);
        }

        this.connections = result;
    }


    public static Config fromFile(String configFile) throws IOException {

        if (configFile == null || configFile.isEmpty())
            configFile = DEFAULT_CONFIG_FILENAME;

        Map conf = new HashMap(64);
        Map confDefaults = readConfigFile("defaults");          // read defaults template
        conf.putAll(Util.flattenKeys(confDefaults));

        Map confFromFile = readConfigFile(configFile);

        String templateName = (String)confFromFile.getOrDefault("template", "");
        if (!templateName.isEmpty()){
            Map confTemplate = readConfigFile(templateName);
            conf.putAll(Util.flattenKeys(confTemplate));                          // add template config

//            confTemplate.putAll(confFromFile);
//            confFromFile = flattenAndPopulate(confTemplate);    // populate again in case the template contains keys that should be evaluated
        }

        conf.putAll(Util.flattenKeys(confFromFile));                              // add file config
        conf = Util.flattenAndPopulate(conf);                        // flatten and populate

        Config result = new Config(conf);

        return result;
    }


    public static Config fromFile() throws IOException {
        return fromFile(null);
    }


    public static Map readConfigFile(String configFile) throws IOException {

        InputStream is = null;
        File f = new File(configFile);

        if (f.exists()){
            System.out.println("Using config file " + f.getCanonicalPath());
            is = new FileInputStream(f);
        }
        else {

            is = getTemplateInputStream(configFile);
        }

        java.util.Scanner scanner = new java.util.Scanner(is).useDelimiter("\\A");
        String jsonString = scanner.hasNext() ? scanner.next().trim() : "";

        Map flat = Util.parseJsonToMap(jsonString);
        return flat;
    }


    private static InputStream getTemplateInputStream(String configFile) throws IOException {
        InputStream is;
        String resourcePath = "/templates/" + configFile + ".conf";
        URL resource = PgMigrator.class.getResource(resourcePath);

        if (resource == null) {

            File f = new File(configFile);
            throw new IllegalArgumentException(
                    "Config file not found at "
                            + f.getCanonicalPath()
                            + " and not as a resource at " + resourcePath
            );
        }

        System.out.println("Using config file " + resource.getPath());

        is = PgMigrator.class.getResourceAsStream(resourcePath);
        return is;
    }


    public Connection connect(String connectionName) throws SQLException {

        Map<String, Object> connInfo = (Map<String, Object>) connections.get(connectionName);

        String connString = (String)connInfo.get("connectionString");
        Properties props = (Properties)connInfo.get("properties");

        Connection result = DriverManager.getConnection(connString, props);

        return result;
    }


    public Map<String, Object> getCopyTask(String name){

        Map<String, Object> result = (Map)this.dml.get(name);

        return result;
    }


    public String getTargetTableName(Table table){

        String transformTable = (String)this.config.getOrDefault("table_transform", "");

        String name = this.translateTable(table.name);
        String tableName = transform(name, transformTable);
        String schemaName = this.translateSchema(table.schemaName);

        String result = schemaName.isEmpty() ? tableName : schemaName + "." + tableName;

        return result;
    }


    public String getTargetColumnName(String colName) {
        String transformation = (String)this.config.getOrDefault("column_transform", "");
        String name = this.translateColumn(colName);
        return this.transform(name, transformation);
    }


    public static String transform(String input, String type){

        if (type != null){

            if (type.equalsIgnoreCase("lower_case"))
                return input.toLowerCase();

            if (type.equalsIgnoreCase("upper_case"))
                return input.toUpperCase();

            if (type.equalsIgnoreCase("camel_to_snake_case"))
                return Util.convertCamelToSnakeCase(input);
			
			if (type.equalsIgnoreCase("dont_change"))
                return Util.convertDontChangeCase(input);
        }

        return input;
    }


/*
    public static String transformTable(Map table, String transformTable){

        return transform((String)table.get(TABLE_NAME), transformTable);
    }


    public static String getTargetColumnName(Map col, String getTargetColumnName){

        return transform((String)col.get(COLUMN_NAME), getTargetColumnName);
    }
//*/

    public String translateType(Column col, Map<String, String> typeMapping){

        if (col.isIdentity){
            if (typeMapping.containsKey(col.type + "_identity")){
                return typeMapping.get(col.type + "_identity").toUpperCase();
            }
        }

        if (col.type.contains("CHAR") && col.maxLength > 0){
            // MAX length on SQL Server is displayed as -1
            if (typeMapping.containsKey(col.type + "_invalid")){
                return typeMapping.get(col.type + "_invalid").toUpperCase();
            }
        }

        String result = typeMapping.getOrDefault(col.type, col.type).toUpperCase();

        return result;
    }


    public String buildColumnDdlLine(Column col){

        String tgtColumn = getTargetColumnName(col.name);

        StringBuilder sb = new StringBuilder(256);

        if (tgtColumn.length() > 20)
            sb.append(tgtColumn);
        else
            sb.append(String.format("%-20s", tgtColumn));

        sb.append('\t');

        Map<String, String> typeMapping = (Map<String, String>)this.ddl.get("sql_type_mapping");
        String targetType = this.translateType(col, typeMapping);

        boolean isBool = targetType.startsWith("BOOL");
        boolean isChar = targetType.contains("CHAR");

        String fullTargetType = targetType;

        if (isChar && col.maxLength > 0){

            fullTargetType += "(" + col.maxLength + ")";
//            sb.append("(").append(col.maxLength).append(")");
        }

        if (fullTargetType.length() > 12)
            sb.append(fullTargetType);
        else
            sb.append(String.format("%-12s", fullTargetType));

        sb.append(!col.isNullable ? "\tNOT NULL" : "");

        if (!col.defaultVal.isEmpty()){
            if (col.isComputed) {

                sb.append("  -- COMPUTED ");
                sb.append(col.defaultVal);
            }
            else {

                String defaultVal = null;
                for (Map.Entry<String, String> replace : columnDefaultReplace.entrySet()){

                    if (col.defaultVal.matches(replace.getKey())){

                        defaultVal = col.defaultVal.replaceFirst(replace.getKey(), replace.getValue());

                        if (isBool){
                            if (defaultVal.equals("0"))
                                defaultVal = "false";
                            else if (defaultVal.equals("1"))
                                defaultVal = "true";
                        }

                        break;
                    }
                }

                if (defaultVal != null){
                    sb.append("  DEFAULT ");
                    sb.append(defaultVal);
                }
                else {
                    sb.append("  -- DEFAULT ");
                    sb.append(col.defaultVal);
                }
            }
        }

        return sb.toString().trim();
    }

}
