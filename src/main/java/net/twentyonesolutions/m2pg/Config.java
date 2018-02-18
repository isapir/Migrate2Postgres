package net.twentyonesolutions.m2pg;

import com.google.gson.Gson;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
             "on_error"
            ,"select"
            ,"source_column_quote_prefix"
            ,"source_column_quote_suffix"
            ,"threads"
        };

        for (String k : keys){
            result.put(k, config.getOrDefault(prefix + k, ""));
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


    private static Map<String, Object> flattenAndPopulate(Map<String, Object> config) {

        Map<String, Object> result = Util.flattenKeys(config);

        Map systemProperties = new TreeMap(String.CASE_INSENSITIVE_ORDER);
        systemProperties.putAll(System.getProperties());

        // populate template %v.a.l.ues% with values from JSON path
        Pattern pattern = Pattern.compile("\\%[a-zA-Z_\\.]+\\%");

        for (Map.Entry<String, Object> e : result.entrySet()){

            Object v = e.getValue();

            if (v instanceof String){

                String origValue = (String)v;
                Matcher matcher = pattern.matcher(origValue);
                boolean hasMatches = matcher.find();
                if (hasMatches){

                    String fullPath = e.getKey();
                    String parentPath = fullPath.contains(".") ? fullPath.substring(0, fullPath.lastIndexOf('.')) : "";
                    String localPath = fullPath.contains(".") ? fullPath.substring(fullPath.lastIndexOf('.') + 1) : fullPath;

                    List<int[]> substringPosition = new ArrayList<int[]>();
                    do {
                        substringPosition.add(new int[]{ matcher.start(), matcher.end() });
                        hasMatches = matcher.find();
                    }
                    while (hasMatches);

                    if (!substringPosition.isEmpty()){

                        StringBuilder sb = new StringBuilder(origValue.length() * 2);
                        int pos = 0;

                        for (int[] pair : substringPosition){
                            String substring = origValue.substring(pair[0], pair[1]);
                            String localKey = substring.substring(1, substring.length() - 1);       // remove %-% signs
                            String key = parentPath.isEmpty() ? localKey : parentPath + "." + localKey;

                            String value;   // currently supporting only String replacements

                            // check SystemProperty, full key, local key, default to original value
                            if (systemProperties.containsKey(localKey))
                                value = (String)systemProperties.get(localKey);
                            else if (result.containsKey(key))
                                value = (String)result.get(key);
                            else if (result.containsKey(localKey))
                                value = (String)result.get(localKey);
                            else value = substring;

                            sb.append(origValue.substring(pos, pair[0]));
                            pos = pair[1];
                            sb.append(value);
                        }

                        if (origValue.length() > pos)
                            sb.append(origValue.substring(pos));

                        String populated = sb.toString();

                        // set the populated value to the flat key
                        result.put(fullPath, populated);

                        if (!origValue.equals(populated)){
                            // set the populated value to the hierarchical key if a Map is found in its location and its value is not of type Map
                            String[] pathParts = parentPath.split("\\.");
                            Map m = result;

                            for (String part : pathParts){

                                if (m.get(part) instanceof Map){
                                    m = (Map)m.get(part);
                                }
                                else {
                                    m = null;
                                    break;
                                }
                            }

                            if (m instanceof Map && !(m.get(localPath) instanceof Map))
                                m.put(localPath, populated);
                        }
                    }
                }
            }
        }

        return result;
    }


    public static Config fromFile(String configFile) throws IOException {

        if (configFile == null || configFile.isEmpty())
            configFile = DEFAULT_CONFIG_FILENAME;

        Map conf = readConfigFile(configFile);

        String templateName = (String)conf.getOrDefault("template", "");
        if (!templateName.isEmpty()){
            Map template = readConfigFile(templateName);
            template.putAll(conf);
            conf = flattenAndPopulate(template);    // populate again in case the template contains keys that should be evaluated
        }

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

            String resourcePath = "/templates/" + configFile + ".conf";
            URL resource = PgMigrator.class.getResource(resourcePath);

            if (resource == null)
                throw new IllegalArgumentException(
                        "Config file not found at "
                                + f.getCanonicalPath()
                                + " and not as a resource at " + resourcePath
                );

            System.out.println("Using config file " + resource.getPath());

            is = PgMigrator.class.getResourceAsStream(resourcePath);
        }

        java.util.Scanner scanner = new java.util.Scanner(is).useDelimiter("\\A");
        String jsonString = scanner.hasNext() ? scanner.next().trim() : "";

        if (!jsonString.startsWith("{"))    // json string might close with } even if it's missing the top level braces
            jsonString = "{\n" + jsonString + "\n}";

        Gson gson = new Gson();

        Map conf = gson.fromJson(jsonString, Map.class);

        Map map  = (Map) Util.getJsonElement(conf, "");
        Map flat = flattenAndPopulate(map);

        return flat;
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
