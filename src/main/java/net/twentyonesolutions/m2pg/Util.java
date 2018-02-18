package net.twentyonesolutions.m2pg;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

public class Util {

    /**
     * returns true if the object is a boolean true or its toString() method starts with 't', 'y', or '1'
     *
     * @param value
     * @return
     */
    public static boolean isSqlTrue(Object value){

        if (value == null)
            return false;

        if (value instanceof Boolean)
            return (Boolean)value;

        String s = value.toString();

        if (s.isEmpty())
            return false;

        char c = s.trim().toLowerCase().charAt(0);

        return (c == 't' || c == 'y' || c == '1');
    }


    public static boolean isEmpty(String value){
        return value == null || value.isEmpty();
    }


    /**
     * converts a string like thisIsString1 to this_is_string_1
     *
     * @param camelCaseString
     * @return
     */
    public static String convertCamelToSnakeCase(String camelCaseString){

        return camelCaseString
                .replace(' ', '_')
                .replaceAll("([^_A-Z0-9])([A-Z0-9])", "$1_$2")
                .toLowerCase();
    }


    public static Map<String, Object> flattenKeys(Map<String, Object> map){

        Map<String, Object> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        return flattenKeys(map, "", result);
    }

    private static Map flattenKeys(Map<String, Object> json, String path, Map result) {

        for (Map.Entry<String, Object> e : json.entrySet()){

            StringJoiner sj = new StringJoiner(".");
            if (!path.isEmpty())
                sj.add(path);
            sj.add(e.getKey());

            String k = sj.toString();
            Object v = e.getValue();

            result.put(k, v);

            if (v instanceof Map){

                result.putAll(flattenKeys((Map<String, Object>)v, k, result));
            }
            else {

//                result.put(k, v);
            }
        }

        return result;
    }


    public static Map<String, ? extends Object> createCaseInsensitiveMap(String key) {
        return new TreeMap(String.CASE_INSENSITIVE_ORDER);
    }

    public static Map<String, ? extends Object> createCaseInsensitiveMap() {
        return createCaseInsensitiveMap(null);
    }

    public static List<Map<String, ? extends Object>> createColumnList(String key) {
        return new ArrayList();
    }

    public static List<Map<String, ? extends Object>> createColumnList() {
        return createColumnList(null);
    }


    /**
     * Returns a JSON sub-element from the given JsonElement and the given path
     *
     * @param json - a Gson JsonElement
     * @param path - a JSON path, e.g. a.b.c[2].d
     * @return
     */
    public static JsonElement getJsonElement(JsonElement json, String path){

        String[] parts = path.split("\\.|\\[|\\]");
        JsonElement result = json;

        for (String key : parts) {

            key = key.trim();
            if (key.isEmpty())
                continue;

            if (result == null){
                result = JsonNull.INSTANCE;
                break;
            }

            if (result.isJsonObject()){
                result = ((JsonObject)result).get(key);
            }
            else if (result.isJsonArray()){
                int ix = Integer.valueOf(key) - 1;
                result = ((JsonArray)result).get(ix);
            }
            else break;
        }

        return result;
    }


    /**
     * returns a Map, List, String, or null
     *
     * might throw an IndexOutOfBoundsException, NumberFormatException
     *
     * @param map
     * @param path
     * @return
     */
    public static Object getJsonElement(Map map, String path){

        String[] parts = path.split("\\.|\\[|\\]");

        int ix;
        List list;
        Map m;
        Object curr;

        if (map instanceof Map && !(map instanceof TreeMap)){
            // convert other types of Map to TreeMap with CASE_INSENSITIVE_ORDER
            m = new TreeMap(String.CASE_INSENSITIVE_ORDER);
            m.putAll(map);
            map = m;
        }

        curr = map;

        for (String key : parts){

            key = key.trim();
            if (key.isEmpty())
                continue;

            if (curr instanceof Map){

                m = (Map)curr;
                curr = m.get(key);
            }
            else if (curr instanceof List){

                list = (List)curr;
                ix = Integer.valueOf(key) - 1;
                curr = list.get(ix);
            }
        }

        return curr;
    }

}
