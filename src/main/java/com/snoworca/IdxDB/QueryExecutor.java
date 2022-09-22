package com.snoworca.IdxDB;

import com.snoworca.IdxDB.collection.FindOption;
import com.snoworca.IdxDB.collection.IndexCollection;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

public class QueryExecutor {



    protected static JSONObject execute(IdxDB store, JSONObject jsonQuery) {
        String method = jsonQuery.optString("method");
        if(method == null) {
            return makeErrorJSONObject("No 'method' in the query.");
        }
        JSONObject argument = jsonQuery.optJSONObject("argument");
        if(argument == null || argument.isEmpty()) {
            return makeErrorJSONObject("No object of 'argument' in query method '" + method + "'.");
        }
        if("createSet".equalsIgnoreCase(method)) {
            return executeCreateSetMethod(store, argument);
        }
        else if("add".equalsIgnoreCase(method)) {
            return executeAddMethod(store, argument);
        }
        else if("findByIndex".equalsIgnoreCase(method) || "removeByIndex".equalsIgnoreCase(method)) {
            return executeByIndexMethod(store, argument, method);
        }
        else if("addOrReplace".equalsIgnoreCase(method)) {
            return executeAddOrReplaceMethod(store, argument);
        }
        else if("size".equalsIgnoreCase(method)) {
            return executeSizeMethod(store,argument);
        }
        else if("list".equalsIgnoreCase(method)) {
            return executeListMethod(store,argument);
        }
        return null;
    }


    public static  JSONObject executeCreateSetMethod(IdxDB store, JSONObject argument) {
        String name = argument.optString("name");
        int limit = argument.optInt("limit", Short.MAX_VALUE * 2);
        JSONObject index = argument.optJSONObject("index");
        if(name == null || name.isEmpty()) {
            return makeErrorJSONObject("'name' is missing from the query argument.");
        }
        if(index == null || index.isEmpty()) {
            return makeErrorJSONObject("'index' is missing from the query argument.");
        }
        if(store.getSet(name) != null) {
            return makeErrorJSONObject("A set with the name '" + name + "' already exists.");
        }
        IdxDB.IndexTreeBuilder indexTreeBuilder = store.newIndexTreeBuilder(name).memCacheSize(limit);
        String firstIndexKey = index.keys().next();
        int sortMethod = index.optInt(firstIndexKey, 1);
        indexTreeBuilder.index(firstIndexKey, sortMethod);
        indexTreeBuilder.create();
        return new JSONObject().put("isError", false).put("success", true).put("message", "ok");
    }


    public static  JSONObject executeListMethod(IdxDB store, JSONObject argument) {
        String name = argument.optString("name");
        int limit = argument.optInt("limit", Integer.MAX_VALUE);
        boolean revers = argument.optBoolean("revers", false);

        IndexCollection indexCollection = null;
        if(name == null || name.isEmpty()) {
            return makeErrorJSONObject("'name' is missing from the query argument.");
        }
        if( (indexCollection = store.get(name)) == null) {
            return makeErrorJSONObject("Collection '" + name + "' not found.");
        }

        List<JSONObject> jsonObjects = indexCollection.list(limit, revers);
        return new JSONObject().put("isError", false).put("success", true).put("message", "ok").put("data",new JSONArray(jsonObjects));
    }


    public static  JSONObject executeAddMethod(IdxDB store, JSONObject argument) {
        String name = argument.optString("name");
        Object data = argument.opt("data");
        IndexCollection indexCollection = null;
        if(name == null || name.isEmpty()) {
            return makeErrorJSONObject("'name' is missing from the query argument.");
        }
        if( (indexCollection = store.get(name)) == null) {
            return makeErrorJSONObject("Collection '" + name + "' not found.");
        }
        if(data == null) {
            return makeErrorJSONObject("'data' is missing from the query argument.");
        }
        boolean isSuccess = false;
        if(data instanceof JSONObject) {
            if(((JSONObject)data).isEmpty()) {
                return makeErrorJSONObject("'data' is empty.");
            }
            isSuccess = indexCollection.add((JSONObject)data);
        }
        else if(data instanceof JSONArray) {
            if(((JSONArray)data).isEmpty()) {
                return makeErrorJSONObject("'data' is empty.");
            }
            isSuccess = indexCollection.addAll((JSONArray)data);
        }
        return new JSONObject().put("isError", false).put("success", isSuccess).put("message", isSuccess ? "ok" : "fail");
    }

    public static  JSONObject executeSizeMethod(IdxDB store, JSONObject argument) {
        String name = argument.optString("name");
        IndexCollection indexCollection = null;
        if(name == null || name.isEmpty()) {
            return makeErrorJSONObject("'name' is missing from the query argument.");
        }
        if( (indexCollection = store.get(name)) == null) {
            return makeErrorJSONObject("Collection '" + name + "' not found.");
        }
        int size = indexCollection.size();
        return new JSONObject().put("isError", false).put("success", true).put("message", "ok").put("data", size);
    }

    public static  JSONObject executeAddOrReplaceMethod(IdxDB store, JSONObject argument) {
        String name = argument.optString("name");
        Object data = argument.opt("data");
        IndexCollection indexCollection = null;
        if(name == null || name.isEmpty()) {
            return makeErrorJSONObject("'name' is missing from the query argument.");
        }
        if( (indexCollection = store.get(name)) == null) {
            return makeErrorJSONObject("Collection '" + name + "' not found.");
        }
        if(data == null) {
            return makeErrorJSONObject("'data' is missing from the query argument.");
        }
        boolean isSuccess = false;
        if(data instanceof JSONObject) {
            if(((JSONObject)data).isEmpty()) {
                return makeErrorJSONObject("'data' is empty.");
            }
            isSuccess = indexCollection.addOrReplace((JSONObject)data);
        }
        else if(data instanceof JSONArray) {
            if(((JSONArray)data).isEmpty()) {
                return makeErrorJSONObject("'data' is empty.");
            }
            isSuccess = indexCollection.addOrReplaceAll((JSONArray)data);
        }
        return new JSONObject().put("isError", false).put("success", isSuccess).put("message", isSuccess ? "ok" : "fail");
    }





    private static JSONObject toSingleJSONObject(Object obj) {
        if(obj instanceof JSONObject) {
            if(((JSONObject)obj).isEmpty()) return null;
            return (JSONObject)obj;
        }
        if(obj instanceof JSONArray) {
            if(((JSONArray)obj).isEmpty()) return null;
            return ((JSONArray)obj).optJSONObject(0);
        }
        return null;
    }


    public static  JSONObject executeByIndexMethod(IdxDB store, JSONObject argument, String method) {
        String name = argument.optString("name");
        JSONObject where = toSingleJSONObject(argument.opt("where"));
        IndexCollection indexCollection = null;
        if(name == null || name.isEmpty()) {
            return makeErrorJSONObject("'name' is missing from the query argument.");
        }
        if((indexCollection = store.get(name)) == null) {
            return makeErrorJSONObject("Collection '" + name + "' not found.");
        }
        if(where == null || where.isEmpty()) {
            return makeErrorJSONObject("'where' is missing from the query argument.");
        }
        String key = null;
        for(String keyItem : where.keySet()) {
            if(!keyItem.startsWith("$")) {
                key = keyItem;
                break;
            }
        }
        if(key == null) {
            return makeErrorJSONObject("The index key does not exist in the 'where' object.");
        }
        if(!indexCollection.indexKeys().contains(key)) {
            return makeErrorJSONObject("The key '" + key + "' is not an index .");
        }
        Object indexValue = where.opt(key);
        String op = where.optString("$op", "eq");
        int limit = argument.optInt("limit", Integer.MAX_VALUE);
        FindOption findOption = new FindOption();
        findOption.setOp(OP.fromString(op));

        JSONArray data;
        if("findByIndex".equalsIgnoreCase(method)) {
            List<JSONObject> list = indexCollection.findByIndex(indexValue,findOption,limit);
            data = new JSONArray(list);
        } else {
            List<Object> list = indexCollection.removeByIndex(indexValue,findOption);
            data = new JSONArray(list);
        }

        return new JSONObject().put("isError", false).put("success", !data.isEmpty()).put("message","ok").put("data", data);
    }




    public static JSONObject makeErrorJSONObject(String message) {
        return new JSONObject().put("isError", true).put("success", false).put("message", message);
    }


}
