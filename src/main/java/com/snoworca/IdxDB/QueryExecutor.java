package com.snoworca.IdxDB;

import com.snoworca.IdxDB.collection.FindOption;
import com.snoworca.IdxDB.collection.IndexCollection;
import com.snoworca.IdxDB.collection.IndexSetBuilder;
import com.snoworca.IdxDB.collection.IndexMapBuilder;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class QueryExecutor {


    protected static String execute(IdxDB idxDB, String query) {
        query = query.trim();
        if(query.indexOf('[') == 0) {
            return execute(idxDB, new CSONArray(query)).toString();
        }
        return execute(idxDB, new CSONObject(query)).toString();
    }

    protected static CSONArray execute(IdxDB idxDB, CSONArray csonArray) {
        CSONArray result = new CSONArray();
        for(int i = 0; i < csonArray.size(); i++) {
            result.add(execute(idxDB, csonArray.getObject(i)));
        }
        return result;
    }

    protected static CSONObject execute(IdxDB idxDB, CSONObject jsonQuery) {
        String method = jsonQuery.optString("method");
        if (method == null) {
            return makeErrorCSONObject("No 'method' in the query.");
        }
        CSONObject argument = jsonQuery.optObject("argument");
        if (argument == null || argument.isEmpty()) {
            return makeErrorCSONObject("No object of 'argument' in query method '" + method + "'.");
        }
        try {
            if ("newIndexTreeSet".equalsIgnoreCase(method) || "newIndexLinkedMap".equalsIgnoreCase(method)) {
                return executeNewCollectionMethod(idxDB,method, argument);
            }
            else if ("dropCollection".equalsIgnoreCase(method)) {
                return executeDropCollectionMethod(idxDB, argument);
            } else if ("add".equalsIgnoreCase(method)) {
                return executeAddMethod(idxDB, argument);
            } else if ("findByIndex".equalsIgnoreCase(method) || "removeByIndex".equalsIgnoreCase(method)) {
                return executeByIndexMethod(idxDB, argument, method);
            } else if ("addOrReplace".equalsIgnoreCase(method)) {
                return executeAddOrReplaceMethod(idxDB, argument);
            } else if ("size".equalsIgnoreCase(method)) {
                return executeSizeMethod(idxDB, argument);
            } else if ("list".equalsIgnoreCase(method)) {
                return executeListMethod(idxDB, argument);
            }
        } catch (Throwable e) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(baos);
            e.printStackTrace(printStream);
            printStream.flush();
            printStream.close();
            return new CSONObject().put("isError", true).put("success", false).put("message", "Internal error: " +  new String(baos.toByteArray(), StandardCharsets.UTF_8));
        }

        return new CSONObject().put("isError", true).put("success", false).put("message", "Method '" + method + "' is undefined.");
    }


    public static  CSONObject executeNewCollectionMethod(IdxDB store,String method, CSONObject argument) {
        String collection = argument.optString("collection");
        CSONObject index = argument.optObject("index");
        if(collection == null || collection.isEmpty()) {
            return makeErrorCSONObject("'collection' is missing from the query argument.");
        }
        if(index == null || index.isEmpty()) {
            return makeErrorCSONObject("'index' is missing from the query argument.");
        }
        if(store.get(collection) != null) {
            return makeErrorCSONObject("A set with the collection '" + collection + "' already exists.");
        }
        String firstIndexKey = index.keySet().iterator().next();
        int sortMethod = index.optInteger(firstIndexKey, 1);
        int memCacheSize = index.optInteger("memCacheSize", 1000);
        boolean isFileStore = index.optBoolean("fileStore", true);
        boolean accessOrder = index.optBoolean("accessOrder", false);
        if("newIndexTreeSet".equalsIgnoreCase(method)) {
            IndexSetBuilder indexSetBuilder = store. newIndexTreeSetBuilder(collection);
            indexSetBuilder.index(firstIndexKey, sortMethod);
            indexSetBuilder.memCacheSize(memCacheSize);
            indexSetBuilder.setFileStore(isFileStore);
            indexSetBuilder.create();
        } else if("newIndexLinkedMap".equalsIgnoreCase(method)) {
            IndexMapBuilder indexMapBuilder = store.newIndexMapBuilder(collection);
            indexMapBuilder.index(firstIndexKey, sortMethod);
            indexMapBuilder.memCacheSize(memCacheSize);
            indexMapBuilder.setAccessOrder(accessOrder);
            indexMapBuilder.create();
        }
        return new CSONObject().put("isError", false).put("success", true).put("message", "ok");
    }

    public static  CSONObject executeDropCollectionMethod(IdxDB store, CSONObject argument) {
        String collection = argument.optString("collection");
        if(collection == null || collection.isEmpty()) {
            return makeErrorCSONObject("'collection' is missing from the query argument.");
        }
        if(store.get(collection) != null) {
            return makeErrorCSONObject("A set with the collection '" + collection + "' already exists.");
        }
        return new CSONObject().put("isError", false).put("success", store.dropCollection(collection)).put("message", "ok");
    }


    public static  CSONObject executeListMethod(IdxDB store, CSONObject argument) {
        String collection = argument.optString("collection");
        int limit = argument.optInteger("limit", Integer.MAX_VALUE);
        boolean revers = argument.optBoolean("revers", false);

        IndexCollection indexCollection = null;
        if(collection == null || collection.isEmpty()) {
            return makeErrorCSONObject("'collection' is missing from the query argument.");
        }
        if( (indexCollection = store.get(collection)) == null) {
            return makeErrorCSONObject("Collection '" + collection + "' not found.");
        }
        List<CSONObject> jsonObjects = indexCollection.list(limit, revers);

        return new CSONObject().put("isError", false).put("success", true).put("message", "ok").put("data",new CSONArray(jsonObjects));
    }


    public static CSONObject executeAddMethod(IdxDB store, CSONObject argument) {
        String collection = argument.optString("collection");
        Object data = argument.opt("data");
        IndexCollection indexCollection = null;
        if(collection == null || collection.isEmpty()) {
            return makeErrorCSONObject("'collection' is missing from the query argument.");
        }
        if( (indexCollection = store.get(collection)) == null) {
            return makeErrorCSONObject("Collection '" + collection + "' not found.");
        }
        if(data == null) {
            return makeErrorCSONObject("'data' is missing from the query argument.");
        }
        boolean isSuccess = false;
        if(data instanceof CSONObject) {
            if(((CSONObject)data).isEmpty()) {
                return makeErrorCSONObject("'data' is empty.");
            }
            isSuccess = indexCollection.add((CSONObject)data);
        }
        else if(data instanceof CSONArray) {
            if(((CSONArray)data).isEmpty()) {
                return makeErrorCSONObject("'data' is empty.");
            }
            isSuccess = indexCollection.addAll((CSONArray) data);
        }
        CSONObject result = indexCollection.commit().toCsonObject();
        return new CSONObject().put("isError", false).put("success", isSuccess).put("message", isSuccess ? "ok" : "fail").put("result", result);
    }

    public static  CSONObject executeSizeMethod(IdxDB store, CSONObject argument) {
        String collection = argument.optString("collection");
        IndexCollection indexCollection = null;
        if(collection == null || collection.isEmpty()) {
            return makeErrorCSONObject("'collection' is missing from the query argument.");
        }
        if( (indexCollection = store.get(collection)) == null) {
            return makeErrorCSONObject("Collection '" + collection + "' not found.");
        }
        int size = indexCollection.size();
        return new CSONObject().put("isError", false).put("success", true).put("message", "ok").put("data", size);
    }

    public static  CSONObject executeAddOrReplaceMethod(IdxDB store, CSONObject argument) {
        String collection = argument.optString("collection");
        Object data = argument.opt("data");
        IndexCollection indexCollection = null;
        if(collection == null || collection.isEmpty()) {
            return makeErrorCSONObject("'collection' is missing from the query argument.");
        }
        if( (indexCollection = store.get(collection)) == null) {
            return makeErrorCSONObject("Collection '" + collection + "' not found.");
        }
        if(data == null) {
            return makeErrorCSONObject("'data' is missing from the query argument.");
        }
        boolean isSuccess = false;
        if(data instanceof CSONObject) {
            if(((CSONObject)data).isEmpty()) {
                return makeErrorCSONObject("'data' is empty.");
            }
            isSuccess = indexCollection.addOrReplace((CSONObject)data);
        }
        else if(data instanceof CSONArray) {
            if(((CSONArray)data).isEmpty()) {
                return makeErrorCSONObject("'data' is empty.");
            }
            isSuccess = indexCollection.addOrReplaceAll((CSONArray) data);
        }
        CSONObject result = indexCollection.commit().toCsonObject();
        return new CSONObject().put("isError", false).put("success", isSuccess).put("message", isSuccess ? "ok" : "fail").put("result", result);
    }





    private static CSONObject toSingleCSONObject(Object obj) {
        if(obj instanceof CSONObject) {
            if(((CSONObject)obj).isEmpty()) return null;
            return (CSONObject)obj;
        }
        if(obj instanceof CSONArray) {
            if(((CSONArray)obj).isEmpty()) return null;
            return ((CSONArray)obj).optObject(0);
        }
        return null;
    }


    public static  CSONObject executeByIndexMethod(IdxDB store, CSONObject argument, String method) {
        String collection = argument.optString("collection");
        CSONObject where = toSingleCSONObject(argument.opt("where"));
        IndexCollection indexCollection = null;
        if(collection == null || collection.isEmpty()) {
            return makeErrorCSONObject("'collection' is missing from the query argument.");
        }
        if((indexCollection = store.get(collection)) == null) {
            return makeErrorCSONObject("Collection '" + collection + "' not found.");
        }
        if(where == null || where.isEmpty()) {
            return makeErrorCSONObject("'where' is missing from the query argument.");
        }
        String key = null;
        for(String keyItem : where.keySet()) {
            if(!keyItem.startsWith("$")) {
                key = keyItem;
                break;
            }
        }
        if(key == null) {
            return makeErrorCSONObject("The index key does not exist in the 'where' object.");
        }
        if(!indexCollection.indexKeys().contains(key)) {
            return makeErrorCSONObject("The key '" + key + "' is not an index .");
        }
        Object indexValue = where.opt(key);
        String op = where.optString("$op", "eq");
        int limit = argument.optInteger("limit", Integer.MAX_VALUE);
        FindOption findOption = new FindOption();
        findOption.setOp(OP.fromString(op));

        CSONArray data;
        if("findByIndex".equalsIgnoreCase(method)) {
            List<CSONObject> list = indexCollection.findByIndex(indexValue,findOption,limit);
            data = new CSONArray(list);
            return new CSONObject().put("isError", false).put("success","ok").put("data", data);
        } else {
            indexCollection.removeByIndex(indexValue,findOption);
            CSONObject result = indexCollection.commit().toCsonObject();
            return new CSONObject().put("isError", false).put("success",true).put("message", "ok").put("result", result);
        }


    }




    public static CSONObject makeErrorCSONObject(String message) {
        return new CSONObject().put("isError", true).put("success", false).put("message", message);
    }


}
