package com.snoworca.IdxDB.collection;

import org.json.JSONObject;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

class JSONItem implements Comparable<JSONItem> {

    JSONItem(JSONObject jsonObject, String key, int sort) {
        this(null, jsonObject, key, sort);
    }

    JSONItem(FileCacheDelegator fileCacheDelegator, String key, int sort) {
        this(fileCacheDelegator,null, key, sort);
    }


    JSONItem(FileCacheDelegator fileCacheDelegator, JSONObject jsonObject, String key, int sort) {
        this.fileCacheDelegator = fileCacheDelegator;
        this.indexKey = key;
        this.sort = sort;
        this.jsonObject = jsonObject;
        if(this.jsonObject == null && fileCacheDelegator != null) {
            isFileCache = true;
        }
        this.indexValue = jsonObject.opt(indexKey);
    }


    private JSONObject jsonObject;
    private String indexKey = null;
    private Object indexValue;
    private int sort = 0;
    private boolean isFileCache = false;
    private long filePos = -1;
    private FileCacheDelegator fileCacheDelegator;
    private boolean isChanged = false;


    public JSONObject getJsonObject() {
        if(jsonObject == null) {
            byte[] buffer = fileCacheDelegator.load(filePos);
            JSONObject loadJSON = new JSONObject(new String(buffer, StandardCharsets.UTF_8));
            if(!isFileCache) jsonObject = loadJSON;
            else return loadJSON;
        }
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        this.isChanged = true;
    }

    public long getFilePos() {
        return filePos;
    }

    public void storeFileIfNeed() {
        if(filePos > -1) return;
        filePos = fileCacheDelegator.cache(jsonObject.toString().getBytes(StandardCharsets.UTF_8));
    }



    public void setFileStore(boolean fileCache) {
        if(fileCache) {
            if(!isFileCache || isChanged) {
                filePos = fileCacheDelegator.cache(jsonObject.toString().getBytes(StandardCharsets.UTF_8));
            }
            this.jsonObject = null;
        }
        else if(this.jsonObject == null) {
            this.jsonObject = getJsonObject();
        }
        isFileCache = fileCache;
    }

    protected void setFilePos(long pos) {
        this.filePos = pos;
    }


    @Override
    public int compareTo(JSONItem o) {
        Object targetObj = o.getIndexValue();
        Object thisObj = indexValue;
        if(targetObj == null && thisObj != null) {
            return sort;
        } else if(targetObj != null && thisObj == null) {
            return -sort;
        } else if(targetObj == null && thisObj == null) {
            return 0;
        }

        if(targetObj instanceof String && thisObj instanceof String) {
            return ((String)thisObj).compareTo((String)targetObj) * sort;
        }
        if(targetObj instanceof Number && thisObj instanceof  Number) {
            return compareTo((Number)thisObj, (Number)targetObj) * sort;
        }
        if(targetObj instanceof Boolean && thisObj instanceof  Boolean) {
            return thisObj.equals(targetObj) ? 0 :
                   thisObj.equals(Boolean.TRUE) && thisObj.equals(Boolean.FALSE) ? sort : -1 * sort;
        }
        return 0;
    }

    public Object getIndexValue() {
        return this.indexValue;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) return true;
        else if(obj == null) return false;
        if(obj instanceof JSONItem && compareTo((JSONItem)obj) == 0) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        Object obj = indexValue;
        if(obj == null) return 0;
        return obj.hashCode();
    }




    @Override
    public String toString() {
        return jsonObject == null ? "" : jsonObject.toString();
    }

    private static int compareTo(Number n1, Number n2) {
        // ignoring null handling
        BigDecimal b1 = BigDecimal.valueOf(n1.doubleValue());
        BigDecimal b2 = BigDecimal.valueOf(n2.doubleValue());
        return b1.compareTo(b2);
    }


}
