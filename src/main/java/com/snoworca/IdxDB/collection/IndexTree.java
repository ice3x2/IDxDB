package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CompareUtil;
import com.snoworca.IdxDB.OP;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

//TODO 데이터 넣을 때
public class IndexTree implements IndexCollection{

    private final int memCacheLimit;
    private String indexKey = "";
    private TreeSet<String> indexKeySet;
    private int indexSort = 0;
    private TreeSet<JSONItem> itemSet = new TreeSet<>();
    //TODO collection commands list 추가해야한다.
    

    private FileCacheDelegator fileCacheDelegator;



    IndexTree(FileCacheDelegator fileCacheDelegator, Integer memCacheLimit, String indexKey, Integer indexSort) {
        this.indexKey = indexKey;
        this.indexSort = indexSort;
        this.memCacheLimit = memCacheLimit;
        this.fileCacheDelegator = fileCacheDelegator;
    }

    @Override
    public int size() {
        return itemSet.size();
    }

    @Override
    public synchronized void commit() {
        if(fileCacheDelegator == null) return;
        int count = 0;

        for (JSONItem jsonItem : itemSet) {
            jsonItem.storeFileIfNeed();
            jsonItem.setFileStore(count < memCacheLimit);
            ++count;
        }
        //TODO 1. commit 실행 순간 tree 를 모두 순회하면서 mem cache limit 보다 크면 파일로 저장. 아니면 메모리로 캐쉬해 놓음.
        //TODO 2. 명령을 순차적으로 저장함.

    }

    @Override
    public boolean isEmpty() {
        return itemSet.isEmpty();
    }


    @Override
    public Set<String> indexKeys() {
        if(indexKeySet != null) return indexKeySet;
        TreeSet<String> set = new TreeSet<>();
        set.add(indexKey);
        indexKeySet = set;
        return set;
    }


    private JSONItem getByIndexValue(Object value) {
        return get(new JSONObject().put(indexKey, value));
    }

    private JSONItem get(JSONObject jsonObject) {
        JSONItem item = new JSONItem(jsonObject,indexKey, indexSort);
        JSONItem foundItem = itemSet.floor(item);
        if(foundItem == null || !CompareUtil.compare(foundItem.getIndexValue(), jsonObject.opt(indexKey), OP.eq)) {
            return null;
        }
        return foundItem;

    }

    @Override
    public boolean addOrReplace(JSONObject jsonObject) {
        if(jsonObject == null || jsonObject.opt(indexKey) == null) return false;
        JSONItem foundItem = get(jsonObject);
        if(foundItem == null) {
            return add(jsonObject);
        }
        foundItem.setJsonObject(jsonObject);
        return true;
    }

    @Override
    public boolean addOrReplaceAll(JSONArray jsonArray) {
        boolean isSuccess = true;
        for(int i = 0, n = jsonArray.length(); i < n; ++i) {
            JSONObject jsonObject = jsonArray.optJSONObject(i);
            if(jsonObject == null || jsonObject.opt(indexKey) == null) return false;
        }
        for(int i = 0, n = jsonArray.length(); i < n; ++i) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            isSuccess = isSuccess & addOrReplace(jsonObject);
        }
        return isSuccess;
    }

    @Override
    public boolean add(JSONObject jsonObject) {
        JSONItem item = new JSONItem(jsonObject, indexKey, indexSort);
        boolean success = itemSet.add(item);
        if(fileCacheDelegator == null && itemSet.size() > memCacheLimit) {
            itemSet.pollLast();
        }
        return success;
    }

    @Override
    public boolean addAll(JSONArray jsonArray) {
        ArrayList<JSONObject> jsonObjects = new ArrayList<>();
        for(int i = 0, n = jsonArray.length(); i < n; ++i) {
            JSONObject jsonObject = jsonArray.optJSONObject(i);
            if(jsonObject == null) return false;
            jsonObjects.add(jsonObject);
        }
        return addAll(jsonObjects);

    }

    @Override
    public List<JSONObject> findByIndex(Object indexValue, FindOption option, int limit) {
        OP op = option.getOp();
        if(op == null) op = OP.eq;
        ArrayList<JSONObject> result = new ArrayList<>();
        if(limit < 1) return result;
        switch (op) {
            case eq:
                JSONObject jsonObject = findByIndex(indexValue);
                if(jsonObject != null) result.add(jsonObject);
                break;
            case gte:
            case gt:
                if(indexSort > 0) {
                    result = (ArrayList<JSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.tailSet(makeIndexItem(indexValue),op == OP.gte), limit );
                } else {
                    result = (ArrayList<JSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.gte), limit);
                }
                break;
            case lte:
            case lt:
                if(indexSort > 0) {
                    result = (ArrayList<JSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.lte), limit);
                } else {
                    result = (ArrayList<JSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.tailSet(makeIndexItem(indexValue), op == OP.lte), limit);
                }
                break;
        }
        return result;
    }

    @Override
    public List<Object> removeByIndex(Object indexValue, FindOption option) {
        OP op = option.getOp();
        if(op == null) op = OP.eq;
        ArrayList<Object> results = new ArrayList<>();
        switch (op) {
            case eq:
                if(removeIndex(indexValue)) {
                    results.add(indexValue);
                }
                break;
            case gte:
            case gt:
                if(indexSort > 0) {
                    results = removeByJSONItems(itemSet.tailSet(makeIndexItem(indexValue), op == OP.gte));
                } else {
                    results = removeByJSONItems(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.gte));
                }
                break;
            case lte:
            case lt:
                if(indexSort > 0) {
                    results = removeByJSONItems(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.lte));
                } else {
                    results = removeByJSONItems(itemSet.tailSet(makeIndexItem(indexValue), op == OP.lte));
                }
                break;
        }
        return results;
    }

    @Override
    public List<JSONObject> list(int limit, boolean reverse) {
        if(reverse) {
            return (List<JSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.descendingSet(), limit);
        }
        return (List<JSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet, limit);
    }

    private Collection<JSONObject> jsonItemCollectionsToJsonObjectCollection(Collection<JSONItem> jsonItems, int limit) {
        int count = 0;
        ArrayList<JSONObject> jsonObjects = new ArrayList<>();
        for(JSONItem item : jsonItems) {
            if(count >= limit) {
                break;
            }
            jsonObjects.add(item.getJsonObject());
            ++count;
        }
        return jsonObjects;
    }


    private ArrayList<Object> removeByJSONItems(Collection<JSONItem> jsonItems) {
        ArrayList<Object> removedIndexList = new ArrayList<>();
        for(JSONItem item : jsonItems) {
            removedIndexList.add(item.getIndexValue());
        }

        jsonItems.clear();
        return removedIndexList;
    }


    /* 아직은 구현하지 않음.
    @Override
    public List<JSONObject> find(FindFilter findFilter, int limit) {
        List<FindFilter> filters = findFilter.getSubFilters();
        OP rootOP = null;
        Collection<JSONObject>[] results;
        if(filters == null && findFilter.getKey() != null) {
            results = new Collection[1];
            results[0] = new ArrayList<>();
            filters = new ArrayList<>();
            filters.add(findFilter);
        }
        else if(filters == null) {
            return new ArrayList<>();
        }
        else {
            results = new Collection[filters.size()];
            rootOP = findFilter.getOp();
            findFilter.getOp();
        }
        if (rootOP != OP.and && rootOP != OP.or && rootOP != OP.not && rootOP != OP.nor) {
            rootOP = OP.and;
        }

        for(int i = 0, n = filters.size(); i < n; ++i) {
            FindFilter filter = filters.get(i);
            String key = filter.getKey();
            Object value = filter.getValue();
            OP op = filter.getOp();
            if(indexKey.equals(key)) {
                itemTreeSet.tailSet()
            }
        }

        return null;
    }
    */


    public boolean replace(JSONObject jsonObject) {
        JSONItem currentItem = get(jsonObject);
        if(currentItem == null) {
            return false;
        }
        currentItem.setJsonObject(jsonObject);
        return true;
    }



    private JSONItem makeIndexItem(Object index) {
        JSONObject indexJson = new JSONObject().put(indexKey, index);
        JSONItem indexItem = new JSONItem(indexJson, indexKey, indexSort);
        return indexItem;
    }

    private JSONObject findByIndex(Object indexValue) {
        JSONItem foundItem = get(new JSONObject().put(indexKey, indexValue));
        return foundItem == null ? null : foundItem.getJsonObject();
    }

    public boolean removeIndex(Object indexValue) {
        JSONItem indexItem = makeIndexItem(indexValue);
        return itemSet.remove(indexItem);
    }

    public JSONObject last() {
        if(itemSet.isEmpty()) return null;
        return itemSet.last().getJsonObject();
    }

    @Override
    public boolean remove(JSONObject o) {
        JSONItem item = new JSONItem((JSONObject) o, indexKey, indexSort);
        return itemSet.remove(item);
    }


    public boolean addAll(Collection<? extends JSONObject> c) {
        Collection<?> list = objectCollectionToJSONItemCollection(c);
        boolean success = itemSet.addAll((Collection<? extends JSONItem>) list);
        if(success && fileCacheDelegator == null) {
            while(itemSet.size() > memCacheLimit) {
                itemSet.pollLast();
            }
        }
        return success;

    }


    private Collection<?> objectCollectionToJSONItemCollection(Collection<?> c) {
        ArrayList<JSONItem> list = new ArrayList<>();
        for(Object obj : c) {
            if(obj instanceof JSONObject) {
                list.add(new JSONItem((JSONObject) obj, indexKey, indexSort));
            } else {
                list.add(new JSONItem(new JSONObject().put(indexKey, obj), indexKey, indexSort));
            }
        }
        return list;
    }


    public void clear() {
        itemSet.clear();
    }

    @Override
    public Iterator<JSONObject> iterator() {
        final Iterator<JSONItem> iterator = itemSet.iterator();

        return new Iterator<JSONObject>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public JSONObject next() {
                return iterator.next().getJsonObject();
            }

            @Override
            public void remove() {
                iterator.remove();;
            }
        };
    }
}
