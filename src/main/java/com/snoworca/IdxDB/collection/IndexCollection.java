package com.snoworca.IdxDB.collection;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.Set;

public interface IndexCollection extends Iterable<JSONObject> {

    public Set<String> indexKeys();

    public boolean add(JSONObject jsonObject);
    public boolean addAll(JSONArray jsonObject);

    public boolean addOrReplace(JSONObject jsonObject);
    public boolean addOrReplaceAll(JSONArray jsonObject);

    /**
     * 아직은 구현하지 않는다. 추후 필요해지면 구현.
     * public List<JSONObject> find(FindFilter findFilter,int limit);
     */

    public List<JSONObject> findByIndex(Object start,FindOption options,int limit);

    public List<Object> removeByIndex(Object start,FindOption options);

    public List<JSONObject> list(int limit,boolean reverse);

    public int size();

    public void commit();

    public boolean isEmpty();
    public boolean remove(JSONObject o);

}
