package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

import java.util.List;
import java.util.Set;

public interface IndexCollection extends Iterable<CSONObject> {

    public String getName();

    public Set<String> indexKeys();

    public boolean add(CSONObject csonArray);
    public boolean addAll(CSONArray csonArray);

    public boolean addOrReplace(CSONObject csonArray);
    public boolean addOrReplaceAll(CSONArray csonArray);

    /**
     * 아직은 구현하지 않는다. 추후 필요해지면 구현.
     * public List<CSONObject> find(FindFilter findFilter,int limit);
     */

    public List<CSONObject> findByIndex(Object index,FindOption options,int limit);

    public CSONObject findOneByIndex(Object index);

    public void removeByIndex(Object indexValue);
    public void removeByIndex(Object indexValue, FindOption options);

    public List<CSONObject> list(int limit,boolean reverse);

    public List<CSONObject> list(int start, int limit,boolean reverse);

    public int size();

    public CommitResult commit();

    public void rollback();


    public boolean isEmpty();
    public boolean remove(CSONObject o);

    public long getHeadPos();

    public void clear();

    long findIndexPos(Object indexValue);




}
