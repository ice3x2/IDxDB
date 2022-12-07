package com.snoworca.IdxDB.collections;

import com.snoworca.cson.CSONObject;

import java.util.Collection;

public interface IDxCollection {

    public void save(CSONObject csonObject);
    public void update(CSONObject csonObject);
    public void saveAll(Collection<CSONObject> csonObject);
    public CommitResult commit();
    public void clearTransaction();
    public void clear();
    public void delete(CSONObject query);
    public Collection<CSONObject> find(CSONObject query);
    public void findAll();

}
