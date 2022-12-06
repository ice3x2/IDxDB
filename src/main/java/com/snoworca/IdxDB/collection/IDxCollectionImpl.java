package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class IDxCollectionImpl extends IDxTransactionAbstractCollection implements IDxCollection, Restorable{

    private ArrayList<StoredInfo> dataPositions = new ArrayList<>();
    private AtomicBoolean isInitializationCompleted = new AtomicBoolean(false);

    private HashMap<String,IndexTable> multiKeyIndexMap = new HashMap<>();
    private HashMap<String,IndexTable> indexMap = new HashMap<>();



    @Override
    public CommitResult commit() {
        CommitResult result = new CommitResult();

        return result;
    }

    @Override
    public Collection<CSONObject> find(CSONObject query) {
        return null;
    }

    @Override
    public void findAll() {

    }

    @Override
    public void restore(StoredInfo storedInfo) {
        dataPositions.add(storedInfo);
    }

    @Override
    public void end() {
        // 인덱스 넣어야함.

        isInitializationCompleted.set(true);
    }
}
