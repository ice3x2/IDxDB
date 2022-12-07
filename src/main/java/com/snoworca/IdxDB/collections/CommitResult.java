package com.snoworca.IdxDB.collections;

import com.snoworca.cson.CSONObject;

public class CommitResult {
    private int add = 0;
    private int replace = 0;
    private int remove = 0;


    public int getAddCount() {
        return add;
    }

    public int getReplaceCount() {
        return add;
    }

    public int getRemoveCount() {
        return add;
    }

    void incrementCountOfAdd() {
        add++;
    }

    void incrementCountOfReplace() {
        replace++;
    }

    void incrementCountOfRemove() {
        remove++;
    }

    public CSONObject toCsonObject() {
        return new CSONObject().put("add", add).put("replace", replace).put("remove", remove);
    }

    @Override
    public String toString() {
        return toCsonObject().toString();
    }
}
