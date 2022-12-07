package com.snoworca.IdxDB.collections.index;

import com.snoworca.IdxDB.collection.StoredInfo;

public class IndexInfo {

    IndexInfo(StoredInfo positionInfo, int sort, String indexKey) {
        this.positionInfo = positionInfo;
        this.indexKey = indexKey;
        this.sort = sort;
    }
    protected final StoredInfo positionInfo;
    protected final String indexKey;
    protected final int sort;

}
