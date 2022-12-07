package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.collections.index.IndexTable;

import java.util.TreeSet;

public class TreeSetIndexTable extends IndexTable {
    TreeSet<CSONItem> treeSet = new TreeSet<CSONItem>();
}
