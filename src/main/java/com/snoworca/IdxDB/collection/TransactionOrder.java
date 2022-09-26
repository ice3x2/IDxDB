package com.snoworca.IdxDB.collection;

class TransactionOrder {
    final static int ORDER_ADD = 1;
    final static int ORDER_ADD_OR_REPLACE = 2;
    final static int ORDER_REPLACE = 3;
    final static int ORDER_REMOVE = 4;

    private int order;
    private CSONItem item;

    TransactionOrder(int order, CSONItem item) {
        this.order = order;
        this.item = item;
    }

    public int getOrder() {
        return order;
    }

    public CSONItem getItem() {
        return item;
    }
}
