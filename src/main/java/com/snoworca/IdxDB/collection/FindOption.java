package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.OP;

public class FindOption {
    private OP op;

    public static FindOption fromOP(OP op) {
        return new FindOption().setOp(op);

    }

    public OP getOp() {
        return op;
    }

    public FindOption setOp(OP op) {
        this.op = op; return this;
    }
}
