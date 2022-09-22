package com.snoworca.IdxDB;

import java.util.List;

public class FindFilter {

    private String key;

    private Object value;
    private OP op;
    private List<FindFilter> subFilters;




    public FindFilter(OP op, List<FindFilter> subFiltersList) {
        subFilters = subFiltersList;
        this.op = op;
    }


    public FindFilter(String key, Object value, OP op) {
        this.key = key;
        this.value = value;
        this.op = op;
    }


    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public OP getOp() {
        return op;
    }

    public List<FindFilter> getSubFilters() {
        return subFilters;
    }

}
