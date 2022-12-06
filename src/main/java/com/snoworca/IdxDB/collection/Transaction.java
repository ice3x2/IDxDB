package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

import java.util.Collection;

public class Transaction {

    public static enum TransactionType {
        SAVE,SAVE_ALL, UPDATE, DELETE, CLEAR
    }

    private TransactionType type = TransactionType.SAVE;

    private Collection<CSONObject> datas;
    private CSONObject data;


    private Transaction() {
    }

    static Transaction newClearTransaction() {
        Transaction transaction = new Transaction();
        transaction.type = TransactionType.CLEAR;
        return transaction;
    }

    Transaction(TransactionType type, CSONObject data) {
        this.type = type;
        this.data = data;
    }

    Transaction(TransactionType type,Collection<CSONObject> datas) {
        this.type = type;
        this.datas = datas;
    }


    public Collection<CSONObject> getDatas() {
        return datas;
    }

    public void setDatas(Collection<CSONObject> datas) {
        this.datas = datas;
    }

    public CSONObject getData() {
        return data;
    }

    public void setData(CSONObject data) {
        this.data = data;
    }

    public TransactionType getType() {
        return type;
    }

    public void setType(TransactionType type) {
        this.type = type;
    }


}
