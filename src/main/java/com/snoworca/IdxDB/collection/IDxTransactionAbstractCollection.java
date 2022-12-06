package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.locks.ReentrantLock;

abstract class IDxTransactionAbstractCollection implements IDxCollection{

    private final ArrayList<Transaction> transactionCache = new ArrayList<>();
    private final ReentrantLock transactionCacheLock = new ReentrantLock();


    @Override
    public void save(CSONObject csonObject) {
        try {
            transactionCacheLock.lock();
            transactionCache.add(new Transaction(Transaction.TransactionType.SAVE, csonObject));
        } finally {
            transactionCacheLock.unlock();
        }

    }

    @Override
    public void update(CSONObject csonObject) {
        try {
            transactionCacheLock.lock();
            transactionCache.add(new Transaction(Transaction.TransactionType.UPDATE, csonObject));
        } finally {
            transactionCacheLock.unlock();
        }
    }

    @Override
    public void saveAll(Collection<CSONObject> csonObject) {
        try {
            transactionCacheLock.lock();
            transactionCache.add(new Transaction(Transaction.TransactionType.SAVE_ALL, csonObject));
        } finally {
            transactionCacheLock.unlock();
        }
    }


    @Override
    public void clearTransaction() {
        try {
            transactionCacheLock.lock();
            transactionCache.clear();
        } finally {
            transactionCacheLock.unlock();
        }
    }

    @Override
    public void clear() {
        try {
            transactionCacheLock.lock();
            transactionCache.add(Transaction.newClearTransaction());
        } finally {
            transactionCacheLock.unlock();
        }
    }

    @Override
    public void delete(CSONObject query) {
        try {
            transactionCacheLock.lock();
            transactionCache.add(new Transaction(Transaction.TransactionType.DELETE, query));
        } finally {
            transactionCacheLock.unlock();
        }
    }


    protected ArrayList<Transaction> releaseTransactions() {
        try {
            transactionCacheLock.lock();
            ArrayList<Transaction> transactionCacheCopy = new ArrayList<>(transactionCache);
            transactionCache.clear();
            return transactionCacheCopy;
        } finally {
            transactionCacheLock.unlock();
        }
    }





}
