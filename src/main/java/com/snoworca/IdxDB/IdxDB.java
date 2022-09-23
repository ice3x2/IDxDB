package com.snoworca.IdxDB;

import com.snoworca.IdxDB.collection.*;

import com.snoworca.IdxDB.dataStore.DataBlock;
import com.snoworca.IdxDB.dataStore.DataIO;
import com.snoworca.IdxDB.dataStore.DataIOConfig;
import com.snoworca.cson.CSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class IdxDB {

    private DataIO dataIO;
    private long topStoreInfoStorePos = 0;
    private long createTime = 0;

    private final static String META_INFO_TYPE_ENTRY = "entry";

    private ConcurrentHashMap<String, IndexCollection> indexCollectionMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Long> indexCollectionInfoStorePosMap = new ConcurrentHashMap<>();
    private ReentrantLock collectionMutableLock = new ReentrantLock();


    public static IdxDBMaker newMaker(File file) {
        return new IdxDBMaker(file);
    }

    public static class  IdxDBMaker {
        private File dbFile;
        private int dataReaderCapacity = 3;
        private final static long version = 1;

        public IdxDBMaker(File file) {
            this.dbFile = file;
        }

        public IdxDBMaker dataReaderCapacity(int capacity) {
            if(capacity < 1) capacity = 1;
            dataReaderCapacity = capacity;
            return this;
        }


        public IdxDB make() throws IOException {
            IdxDB idxDB = new IdxDB();
            DataIOConfig dataIOConfig = new DataIOConfig();
            dataIOConfig.setReaderCapacity(dataReaderCapacity);
            boolean existDBFile = dbFile.isFile() && dbFile.length() > 0;
            idxDB.dataIO = new DataIO(dbFile, dataIOConfig);
            idxDB.dataIO.open();
            if(existDBFile) {
                loadDB(idxDB);
            } else {
                initDB(idxDB);
            }
            return idxDB;
        }

        private void loadDB(IdxDB db) throws IOException {
            DataBlock dataBlock = db.dataIO.get(0);
            CSONObject csonObject = new CSONObject(dataBlock.getData());
            db.createTime = csonObject.getLong("create");
            //TODO type 확인하고 일치하지 않을경우 exception
            long nextPos = dataBlock.getHeader().getNext();
            loadCollections(db, nextPos);
        }

        private void loadCollections(IdxDB db, long collectionInfoPos) {
            if(collectionInfoPos < 0) return;
            Iterator<DataBlock> dataBlockIterator = db.dataIO.iterator(collectionInfoPos);
            while(dataBlockIterator.hasNext()) {
                DataBlock dataBlock = dataBlockIterator.next();
                byte[] buffer = dataBlock.getData();
                CSONObject csonObject = new CSONObject(buffer);
                String name = csonObject.getString("name");
                db.indexCollectionInfoStorePosMap.put(name, dataBlock.getPos());
                if(IndexSet.class.getName().equals(csonObject.getString("className"))) {
                    IndexSet indexSet = new IndexSet(db.dataIO, IndexSetOption.fromCSONObject(csonObject));
                    db.indexCollectionMap.put(indexSet.getName(), indexSet);
                }
            }

        }



        private void initDB(IdxDB db) throws IOException {
            CSONObject dbInfo = new CSONObject().put("type", META_INFO_TYPE_ENTRY).put("name", "idxDB").put("version", version).put("create", System.currentTimeMillis());
            DataBlock dataBlock = db.dataIO.write(dbInfo.toByteArray());
            db.topStoreInfoStorePos = dataBlock.getPos();
        }

    }


    public boolean dropCollection(String collectionName) {

        Long pos = indexCollectionInfoStorePosMap.remove(collectionName);
        if(pos == null) {
            return false;
        }
        try {
            dataIO.unlink(pos);
            IndexCollection indexCollection = indexCollectionMap.remove(collectionName);
            if(indexCollection != null) {
                indexCollection.clear();
                indexCollection.commit();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }


    private IdxDB() {

    }


    public boolean isClosed() {
        return dataIO == null;
    }


    public void close() {
        dataIO.close();
        dataIO = null;
    }




    public IndexSetBuilder newIndexSetBuilder(String name) {
        CollectionCreateCallback callback = new CollectionCreateCallback() {
            @Override
            public void onCreate(IndexCollection indexCollection) {
                String name = indexCollection.getName();
                indexCollectionMap.put(name, indexCollection);
                long headPos = indexCollection.getHeadPos();
                CSONObject optionInfo = ((IndexSet)indexCollection).getOptionInfo();
                optionInfo.put("headPos", headPos);
                byte[] buffer = optionInfo.toByteArray();
                try {
                    DataBlock dataBlock = dataIO.write(buffer);
                    long pos = dataBlock.getPos();
                    long lastPos = topStoreInfoStorePos;
                    dataIO.setNextPos(lastPos, pos);
                    dataIO.setPrevPos(pos, lastPos);
                    dataIO.setPrevPos(0, -1);
                    indexCollectionInfoStorePosMap.put(name, pos);
                    topStoreInfoStorePos = pos;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        return new IndexSetBuilder(callback, dataIO,name, collectionMutableLock);
    }



    public CSONObject executeCSONQuery(CSONObject query) {
        return QueryExecutor.execute(this,query);
    }

    public String executeQuery(String query) {
        return QueryExecutor.execute(this,new CSONObject(query)).toString();
    }


    public IndexCollection get(String name) {
        return indexCollectionMap.get(name);
    }




}
