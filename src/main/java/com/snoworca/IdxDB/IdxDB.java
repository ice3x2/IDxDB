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


    private IdxDB() {

    }


    public boolean isClosed() {
        return dataIO == null;
    }


    public void close() {
        dataIO.close();
        dataIO = null;
    }




    public IndexSetBuilder newIndexTreeBuilder(String name) {
        CollectionCreateCallback callback = new CollectionCreateCallback() {
            @Override
            public void onCreate(IndexCollection indexCollection) {
                String name = ((IndexSet)indexCollection).getName();
                indexCollectionMap.put(name, (IndexCollection)indexCollection);
                long headPos = ((IndexSet)indexCollection).getHeadPos();
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
                    topStoreInfoStorePos = pos;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        return new IndexSetBuilder(callback, dataIO,name, collectionMutableLock);
    }



    public CSONObject executeQuery(CSONObject query) {
        return QueryExecutor.execute(this,query);
    }



    public IndexCollection get(String name) {
        return indexCollectionMap.get(name);
    }

    private StoreDelegator storeDelegator = new StoreDelegator() {
        @Override
        public long cache(byte[] buffer) {
            try {

                return dataIO.write(buffer).getPos();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] load(long pos) {
            try {
                return dataIO.get(pos).getData();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };



}
