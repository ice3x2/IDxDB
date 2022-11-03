package com.snoworca.IdxDB;



import com.snoworca.IdxDB.collection.*;
import com.snoworca.IdxDB.store.DataBlock;
import com.snoworca.IdxDB.store.DataStore;
import com.snoworca.IdxDB.store.DataStoreOptions;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class IdxDB {

    private static final int DB_INFO_ID = 0;
    private static final int START_ID = 10000;

    private final AtomicInteger lastCollectionID = new AtomicInteger(START_ID);

    private DataStore dataStore;

    private final static String META_INFO_TYPE_ENTRY = "entry";


    private final ConcurrentHashMap<String, IndexCollection> indexCollectionMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, IndexCollection> indexCollectionIDMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Long> indexCollectionInfoStorePosMap = new ConcurrentHashMap<>();
    private final ReentrantLock collectionMutableLock = new ReentrantLock();


    private static void setLoggerDelegator(LoggerDelegator loggerDelegator) {
        IdxDBLogger.setLoggerDelegator(loggerDelegator);
    }

    public static IdxDBMaker newMaker(File file) {
        return new IdxDBMaker(file);
    }

    public static class  IdxDBMaker {
        private File dbFile;

        private DataStoreOptions dataStoreOptions = new DataStoreOptions();
        private final static long version = 1;


        public IdxDBMaker(File file) {
            this.dbFile = file;
        }

        public IdxDBMaker dataReaderSize(int size) {
            if(size < 1) size = 1;
            dataStoreOptions.setReaderSize(size);
            return this;
        }

        public IdxDBMaker setDataStoreIterableBufferSize(int size) {
            if(size < 1024 * 1024 * 32) size = 1024 * 1024 * 32;
            dataStoreOptions.setIterableBufferSize(size);
            return this;
        }

        public IdxDBMaker setDataCapacityRatio(float capacityRatio) {
            dataStoreOptions.setCapacityRatio(capacityRatio);
            return this;
        }


        public IdxDBMaker compressionType(CompressionType compressionType) {
            this.dataStoreOptions.setCompressionType(compressionType);
            return this;
        }

        @Override
        public String toString() {
            return new CSONObject()
                    .put("dbFile", dbFile.getAbsolutePath())
                    .put("version", version)
                    .put("dataStoreOptions", new CSONObject(dataStoreOptions.toString()))
                    .toString();
        }

        public IdxDB make() throws IOException {
            IdxDB idxDB = new IdxDB();
            DataStoreOptions dataStoreOption = new DataStoreOptions();
            boolean existDBFile = dbFile.isFile() && dbFile.length() > 0;
            if(IdxDBLogger.isInfo()) {
                IdxDBLogger.info("IdxDBMaker.make() - " + this.toString());
            }
            idxDB.dataStore = new DataStore(dbFile, dataStoreOption);
            idxDB.dataStore.open();

            if(existDBFile) {
                loadDB(idxDB);
            } else {
                initDB(idxDB);
            }
            return idxDB;
        }

        private void loadDB(IdxDB db) throws IOException {
             for(DataBlock dataBlock : db.dataStore) {
                 int id = dataBlock.getCollectionId();
                 if(id == DB_INFO_ID) {
                     continue;
                 } else if(id < START_ID){
                     continue;
                 }
                 IndexCollection indexCollection = db.indexCollectionIDMap.get(id);
                if(indexCollection == null) {
                    CSONObject collectionOption = new CSONObject(dataBlock.getData());
                    indexCollection = restoreIndexCollection(db, id, collectionOption);
                    db.indexCollectionInfoStorePosMap.put(id, dataBlock.getPosition());
                    db.lastCollectionID.set(id + 1);
                    continue;
                }
                ((Restorable) indexCollection).restore(new StoredInfo(dataBlock.getPosition(), dataBlock.getCapacity(), new CSONObject(dataBlock.getData())));

             }
             Collection<IndexCollection> indexCollections =  db.indexCollectionMap.values();
             for(IndexCollection indexCollection : indexCollections) {
                 ((Restorable)indexCollection).end();
             }
        }

        private IndexCollection restoreIndexCollection(IdxDB db,int id, CSONObject collectionOption) {

            String className = collectionOption.optString("className");
            IndexCollection indexCollection = null;
            if(IndexTreeSet.class.getName().equals(className)) {
                IndexTreeSet indexTreeSet = new IndexTreeSet(id, db.dataStore, IndexSetOption.fromCSONObject(collectionOption));
                db.indexCollectionMap.put(indexTreeSet.getName(), indexTreeSet);
                indexCollection = indexTreeSet;
            } else if(IndexLinkedMap.class.getName().equals(className)) {
                IndexLinkedMap indexLinkedMap = new IndexLinkedMap(id, db.dataStore, IndexMapOption.fromCSONObject(collectionOption));
                db.indexCollectionMap.put(indexLinkedMap.getName(), indexLinkedMap);
                indexCollection = indexLinkedMap;
            }
            if(indexCollection != null) {
                db.indexCollectionIDMap.put(id, indexCollection);
                db.indexCollectionMap.put(indexCollection.getName(), indexCollection);
            }
            return indexCollection;
        }


        private void initDB(IdxDB db) throws IOException {
            CSONObject dbInfo = new CSONObject().put("type", META_INFO_TYPE_ENTRY).put("name", "idxDB").put("version", version).put("create", System.currentTimeMillis());
            db.dataStore.write(DB_INFO_ID,dbInfo.toBytes());
        }

    }


    public boolean dropCollection(String collectionName) {
        IndexCollection indexCollection = indexCollectionMap.remove(collectionName);
        if(indexCollection == null) {
            return false;
        }
        Long pos = indexCollectionInfoStorePosMap.remove(indexCollection.getID());
        try {
            indexCollection.clear();
            indexCollection.commit();
            dataStore.unlink(pos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }


    private IdxDB() {

    }


    public boolean isClosed() {
        return dataStore == null;
    }


    public void close() {
        dataStore.close();
        dataStore = null;
    }



    private CollectionCreateCallback makeCollectionCreateCallback(String name) {
        return new CollectionCreateCallback() {
            @Override
            public void onCreate(IndexCollection indexCollection) {
                String name = indexCollection.getName();
                indexCollectionMap.put(name, indexCollection);
                indexCollectionIDMap.put(indexCollection.getID(), indexCollection);
                long headPos = indexCollection.getHeadPos();
                CSONObject optionInfo = ((IndexCollectionBase)indexCollection).getOptionInfo();
                optionInfo.put("headPos", headPos);
                byte[] buffer = optionInfo.toBytes();
                try {
                    DataBlock dataBlock =dataStore.write(indexCollection.getID(), buffer);
                    indexCollectionInfoStorePosMap.put(indexCollection.getID(),dataBlock.getPosition());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    };

    public IndexMapBuilder newIndexMapBuilder(String name) {
        return new IndexMapBuilder(makeCollectionCreateCallback(name),lastCollectionID.getAndIncrement(), dataStore,name, collectionMutableLock);
    }



    public IndexSetBuilder newIndexTreeSetBuilder(String name) {
        return new IndexSetBuilder(makeCollectionCreateCallback(name),lastCollectionID.getAndIncrement(), dataStore,name, collectionMutableLock);
    }


    public CSONArray executeCSONQuery(CSONArray query) {
        return QueryExecutor.execute(this,query);
    }

    public CSONObject executeCSONQuery(CSONObject query) {
        return QueryExecutor.execute(this,query);
    }

    public String executeQuery(String query) {
        return QueryExecutor.execute(this,query);
    }


    public IndexCollection get(String name) {
        return indexCollectionMap.get(name);
    }




}
