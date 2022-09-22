package com.snoworca.IdxDB;

import com.snoworca.IdxDB.collection.FileCacheDelegator;
import com.snoworca.IdxDB.collection.IndexCollection;
import com.snoworca.IdxDB.collection.IndexTree;

import com.snoworca.IdxDB.dataStore.DataIO;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IdxDB {

    private DataIO dataIO;

    private ConcurrentHashMap<String, IndexTree> setMap = new ConcurrentHashMap<>();

    public static IdxDB create(File file) {
        IdxDB idxDB = new IdxDB();
        idxDB.dataIO = new DataIO(file);
        return idxDB;
    }

    public IndexTreeBuilder newIndexTreeBuilder(String name) {
        return new IndexTreeBuilder(name);
    }

    public JSONObject executeQuery(JSONObject query) {
        return QueryExecutor.execute(this,query);
    }

    public IndexTree getSet(String name) {
        return setMap.get(name);
    }

    public IndexCollection get(String name) {
        return setMap.get(name);
    }

    private FileCacheDelegator fileCacheDelegator = new FileCacheDelegator() {
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

    public class IndexTreeBuilder {

        private String name;

        private String indexKey = "";
        private int sort = 0;
        private int memCacheSize = Integer.MAX_VALUE;

        private boolean fileStoreMode = false;

        private Map<String, Object> toOptionMap() {


            return null;

        }


        IndexTreeBuilder(String name) {
            this.name = name;
        }

        public IndexTreeBuilder setFileStore(boolean enable) {
            this.fileStoreMode = enable;
            return this;
        }


        public IndexTreeBuilder index(String key, int sort) {
            this.indexKey = key;
            this.sort = sort;
            return this;
        }


        public IndexTreeBuilder memCacheSize(int limit) {
            this.memCacheSize = limit;
            return this;
        }

        public IndexTree create() {
            try {
                Constructor<IndexTree> constructor = IndexTree.class.getDeclaredConstructor(FileCacheDelegator.class, Integer.class, String.class, Integer.class);
                constructor.setAccessible(true);
                IndexTree set = constructor.newInstance( (fileStoreMode ? fileCacheDelegator : null), memCacheSize, indexKey, sort);
                constructor.setAccessible(false);
                setMap.put(this.name, set);
                return  set;
            } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                     IllegalAccessException e) {
                throw new RuntimeException(e);
            }

        }

    }

}
