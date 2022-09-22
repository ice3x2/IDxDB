package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.util.Files;
import com.snoworca.IdxDB.util.NumberBufferConverter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class IndexStore {

    private final static long PREFIX = 8491789012389013251L;
    private File indexFile;
    private Class<? extends IndexCollection> type;
    private String name;
    private String indexKey;

    IndexStore(Class<? extends IndexCollection> type, String name,File dbFile) {
        this.type = type;
        String dbFileName = dbFile.getName();
        String fileName = Files.getNameWithoutExtension(dbFileName) + "." + name + ".idx";
        indexFile = new File(dbFile.getParentFile(), fileName);
    }


    public void store(Iterator<JSONItem> iterator) throws IOException {
        File indexTmpFile = new File(indexFile.getAbsolutePath() + ".tmp");
        if(indexTmpFile.exists()) indexTmpFile.delete();
        byte[] intBuffer = new byte[4];
        byte[] longBuffer = new byte[8];


        indexTmpFile.createNewFile();
        FileOutputStream fos = new FileOutputStream(indexTmpFile);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fos,1024 * 512);

        NumberBufferConverter.fromLong(PREFIX, longBuffer, 0);
        bufferedOutputStream.write(longBuffer);



        while(iterator.hasNext()) {
            JSONItem item = iterator.next();
            long pos = item.getFilePos();
            NumberBufferConverter.fromLong(pos, longBuffer, 0);
            bufferedOutputStream.write(longBuffer);
        }



    }




}
