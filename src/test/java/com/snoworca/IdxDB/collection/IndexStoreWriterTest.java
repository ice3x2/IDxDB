package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.util.NumberBufferConverter;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexStoreWriterTest {

    @Test
    public void fileSizeTest() throws Exception {
        File file = new File("./collection.db.idx");
        file.delete();
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        IndexStoreWriter indexStoreWriter = new IndexStoreWriter(file);
        int testCase = 1000000;
        IndexStoreWriter.IndexBlock[] indexBlocks = new IndexStoreWriter.IndexBlock[testCase];
        for(int i = 0; i < testCase; i++) {
            indexBlocks[i] = IndexStoreWriter.IndexBlock.createPutIndexBlock(0, i, i, (long)i);
            indexStoreWriter.putIndex(indexBlocks[i]);
        }
        indexStoreWriter.close();
        System.out.println((file.length() / 1024 / 1024) + "mb");

        IndexStoreReader indexStoreReader = new IndexStoreReader(file);
        List<IndexStoreWriter.IndexBlock> readIndexBlocks = new ArrayList<>();
        int readCount = 0;
        do {
          readIndexBlocks = indexStoreReader.readIndexBlocks(1024 * 1024 * 32);
          for(int i = 0, n = readIndexBlocks.size(); i < n; ++i) {
              IndexStoreWriter.IndexBlock indexBlock = readIndexBlocks.get(i);
              assertEquals((long)indexBlocks[readCount].getIndex(), (long)indexBlock.getIndex());
              assertEquals(indexBlocks[readCount].getCapacity(), indexBlock.getCapacity());
              assertEquals(indexBlocks[readCount].getPos(), indexBlock.getPos());
              assertEquals(indexBlocks[readCount].getCollectionID(), indexBlock.getCollectionID());
              readCount++;
          }

          System.out.println(readIndexBlocks.size());
        } while(readIndexBlocks.size() > 0);



        file.delete();



    }




    @Test
    public void performanceTest() throws IOException {

        File file = new File("./collection.db");
        long start = System.currentTimeMillis();
        FileOutputStream fos = new FileOutputStream(file);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fos, 512 * 1024);
        byte[] longBuffer = new byte[8];
        for(int i = 0; i < 18000000; ++i) {
            NumberBufferConverter.fromLong(i,longBuffer, 0);
            bufferedOutputStream.write(longBuffer, 0, 8);
        }
        System.out.println((System.currentTimeMillis() - start) + "ms");



        bufferedOutputStream.flush();
        bufferedOutputStream.close();
        fos.close();

        file.delete();

    }


}