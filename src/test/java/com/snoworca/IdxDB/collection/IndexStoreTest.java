package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.util.NumberBufferConverter;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

class IndexStoreTest {

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