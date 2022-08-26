package com.snoworca.IDxDB.data;

import org.junit.jupiter.api.Test;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

class DataIOTest {

    private static String makeRandomString() {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = ThreadLocalRandom.current().nextInt(11) + 5;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)(random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        String generatedString = buffer.toString();
        return generatedString;
    }

    @Test
    void find() {
    }


    @Test
    void writeAndNext() {
        String randomString = makeRandomString();
        File file = new File("./testdata");
        DataIO dataIO = new DataIO(file);

        DataBlock firstBlock = null;
        DataBlock dataBlock = null;
        for (int i = 0; i < 1000; ++i) {
            dataBlock = DataBlock.newDataBlock(randomString);
            dataIO.write(dataBlock);
            if (firstBlock == null) firstBlock = dataBlock;
        }

        DataBlock nextBlock =  dataIO.next(dataBlock);
    }

    @Test
    void start() {
    }

    @Test
    void next() {
    }
}