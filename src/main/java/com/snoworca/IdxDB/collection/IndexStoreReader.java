package com.snoworca.IdxDB.collection;


import com.snoworca.IdxDB.util.DataInputStream;
import com.snoworca.IdxDB.util.PrimitiveTypeSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class IndexStoreReader {

    private final static int DEFAULT_READ_BUFFER = 1024 * 1024 * 256;

    private FileInputStream fileInputStream;
    private BufferedInputStream bufferedInputStream;
    private DataInputStream dataInputStream;


    public IndexStoreReader(File file) throws FileNotFoundException {
        fileInputStream = new FileInputStream(file);
        bufferedInputStream = new BufferedInputStream(fileInputStream, DEFAULT_READ_BUFFER);
        dataInputStream = new DataInputStream(bufferedInputStream);
    }

    public List<IndexStoreWriter.IndexBlock> readIndexBlocks(long maxReadLength) throws IOException {
        if(maxReadLength < 128) {
            maxReadLength = 128;
        }
        ArrayList<IndexStoreWriter.IndexBlock> indexBlocks = new ArrayList<IndexStoreWriter.IndexBlock>((int)(maxReadLength / 21));
        long readLength = 0;
        while(readLength < maxReadLength) {
            byte cmd = dataInputStream.readByte();
            if(cmd == -1) return indexBlocks;
            int collectionID = dataInputStream.readInt();
            int pos = dataInputStream.readInt();
            int capacity = dataInputStream.readInt();
            int size = dataInputStream.readInt();
            byte[] indexBuffer = new byte[size];
            int readCount = 0;
            while (readCount < size) {
                readCount += dataInputStream.read(indexBuffer, readCount, size - readCount);
            }
            Object index = PrimitiveTypeSerializer.deserializeAnything(indexBuffer);
            if(cmd == IndexStoreWriter.CMD_PUT) {
                indexBlocks.add(IndexStoreWriter.IndexBlock.createPutIndexBlock(collectionID, pos, capacity, index));
            } else if(cmd == IndexStoreWriter.CMD_REMOVE) {
                indexBlocks.add(IndexStoreWriter.IndexBlock.createRemoveIndexBlock(collectionID, pos, capacity, index));
            }
            // cmd(1) + collectionID(4) + pos(4) + capacity(4) + indexBufferSize(4) + indexBuffer(n)
            readLength += 1 + 4 + 4 + 4 + 4 + size;
        }
        return indexBlocks;
    }


}
