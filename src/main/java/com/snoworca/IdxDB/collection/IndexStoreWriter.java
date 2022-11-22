package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.util.DataOutputStream;
import com.snoworca.IdxDB.util.PrimitiveTypeSerializer;

import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexStoreWriter {

    private final File indexStoreFile;

    private OutputStream outputStream;
    private BufferedOutputStream bufferedOutputStream;

    private AtomicBoolean isClosed = new AtomicBoolean(true);

    private ScheduledExecutorService scheduledExecutorService;

    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private int flushTimeout = 15000;



    public static final byte CMD_PUT = 1;
    public static final byte CMD_REMOVE = 2;

    public IndexStoreWriter(File indexStoreFile) throws IOException {
        this.indexStoreFile = indexStoreFile;
        outputStream = Files.newOutputStream(indexStoreFile.toPath());
        bufferedOutputStream = new BufferedOutputStream(outputStream);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("IndexStoreWriter-Write-Thread");
                return thread;
            }
        });
        startAutoFlush();
        isClosed.set(false);
    }

    private void startAutoFlush() {
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
                try {
                    if(System.currentTimeMillis() - bufferedOutputStream.getLastFlushTime() > flushTimeout) {
                        lock.lock();
                        bufferedOutputStream.flush();
                        lock.unlock();
                    }
                } catch (IOException e) {
                    lock.unlock();
                    e.printStackTrace();
                }
            }
        }, 1000, 1000, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public void pushIndex(IndexBlock indexBlock) throws IOException {
        writeIndex(indexBlock.cmd,indexBlock.collectionID, indexBlock.pos,indexBlock.capacity, indexBlock.index);
    }


    public static class IndexBlock {
        byte cmd;
        int collectionID;
        int pos;
        int capacity;
        Object index;

        public static IndexBlock createPutIndexBlock(int collectionID, int pos, int capacity, Object index) {
            IndexBlock indexBlock = new IndexBlock();
            indexBlock.cmd = CMD_PUT;
            indexBlock.collectionID = collectionID;
            indexBlock.pos = pos;
            indexBlock.capacity = capacity;
            indexBlock.index = index;
            return indexBlock;
        }


        public static IndexBlock createRemoveIndexBlock(int collectionID, int pos, int capacity, Object index) {
            IndexBlock indexBlock = new IndexBlock();
            indexBlock.cmd = CMD_REMOVE;
            indexBlock.collectionID = collectionID;
            indexBlock.pos = pos;
            indexBlock.capacity = capacity;
            indexBlock.index = index;
            return indexBlock;
        }

        public int getCapacity() {
            return capacity;
        }

        public boolean isPut() {
            return cmd == CMD_PUT;
        }

        public boolean isRemove() {
            return cmd == CMD_REMOVE;
        }

        public int getCollectionID() {
            return collectionID;
        }

        public int getPos() {
            return pos;
        }

        public Object getIndex() {
            return index;
        }

        @Override
        public String toString() {
            return "IndexBlock{" +
                    "cmd=" + cmd +
                    ", collectionID=" + collectionID +
                    ", pos=" + pos +
                    ", capacity=" + capacity +
                    ", index=" + index +
                    '}';
        }

        private IndexBlock() {
        }

    }

    public void putIndex(IndexBlock indexBlock)  {
        writeIndex(CMD_PUT,indexBlock.collectionID, indexBlock.pos, indexBlock.capacity, indexBlock.index);
    }


    private void writeIndex(byte cmd,int collectionID, int pos, int capacity, Object index)  {
        ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
        try {
            lock.lock();
            DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);
            dataOutputStream.writeByte(cmd);
            dataOutputStream.writeInt(collectionID);
            dataOutputStream.writeInt(pos);
            dataOutputStream.writeInt(capacity);
            byte[] indexObject = PrimitiveTypeSerializer.serializeAnything(index);
            dataOutputStream.writeInt(indexObject.length);
            dataOutputStream.write(indexObject);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void flush() throws IOException {
        scheduledExecutorService.execute(() -> {
            ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
            try {
                lock.lock();
                bufferedOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        });
    }

    public void close() throws IOException {
        ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
        try {
            lock.lock();
            if (isClosed.get()) return;
            List<Runnable> runnableList = scheduledExecutorService.shutdownNow();
            for (Runnable runnable : runnableList) {
                runnable.run();
            }
            bufferedOutputStream.flush();
            bufferedOutputStream.close();
            outputStream.close();
            isClosed.set(true);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        System.out.println("IndexStoreWriter finalize");
        close();
        super.finalize();
    }

    private static class BufferedOutputStream extends FilterOutputStream {

        protected byte buf[];
        protected int count;
        private long lastFlushTime = System.currentTimeMillis();


        public BufferedOutputStream(OutputStream out) {
            this(out, 8192);
        }

        public BufferedOutputStream(OutputStream out, int size) {
            super(out);
            if (size <= 0) {
                throw new IllegalArgumentException("Buffer size <= 0");
            }
            buf = new byte[size];
        }


        private void flushBuffer() throws IOException {
            if (count > 0) {
                lastFlushTime = System.currentTimeMillis();
                out.write(buf, 0, count);
                count = 0;
            }
        }

        public long getLastFlushTime() {
            return lastFlushTime;
        }

        public void write(int b) throws IOException {
            if (count >= buf.length) {
                flushBuffer();
            }
            buf[count++] = (byte)b;
        }

        public void write(byte b[]) throws IOException {
            write(b, 0, b.length);
        }

        public void write(byte b[], int off, int len) throws IOException {
            if (len >= buf.length) {
            /* If the request length exceeds the size of the output buffer,
               flush the output buffer and then write the data directly.
               In this way buffered streams will cascade harmlessly. */
                flushBuffer();
                out.write(b, off, len);
                return;
            }
            if (len > buf.length - count) {
                flushBuffer();
            }
            System.arraycopy(b, off, buf, count, len);
            count += len;
        }

        public void flush() throws IOException {
            flushBuffer();
            out.flush();
        }
    }



}
