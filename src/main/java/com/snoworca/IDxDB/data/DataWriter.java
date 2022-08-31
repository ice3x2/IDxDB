package com.snoworca.IDxDB.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class DataWriter {

    private File dataFile;
    FileOutputStream fos;
    private FileChannel fileChannel;
    private AtomicLong fileLength = new AtomicLong(0);

    private ReentrantLock lock = new ReentrantLock();
    private boolean isLock = false;

    public DataWriter(File file) {
        this.dataFile = file;
        this.fileLength.set(file.length());
    }

    public long length() {
        return fileLength.get();
    }

    public boolean writeable() {
        return isLock;
    }

    public void open() throws IOException {
        lock.lock();
        isLock = true;
        try {
            if (!this.dataFile.exists()) this.dataFile.createNewFile();
            fos = new FileOutputStream(dataFile);
            fileChannel = fos.getChannel();
        } catch (IOException e) {
            throw e;
        } finally {
            isLock = false;
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        isLock = true;
        try {
            fileChannel.close();
        } catch (Exception ignored) {}
        try {
            fos.close();
        } catch (Exception ignored) {}
        isLock = false;
        lock.unlock();
    }

    public void write(DataBlock block) throws IOException {
        lock.lock();
        isLock = true;
        try {
            byte[] buffer = block.toBuffer();
            block.setPos(fileLength.get());
            fileChannel.write(ByteBuffer.wrap(buffer));
            this.fileLength.addAndGet(buffer.length);
        } catch (IOException e) {
            throw e;
        } finally {
            isLock = false;
            lock.unlock();
        }
    }

}
