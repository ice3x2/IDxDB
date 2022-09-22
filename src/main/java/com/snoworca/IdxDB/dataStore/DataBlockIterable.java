package com.snoworca.IdxDB.dataStore;


import java.io.IOException;
import java.util.Iterator;

public class DataBlockIterable implements Iterable<DataBlock> {

    private long startPos;
    private DataIO dataIO;

    DataBlockIterable(long startPos, DataIO dataIO) {
        this.startPos = startPos;
        this.dataIO = dataIO;
    }

    @Override
    public Iterator<DataBlock> iterator() {
        return new DataBlockIterator(dataIO, startPos);
    }

    public static class DataBlockIterator implements Iterator<DataBlock> {

            long pos;
            DataIO dataIO;
            DataBlock prev;
            DataBlock current;
            DataBlock next;
            boolean isInit = false;

            DataBlockIterator(DataIO dataIO, long pos) {
                this.pos = pos;
                this.dataIO = dataIO;
                init();
            }


            private void init() {
                try {
                    next = dataIO.get(pos);
                } catch (IOException e) {
                    next = null;
                } finally {
                    isInit = true;
                }
            }


            private boolean moveNext() {
                long nextPos = 0;

                prev = current;
                current = next;
                nextPos = current.getHeader().getNext();
                next = null;

                if(nextPos < 0) {
                    return false;
                }
                try {
                    next = dataIO.get(nextPos);
                    return true;
                } catch (IOException e) {
                    next = null;
                    return false;
                }
            }


            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public DataBlock next() {
                if(!moveNext()) {
                    DataBlock result = current;
                    current = null;
                    return result;

                }
                return current;
            }


        @Override
        public void remove() {
            if(current == null) {
                throw new IllegalStateException();
            }
            try {
                dataIO.unlink(current.getPos());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
