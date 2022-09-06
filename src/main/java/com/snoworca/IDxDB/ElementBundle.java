package com.snoworca.IDxDB;

public class  ElementBundle<E> {
    private E memElement;
    private long position;


    public E getMemElement() {
        return memElement;
    }

    public E getElement() {
        return null;
    }

    public void setMemElement(E memElement) {
        this.memElement = memElement;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }


}
