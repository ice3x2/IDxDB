package com.snoworca.IDxDB;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class XList<E> implements List<E> {


    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private ArrayList<ElementBundle<E>> elements = new ArrayList<>();
    private ArrayList<IndexCommand> commands = new ArrayList<>();


    protected XList() {

    }



    @Override
    public int size() {
        ReentrantReadWriteLock.ReadLock lock = readWriteLock.readLock();
        lock.lock();
        int size = elements.size();
        lock.unlock();
        return size;
    }

    @Override
    public boolean isEmpty() {
        ReentrantReadWriteLock.ReadLock lock = readWriteLock.readLock();
        lock.lock();
        boolean isEmpty = elements.isEmpty();
        lock.unlock();
        return isEmpty;
    }

    @Override
    public boolean contains(Object o) {
        ReentrantReadWriteLock.ReadLock lock = readWriteLock.readLock();
        lock.lock();
        boolean contains = elements.contains(o);
        lock.unlock();
        return contains;
    }

    @Override
    public Iterator<E> iterator() {

    }

    @Override
    public Object[] toArray() {

    }

    @Override
    public <T> T[] toArray(T[] a) {

    }

    @Override
    public boolean add(E e) {
        commands.add(new IndexCommand(IndexCommand.TYPE_XLIST, IndexCommand.CMD_ADD, e));
        return true;
    }

    @Override
    public boolean remove(Object o) {
        commands.add(new IndexCommand(IndexCommand.TYPE_XLIST, IndexCommand.CMD_REMOVE_BY_OBJECT, o));
        return true;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        commands.add(new IndexCommand(IndexCommand.TYPE_XLIST, IndexCommand.CMD_ADD_ALL, c));
        return true;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        commands.add(new IndexCommand(IndexCommand.TYPE_XLIST, IndexCommand.CMD_ADD_ALL_WITH_INDEX,index, c));
        return true;
    }


    @Override
    public boolean removeAll(Collection<?> c) {
        commands.add(new IndexCommand(IndexCommand.TYPE_XLIST, IndexCommand.CMD_REMOVE_ALL,c));
        return true;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        commands.add(new IndexCommand(IndexCommand.TYPE_XLIST, IndexCommand.CMD_RETAIN_ALL,c));
        return true;
    }

    @Override
    public void clear() {
        commands.add(new IndexCommand(IndexCommand.TYPE_XLIST, IndexCommand.CMD_CLEAR));
    }

    @Override
    public E get(int index) {
        return null;
    }

    @Override
    public E set(int index, E element) {
        rangeCheck(index);
        commands.add(new IndexCommand(IndexCommand.TYPE_XLIST, IndexCommand.CMD_CLEAR));
        return elements.get(index).getElement();

    }

    private void rangeCheck(int index) {
        if (index >= elements.size())
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
    }

    private String outOfBoundsMsg(int index) {
        return "Index: "+index+", Size: "+ elements.size();
    }

    @Override
    public void add(int index, E element) {

    }

    @Override
    public E remove(int index) {
        return null;
    }

    @Override
    public int indexOf(Object o) {
        return 0;
    }

    @Override
    public int lastIndexOf(Object o) {
        return 0;
    }

    @Override
    public ListIterator<E> listIterator() {
        return null;
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        return null;
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        return null;
    }


    public void commit() {

    }
}
