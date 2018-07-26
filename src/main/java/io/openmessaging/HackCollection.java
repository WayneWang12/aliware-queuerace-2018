package io.openmessaging;


import java.util.Collection;
import java.util.Iterator;

public class HackCollection<E> implements Collection<E> {

    private Object[] arrays;
    private int size = 0;
    private HackCollection() {}
    public HackCollection(E[] arrays){
        this.arrays = arrays;
    }

    public HackCollection(int length) {
        arrays = new Object[length];
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        if (size == 0) {
            return Boolean.TRUE;
        }
        else {
            return Boolean.FALSE;
        }
    }

    @Override
    public boolean contains(Object o) {
        for (int i = 0; i < size; i++) {
            if (arrays[i].equals(o)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    @Override
    public Iterator iterator() {
        return new QueueItr();
    }

    @Override
    public Object[] toArray() {
        return arrays;
    }

    @Override
    public boolean add(Object o) {
        arrays[size++] = o;
        return Boolean.TRUE;
    }


    public E next() {
        return (E) arrays[size++];
    }


    @Override
    public boolean remove(Object o) {
        System.out.println("想啥呢，效率这么慢还想着删除！！！！");
        return Boolean.TRUE;
    }

    @Override
    public boolean addAll(Collection c) {
        System.out.println("自定义的Collection不存在批量操作！！！！");
        return Boolean.TRUE;
    }

    @Override
    public void clear() {
        size = 0;
    }

    public void plus(){
        this.size++;
    }

    @Override
    public boolean retainAll(Collection c) {
        return Boolean.TRUE;
    }

    @Override
    public boolean removeAll(Collection c) {
        return Boolean.TRUE;
    }

    @Override
    public boolean containsAll(Collection c) {
        return Boolean.TRUE;
    }

    @Override
    public Object[] toArray(Object[] a) {
        return arrays;
    }

    private class QueueItr implements Iterator<E> {
        private int index = 0;
        @Override
        public boolean hasNext() {
            if (size > index) {
                return Boolean.TRUE;
            }
            else {
                return Boolean.FALSE;
            }
        }

        @Override
        public E next() {
            return (E) arrays[index++];
        }
    }
}
