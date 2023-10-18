package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class PeekingIterator implements Iterator<Entry<MemorySegment>> {
//
//    private final Iterator<Entry<MemorySegment>> it;
//    private final int priority;
//    private Entry<MemorySegment> current = null;
//    private boolean done = false;
//
//    public PeekingIterator(Iterator<Entry<MemorySegment>> iterator, int priority) {
//        it = iterator;
//        this.priority = priority;
//        moveIterator();
//    }
//
//    public Entry<MemorySegment> peek() {
//        return current;
//    }
//
//    public int getPriority() {
//        return priority;
//    }
//
//    @Override
//    public Entry<MemorySegment> next() {
//        if (done) {
//            return null;
//        }
//        Entry<MemorySegment> res = current;
//        moveIterator();
//
//        return res;
//    }
//
//    @Override
//    public boolean hasNext() {
//        return current != null;
//    }
//
//    public void moveIterator() {
//        if (it.hasNext()) {
//            current = it.next();
//        } else {
//            done = true;
//            current = null;
//        }
//    }

    private final Iterator<Entry<MemorySegment>> delegate;
    private Entry<MemorySegment> current;
    private final int priority;

    public PeekingIterator(Iterator<Entry<MemorySegment>> delegate, int priority) {
        this.priority = priority;
        this.delegate = delegate;
    }

    public int getPriority() {
        return priority;
    }

    public Entry<MemorySegment> peek() {
        if (current == null) {
            current = delegate.next();
        }
        return current;
    }

    @Override
    public boolean hasNext() {
        return current != null || delegate.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> peek = peek();
        current = null;
        return peek;
    }
}
