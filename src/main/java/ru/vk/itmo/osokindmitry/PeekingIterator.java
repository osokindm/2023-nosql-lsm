package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class PeekingIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> it;
    private final int priority;
    private Entry<MemorySegment> next = null;
    private boolean done = false;

    public PeekingIterator(Iterator<Entry<MemorySegment>> iterator, int priority) {
        it = iterator;
        this.priority = priority;
        moveIterator();
    }

    public Entry<MemorySegment> peek() {
        return next;
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (done) {
            return null;
        }
        Entry<MemorySegment> res = next;
        moveIterator();

        return res;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    public void moveIterator() {
        if (it.hasNext()) {
            next = it.next();
        } else {
            done = true;
            next = null;
        }
    }
}
