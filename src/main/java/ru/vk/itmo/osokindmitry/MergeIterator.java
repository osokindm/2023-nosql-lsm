package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    private final PriorityQueue<PeekingIterator> pq;

    public MergeIterator(List<PeekingIterator> iterators) throws IOException {
        pq = new PriorityQueue<>((a, b) -> {

            if (InMemoryDao.compare(a.peek().key(), b.peek().key()) == 0) {
                return a.getPriority() - b.getPriority();
            } else {
                return InMemoryDao.compare(a.peek().key(), b.peek().key());
            }
        });
        pq.addAll(iterators);


    }

    @Override
    public boolean hasNext() {
        return !pq.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        PeekingIterator it = pq.poll();
        Entry<MemorySegment> next = it.next();
        if (it.hasNext()) {
            pq.offer(it);
        }
        return next;
    }

}
