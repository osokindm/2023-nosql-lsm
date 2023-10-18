package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.*;
import java.util.stream.Collectors;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

//    private final PriorityQueue<PeekingIterator> pq;
//    private Entry<MemorySegment> next;
//
//    public MergeIterator(List<PeekingIterator> iterators) throws IOException {
//        pq = new PriorityQueue<>((a, b) -> {
//
//            int diff = InMemoryDao.compare(a.peek().key(), b.peek().key());
//            if (diff == 0) {
//                return Integer.compare(a.getPriority(), b.getPriority());
//            } else if (diff < 0) {
//                return -1;
//            } else {
//                return 1;
//            }
//        });
//        pq.addAll(iterators);
//        next = getNext();
//    }
//
//    @Override
//    public boolean hasNext() {
//        return !pq.isEmpty();
////        return next != null;
//    }
//
//    @Override
//    public Entry<MemorySegment> next() {
//        if (!hasNext()) {
//            throw new NoSuchElementException();
//        }
//        Entry<MemorySegment> res = next;
//        next = getNext();
//        return res;
//    }
//
//    private Entry<MemorySegment> getNext() {
//        PeekingIterator it = pq.poll();
//        if (it == null) {
//            return null;
//        }
//        Entry<MemorySegment> entry = it.peek();
//        if (it.hasNext()) {
//            it.next();
//            pq.offer(it);
//        }
//        skipIfEqualTo(entry.key());
//        return entry;
//    }
//
//    private void skipIfEqualTo(MemorySegment key) {
//        while (!pq.isEmpty() && InMemoryDao.compare(key, pq.peek().peek().key()) == 0) {
//            PeekingIterator it = pq.poll();
//            if (it.hasNext()) {
//                it.next();
//                pq.offer(it);
//            }
//        }
//    }
private final PriorityQueue<PeekingIterator> iteratorQueue;

    /**
     * create Merge iterator from PeekIterators.
     *
     * @param iterators - list ordered by ascending iterators priority
     */
    public MergeIterator(List<PeekingIterator> iterators) {
        List<PeekingIterator> iteratorsCopy = iterators.stream().filter(Objects::nonNull)
                .filter(Iterator::hasNext).toList();
        this.iteratorQueue = new PriorityQueue<>((a, b) -> {

            int diff = InMemoryDao.compare(a.peek().key(), b.peek().key());
            if (diff == 0) {
                return Integer.compare(a.getPriority(), b.getPriority());
            } else if (diff < 0) {
                return -1;
            } else {
                return 1;
            }
        });
        iteratorQueue.addAll(iteratorsCopy);
        skipTombStones();
    }

    @Override
    public boolean hasNext() {
        return !iteratorQueue.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        PeekingIterator curr = iteratorQueue.poll();
        if (curr == null) {
            throw new NoSuchElementException();
        }
        Entry<MemorySegment> result = curr.next();
        if (curr.hasNext()) {
            iteratorQueue.add(curr);
        }
        deleteByKey(result.key());
        skipTombStones();
        return result;
    }

    private void skipTombStones() {
        while (!iteratorQueue.isEmpty() && (iteratorQueue.peek().peek().value() == null)) {
            PeekingIterator it = iteratorQueue.poll();
            if (it == null) {
                return;
            }
            MemorySegment keyToDelete = it.next().key();
            deleteByKey(keyToDelete);
            if (it.hasNext()) {
                iteratorQueue.add(it);
            }
        }
    }

    private void deleteByKey(MemorySegment keyToDelete) {
        while (!iteratorQueue.isEmpty()) {
            PeekingIterator curr = iteratorQueue.poll();
            if (!curr.hasNext()) {
                continue;
            }
            if (InMemoryDao.compare(curr.peek().key(), keyToDelete) != 0) {
                iteratorQueue.add(curr);
                break;
            }
            curr.next();
            if (curr.hasNext()) {
                iteratorQueue.add(curr);
            }
        }
    }
}
