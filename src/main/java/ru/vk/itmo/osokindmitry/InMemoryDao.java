package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = InMemoryDao::compare;

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable
            = new ConcurrentSkipListMap<>(comparator);

    private final Arena arena;
    private final Path path;


    public InMemoryDao() {
        path = Path.of("C:\\Users\\dimit\\AppData\\Local\\Temp");
        arena = Arena.ofConfined();
    }

    public InMemoryDao(Config config) {
//        FileHandler fileHandler = new FileHandler(config.basePath().resolve(FILE_NAME_PATTERN).toString());

        path = config.basePath();
        arena = Arena.ofConfined();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = memTable.get(key);

        Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);
        if (entry == null && path.toFile().exists()) {

            try (FileChannel fc = FileChannel.open(path, openOptions)) {
                if (fc.size() != 0) {
                    MemorySegment mappedSegment = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size(), arena);
                    entry = binarySearch(mappedSegment, key);
                }
            } catch (IOException e) {
                return null;
            }
        }

        return entry;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        FileReader fr = new FileReader(path, arena);
        Iterator<Entry<MemorySegment>> mTableIterator;

        if (from == null && to == null) {
            mTableIterator = memTable.values().iterator();
        } else if (from == null) {
            mTableIterator = memTable.headMap(to).values().iterator();
        } else if (to == null) {
            mTableIterator = memTable.tailMap(from).values().iterator();
        } else {
            mTableIterator = memTable.subMap(from, to).values().iterator();
        }

        List<PeekingIterator> peekingIterators = new ArrayList<>();
        peekingIterators.add(new PeekingIterator(mTableIterator, 0));

        try {
            List<FileIterator> fileIterators = fr.getFileIterators(from);
            for (int i = 0; i < fileIterators.size(); i++) {
                peekingIterators.add(new PeekingIterator(fileIterators.get(i), i));
            }
            return new MergeIterator(peekingIterators, from, to);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        FileWriter fw = new FileWriter(path, arena, getSsTableSize());
//        try (
//                FileChannel fc = FileChannel.open(
//                        path,
//                        Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
//        ) {

//            MemorySegment ssTable = fc.map(FileChannel.MapMode.READ_WRITE, 0, getSsTableSize(), arena);
//
//            ssTable.set(ValueLayout.JAVA_INT_UNALIGNED, 0, memTable.size());
//            createSearchTree(ssTable, 0, memTable.size(), memTable.values().iterator(), (long) Long.BYTES * memTable.size() + Integer.BYTES);
        fw.flushToSegment(memTable.values().iterator(), memTable.size());
    }

    private long createSearchTree(MemorySegment ssTable, int lo, int hi, Iterator<Entry<MemorySegment>> it, long offset) {

        if (hi < lo) {
            throw new IllegalArgumentException();
        }

        int mid = (lo + hi) >>> 1;

        long entryOffset = offset;
        if (lo < mid) {
            entryOffset = createSearchTree(ssTable, lo, mid - 1, it, offset);
        }

        long changingOffset = entryOffset;

        if (it.hasNext()) {
            Entry<MemorySegment> entry = it.next();
            ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, changingOffset, entry.key().byteSize());
            changingOffset += Long.BYTES;
            MemorySegment.copy(
                    entry.key(),
                    0,
                    ssTable,
                    changingOffset,
                    entry.key().byteSize()
            );
            changingOffset += entry.key().byteSize();

            ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, changingOffset, entry.value().byteSize());
            changingOffset += Long.BYTES;
            MemorySegment.copy(
                    entry.value(),
                    0,
                    ssTable,
                    changingOffset,
                    entry.value().byteSize()
            );
            changingOffset += entry.value().byteSize();

            int middle = ((hi - lo) >>> 1) + lo;
            ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, (long) middle * Long.BYTES + Integer.BYTES, entryOffset);
        }

        if (mid < hi) {
            changingOffset = createSearchTree(ssTable, mid + 1, hi, it, changingOffset);
        }

        return changingOffset;
    }

    @Override
    public void close() throws IOException {
        flush();
        if (!arena.scope().isAlive()) {
            arena.close();
        }
    }

    public static int compare(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);

        if (offset == -1) {
            return 0;
        } else if (offset == segment1.byteSize()) {
            return -1;
        } else if (offset == segment2.byteSize()) {
            return 1;
        }
        byte b1 = segment1.get(ValueLayout.JAVA_BYTE, offset);
        byte b2 = segment2.get(ValueLayout.JAVA_BYTE, offset);
        return Byte.compare(b1, b2);
    }


    private Entry<MemorySegment> binarySearch(MemorySegment mappedSegment, MemorySegment key) {
        long lo = 0;
        long hi = mappedSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);

        while (lo < hi) {
            long mid = ((hi - lo) >>> 1) + lo;
            long entryOffset = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES + Integer.BYTES);
            long keySize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset);
            entryOffset += Long.BYTES;
            MemorySegment slicedKey = mappedSegment.asSlice(entryOffset, keySize);
            entryOffset += keySize;
            int diff = compare(slicedKey, key);
            if (diff < 0) {
                lo = mid + 1;
            } else if (diff > 0) {
                hi = mid;
            } else {
                long entrySize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset);
                entryOffset += Long.BYTES;
                return new BaseEntry<>(key, mappedSegment.asSlice(entryOffset, entrySize));
            }
        }
        return null;
    }

    /**
     * @return sum: number of elements, offsets, sizes of entries, numbers that store sizes of entries
     */
    private long getSsTableSize() {
        long entriesSize = 0;
        for (Entry<MemorySegment> value : memTable.values()) {
            entriesSize += value.key().byteSize() + value.value().byteSize();
        }
        return Integer.BYTES
                + (long) Long.BYTES * memTable.size()
                + entriesSize
                + (long) Long.BYTES * memTable.size() * 2;
    }

}
