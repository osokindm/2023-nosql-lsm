package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;

public class FileIterator implements Iterator<Entry<MemorySegment>> {

    private final MemorySegment mappedFile;
    private final long offsetTo;
    private long offsetFrom;
//    private boolean hasNext;

    public FileIterator(MemorySegment mappedFile, long from, long to) {
        this.mappedFile = mappedFile;
        offsetTo = to;
        offsetFrom = from;
    }

    @Override
    public boolean hasNext() {
        return offsetFrom < offsetTo;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            return null;
        }

        long keySize = mappedFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetFrom);
        offsetFrom += Long.BYTES;
        MemorySegment slicedKey = mappedFile.asSlice(offsetFrom, keySize);
        offsetFrom += keySize;

        long valueSize = mappedFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetFrom);
        offsetFrom += Long.BYTES;
        if (valueSize == -1) {
            return null;
        }
        MemorySegment slicedValue = mappedFile.asSlice(offsetFrom, valueSize);
        offsetFrom += valueSize;
        return new BaseEntry<>(slicedKey, slicedValue);
    }

}
