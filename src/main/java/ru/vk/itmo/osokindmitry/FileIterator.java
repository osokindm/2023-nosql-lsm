package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;

public class FileIterator implements Iterator<Entry<MemorySegment>> {

    private final MemorySegment segment;
    private long offset;

    public FileIterator(MemorySegment mappedFile, FileReader fileReader, MemorySegment from) {
        segment = mappedFile;
        offset = fileReader.getOffset(mappedFile, from);
    }

    @Override
    public boolean hasNext() {
        return offset < segment.byteSize();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            return null;
        }

        // FileReader readNext

        long keySize = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        MemorySegment slicedKey = segment.asSlice(offset, keySize);
        offset += keySize;
        long entrySize = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        return new BaseEntry<>(slicedKey, segment.asSlice(offset, entrySize));
    }

}
