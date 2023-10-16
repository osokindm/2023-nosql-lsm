package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;

public class FileIterator implements Iterator<Entry<MemorySegment>> {

    private final MemorySegment mappedFile;
    private final MemorySegment to;
    private final FileReader fileReader;
    private long offset;
    private boolean hasNext;

    public FileIterator(MemorySegment mappedFile, FileReader fileReader, MemorySegment from, MemorySegment to) {
        this.mappedFile = mappedFile;
        this.fileReader = fileReader;
        this.to = to;
        offset = fileReader.getOffset(mappedFile, from);
    }

    @Override
    public boolean hasNext() {
        return offset < mappedFile.byteSize();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            return null;
        }

        // FileReader readNext

        long keySize = mappedFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        MemorySegment slicedKey = mappedFile.asSlice(offset, keySize);
        offset += keySize;
        long entrySize = mappedFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        return new BaseEntry<>(slicedKey, mappedFile.asSlice(offset, entrySize));
    }

}
