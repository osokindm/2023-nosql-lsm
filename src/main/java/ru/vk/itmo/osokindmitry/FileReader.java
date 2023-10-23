package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class FileReader {

    private final List<SsTable> files = new ArrayList<>();


    public FileReader(Path path, Arena arena) throws IOException {

        try (Stream<Path> paths = Files.list(path).sorted()) {

            List<Path> pathList = paths.toList();
            for (Path p : pathList) {
                files.addFirst(new SsTable(p, arena));
            }
        }
    }

    public int getFilesNumber() {
        return files.size();
    }

    public List<FileIterator> getFileIterators(MemorySegment from, MemorySegment to) throws IOException {
        final List<FileIterator> fileIterators = new ArrayList<>();

        for (SsTable file : files) {
            long offsetFrom = from == null ? file.getInitialOffset(file.getSize()) : file.getOffsetBinary(from, true);
            long offsetTo = to == null ? file.byteSize() : file.getOffsetBinary(to, false);

            fileIterators.add(new FileIterator(file.getMappedSegment(), offsetFrom, offsetTo));
        }
        return fileIterators;
    }

    public Entry<MemorySegment> get(MemorySegment key) throws IOException {
        for (SsTable file : files) {
            Entry<MemorySegment> entry = file.binarySearch(key);
                if (entry != null) {
                    return entry;
                }
        }
        return null;
    }

    public long getOffset(MemorySegment mappedSegment, MemorySegment key) {

        int size = mappedSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        long initialOffset = (long) size * Long.BYTES + Integer.BYTES;
        if (key == null) {
            return initialOffset;
        }

        long keySize;
        long valueSize;
        MemorySegment slicedKey;
        do {
            keySize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, initialOffset);
            initialOffset += Long.BYTES;
            slicedKey = mappedSegment.asSlice(initialOffset, keySize);

            valueSize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, initialOffset);
            initialOffset += Long.BYTES + valueSize;
        } while ((InMemoryDao.compare(slicedKey, key) < 0) || initialOffset < mappedSegment.byteSize());
        return initialOffset - keySize - valueSize - Long.BYTES * 2;
    }

}
