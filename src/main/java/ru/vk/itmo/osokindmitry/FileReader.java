package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Stream;

public class FileReader {

    private final Path path;
    private final Arena arena;

    public FileReader(Path path, Arena arena) {
        this.path = path;
        this.arena = arena;

    }

    public List<FileIterator> getFileIterators(MemorySegment from, MemorySegment to) throws IOException {
        final List<FileIterator> fileIterators = new ArrayList<>();

        try (Stream<Path> paths = Files.list(path).sorted(Comparator.comparing(Path::getFileName))) {
            Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);

            paths.forEach(p -> {
                try (FileChannel channel = FileChannel.open(p, openOptions)){
                    MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
                    fileIterators.add(new FileIterator(segment, this, from, to));
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            });

        }
        return fileIterators;
    }

    public Entry<MemorySegment> get(MemorySegment key) throws IOException {
        try (Stream<Path> paths = Files.list(path).sorted(Comparator.comparing(Path::getFileName).reversed())) {
            Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);

            for (Path ssTablePath : paths.toList()) {
                try (FileChannel channel = FileChannel.open(ssTablePath, openOptions)){
                    MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
                    Entry<MemorySegment> entry = binarySearch(segment, key);
                    if (entry != null) {
                        return entry;
                    }
                }
            }
        }
        return null;
    }

//    public long getOffsetBinary(MemorySegment mappedSegment, MemorySegment key) {
//
//    }

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
            int diff = InMemoryDao.compare(slicedKey, key);
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

    private long readLong(MemorySegment segment, long offset) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
    }

}
