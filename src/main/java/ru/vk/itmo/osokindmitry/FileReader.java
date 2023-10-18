package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

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

    private final Path basePath;
    private final List<MemorySegment> files = new ArrayList<>();


    public FileReader(Path path, Arena arena) throws IOException {
        this.basePath = path;

        try (Stream<Path> paths = Files.list(basePath).sorted()) {
            Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);

            List<Path> pathList = paths.toList();
            for (Path p : pathList) {
                try (FileChannel channel = FileChannel.open(p, openOptions)) {
                    files.addFirst(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena));
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }
        }
    }

//    public int getMappedFiles() throws IOException {
//        int n;
//
//        try (Stream<Path> paths = Files.list(basePath)) {
//            n = paths.toList().size();
//
//        }
////        return Files.list(basePath).toList().size();
//        return n;
//    }

    public int getFilesNumber() {
        return files.size();
    }

    public List<FileIterator> getFileIterators(MemorySegment from, MemorySegment to) throws IOException {
        final List<FileIterator> fileIterators = new ArrayList<>();

        for (MemorySegment file : files) {
            long offsetFrom = from == null ? getInitialOffset(file.byteSize()) : getOffsetBinary(file, from);
            long offsetTo = to == null ? file.byteSize() : getOffsetBinary(file, to);

            fileIterators.add(new FileIterator(file, offsetFrom, offsetTo));
        }

//        for (int i = filesNumber - 1; i >= 0; i--) {
//            Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);
//            Path filePath = basePath.resolve(FILE_NAME + i + ".txt");
//            try (FileChannel channel = FileChannel.open(filePath, openOptions)) {
//
//                MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
//
//                long offsetFrom = from == null ? getInitialOffset(segment.byteSize()) : getOffsetBinary(segment, from);
//                long offsetTo = to == null ? segment.byteSize() : getOffsetBinary(segment, to);
//
//                fileIterators.add(new FileIterator(segment, offsetFrom, offsetTo));
//
//            }
//        }


//        try (Stream<Path> paths = Files.list(basePath).sorted(Comparator.comparing(Path::getFileName).reversed())) {
//            Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);
//
//            List<Path> pathList = paths.toList();
//            for (Path p : pathList) {
//                try (FileChannel channel = FileChannel.open(p, openOptions)) {
//
//                    MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
//
//                    long offsetFrom = from == null ? getInitialOffset(segment.byteSize()) : getOffsetBinary(segment, from);
//                    long offsetTo = to == null ? segment.byteSize() : getOffsetBinary(segment, to);
//
//                    fileIterators.add(new FileIterator(segment, offsetFrom, offsetTo));
//
//                } catch (IOException e) {
//                    throw new RuntimeException();
//                }
//            }
//        }
        return fileIterators;
    }

    public Entry<MemorySegment> get(MemorySegment key) throws IOException {
        for (MemorySegment file : files) {
            Entry<MemorySegment> entry = binarySearch(file, key);
                if (entry != null) {
                    if (entry.value() == null) {
                        return null;
                    }
                    return entry;
                }
        }
//        for (int i = filesNumber - 1; i >= 0; i--) {
//
//            Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);
//            Path filePath = basePath.resolve(FILE_NAME + i + ".txt");
//
//            try (FileChannel channel = FileChannel.open(filePath, openOptions)) {
//                MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
//                Entry<MemorySegment> entry = binarySearch(segment, key);
//                if (entry != null) {
//                    return entry;
//                }
//            }
//        }

//        try (Stream<Path> paths = Files.list(basePath).sorted(Comparator.comparing(Path::getFileName).reversed())) {
//            Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);
//
//            for (Path ssTablePath : paths.toList()) {
//                try (FileChannel channel = FileChannel.open(ssTablePath, openOptions)) {
//                    MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
//                    Entry<MemorySegment> entry = binarySearch(segment, key);
//                    if (entry != null) {
//                        return entry;
//                    }
//                }
//            }
//        }
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

    public long getOffsetBinary(MemorySegment mappedSegment, MemorySegment key) {

        long size = mappedSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        long lo = 0;
        long hi = size - 1;
        long mid;
        while (lo <= hi) {
            mid = lo + ((hi - lo) >>> 1);
            long entryOffset = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, getInitialOffset(mid));
            long offset = entryOffset;
            long keySize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset);
            entryOffset += Long.BYTES;
            MemorySegment slicedKey = mappedSegment.asSlice(entryOffset, keySize);
            int diff = InMemoryDao.compare(slicedKey, key);
            if (diff < 0) {
                lo = mid + 1;
            } else if (diff > 0) {
                hi = mid - 1;
            } else {
                return offset;
            }
        }
        return getInitialOffset(size);
    }

    private long getInitialOffset(long size) {
        return Integer.BYTES + size * Long.BYTES;
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
                if (entrySize == 0) {
                    return new BaseEntry<>(key, null);
                }
                entryOffset += Long.BYTES;
                return new BaseEntry<>(key, mappedSegment.asSlice(entryOffset, entrySize));
            }
        }
        return null;
    }

}
