package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

public class SsTable {
    private final long size;
    private final MemorySegment mappedSegment;

    public SsTable(Path path, Arena arena) {
        Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);
        try (FileChannel channel = FileChannel.open(path, openOptions)) {
            mappedSegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        } catch (IOException e) {
            throw new RuntimeException();
        }
        size = mappedSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
    }

    public long getSize() {
        return size;
    }

    public long byteSize() {
        return mappedSegment.byteSize();
    }

    public MemorySegment getMappedSegment() {
        return mappedSegment;
    }

    public long getOffsetBinary(MemorySegment key, boolean isFrom) {
//
//        long size = mappedSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
//        long lo = 0;
//        long hi = size - 1;
//        long mid = size;
//        long entryOffset = mappedSegment.byteSize();
//        while (lo <= hi) {
//            mid = lo + ((hi - lo) >>> 1);
//            entryOffset = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, getInitialOffset(mid));
//            long offset = entryOffset;
//            long keySize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset);
//            entryOffset += Long.BYTES;
//            MemorySegment slicedKey = mappedSegment.asSlice(entryOffset, keySize);
//            int diff = InMemoryDao.compare(slicedKey, key);
//            if (diff < 0) {
//                if (mid == hi && hi == size - 1) {
//                    return mappedSegment.byteSize();
//                }
//                lo = mid + 1;
//            } else if (diff > 0) {
//                if (mid == lo && lo == 0) {
//                    return entryOffset - Long.BYTES;
//                }
//                hi = mid - 1;
//            } else {
//                return offset;
//            }
//        }
////        return getInitialOffset(size);
//
//        return isFrom ? entryOffset - Long.BYTES : mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, getInitialOffset(mid + 1));

        long lowerBond = 0;
        long higherBond = size - 1;
//        long lastPosition = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, getInitialOffset(size));
        long lastPosition = mappedSegment.byteSize();
        long middle = higherBond / 2;
        long result = mappedSegment.byteSize();
        int comparison = 0;
        while (lowerBond <= higherBond) {
            lastPosition = result - Long.BYTES;

            result = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, getInitialOffset(middle));
            long keySize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, result);
            result += Long.BYTES;
            MemorySegment slicedKey = mappedSegment.asSlice(result, keySize);
            comparison = InMemoryDao.compare(key, slicedKey);
            if (comparison > 0) {
                lowerBond = middle + 1;
            } else if (comparison < 0) {
                higherBond = middle - 1;
            } else {
                return result;
            }
            middle = (lowerBond + higherBond) / 2;
        }

        if (higherBond == size -1) {
            return mappedSegment.byteSize();
        }
        if (comparison < 0) {
            return result - Long.BYTES;
        }

        return lastPosition;
    }



    public Entry<MemorySegment> binarySearch(MemorySegment key) {
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
                if (entrySize == -1) {
                    return new BaseEntry<>(key, null);
                }
                entryOffset += Long.BYTES;
                return new BaseEntry<>(key, mappedSegment.asSlice(entryOffset, entrySize));
            }
        }
        return null;
    }

    public long getInitialOffset(long size) {
        return Integer.BYTES + size * Long.BYTES;
    }

}
