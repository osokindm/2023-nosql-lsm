package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Set;

public class FileWriter {

    private static final String FILE_NAME = "sstable";

    private final MemorySegment ssTable;

    public FileWriter(Path path, Arena arena, long segmentSize, int fileId) throws IOException {
        Path resolvedPath = path.resolve(FILE_NAME + fileId + ".txt");
        try (
                FileChannel fc = FileChannel.open(
                        resolvedPath,
                        Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
        ) {
            ssTable = fc.map(FileChannel.MapMode.READ_WRITE, 0, segmentSize, arena);
        }
    }

    public void flushToSegment(Iterator<Entry<MemorySegment>> it, int memTableSize) {
        ssTable.set(ValueLayout.JAVA_INT_UNALIGNED, 0, memTableSize);

        long offsetIndex = Integer.BYTES;
        long offsetEntry = offsetIndex + (long) Long.BYTES * memTableSize;
        while (it.hasNext()) {
            Entry<MemorySegment> entry = it.next();

            ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, offsetEntry);
            offsetIndex += Long.BYTES;

            ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetEntry, entry.key().byteSize());
            offsetEntry += Long.BYTES;
            MemorySegment.copy(
                    entry.key(),
                    0,
                    ssTable,
                    offsetEntry,
                    entry.key().byteSize()
            );
            offsetEntry += entry.key().byteSize();

            if (entry.value() == null) {
                ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetEntry, -1);
            } else {
                ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetEntry, entry.value().byteSize());
                MemorySegment.copy(
                        entry.value(),
                        0,
                        ssTable,
                        offsetEntry + Long.BYTES,
                        entry.value().byteSize()
                );
                offsetEntry += entry.value().byteSize();
            }

            offsetEntry += Long.BYTES;
        }
    }


}
