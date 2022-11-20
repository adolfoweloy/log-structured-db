package com.adolfoeloy.hashindex;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LogDatabase implements KeyValueStore<Long, String> {
    private final RandomAccessFile randomAccessFile;
    private final Map<Long, Long> hashIndex = new HashMap<>();

    public LogDatabase(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    public static KeyValueStore<Long, String> createDB() {
        try {
            return new LogDatabase(new RandomAccessFile("database", "rw"));
        } catch (FileNotFoundException e) {
            throw new LogDatabaseException(e);
        }
    }

    @Override
    public void set(Long key, String value) {
        try {
            long currentPointer = randomAccessFile.getFilePointer();
            randomAccessFile.writeUTF(value);
            hashIndex.put(key, currentPointer);
        } catch (IOException e) {
            throw new LogDatabaseException(e);
        }
    }

    @Override
    public Optional<String> get(Long key) {
        Long offset = hashIndex.get(key);

        if (offset != null) {
            try {
                randomAccessFile.seek(offset);

                int length = randomAccessFile.readShort();
                byte[] contentValueBuffer = new byte[(length)];

                randomAccessFile.read(contentValueBuffer);
                return Optional.of(
                        new String(contentValueBuffer,
                        StandardCharsets.UTF_8
                ));
            } catch (IOException e) {
                throw new LogDatabaseException(e);
            }
        }

        return Optional.empty();
    }
}
