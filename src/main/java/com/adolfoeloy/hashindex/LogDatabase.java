package com.adolfoeloy.hashindex;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LogDatabase implements KeyValueStore<Long, String> {
    private final Segment segment;

    /**
     * This requires that all keys can be kept in memory, which means that
     * this kind of store engine is well suited to cases where a given key is updated frequently
     * instead of having new ones created often.
     *
     * TODO: Each segment has its own hash-index. So it is sensible to move the hashIndex closer to the Segment.
     */
    private final Map<Long, Long> hashIndex = new HashMap<>();

    public LogDatabase(Segment segment) {
        this.segment = segment;
    }

    public static KeyValueStore<Long, String> createDB() {
        try {
            return new LogDatabase(new Segment(new RandomAccessFile("database", "rw")));
        } catch (FileNotFoundException e) {
            throw new LogDatabaseException(e);
        }
    }

    @Override
    public void set(Long key, String value) {
        try {
            hashIndex.put(key, segment.write(new Segment.Row(key.toString(), value)));
        } catch (IOException e) {
            throw new LogDatabaseException(e);
        }
    }

    @Override
    public Optional<String> get(Long key) {
        Long offset = hashIndex.get(key);

        if (offset == null) return Optional.empty();

        try {
            return Optional.of(segment.read(offset).value());
        } catch (IOException e) {
            throw new LogDatabaseException(e);
        }
    }
}
