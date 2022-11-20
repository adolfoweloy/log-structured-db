package com.adolfoeloy.hashindex;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

class Segment {
    private final RandomAccessFile randomAccessFile;

    Segment(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    /**
     * Writes a String and returns its pointer (or offset).
     * @param row
     * @return the data offset that can be used to randomly access the value written
     * @throws IOException
     */
    long write(Row row) throws IOException {
        long currentPointer = randomAccessFile.getFilePointer();
        randomAccessFile.writeUTF(String.format("%s,%s", row.key, row.value));
        return currentPointer;
    }

    Row read(long offset) throws IOException {
        // a seek operation is fast, but it can still be improved with a cache
        // consider the comparison of time to fetch data from http://norvig.com/21-days.html#answers
        // - seek operation should take 8,000,000 nanosec on an average computer
        // - fetch from main memory should take 100 nanosec on an average computer
        randomAccessFile.seek(offset);
        int length = randomAccessFile.readShort();
        byte[] contentValueBuffer = new byte[length];
        randomAccessFile.read(contentValueBuffer);

        String row = new String(contentValueBuffer, StandardCharsets.UTF_8);
        String[] keyValue = row.split(",");
        return new Row(keyValue[0], keyValue[1]);
    }

    Segment compact() {
        throw new UnsupportedOperationException("not implemented yet");
    }

    public record Row(String key, String value) {}
}
