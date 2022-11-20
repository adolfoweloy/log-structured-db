package com.adolfoeloy.hashindex;

public class TestLogDatabase {
    public static void main(String[] args) {
        KeyValueStore<Long, String> db = LogDatabase.createDB();
        db.set(1L, "Brazil");
        db.set(2L, "Australia");
        db.set(3L, "Deutschland");
        db.set(1L, "Brasil");

        System.out.println(db.get(1L));
        System.out.println(db.get(3L));
    }
}
