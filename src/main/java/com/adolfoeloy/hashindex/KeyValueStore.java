package com.adolfoeloy.hashindex;

import java.util.Optional;

public interface KeyValueStore<K, V> {
    void set(K key, V value);

    Optional<V> get(K key);
}
