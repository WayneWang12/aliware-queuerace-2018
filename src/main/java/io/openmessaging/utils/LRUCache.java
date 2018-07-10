package io.openmessaging.utils;

import io.openmessaging.Block;
import io.openmessaging.FileManager;
import sun.nio.ch.DirectBuffer;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class LRUCache < K, V > extends LinkedHashMap < K, V > {

    private int capacity; // Maximum number of items in the cache.

    public LRUCache(int capacity) {
        super(capacity+1, 1.0f, true); // Pass 'true' for accessOrder.
        this.capacity = capacity;
    }

    protected boolean removeEldestEntry(Entry entry) {
        boolean b = size() > this.capacity;
        if(b) {
            if(entry.getValue() instanceof Block) {
                while (!FileManager.blocksPool.offer((Block) entry.getValue()));
            }
        }
        return b;
    }
}
