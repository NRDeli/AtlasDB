package com.atlasdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class KVStoreTest {

    private KVStore kv;

    @BeforeEach
    void setup() {
        kv = new KVStore();
    }

    @Test
    void putAndGet() {
        kv.put("k","v");
        assertEquals("v", kv.get("k"));
    }

    @Test
    void overwrite() {
        kv.put("k","v1");
        kv.put("k","v2");
        assertEquals("v2", kv.get("k"));
    }

    @Test
    void delete() {
        kv.put("x","y");
        kv.delete("x");
        assertNull(kv.get("x"));
    }

    @Test
    void size() {
        kv.put("a","1");
        kv.put("b","2");
        assertEquals(2, kv.size());
    }
}
