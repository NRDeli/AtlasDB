package com.atlasdb.log;

import java.io.Serializable;

/**
 * Represents a state machine operation.
 */
public class Operation implements Serializable {

    public enum Type {
        PUT,
        DELETE
    }

    private final Type type;
    private final String key;
    private final String value;

    public Operation(Type type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public static Operation put(String key, String value) {
        return new Operation(Type.PUT, key, value);
    }

    public static Operation delete(String key) {
        return new Operation(Type.DELETE, key, null);
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}