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
    
    public static Operation fromWalLine(String line) {
        if (line == null || line.isBlank()) return null;
    
        String[] parts = line.split("\t", -1);
        if (parts.length < 2) return null;
    
        Type t = Type.valueOf(parts[0]);
        String k = unescape(parts[1]);
        String v = parts.length >= 3 ? unescape(parts[2]) : "";
    
        if (t == Type.PUT) return Operation.put(k, v);
        if (t == Type.DELETE) return Operation.delete(k);
        return null;
    }
    
    private static String unescape(String s) {
        if (s == null) return "";
        return s.replace("\\t", "\t").replace("\\n", "\n");
    }

    public String toWalLine() {
        // Format: TYPE \t key \t value
        // value is empty for DELETE
        String safeKey = key == null ? "" : key.replace("\t", "\\t").replace("\n", "\\n");
        String safeVal = value == null ? "" : value.replace("\t", "\\t").replace("\n", "\\n");
        return type.name() + "\t" + safeKey + "\t" + safeVal;
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