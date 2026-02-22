package com.atlasdb.log;

import java.util.Objects;

public final class Operation {

    public enum Type { PUT, DELETE }

    private final Type type;
    private final String key;
    private final String value;

    private Operation(Type type, String key, String value) {
        this.type = Objects.requireNonNull(type);
        this.key = Objects.requireNonNull(key);
        this.value = value; // null allowed for DELETE
    }

    public static Operation put(String key, String value) {
        return new Operation(Type.PUT, key, value == null ? "" : value);
    }

    public static Operation delete(String key) {
        return new Operation(Type.DELETE, key, null);
    }

    public Type getType() { return type; }
    public String getKey() { return key; }
    public String getValue() { return value; }

    /**
     * WAL line format (V1):
     * PUT|<escapedKey>|<escapedValue>
     * DEL|<escapedKey>
     */
    public String toWalLine() {
        if (type == Type.PUT) {
            return "PUT|" + esc(key) + "|" + esc(value == null ? "" : value);
        }
        return "DEL|" + esc(key);
    }

    public static Operation fromWalLine(String line) {
        if (line == null) return null;
        line = line.trim();
        if (line.isEmpty()) return null;

        String[] parts = line.split("\\|", -1);
        if (parts.length < 2) return null;

        String kind = parts[0];
        if ("PUT".equals(kind)) {
            if (parts.length < 3) return null;
            return put(unesc(parts[1]), unesc(parts[2]));
        }
        if ("DEL".equals(kind)) {
            return delete(unesc(parts[1]));
        }
        return null;
    }

    // Escape \, \n, \r, and |
    private static String esc(String s) {
        if (s == null) return "";
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\': out.append("\\\\"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '|':  out.append("\\p"); break;
                default:   out.append(c);
            }
        }
        return out.toString();
    }

    private static String unesc(String s) {
        if (s == null) return "";
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c != '\\') {
                out.append(c);
                continue;
            }
            if (i + 1 >= s.length()) break;
            char n = s.charAt(++i);
            switch (n) {
                case '\\': out.append('\\'); break;
                case 'n':  out.append('\n'); break;
                case 'r':  out.append('\r'); break;
                case 'p':  out.append('|'); break;
                default:   out.append(n);
            }
        }
        return out.toString();
    }
}