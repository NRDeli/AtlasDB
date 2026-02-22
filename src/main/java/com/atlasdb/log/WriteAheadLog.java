package com.atlasdb.log;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Line-based WAL: one operation per line.
 * Safer than ObjectOutputStream append.
 */
public class WriteAheadLog {

    private final Path path;

    public WriteAheadLog(String walPath) {
        this.path = Paths.get(walPath);
    }

    public synchronized void append(Operation op) {
        try {
            Files.createDirectories(path.toAbsolutePath().getParent() == null
                    ? Paths.get(".")
                    : path.toAbsolutePath().getParent());

            String line = op.toWalLine() + "\n";
            Files.write(path, line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("WAL append failed", e);
        }
    }

    public synchronized List<Operation> readAll() {
        if (!Files.exists(path)) return List.of();

        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            List<Operation> ops = new ArrayList<>();
            for (String line : lines) {
                Operation op = Operation.fromWalLine(line);
                if (op != null) ops.add(op);
            }
            return ops;
        } catch (IOException e) {
            throw new RuntimeException("WAL read failed", e);
        }
    }

    public synchronized void clear() {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RuntimeException("WAL clear failed", e);
        }
    }
}