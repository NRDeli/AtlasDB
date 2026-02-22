package com.atlasdb.log;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WriteAheadLog {

    private static final String HEADER = "ATLASDB_WAL_V1";
    private final Path path;

    public WriteAheadLog(String walPath) {
        this.path = Paths.get(walPath);
    }

    public synchronized void append(Operation op) {
        try {
            ensureHeader();
            String line = op.toWalLine() + "\n";
            Files.write(path, line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("WAL append failed", e);
        }
    }

    public synchronized List<Operation> readAll() {
        if (!Files.exists(path)) return Collections.emptyList();

        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            if (lines.isEmpty()) return Collections.emptyList();

            // If file isn't in our format (ex: old ObjectOutputStream WAL), rotate it away.
            if (!HEADER.equals(lines.get(0).trim())) {
                rotateCorruptWal("unsupported-format");
                return Collections.emptyList();
            }

            ArrayList<Operation> ops = new ArrayList<>();
            for (int i = 1; i < lines.size(); i++) {
                Operation op = Operation.fromWalLine(lines.get(i));
                if (op != null) ops.add(op);
            }
            return ops;
        } catch (IOException e) {
            // If it's unreadable (ex: binary garbage), rotate it away so the node can boot.
            rotateCorruptWal("unreadable");
            return Collections.emptyList();
        }
    }

    private void ensureHeader() throws IOException {
        if (!Files.exists(path)) {
            Files.createDirectories(path.toAbsolutePath().getParent() == null
                    ? Paths.get(".")
                    : path.toAbsolutePath().getParent());
            Files.write(path, (HEADER + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            return;
        }
        if (Files.size(path) == 0) {
            Files.write(path, (HEADER + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    private void rotateCorruptWal(String reason) {
        try {
            if (!Files.exists(path)) return;
            String ts = String.valueOf(Instant.now().toEpochMilli());
            Path moved = path.resolveSibling(path.getFileName() + ".corrupt." + reason + "." + ts);
            Files.move(path, moved, StandardCopyOption.REPLACE_EXISTING);
            // Next append/read will recreate with a proper header.
        } catch (IOException ignored) {
            // worst case: leave it; engine may fail later, but we tried.
        }
    }
}