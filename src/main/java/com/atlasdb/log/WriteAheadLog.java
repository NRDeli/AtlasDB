package com.atlasdb.log;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Durable write-ahead log for AtlasDB.
 */
public class WriteAheadLog {

    private final File file;

    public WriteAheadLog(String path) {
        this.file = new File(path);
    }

    public synchronized void append(Operation op) {
        try (FileOutputStream fos = new FileOutputStream(file, true);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {

            oos.writeObject(op);

        } catch (IOException e) {
            throw new RuntimeException("WAL append failed", e);
        }
    }

    public synchronized List<Operation> readAll() {
        List<Operation> ops = new ArrayList<>();
        if (!file.exists()) return ops;

        try (FileInputStream fis = new FileInputStream(file);
             ObjectInputStream ois = new ObjectInputStream(fis)) {

            while (true) {
                Operation op = (Operation) ois.readObject();
                ops.add(op);
            }

        } catch (EOFException eof) {
            return ops;
        } catch (Exception e) {
            throw new RuntimeException("WAL read failed", e);
        }
    }
}