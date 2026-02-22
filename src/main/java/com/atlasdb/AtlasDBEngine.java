package com.atlasdb;

import com.atlasdb.log.Operation;
import com.atlasdb.log.WriteAheadLog;
import com.atlasdb.replication.ReplicationManager;
import com.atlasdb.replication.Role;

import java.util.List;

/**
 * AtlasDB state machine with WAL durability.
 */
public class AtlasDBEngine {

    private final KVStore store;
    private final WriteAheadLog wal;
    private final ReplicationManager replicationManager;

    public AtlasDBEngine(String walPath) {
        this.store = new KVStore();
        this.wal = new WriteAheadLog(walPath);
        this.replicationManager = new ReplicationManager(Role.LEADER);
        recover();
    }

    public boolean isLeader() {
        return replicationManager.isLeader();
    }

    public void put(String key, String value) {
        Operation op = Operation.put(key, value);
        wal.append(op);
        replicationManager.append(op);
        apply(op);
    }

    public void delete(String key) {
        Operation op = Operation.delete(key);
        wal.append(op);
        replicationManager.append(op);
        apply(op);
    }

    public String get(String key) {
        return store.get(key);
    }

    private void apply(Operation op) {
        switch (op.getType()) {
            case PUT:
                store.put(op.getKey(), op.getValue());
                break;
            case DELETE:
                store.delete(op.getKey());
                break;
        }
    }

    private void recover() {
        List<Operation> ops = wal.readAll();
        for (Operation op : ops) {
            apply(op);
        }
    }
}