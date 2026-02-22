package com.atlasdb;

import com.atlasdb.log.Operation;
import com.atlasdb.log.WriteAheadLog;
import com.atlasdb.replication.ReplicationManager;
import com.atlasdb.replication.Role;

import com.atlasdb.cluster.ReplicationPacket;

import java.util.List;

/**
 * AtlasDB state machine with WAL durability.
 */
public class AtlasDBEngine {

    private final KVStore store;
    private final WriteAheadLog wal;
    private final ReplicationManager replicationManager;
    private int lastAppliedIndex = 0;
    private String leaderUrl;   // null if this node is leader

    public AtlasDBEngine(String walPath) {
        this(walPath, Role.LEADER);
    }

    public AtlasDBEngine(String walPath, Role role) {
        this.store = new KVStore();
        this.wal = new WriteAheadLog(walPath);
        this.replicationManager = new ReplicationManager(role);
        this.leaderUrl = leaderUrl;
        recover();
    }

    public boolean isLeader() {
        return replicationManager.isLeader();
    }

    public String get(String key) {
        return store.get(key);
    }
    public int getLastAppliedIndex() {
        return lastAppliedIndex;
    }
    public String getLeaderUrl() {
        return leaderUrl;
    }
    public List<Operation> getReplicationDelta(int fromIndexInclusive) {
        if (!replicationManager.isLeader()) {
            throw new IllegalStateException("Only leader can serve replication delta");
        }
        return replicationManager.getFromIndex(fromIndexInclusive);
    }

    public void put(String key, String value) {
        Operation op = Operation.put(key, value);
        wal.append(op);
        replicationManager.append(op);
        apply(op);
        lastAppliedIndex++;
    }

    public void delete(String key) {
        Operation op = Operation.delete(key);
        wal.append(op);
        replicationManager.append(op);
        apply(op);
        lastAppliedIndex++;
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
            lastAppliedIndex++;
        }
    }

    public void receiveReplication(ReplicationPacket packet) {
        // Followers only
        if (replicationManager.isLeader()) {
            throw new IllegalStateException("Leader should not receive replication packets");
        }
    
        // Simple safety: only accept the next expected index
        if (packet.getFromIndexInclusive() != lastAppliedIndex) {
            throw new IllegalStateException(
                    "Out of sync: expected fromIndex=" + lastAppliedIndex +
                    " but got " + packet.getFromIndexInclusive()
            );
        }
    
        for (Operation op : packet.getOps()) {
            wal.append(op);   // durable on follower
            apply(op);        // apply to follower state machine
            lastAppliedIndex++;
        }
    }
}