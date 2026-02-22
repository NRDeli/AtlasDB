package com.atlasdb.replication;

import com.atlasdb.log.Operation;
import java.util.ArrayList;
import java.util.List;

public class ReplicationManager {

    private Role role;
    private final List<Operation> replicationLog = new ArrayList<>();

    public ReplicationManager(Role initialRole) {
        this.role = initialRole;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public Role getRole() {
        return role;
    }

    public void becomeLeader() {
        role = Role.LEADER;
    }

    public void becomeFollower() {
        role = Role.FOLLOWER;
    }

    /** Leader appends to replication log */
    public void append(Operation op) {
        if (!isLeader()) return;
        replicationLog.add(op);
    }

    public List<Operation> getReplicationLog() {
        return replicationLog;
    }

    public List<Operation> getFromIndex(int indexInclusive) {
        if (indexInclusive < 0) indexInclusive = 0;
        if (indexInclusive >= replicationLog.size()) return List.of();
        return new ArrayList<>(replicationLog.subList(indexInclusive, replicationLog.size()));
    }

    public int size() {
        return replicationLog.size();
    }
}