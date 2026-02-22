package com.atlasdb.replication;

import com.atlasdb.net.HttpForwarder;
import com.atlasdb.log.Operation;
import java.util.ArrayList;
import java.util.List;

public class ReplicationManager {

    private Role role;
    private final List<Operation> replicationLog = new ArrayList<>();
    private final List<String> followers;

    // Leader constructor
    public ReplicationManager(Role role, List<String> followers) {
        this.role = role;
        this.followers = followers;
    }

    // Follower constructor
    public ReplicationManager(Role role) {
        this.role = role;
        this.followers = List.of();
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
        if (role == Role.LEADER) {
            replicateToFollowers(op);
        }
        replicationLog.add(op);
    }

    private void replicateToFollowers(Operation op) {
        for (String f : followers) {
            try {
                HttpForwarder.forward(
                        "POST",
                        f + "/replicate",
                        op.toWalLine()
                );
            } catch (Exception ignored) {}
        }
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