package com.atlasdb.cluster;

import com.atlasdb.AtlasDBEngine;
import com.atlasdb.log.Operation;

import java.util.ArrayList;
import java.util.List;

public class ClusterSimulator {

    private final AtlasDBEngine leader;
    private final List<AtlasDBEngine> followers = new ArrayList<>();
    private int nextFromIndex = 0;

    public ClusterSimulator(AtlasDBEngine leader) {
        this.leader = leader;
    }

    public void addFollower(AtlasDBEngine follower) {
        followers.add(follower);
    }

    public void replicateOnce() {
        List<Operation> delta = leader.getReplicationDelta(nextFromIndex);
        if (delta.isEmpty()) return;

        ReplicationPacket packet = new ReplicationPacket(nextFromIndex, delta);

        for (AtlasDBEngine f : followers) {
            f.receiveReplication(packet);
        }

        nextFromIndex += delta.size();
    }
}