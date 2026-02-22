package com.atlasdb.cluster;

import com.atlasdb.AtlasDBEngine;
import com.atlasdb.log.Operation;

import java.util.ArrayList;
import java.util.List;

/**
 * In-process cluster simulation: one leader, N followers.
 * This lets you demonstrate replication + commit index without networking yet.
 */
public class ClusterSimulator {

    private final AtlasDBEngine leader;
    private final List<AtlasDBEngine> followers = new ArrayList<>();

    public ClusterSimulator(AtlasDBEngine leader, List<AtlasDBEngine> followers) {
        this.leader = leader;
        this.followers.addAll(followers);
    }

    public void replicateOnce() {
        int fromIndex = followers.isEmpty() ? 0 : followers.get(0).getLastAppliedIndex();

        List<Operation> delta = leaderReplicationDelta(fromIndex);
        ReplicationPacket packet = new ReplicationPacket(fromIndex, delta);

        for (AtlasDBEngine follower : followers) {
            follower.receiveReplication(packet);
        }
    }

    private List<Operation> leaderReplicationDelta(int fromIndex) {
        // We expose leader replication log via a helper method below (added in Engine)
        return leader.getReplicationDelta(fromIndex);
    }
}