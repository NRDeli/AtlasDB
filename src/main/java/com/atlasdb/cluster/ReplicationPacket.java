package com.atlasdb.cluster;

import com.atlasdb.log.Operation;
import java.util.List;

/**
 * Simple payload the leader sends to followers.
 * In real systems this would include term, prevLogIndex, etc.
 */
public class ReplicationPacket {

    private final int fromIndexInclusive;
    private final List<Operation> ops;

    public ReplicationPacket(int fromIndexInclusive, List<Operation> ops) {
        this.fromIndexInclusive = fromIndexInclusive;
        this.ops = ops;
    }

    public int getFromIndexInclusive() {
        return fromIndexInclusive;
    }

    public List<Operation> getOps() {
        return ops;
    }
}