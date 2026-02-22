package com.atlasdb.net;

import com.atlasdb.AtlasDBEngine;
import com.atlasdb.log.Operation;

import java.util.ArrayList;
import java.util.List;

public class HttpClusterReplicator {

    private final AtlasDBEngine leader;
    private final List<String> followerUrls = new ArrayList<>();
    private int nextFromIndex = 0;

    public HttpClusterReplicator(AtlasDBEngine leader, List<String> followerUrls) {
        this.leader = leader;
        this.followerUrls.addAll(followerUrls);
    }

    public void replicateOnce() {
        List<Operation> delta = leader.getReplicationDelta(nextFromIndex);
        if (delta.isEmpty()) return;

        for (String url : followerUrls) {
            HttpReplicator.replicate(url, nextFromIndex, delta);
        }

        nextFromIndex += delta.size();
    }
}