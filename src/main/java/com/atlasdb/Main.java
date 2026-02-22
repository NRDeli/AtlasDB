package com.atlasdb;

import com.atlasdb.cluster.ClusterSimulator;
import com.atlasdb.replication.Role;

import java.util.List;

public class Main {
    public static void main(String[] args) {

        // Use separate WAL files per node
        AtlasDBEngine leader = new AtlasDBEngine("atlasdb-leader.wal", Role.LEADER);
        AtlasDBEngine follower1 = new AtlasDBEngine("atlasdb-f1.wal", Role.FOLLOWER);
        AtlasDBEngine follower2 = new AtlasDBEngine("atlasdb-f2.wal", Role.FOLLOWER);

        ClusterSimulator cluster = new ClusterSimulator(leader, List.of(follower1, follower2));

        System.out.println("Leader writing...");
        leader.put("x", "100");
        leader.put("y", "200");
        leader.delete("x");

        System.out.println("Replicating to followers...");
        cluster.replicateOnce();

        System.out.println("Follower1 y=" + follower1.get("y"));
        System.out.println("Follower2 y=" + follower2.get("y"));
        System.out.println("Follower1 x=" + follower1.get("x"));
    }
}