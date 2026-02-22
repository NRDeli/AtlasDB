package com.atlasdb;

import com.atlasdb.cluster.ClusterSimulator;
import com.atlasdb.cluster.ReplicationPacket;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ReplicationIntegrationTest {

    private Path tempDir;

    @BeforeEach
    void setup() throws Exception {
        tempDir = Files.createTempDirectory("atlasdb-test-");
    }

    private String p(String name) {
        return tempDir.resolve(name).toString();
    }

    @Test
    void leaderWritesReplicateToFollowers() {
        AtlasDBEngine leader = new AtlasDBEngine(p("leader.wal"), List.of());

        // leaderUrl only matters for HTTP forwarding; tests replicate in-memory via ClusterSimulator
        AtlasDBEngine f1 = new AtlasDBEngine(p("f1.wal"), "http://leader");
        AtlasDBEngine f2 = new AtlasDBEngine(p("f2.wal"), "http://leader");

        ClusterSimulator cluster = new ClusterSimulator(leader);
        cluster.addFollower(f1);
        cluster.addFollower(f2);

        leader.put("a", "1");
        leader.put("b", "2");
        leader.delete("a");

        cluster.replicateOnce();

        assertNull(f1.get("a"));
        assertEquals("2", f1.get("b"));

        assertNull(f2.get("a"));
        assertEquals("2", f2.get("b"));
    }

    @Test
    void crashRecoveryReplaysWal() {
        String wal = p("node.wal");

        AtlasDBEngine node1 = new AtlasDBEngine(wal, List.of());
        node1.put("k1", "v1");
        node1.put("k2", "v2");
        node1.delete("k1");

        // "restart"
        AtlasDBEngine node2 = new AtlasDBEngine(wal, List.of());

        assertNull(node2.get("k1"));
        assertEquals("v2", node2.get("k2"));
    }

    @Test
    void followerRejectsOutOfSyncPacket() {
        AtlasDBEngine leader = new AtlasDBEngine(p("leader2.wal"), List.of());
        AtlasDBEngine follower = new AtlasDBEngine(p("follower2.wal"), "http://leader");

        leader.put("x", "100");

        ClusterSimulator cluster = new ClusterSimulator(leader);
        cluster.addFollower(follower);
        cluster.replicateOnce();

        // now follower lastAppliedIndex should be 1, so re-sending fromIndex=0 should be rejected
        Exception ex = assertThrows(IllegalStateException.class, () -> follower.receiveReplication(
                new ReplicationPacket(0, leader.getReplicationDelta(0))
        ));

        assertTrue(ex.getMessage().contains("Out of sync"));
    }
}