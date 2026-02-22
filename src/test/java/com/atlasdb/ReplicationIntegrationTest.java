package com.atlasdb;

import com.atlasdb.cluster.ClusterSimulator;
import com.atlasdb.replication.Role;
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
        AtlasDBEngine leader = new AtlasDBEngine(p("leader.wal"), Role.LEADER);
        AtlasDBEngine f1 = new AtlasDBEngine(p("f1.wal"), Role.FOLLOWER);
        AtlasDBEngine f2 = new AtlasDBEngine(p("f2.wal"), Role.FOLLOWER);

        ClusterSimulator cluster = new ClusterSimulator(leader, List.of(f1, f2));

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

        AtlasDBEngine node1 = new AtlasDBEngine(wal, Role.LEADER);
        node1.put("k1", "v1");
        node1.put("k2", "v2");
        node1.delete("k1");

        // "restart"
        AtlasDBEngine node2 = new AtlasDBEngine(wal, Role.LEADER);

        assertNull(node2.get("k1"));
        assertEquals("v2", node2.get("k2"));
    }

    @Test
    void followerRejectsOutOfSyncPacket() {
        AtlasDBEngine leader = new AtlasDBEngine(p("leader2.wal"), Role.LEADER);
        AtlasDBEngine follower = new AtlasDBEngine(p("follower2.wal"), Role.FOLLOWER);

        leader.put("x", "100");
        // follower hasn't replicated yet, lastAppliedIndex=0, but leader will send fromIndex=0 via simulator.
        // We'll force out-of-sync by manually advancing follower with a replication first,
        // then trying to re-send from 0.

        ClusterSimulator cluster = new ClusterSimulator(leader, List.of(follower));
        cluster.replicateOnce();

        // now follower lastAppliedIndex should be 1
        Exception ex = assertThrows(IllegalStateException.class, () -> follower.receiveReplication(
                new com.atlasdb.cluster.ReplicationPacket(0, leader.getReplicationDelta(0))
        ));

        assertTrue(ex.getMessage().contains("Out of sync"));
    }
}