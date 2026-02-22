package com.atlasdb;

import com.atlasdb.net.HttpClusterReplicator;
import com.atlasdb.net.NodeServer;
import com.atlasdb.replication.Role;

import java.util.Arrays;
import java.util.List;

public class Main {

    // Usage:
    // Leader:   java -jar target/replicated-datastore-1.0-SNAPSHOT.jar leader 8080 leader.wal http://localhost:8081 http://localhost:8082
    // Follower: java -jar target/replicated-datastore-1.0-SNAPSHOT.jar follower 8081 f1.wal

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage:");
            System.out.println(" leader <port> <walPath> <followerUrl...>");
            System.out.println(" follower <port> <walPath>");
            return;
        }

        Role role = args[0].equalsIgnoreCase("leader") ? Role.LEADER : Role.FOLLOWER;
        int port = Integer.parseInt(args[1]);
        String walPath = args[2];

        AtlasDBEngine engine = new AtlasDBEngine(walPath, role);

        NodeServer server = new NodeServer(engine, role, port);
        server.start();
        System.out.println("Node started on port " + port + " role=" + role);

        if (role == Role.LEADER) {
            List<String> followers = args.length > 3
                    ? Arrays.asList(Arrays.copyOfRange(args, 3, args.length))
                    : List.of();
        
            HttpClusterReplicator cluster = new HttpClusterReplicator(engine, followers);
        
            while (true) {
                cluster.replicateOnce();
                Thread.sleep(200);
            }
        } else {
            // followers just serve requests
            Thread.currentThread().join();
        }
    }
}