package com.atlasdb;

import com.atlasdb.net.NodeServer;
import com.atlasdb.replication.Role;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("""
                Usage:
                leader   <port> <walPath> <followerUrl...>
                follower <port> <walPath> <leaderUrl>
                Examples:
                leader   8080 ./data/leader.wal http://localhost:8081 http://localhost:8082
                follower 8081 ./data/f1.wal     http://localhost:8080
            """);
            return;
        }

        String mode = args[0];
        int port = Integer.parseInt(args[1]);
        String wal = args[2];

        if (mode.equalsIgnoreCase("leader")) {
            List<String> followers = new ArrayList<>();
            for (int i = 3; i < args.length; i++) {
                followers.add(args[i]);
            }

            AtlasDBEngine engine = new AtlasDBEngine(wal, List.copyOf(followers));
            NodeServer server = new NodeServer(engine, Role.LEADER, port);

            System.out.println("Leader started on port " + port + " followers=" + followers);
            server.start();
            return;
        }

        if (mode.equalsIgnoreCase("follower")) {
            if (args.length < 4) {
                System.out.println("Follower requires leaderUrl: follower <port> <walPath> <leaderUrl>");
                return;
            }

            String leaderUrl = args[3];

            AtlasDBEngine engine = new AtlasDBEngine(wal, leaderUrl);
            NodeServer server = new NodeServer(engine, Role.FOLLOWER, port);

            System.out.println("Follower started on port " + port + " leader=" + leaderUrl);
            server.start();
            return;
        }

        System.out.println("Unknown mode: " + mode);
    }
}