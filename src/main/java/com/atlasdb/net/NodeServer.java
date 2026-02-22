package com.atlasdb.net;

import com.atlasdb.AtlasDBEngine;
import com.atlasdb.cluster.ReplicationPacket;
import com.atlasdb.log.Operation;
import com.atlasdb.replication.Role;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class NodeServer {

    private final AtlasDBEngine engine;
    private final Role role;
    private final HttpServer server;

    public NodeServer(AtlasDBEngine engine, Role role, int port) throws IOException {
        this.engine = engine;
        this.role = role;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/health", this::handleHealth);
        server.createContext("/kv", this::handleKV);
        server.createContext("/replicate", this::handleReplicate);

        server.setExecutor(null); // default executor
    }

    public void start() {
        server.start();
    }

    private void handleHealth(HttpExchange ex) throws IOException {
        write(ex, 200, "ok role=" + role.name());
    }

    private void handleKV(HttpExchange ex) throws IOException {
        // Routes:
        // GET  /kv/<key>
        // PUT  /kv/<key>   body=value
        // DEL  /kv/<key>

        String path = ex.getRequestURI().getPath(); // /kv/x
        String[] parts = path.split("/", -1);
        if (parts.length < 3 || parts[2].isBlank()) {
            write(ex, 400, "missing key. use /kv/<key>");
            return;
        }
        String key = parts[2];

        String method = ex.getRequestMethod().toUpperCase();

        if (method.equals("GET")) {
            String val = engine.get(key);
            if (val == null) write(ex, 404, "");
            else write(ex, 200, val);
            return;
        }

        // Writes only allowed on leader
        if (!engine.isLeader()) {
            write(ex, 409, "not leader");
            return;
        }

        if (method.equals("PUT")) {
            String value = readBody(ex);
            engine.put(key, value);
            write(ex, 200, "ok");
            return;
        }

        if (method.equals("DELETE")) {
            engine.delete(key);
            write(ex, 200, "ok");
            return;
        }

        write(ex, 405, "method not allowed");
    }

    private void handleReplicate(HttpExchange ex) throws IOException {
        // Followers accept replication packets via POST body:
        // fromIndex\n
        // opLine\n
        // opLine\n
        // ...
        if (!ex.getRequestMethod().equalsIgnoreCase("POST")) {
            write(ex, 405, "POST required");
            return;
        }

        if (engine.isLeader()) {
            write(ex, 409, "leader does not accept replication");
            return;
        }

        String body = readBody(ex);
        String[] lines = body.split("\n");
        if (lines.length < 1) {
            write(ex, 400, "invalid replication payload");
            return;
        }

        int fromIndex;
        try {
            fromIndex = Integer.parseInt(lines[0].trim());
        } catch (Exception e) {
            write(ex, 400, "invalid fromIndex");
            return;
        }

        java.util.ArrayList<Operation> ops = new java.util.ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isBlank()) continue;
            Operation op = Operation.fromWalLine(line);
            if (op != null) ops.add(op);
        }

        engine.receiveReplication(new ReplicationPacket(fromIndex, List.copyOf(ops)));
        write(ex, 200, "ok");
    }

    private static String readBody(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void write(HttpExchange ex, int code, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }
}