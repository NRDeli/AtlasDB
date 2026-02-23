package com.atlasdb.net;

import com.atlasdb.AtlasDBEngine;
import com.atlasdb.cluster.ReplicationPacket;
import com.atlasdb.log.Operation;
import com.atlasdb.replication.Role;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NodeServer {

    private final AtlasDBEngine engine;
    private final Role role;
    private final int port;

    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private final ExecutorService pool = Executors.newCachedThreadPool();

    public NodeServer(AtlasDBEngine engine, Role role, int port) {
        this.engine = engine;
        this.role = role;
        this.port = port;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;

        Thread acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    pool.submit(() -> handleClient(client));
                } catch (IOException e) {
                    if (running) e.printStackTrace();
                }
            }
        }, "atlasdb-accept-" + port);

        acceptThread.setDaemon(false);
        acceptThread.start();
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        pool.shutdownNow();
    }

    // ---------------- HTTP handling ----------------

    private void handleClient(Socket client) {
        try (client;
             InputStream in = client.getInputStream();
             OutputStream out = client.getOutputStream()) {

            HttpRequest req = HttpRequest.parse(in);
            if (req == null) {
                write(out, 400, "bad request");
                return;
            }

            route(req, out);

        } catch (Exception e) {
            // best effort
            try {
                OutputStream out = client.getOutputStream();
                write(out, 500, "internal error");
            } catch (Exception ignored) {}
        }
    }

    private void route(HttpRequest req, OutputStream out) throws IOException {
        String path = req.path;

        if (path.equals("/health")) {
            write(out, 200, "ok role=" + role.name());
            return;
        }

        if (path.startsWith("/kv/")) {
            handleKV(req, out);
            return;
        }

        if (path.equals("/replicate")) {
            handleReplicate(req, out);
            return;
        }

        if (path.startsWith("/metrics")) {
            handleMetrics(req, out);
            return;
        }

        write(out, 404, "not found");
    }

    private void handleKV(HttpRequest req, OutputStream out) throws IOException {
        // Routes:
        // GET    /kv/<key>
        // PUT    /kv/<key>   body=value
        // DELETE /kv/<key>

        String[] parts = req.path.split("/", -1); // ["", "kv", "<key>"]
        if (parts.length < 3 || parts[2].isBlank()) {
            write(out, 400, "missing key. use /kv/<key>");
            return;
        }
        String key = parts[2];

        String method = req.method.toUpperCase();

        if (method.equals("GET")) {
            String val = engine.get(key);
            if (val == null) write(out, 404, "");
            else write(out, 200, val);
            return;
        }

        // Writes only allowed on leader (follower forwards)
        if (!engine.isLeader()) {
            String leader = engine.getLeaderUrl();
            if (leader == null) {
                write(out, 409, "no leader known");
                return;
            }

            String url = leader + "/kv/" + key;
            try {
                String body = method.equals("PUT") ? req.bodyUtf8() : null;
                String resp = HttpForwarder.forward(method, url, body);
                write(out, 200, resp);
            } catch (Exception e) {
                write(out, 502, "forward failed");
            }
            return;
        }

        if (method.equals("PUT")) {
            String value = req.bodyUtf8();
            engine.put(key, value);
            write(out, 200, "ok");
            return;
        }

        if (method.equals("DELETE")) {
            engine.delete(key);
            write(out, 200, "ok");
            return;
        }

        write(out, 405, "method not allowed");
    }

    private void handleReplicate(HttpRequest req, OutputStream out) throws IOException {
        // Followers accept replication packets via POST body:
        // fromIndex\n
        // opLine\n
        // opLine\n
        // ...

        if (!req.method.equalsIgnoreCase("POST")) {
            write(out, 405, "POST required");
            return;
        }

        if (engine.isLeader()) {
            write(out, 409, "leader does not accept replication");
            return;
        }

        String body = req.bodyUtf8();
        String[] lines = body.split("\n");

        if (lines.length < 1) {
            write(out, 400, "invalid replication payload");
            return;
        }

        int fromIndex;
        try {
            fromIndex = Integer.parseInt(lines[0].trim());
        } catch (Exception e) {
            write(out, 400, "invalid fromIndex");
            return;
        }

        ArrayList<Operation> ops = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isBlank()) continue;
            Operation op = Operation.fromWalLine(line);
            if (op != null) ops.add(op);
        }

        engine.receiveReplication(new ReplicationPacket(fromIndex, List.copyOf(ops)));
        write(out, 200, "ok");
    }

    private void handleMetrics(HttpRequest req, OutputStream out) throws IOException {
        String body =
                "role=" + role.name() + "\n" +
                "lastApplied=" + engine.getLastAppliedIndex() + "\n" +
                "commitIndex=" + engine.getCommitIndex() + "\n";

        write(out, 200, body);
    }

    private static void write(OutputStream out, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

        String status = switch (code) {
            case 200 -> "OK";
            case 400 -> "Bad Request";
            case 404 -> "Not Found";
            case 405 -> "Method Not Allowed";
            case 409 -> "Conflict";
            case 502 -> "Bad Gateway";
            default -> "Internal Server Error";
        };

        String headers =
                "HTTP/1.1 " + code + " " + status + "\r\n" +
                "Content-Type: text/plain; charset=utf-8\r\n" +
                "Content-Length: " + bytes.length + "\r\n" +
                "Connection: close\r\n" +
                "\r\n";

        out.write(headers.getBytes(StandardCharsets.UTF_8));
        out.write(bytes);
        out.flush();
    }

    // ---------------- minimal HTTP request parser ----------------

    private static final class HttpRequest {
        final String method;
        final String path;
        final Map<String, String> headers;
        final byte[] body;

        private HttpRequest(String method, String path, Map<String, String> headers, byte[] body) {
            this.method = method;
            this.path = path;
            this.headers = headers;
            this.body = body;
        }

        String bodyUtf8() {
            return new String(body, StandardCharsets.UTF_8);
        }

        static HttpRequest parse(InputStream in) throws IOException {
            BufferedInputStream bin = new BufferedInputStream(in);

            String requestLine = readLine(bin);
            if (requestLine == null || requestLine.isBlank()) return null;

            String[] rl = requestLine.split(" ");
            if (rl.length < 2) return null;

            String method = rl[0].trim();
            String fullPath = rl[1].trim();
            String path = fullPath.split("\\?", 2)[0]; // ignore query

            Map<String, String> headers = new HashMap<>();
            String line;
            while ((line = readLine(bin)) != null) {
                if (line.isEmpty()) break; // end headers
                int idx = line.indexOf(':');
                if (idx <= 0) continue;
                String k = line.substring(0, idx).trim().toLowerCase();
                String v = line.substring(idx + 1).trim();
                headers.put(k, v);
            }

            int contentLen = 0;
            if (headers.containsKey("content-length")) {
                try {
                    contentLen = Integer.parseInt(headers.get("content-length"));
                } catch (Exception ignored) {}
            }

            byte[] body = new byte[0];
            if (contentLen > 0) {
                body = bin.readNBytes(contentLen);
            }

            return new HttpRequest(method, path, headers, body);
        }

        private static String readLine(BufferedInputStream bin) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int prev = -1;
            while (true) {
                int b = bin.read();
                if (b == -1) break;

                if (prev == '\r' && b == '\n') {
                    byte[] arr = baos.toByteArray();
                    int len = arr.length;
                    if (len > 0 && arr[len - 1] == '\r') {
                        return new String(arr, 0, len - 1, StandardCharsets.ISO_8859_1);
                    }
                    return new String(arr, StandardCharsets.ISO_8859_1);
                }

                baos.write(b);
                prev = b;

                if (baos.size() > 64 * 1024) throw new IOException("header line too long");
            }

            if (baos.size() == 0) return null;
            return new String(baos.toByteArray(), StandardCharsets.ISO_8859_1);
        }
    }
}