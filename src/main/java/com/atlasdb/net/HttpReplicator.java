package com.atlasdb.net;

import com.atlasdb.log.Operation;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class HttpReplicator {

    public static void replicate(String followerBaseUrl, int fromIndex, List<Operation> ops) {
        try {
            URL url = new URL(followerBaseUrl + "/replicate");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            // payload:
            // fromIndex\n
            // opLine\n
            StringBuilder sb = new StringBuilder();
            sb.append(fromIndex).append("\n");
            for (Operation op : ops) {
                sb.append(op.toWalLine()).append("\n");
            }

            byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
            conn.getOutputStream().write(payload);

            int code = conn.getResponseCode();
            if (code != 200) {
                throw new RuntimeException("replication failed to " + followerBaseUrl + " code=" + code);
            }
        } catch (Exception e) {
            throw new RuntimeException("replication failed to " + followerBaseUrl, e);
        }
    }
}