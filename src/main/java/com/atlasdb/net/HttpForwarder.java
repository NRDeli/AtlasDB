package com.atlasdb.net;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class HttpForwarder {

    public static String forward(String method, String urlStr, String body) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod(method);
        conn.setDoInput(true);

        if (method.equals("PUT") || method.equals("POST")) {
            conn.setDoOutput(true);
            if (body != null) {
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                conn.setFixedLengthStreamingMode(bytes.length);
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(bytes);
                }
            }
        }

        int code = conn.getResponseCode();
        if (code >= 200 && code < 300) {
            return "ok";
        }
        throw new RuntimeException("HTTP " + code);
    }
}