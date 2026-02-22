package com.atlasdb;

public class Main {

    public static void main(String[] args) {
        AtlasDBEngine db = new AtlasDBEngine("atlasdb.wal");

        System.out.println("AtlasDB starting...");

        db.put("a", "1");
        db.put("b", "2");

        System.out.println("a=" + db.get("a"));
        System.out.println("b=" + db.get("b"));

        db.delete("a");
        System.out.println("a after delete=" + db.get("a"));

        System.out.println("AtlasDB complete.");
    }
}