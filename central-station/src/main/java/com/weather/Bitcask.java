package com.weather;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

public class Bitcask {
    private final File directory;
    private RandomAccessFile activeFile;
    private String activeFileId;
    private long currentOffset = 0;
    
    // The in-memory hash map
    private final ConcurrentHashMap<String, KeyDirEntry> keyDir = new ConcurrentHashMap<>();

    public Bitcask(String dirPath) throws IOException {
        this.directory = new File(dirPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        openNewActiveFile();
        // Load existing hint files on startup
        loadFromHintFiles();
    }

    private void openNewActiveFile() throws IOException {
        this.activeFileId = System.currentTimeMillis() + ".data";
        File file = new File(directory, activeFileId);
        this.activeFile = new RandomAccessFile(file, "rw");
        this.currentOffset = 0;
    }

    public synchronized void put(String key, String value) throws IOException {
        long timestamp = System.currentTimeMillis();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        long startOffset = currentOffset;

        activeFile.writeLong(timestamp);
        activeFile.writeInt(keyBytes.length);
        activeFile.writeInt(valueBytes.length);
        activeFile.write(keyBytes);
        activeFile.write(valueBytes);

        int recordSize = Long.BYTES + Integer.BYTES + Integer.BYTES + keyBytes.length + valueBytes.length;

        keyDir.put(key, new KeyDirEntry(activeFileId, valueBytes.length, startOffset + recordSize - valueBytes.length, timestamp));

        currentOffset += recordSize;

        // Trigger compaction ONLY if file gets larger than 1MB
        if (currentOffset > 1024 * 1024) {
            compact();
        }
    }

    public synchronized void compact() throws IOException {
        // 1. Force the OS to flush the newly written data to disk!
        if (this.activeFile != null) {
            this.activeFile.close();
        }

        System.out.println("Starting Compaction...");
        String compactedFileId = "compacted_" + System.currentTimeMillis() + ".data";
        String hintFileId = compactedFileId + ".hint";

        File dataFile = new File(directory, compactedFileId);
        File hintFile = new File(directory, hintFileId);

        long newOffset = 0;

        try (DataOutputStream dataOut = new DataOutputStream(new FileOutputStream(dataFile));
             DataOutputStream hintOut = new DataOutputStream(new FileOutputStream(hintFile))) {

            for (String key : keyDir.keySet()) {
                KeyDirEntry entry = keyDir.get(key);
                String value = get(key); // Fetch current value
                if (value == null) continue;

                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

                int recordSize = Long.BYTES + Integer.BYTES + Integer.BYTES + keyBytes.length + valueBytes.length;

                // Write to compacted data file
                dataOut.writeLong(entry.timestamp);
                dataOut.writeInt(keyBytes.length);
                dataOut.writeInt(valueBytes.length);
                dataOut.write(keyBytes);
                dataOut.write(valueBytes);

                // Write to Hint file
                hintOut.writeLong(entry.timestamp);
                hintOut.writeInt(keyBytes.length);
                hintOut.writeInt(valueBytes.length);
                hintOut.writeLong(newOffset + recordSize - valueBytes.length);
                hintOut.write(keyBytes);

                // Update the in-memory map to point to the new compacted file
                keyDir.put(key, new KeyDirEntry(compactedFileId, valueBytes.length, newOffset + recordSize - valueBytes.length, entry.timestamp));

                newOffset += recordSize;
            }
        }
        System.out.println("Compaction complete. Created Hint File: " + hintFileId);

        // 2. CRITICAL FIX: Open a brand new file for the NEXT incoming messages
        openNewActiveFile();
    }

    // Uses the in-memory map to jump straight to the data on disk
    public String get(String key) throws IOException {
        KeyDirEntry entry = keyDir.get(key);
        if (entry == null) return null;

        File file = new File(directory, entry.fileId);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(entry.valuePosition);
            byte[] valueBytes = new byte[entry.valueSize];
            raf.readFully(valueBytes);
            return new String(valueBytes, StandardCharsets.UTF_8);
        }
    }

    // Load existing hint files on startup
    private void loadFromHintFiles() throws IOException {
        File[] hintFiles = directory.listFiles((dir, name) -> name.endsWith(".hint"));
        if (hintFiles == null) return;

        for (File hintFile : hintFiles) {
            String dataFileId = hintFile.getName().replace(".hint", "");
            try (DataInputStream in = new DataInputStream(new FileInputStream(hintFile))) {
                while (in.available() > 0) {
                    long timestamp = in.readLong();
                    int keySize = in.readInt();
                    int valueSize = in.readInt();
                    long valuePosition = in.readLong();

                    byte[] keyBytes = new byte[keySize];
                    in.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    // Rebuild map instantly without reading the massive values!
                    keyDir.put(key, new KeyDirEntry(dataFileId, valueSize, valuePosition, timestamp));
                }
            }
        }
    }
    public Set<String> getAllKeys() {
        return keyDir.keySet();
    }
}
