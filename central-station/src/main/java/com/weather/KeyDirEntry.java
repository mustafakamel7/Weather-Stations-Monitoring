package com.weather;

public class KeyDirEntry {
    public String fileId;
    public int valueSize;
    public long valuePosition;
    public long timestamp;

    public KeyDirEntry(String fileId, int valueSize, long valuePosition, long timestamp) {
        this.fileId = fileId;
        this.valueSize = valueSize;
        this.valuePosition = valuePosition;
        this.timestamp = timestamp;
    }
}
